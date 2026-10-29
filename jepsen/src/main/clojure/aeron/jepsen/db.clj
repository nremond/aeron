(ns aeron.jepsen.db
  "A member of the cluster in its container: started, killed, paused and
  wiped. Its media driver, archive, consensus module and service run in one
  JVM (JepsenNode)."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util]]))

(def aeron-dir "/dev/shm/aeron")

(def aeron-version
  "The Aeron the nodes run: run.sh's AERON_VERSION."
  (System/getProperty "aeron.version" "1.54.0"))

(def classpath
  (str "/opt/aeron-jepsen/target/classes"
       ":/opt/m2/io/aeron/aeron-all/" aeron-version "/aeron-all-" aeron-version ".jar"))

(def jvm-flags
  ["--add-opens" "java.base/jdk.internal.misc=ALL-UNNAMED"
   "--add-opens" "java.base/java.util.zip=ALL-UNNAMED"
   "--sun-misc-unsafe-memory-access=allow"
   "-Xmx512m"])

(defn index
  "A node's member id: its place in the test's nodes."
  [test node]
  (.indexOf ^java.util.List (:nodes test) node))

(defn ip
  "n<k> is at 10.77.0.<10+k>: see scripts/cluster.sh."
  [node]
  (str "10.77.0." (+ 10 (Long/parseLong (subs node 1)))))

(def host-ip
  "The host, where Jepsen's clients are, on the nodes' network."
  "10.77.0.1")

(defn java
  "A Java command line on a node."
  [main & args]
  (str/join " " (concat ["java"] jvm-flags ["-cp" classpath main] (map str args))))

(defn probe!
  "Runs ClusterProbe on the current node: its output, or nil when it exits
  non-zero (an await timing out)."
  [& args]
  (try (c/exec* (java "aeron.jepsen.ClusterProbe" aeron-dir (str/join " " args)))
       (catch Exception _ nil)))

(defn parse-state
  "role=2 election=17 ... as a map of longs."
  [line]
  (when line
    (->> (re-seq #"(\w+)=(-?\d+)" line)
         (map (fn [[_ k v]] [(keyword k) (parse-long v)]))
         (into {}))))

(defn- timeouts
  [opts]
  ["--heartbeat-timeout-ms" (:heartbeat-timeout-ms opts)
   "--heartbeat-interval-ms" 100
   "--election-timeout-ms" (:election-timeout-ms opts)
   "--startup-canvass-ms" 5000])

(defn- count-lines
  "How many lines of `file` match `pattern`."
  [file pattern]
  (or (parse-long (str/trim (c/exec* (str "grep -c '" pattern "' " file " 2>/dev/null || true")))) 0))

(defn- running?
  "Whether a process matching `pattern` runs."
  [pattern]
  (not (str/blank? (c/exec* (str "pgrep -f '" pattern "' || true")))))

(defn- launch!
  "Starts `cmd` in the background, its output appended to `file`."
  [file cmd]
  (c/exec* (str "echo START " (System/currentTimeMillis) " >> " file "; "
                "setsid " cmd " >> " file " 2>&1 < /dev/null &")))

(defn- await-ready
  "Waits for one more READY in `file`: :started, :exited (no process
  matching `pattern` left), or :timeout."
  [file before pattern]
  (loop [tries 150]
    (cond (< before (count-lines file "READY")) :started
          (not (running? pattern)) :exited
          (zero? tries) :timeout
          :else (do (Thread/sleep 100) (recur (dec tries))))))

(declare start-member!)

(def processes
  "What a member's processes match: the name bracketed, so the shell running
  pkill does not match itself."
  "[a]eron.jepsen.JepsenNode")

(defn start-node!
  "Starts the member, unless it runs. Waits for its READY."
  [opts test node]
  (if (running? processes)
    :already-running
    (start-member! opts test node)))

(defn- start-member!
  "Starts the member's JVM. A member killed moments ago leaves mark
  files that look active for a few seconds, and Java's archive, consensus
  module and service container refuse to start on them: the JVM is started
  again until they have gone stale, as an operator restarting after a crash
  would."
  [opts test node]
  (let [hosts (str/join "," (map ip (:nodes test)))
        args  (concat ["--id" (index test node) "--hosts" hosts] (timeouts opts))]
    (loop [attempts 30]
      (let [before (count-lines "/data/node.log" "READY")]
        (launch! "/data/node.log" (apply java "aeron.jepsen.JepsenNode" args))
        (let [outcome (await-ready "/data/node.log" before processes)]
          (cond (= :started outcome) :started

                (and (= :exited outcome)
                     (pos? attempts)
                     (re-find #"active mark file detected"
                              (c/exec* "tail -n 20 /data/node.log")))
                (do (Thread/sleep 1000) (recur (dec attempts)))

                :else (throw (ex-info "node did not start" {:node node :outcome outcome}))))))))

(defn kill-node!
  []
  (c/exec* (str "pkill -9 -f '" processes "' || true"))
  (loop [tries 50]
    (when (and (pos? tries)
               (not (str/blank? (c/exec* (str "pgrep -f '" processes "' || true")))))
      (Thread/sleep 100)
      (recur (dec tries)))))

(defn db
  [opts]
  (reify
    db/DB
    (setup! [_ test node]
      (info "starting member" (index test node))
      (start-node! opts test node))
    (teardown! [_ _test _node]
      (kill-node!)
      (c/exec* "rm -rf /data/* /dev/shm/aeron*"))

    db/Kill
    (kill! [_ _test _node] (kill-node!))
    (start! [_ test node] (start-node! opts test node))

    db/Pause
    (pause! [_ _test _node] (c/exec* (str "pkill -STOP -f '" processes "' || true")))
    (resume! [_ _test _node] (c/exec* (str "pkill -CONT -f '" processes "' || true")))

    db/Primary
    (setup-primary! [_ test _node]
      ;; The cluster is up once one member leads and every member is past its election.
      (loop [tries 300]
        (let [states (vals (c/on-nodes test (fn [_ _] (parse-state (probe! "state")))))]
          (cond (and (= 1 (count (filter #(= 2 (:role %)) states)))
                     (every? #(= 17 (:election %)) states))
                (info "cluster elected:" states)

                (zero? tries) (throw (ex-info "no leader elected" {:states states}))

                :else (do (Thread/sleep 200) (recur (dec tries)))))))
    (primaries [_ test]
      (->> (c/on-nodes test (fn [_ _] (parse-state (probe! "state"))))
           (filter (fn [[_ state]] (= 2 (:role state))))
           (map key)))

    db/LogFiles
    (log-files [_ _test _node]
      ["/data/node.log"])))
