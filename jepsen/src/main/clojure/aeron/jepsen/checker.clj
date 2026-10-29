(ns aeron.jepsen.checker
  "Checks beside Elle's: a member truncating its log below its commit
  position, and the cluster serving again once the faults end. Elle sees a
  lost write only when a later read misses it; a cluster that loses
  committed entries and then stalls serves no such read."
  (:require [clojure.java.io :as io]
            [jepsen [checker :as checker]
                    [store :as store]]))

(def ^:private truncation-line
  ;; Election's ClusterEvent, printed by the node's error handler.
  #"Truncating Cluster Log - memberId=(\d+).*this\.commitPosition=(\d+).*newPosition=(\d+)")

(defn truncation
  "Invalid when a member's log was cut below its commit position: entries
  a quorum had committed are gone from it."
  []
  (reify checker/Checker
    (check [_ test _history _opts]
      (let [cuts (for [node   (:nodes test)
                       file   ["node.log"]
                       :let   [f (store/path test node file)]
                       :when  (.exists ^java.io.File f)
                       line   (line-seq (io/reader f))
                       :let   [[_ member commit position] (re-find truncation-line line)]
                       :when  member
                       :let   [commit (parse-long commit), position (parse-long position)]]
                   {:node node, :member (parse-long member), :commit-position commit, :new-position position,
                    :below-commit? (< position commit)})
            below (filter :below-commit? cuts)]
        {:valid?               (empty? below)
         :truncations          (count cuts)
         :below-commit-position (vec below)}))))

(defn liveness
  "Invalid unless a transaction succeeded after the faults' last operation
  (the final heal)."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [last-fault (->> history (filter #(= :nemesis (:process %))) last :time)
            served     (->> history
                            (filter #(and (= :ok (:type %)) (number? (:process %))))
                            (filter #(or (nil? last-fault) (< last-fault (:time %)))))]
        {:valid?            (boolean (seq served))
         :ok-after-faults   (count served)
         :last-fault-time-s (some-> last-fault (/ 1e9))}))))
