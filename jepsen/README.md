# Jepsen tests of Aeron Cluster

Jepsen tests of a three-member Aeron Cluster running Elle's list-append workload, under random faults or staged scenarios.
- **Members:** each is one JVM (`JepsenNode`) with a media driver, an archive, a consensus module and the list-append service.
- **Clients** are Aeron Cluster sessions from the host.

```bash
./run.sh --scenario random --time-limit 120                                     # partitions, kills, pauses
./run.sh --scenario stale-leader --election-timeout-ms 3000 --time-limit 90
./run.sh --scenario stale-election --election-timeout-ms 3000 --time-limit 100
AERON_VERSION=1.53.3 ./run.sh --scenario stale-leader --election-timeout-ms 3000 --time-limit 90
./run.sh serve                                                                   # results in store/, on port 8080
```

`AERON_VERSION` is the `aeron-all` the nodes and clients run, 1.54.0 by default. It can be any version Maven resolves, from Maven Central or the local repository.

## How it runs

- **Nodes** are Docker containers n1..n3 (`scripts/cluster.sh`), at 10.77.0.11 onwards, on a bridge network whose host side, 10.77.0.1, is where Jepsen and its clients run.
- **Their image is empty.** The host's `/usr` and `/etc` are mounted read-only, so a node runs the host's JDK and tools (`iptables`, `pkill`) and nothing is pulled or installed. The suite and the Maven repository are mounted read-only too. Each node's data is a tmpfs at `/data`.
- **Commands** reach the nodes through `docker exec` (`docker.clj`), not SSH.
- **Builds.** The Java side and the classpath come from Maven (`pom.xml`): Jepsen 0.3.14, and Aeron at `AERON_VERSION`.
- **Requirements:** a Linux host, since the nodes run its binaries, with Docker, Maven, a JDK of 21 or later, `iptables` and `procps`.

## What it checks

- **`:append`:** Elle's checker for strict serializability. Every transaction, reads too, goes through the log, and the leader's service answers it.
- **`:truncation`:** fails when a member cut its log below its commit position (`Truncating Cluster Log`).
- **`:liveness`:** fails when no transaction succeeded after the final heal. A cluster that loses committed entries and then stalls serves no read for Elle to catch the loss with.

A transaction ends one of three ways:
- **`:ok`:** answered.
- **`:fail`:** never sent.
- **`:info`:** sent, with no answer within 5 s.

## Scenarios

- **`random`:** Jepsen's partitions (one member, a majority, a majorities ring, the leader), kills and pauses, every `--nemesis-interval` seconds.

The staged scenarios apply their partitions as the members' counters change (`ClusterProbe`: role, election state, module state).

- **`stale-leader`**, a stale leader in SNAPSHOT:
  1. Cut off the leader, then ask it for a snapshot. It stays leader in SNAPSHOT, because its inactive-quorum check runs only in ACTIVE.
  2. When one of the others stands as candidate, cut it from the voter before it publishes its new term.
  3. Reconnect the voter, now canvassing, to the stale leader.
  4. Heal everything.
- **`stale-election`**, a stale leader whose election never completed:
  1. Cut off the leader X. The first candidate, Y, wins T1 with Z's vote, then is cut off.
  2. Reconnect X and Z. Z votes for X's T2, then X is cut off.
  3. Reconnect Z to Y, which is still waiting to complete its T1.
  4. Heal everything.

## Findings (2026-10-29)

| Scenario | 1.54.0 | 1.53.3 |
|---|---|---|
| `random`, 120 s | Valid, 4 runs | Valid, 2 runs |
| `stale-leader` | The stale leader and the voter cut their logs from 66656 to 39296, below their commit position. No transaction succeeds after the heal | The same, 67936 to 37952 |
| `stale-election` | Z follows T1 after voting in T2. After the heal, the T1 members can't truncate to the end of X's T2 log: 43776 is inside one of their frames. They loop on "joinPosition less than logPosition", and no transaction succeeds | The same, at 39584 |

The staged scenarios show, on real members:
- a member that voted in a newer term follows an older one while canvassing;
- truncation ignores the commit position;
- a leader outside ACTIVE never steps down.

Entries a quorum committed, and clients saw acknowledged, are cut from the logs (`stale-leader`) or missing from the new leader's (`stale-election`). Either way, the cluster stops serving.

## Licence

Apache-2.0, as Aeron.
