# ydbd_mock manual test scenario

This file is the script for the final manual run: several `ydbd_mock` instances (one per pile), the unmodified bridge skipper demo, and the `ydb` CLI, all against the mock only.

## Rules (read first)

1. Use only these wrappers. Do not call `ydb`, `sudo`, `kill`, or `python` on your own for anything except what the scenario says (`kill` of the demo process you started, `curl` to `http://127.0.0.1:220xx`).
   - `MOCK=<repo>/ydb/apps/bridge_skipper/ydbd_mock/scripts/mock.sh`
   - `CLI=<repo>/ydb/apps/bridge_skipper/ydbd_mock/scripts/cli.sh` (guarded `ydb`: it refuses anything but mock endpoints)
   - `DEMO=<repo>/ydb/apps/bridge_skipper/ydbd_mock/scripts/demo.sh`
2. Never edit any file in the repo. In particular `ydb/apps/bridge_skipper_demo` must stay untouched. If the mock misbehaves, record it in the report. Do not fix it.
3. Work files live in `~/ydbd_mock_work/run` (`$RUN`): topology, pids, `logs/<pile>.log`, replicas. Put demo output in `$RUN/logs/demo.log`.
4. Every mutating CLI command needs `--assume-yes` (it is a root option, before `admin`).
5. Judge results by the mock's event log (`$MOCK cluster events`), not only by the demo's log.
6. Always finish with the teardown (S12), even after a failure.

Setup of shell variables for every command block below:

```bash
REPO=$(git rev-parse --show-toplevel)
R=$REPO/ydb/apps/bridge_skipper/ydbd_mock/scripts
MOCK=$R/mock.sh; CLI=$R/cli.sh; DEMO=$R/demo.sh
RUN=$HOME/ydbd_mock_work/run
E1=grpc://mock-pile1-1:2135        # any node label works as endpoint; the shim maps it to 127.0.0.1:<port>
E2=grpc://mock-pile2-1:2135
```

Useful commands:

```bash
$MOCK cluster status                          # one line per pile: pid, alive, generation, vector
$MOCK cluster state pile1                     # full JSON /state of one instance
$MOCK cluster now                             # current epoch time, for --after-ts
$MOCK cluster events --after-ts T --kind commit
$MOCK cluster events --after-ts T --kind rpc.start --grep UpdateClusterState
$CLI -e $E1 admin cluster bridge list --format=json
```

Demo lifecycle (used by S4 to S11):

```bash
$DEMO --fresh > $RUN/logs/demo.log 2>&1 &
echo $! > $RUN/demo.pid
# ... later:
kill $(cat $RUN/demo.pid); rm -f $RUN/demo.pid
```

The demo waits about 10 s at startup before its first decision, and it polls every second. After starting it, wait about 25 s before judging "steady state". After a `kill` the failover follows in 1-3 s, so poll `cluster events --kind rpc.start --grep UpdateClusterState` instead of waiting the full 45 s, and keep watching afterwards to prove it stays quiet.

## Report format

For each scenario S1 to S12: `PASS`, `FAIL`, or `SKIP` with one line of why, the exact commands run (or the ones that differ from this file), and the evidence (event lines, `list` output, relevant demo log lines). End with a list of mock bugs or surprises, each with a minimal reproduction. Also report CLI latency numbers (S1, S5, S6).

## S0 Preflight

```bash
$MOCK cluster down            # safe if nothing runs
lsof -nP -iTCP:22000-22199 -sTCP:LISTEN || echo "ports free"
git -C "$REPO" diff --stat -- ydb/apps/bridge_skipper_demo   # must print nothing
```

Expect: no listeners, empty diff.

## S1 CLI smoke (2 piles x 3 nodes)

```bash
$MOCK cluster up --fresh --piles pile1,pile2 --nodes-per-pile 3 \
    --set timings.promote_delay=0.5 --set timings.suspend_delay=0.5 \
    --set timings.connect_delay=0.5 --set timings.sync_step_delay=0.5
$MOCK cluster status
time $CLI -e $E1 admin cluster config fetch
time $CLI -e $E1 admin cluster bridge list --format=json
$CLI -e $E1 admin cluster bridge list --format=csv
$CLI -e $E1 admin cluster bridge list
time $CLI -e $E1 -d "" monitoring healthcheck -v --format=json --no-merge --no-cache --timeout 9000
$CLI monitoring healthcheck --help | grep -- --no-merge
$CLI -e $E1 -d /Root discovery whoami     # an RPC the mock does not implement (registered service)
$CLI -e $E1 -d /Root scheme ls            # a service the mock does not register at all
$MOCK cluster events --pile pile1 --kind rpc.unimplemented
```

Expect:

- `config fetch` prints exactly one YAML document, `metadata.version: 1` (initial storage revision 0 plus one), and six `hosts` entries labelled `mock-pile1-1..3`, `mock-pile2-1..3`, each with `location.bridge_pile_name`.
- `list --format=json`: `{"generation": 1, "conversion_in_progress": false, "piles": [{"pile_name": "pile1", "state": "PRIMARY"}, {"pile_name": "pile2", "state": "SYNCHRONIZED"}]}`. CSV is `pile_name,state,generation`. Pretty is `Pile pile1: PRIMARY` lines plus `Generation: 1`.
- healthcheck: `self_check_result` `GOOD`, empty or absent `issue_log`.
- Record RPC and help timings separately. Historical live RPC calls took 0.12–0.15 s on the measured host; `--help` took about 0.9 s. These are observations, not portable latency requirements.
- `discovery whoami` fails with `CLIENT_CALL_UNIMPLEMENTED` and the event is `/Ydb.Discovery.V1.DiscoveryService/WhoAmI` with `registered: true`. `scheme ls` fails the same way with `/Ydb.Scheme.V1.SchemeService/ListDirectory` and `registered: false`.
- `-e grpc://example.com:2135` and a call with no `-e` are refused by the shim with exit code 3 (`refusing`), and nothing reaches the network.

## S2 CLI state machine, 2 piles (cluster from S1, generation 1)

Run each step, then `list --format=json` against `$E1` and compare. "gen" is the `generation` field.

1. `$CLI -e $E1 --assume-yes admin cluster bridge failover --pile pile2` -> gen 2, pile2 `DISCONNECTED`.
2. The same command again -> gen 3 (a no-op commit still advances the generation).
3. `$CLI -e $E1 --assume-yes admin cluster bridge rejoin --pile pile2` -> gen 4 with pile2 `NOT_SYNCHRONIZED`. Then wait until pile2 is `SYNCHRONIZED` (a few seconds) and check the commit events: `cause` goes `admin` (gen 4), `connect` (gen 5), `synchronized` (gen 6). Final gen 6.
4. Error cases, each with a non-zero exit and an error text on stderr, and no generation change:
   - `takedown --pile pile1 --new-primary pile2` -> `Request must result in a state with one primary pile`
   - `switchover --new-primary pile1` (pile1 is the current primary) -> the same text
   - `failover --pile pile1` (primary, no `--new-primary`) -> the same text
5. `switchover --new-primary pile2` -> gen 7 with pile2 `PROMOTED` and pile1 `PRIMARY`, then gen 8 with pile2 `PRIMARY` and pile1 `SYNCHRONIZED` (cause `promote`).
6. `switchover --new-primary pile1` -> the same in the other direction (gens 9 and 10).
7. `takedown --pile pile2` -> gen 11 pile2 `SUSPENDED`, then gen 12 pile2 `DISCONNECTED` (cause `suspend`). While it is `SUSPENDED`, a `failover --pile pile2` must fail with `cannot update suspended pile: 2` (the number is the pile's 1-based index). Run the failover right after the takedown: the suspend follow-up fires after about 0.5 s with the timings of S1.
8. Without `--assume-yes`: `echo n | $CLI -e $E1 admin cluster bridge failover --pile pile2` sends no `UpdateClusterState` (check `$MOCK cluster events --after-ts T --kind rpc.start --grep UpdateClusterState` is empty) and the generation does not change.

Record the exact CLI stderr text of each error.

A pile that is `DISCONNECTED` (after steps 1, 2, 7) or `NOT_SYNCHRONIZED_1` does not serve gRPC, as on a real cluster (`run.cpp`): `$CLI -e $E2 ... list` fails with `TRANSPORT_UNAVAILABLE` and `$MOCK cluster state pile2` still shows its replica. After step 3 pile2 serves again as soon as the `connect` commit makes it `NOT_SYNCHRONIZED_2`. The reply to the request that disconnects a node is still delivered.

## S3 Three piles, CLI only: failover during synchronization

```bash
$MOCK cluster down
$MOCK cluster up --fresh --piles pile1,pile2,pile3 --nodes-per-pile 3 \
    --set timings.connect_delay=0.5 --set timings.sync_step_delay=3
$CLI -e $E1 --assume-yes admin cluster bridge failover --pile pile3
$CLI -e $E1 --assume-yes admin cluster bridge rejoin --pile pile3
# poll `$MOCK cluster state pile1` until pile3 is stored NOT_SYNCHRONIZED_2, then immediately:
$CLI -e $E1 --assume-yes admin cluster bridge failover --pile pile2
```

Expect: the last command succeeds, disconnects pile2 and preserves pile3's exact `_2` state. A switchover involving other synchronized piles also preserves an omitted `_2`. Explicitly rejoining pile3 again can reset `_2` to `_1`. For simultaneous failures, send one UpdateClusterState batch containing every failed pile as DISCONNECTED; each individual update can otherwise lack the required config quorum. The canonical validator/gRPC suite covers this current behavior.

### Failed resync pile visibility (conformance check)

Start a separate fresh three-pile subscenario; do not reuse the preceding S3 state, where pile2 is disconnected and pile3 may already have finished synchronizing. No demo is running. The default control ports for this topology are 22000, 22001 and 22002.

```bash
$MOCK cluster down
$MOCK cluster up --fresh --piles pile1,pile2,pile3 --nodes-per-pile 3 \
    --set timings.connect_delay=0.5 --set timings.sync_step_delay=3
$MOCK cluster state
curl --fail --silent --show-error -X POST http://127.0.0.1:22000/followups \
    -H 'Content-Type: application/json' --data '{"hold":["synchronized"]}'
curl --fail --silent --show-error -X POST http://127.0.0.1:22001/followups \
    -H 'Content-Type: application/json' --data '{"hold":["synchronized"]}'
curl --fail --silent --show-error -X POST http://127.0.0.1:22002/followups \
    -H 'Content-Type: application/json' --data '{"hold":["synchronized"]}'
$MOCK cluster state
```

Verify the initial state is generation 1, pile1 PRIMARY, and pile2/pile3 SYNCHRONIZED. Each of the three `/state` objects must contain `held_followups: ["synchronized"]` before proceeding. Any curl failure or missing hold fails this setup; do not continue. Installing the gates before starting rejoin avoids racing its final commit. Holding only pile1 is insufficient because any ready pile can propose a follow-up. The `connect`, `sync_step` and `bsc_done` follow-ups remain enabled.

```bash
$CLI -e $E1 --assume-yes admin cluster bridge failover --pile pile3
$CLI -e $E1 --assume-yes admin cluster bridge rejoin --pile pile3
$MOCK cluster state
```

Repeat `$MOCK cluster state` until **all three replicas** show pile3 with `stored: "NOT_SYNCHRONIZED_2"` and public `NOT_SYNCHRONIZED`; pile1/pile2 must remain internally SYNCHRONIZED. Expect generation 4 after the initial generation 1, failover, rejoin and `connect` commits. The hold prevents the final SYNCHRONIZED commit even if the local sync tracker reaches SYNCED. Do not repeat the rejoin command to wait: that could reset `_2` to `_1`.

```bash
$MOCK cluster kill pile3
$MOCK cluster state pile1
$MOCK cluster state pile2
```

Repeat the two survivor state reads until each reports pile3 `connected: false`, still stored NOT_SYNCHRONIZED_2. This waits for failed reachability to be observed rather than relying on a fixed sleep. Then collect both survivor responses:

```bash
$CLI -e $E1 -d "" monitoring healthcheck -v --format=json --no-merge --no-cache --timeout 9000
$CLI -e $E2 -d "" monitoring healthcheck -v --format=json --no-merge --no-cache --timeout 9000
$MOCK cluster events --kind commit
```

Expect neither SelfCheck to emit a BRIDGE_GROUP issue naming pile3, even though the dead `_2` pile still participates in data/config quorum. There must be no `synchronized` commit for pile3 in this fresh run before the observation. This intentionally does not satisfy the old demo's failure detector: capture the matching real-YDB fixture and test the controller's separately designed resync-failure evidence.

After recording the result, restore the process and release the exact gates through the implemented per-pile API:

```bash
$MOCK cluster revive pile3
curl --fail --silent --show-error -X POST http://127.0.0.1:22000/followups \
    -H 'Content-Type: application/json' --data '{"release":["synchronized"]}'
curl --fail --silent --show-error -X POST http://127.0.0.1:22001/followups \
    -H 'Content-Type: application/json' --data '{"release":["synchronized"]}'
curl --fail --silent --show-error -X POST http://127.0.0.1:22002/followups \
    -H 'Content-Type: application/json' --data '{"release":["synchronized"]}'
$MOCK cluster state
```

Repeat the state read until every instance has empty `held_followups` and sees pile3 SYNCHRONIZED. Gates are process-local: reviving pile3 already clears its gate, so its explicit release is harmless, and synchronization may finish during this cleanup. Do not reuse the earlier generation-4 expectation afterward. S4 starts another fresh two-pile cluster; no reset of the old S3 state is required. If this subscenario fails, record it and still perform S12 teardown.

The mock does not synthesize the unavailable-BSC and static-group fallback diagnostics produced by real YDB after primary loss. S6 checks only compatibility with the demo and the mock's synthetic health; real primary-loss acceptance requires a real-YDB fixture.

## S4 Demo, steady state

```bash
$MOCK cluster down
$MOCK cluster up --fresh --piles pile1,pile2 --nodes-per-pile 3
T=$($MOCK cluster now)
$DEMO --fresh > $RUN/logs/demo.log 2>&1 & echo $! > $RUN/demo.pid
sleep 45
$MOCK cluster events --after-ts $T --kind rpc.start --grep UpdateClusterState
tail -30 $RUN/logs/demo.log
```

Expect: the demo starts (capability check and resolve succeed, two piles, no exit), the event list is empty for 45 s, the demo log shows regular state checks and no failover. The demo is left running for S5.

## S5 Kill pile2 (follower), the demo fails it over

Continue from S4 (demo running).

```bash
T=$($MOCK cluster now)
$MOCK cluster kill pile2
sleep 45
$MOCK cluster events --after-ts $T --kind rpc.start --grep UpdateClusterState
$MOCK cluster events --after-ts $T --kind commit
$MOCK cluster status
$CLI -e $E1 admin cluster bridge list --format=json
$CLI -e $E1 -d "" monitoring healthcheck -v --format=json --no-merge --no-cache --timeout 9000
grep -i failover $RUN/logs/demo.log | tail
```

Expect:

- exactly one `UpdateClusterState` (arriving at a pile1 node) with `pile2:DISCONNECTED` and no `--new-primary`, exactly one `commit` event, generation 2;
- `list`: gen 2, pile1 `PRIMARY`, pile2 `DISCONNECTED`;
- health on pile1 is `GOOD` with an empty `issue_log` after the failover (blame disappears), and while pile2 was dead it was `MAINTENANCE_REQUIRED` with a `BRIDGE_GROUP` issue for pile2;
- the demo issued the failover once and is quiet afterwards (wait another 30 s and re-check the events).

Then revive pile2 and check it comes back stale:

```bash
T=$($MOCK cluster now)
$MOCK cluster revive pile2
sleep 15
$MOCK cluster status
$CLI -e $E2 admin cluster bridge list --format=json
$MOCK cluster events --after-ts $T --kind rpc.start --grep UpdateClusterState
```

Expect: pile2 answers generation 1 (its persisted stale replica) while pile1 answers 2, and pile2 is not pulled forward while registry/configured_piles are unchanged (pile1's snapshot says pile2 is `DISCONNECTED`; a `pull.refused` event on pile2 says so). The demo ignores pile2's lower generation: no new `UpdateClusterState`, no crash. Then rejoin:

```bash
$CLI -e $E1 --assume-yes admin cluster bridge rejoin --pile pile2
sleep 20
$CLI -e $E2 admin cluster bridge list --format=json
$MOCK cluster events --after-ts $T --kind commit
```

Expect: pile2 jumps to the current generation, then `connect` and `synchronized` commits, final pile2 `SYNCHRONIZED` on both piles. The demo does not fail over anything during the resync (sync grace). Record CLI latency to a dead pile (`time $CLI -e $E2 admin cluster bridge list` while pile2 is killed, for the report).

## S6 Kill pile1 (primary)

```bash
kill $(cat $RUN/demo.pid); rm -f $RUN/demo.pid
$MOCK cluster reset
T=$($MOCK cluster now)
$DEMO --fresh > $RUN/logs/demo.log 2>&1 & echo $! > $RUN/demo.pid
sleep 25
$MOCK cluster kill pile1
sleep 45
$MOCK cluster events --after-ts $T --kind rpc.start --grep UpdateClusterState
$CLI -e $E2 admin cluster bridge list --format=json
```

Expect: exactly one `UpdateClusterState` at a pile2 node with `pile1:DISCONNECTED,pile2:PRIMARY`; `list` from pile2: generation +1, pile2 `PRIMARY`, pile1 `DISCONNECTED`.

## S7 Partition pile1 | pile2 (mutual blame, both responsive)

```bash
kill $(cat $RUN/demo.pid); rm -f $RUN/demo.pid
$MOCK cluster reset
T=$($MOCK cluster now)
$DEMO --fresh > $RUN/logs/demo.log 2>&1 & echo $! > $RUN/demo.pid
sleep 25
$MOCK cluster partition "pile1|pile2"
sleep 45
$MOCK cluster events --after-ts $T --kind rpc.start --grep UpdateClusterState
$MOCK cluster state pile1 | head -60
$MOCK cluster state pile2 | head -60
```

Expect: the demo reacts within 1-3 s. Exactly one `failover --pile pile2` goes to a pile1 node. pile1 commits alone (generation +1) and a `push.failed` event shows the cut link; pile2 keeps its old replica. After the failover pile1's health is `GOOD` (a `DISCONNECTED` pile is no longer blamed), while pile2's health stays `MAINTENANCE_REQUIRED` with a `BRIDGE_GROUP` issue for pile1. To see the mutual blame, run `healthcheck` against both piles right after the partition (within 1 s), before the demo reacts. Then `$MOCK cluster heal`, wait 20 s: with unchanged registry/configured_piles, pile2 is still stale (it is `DISCONNECTED` in pile1's snapshot), and the demo stays quiet.

## S8 Keeper split (keeper loses pile1, the cluster is fine)

```bash
kill $(cat $RUN/demo.pid); rm -f $RUN/demo.pid
$MOCK cluster reset
T=$($MOCK cluster now)
$DEMO --fresh > $RUN/logs/demo.log 2>&1 & echo $! > $RUN/demo.pid
sleep 25
$MOCK cluster keeper-split pile1
sleep 45
$MOCK cluster events --after-ts $T --kind rpc.start --grep UpdateClusterState
```

Expect: no `UpdateClusterState`: the primary is silent for the keeper, but nobody blames it. Then `$MOCK cluster heal`.

## S9 Older-generation replica is ignored

```bash
kill $(cat $RUN/demo.pid); rm -f $RUN/demo.pid
$MOCK cluster reset
# push the generation up with a few admin commits (switchover there and back)
$CLI -e $E1 --assume-yes admin cluster bridge switchover --new-primary pile2
sleep 3
$CLI -e $E1 --assume-yes admin cluster bridge switchover --new-primary pile1
sleep 3
$CLI -e $E1 admin cluster bridge list --format=json     # note generation G
T=$($MOCK cluster now)
$DEMO --fresh > $RUN/logs/demo.log 2>&1 & echo $! > $RUN/demo.pid
sleep 20
$MOCK cluster replica pile2 --generation 2 --states pile1=PRIMARY,pile2=SYNCHRONIZED --detach
sleep 30
$MOCK cluster events --after-ts $T --kind rpc.start --grep UpdateClusterState
$CLI -e $E2 admin cluster bridge list --format=json    # pile2 answers the older generation
```

Expect: pile2 answers generation 2, pile1 answers G (greater). The demo keeps G, sends nothing, and does not exit. Then `$MOCK cluster replica pile2 --clear` and, optionally, `--generation <G+50> --states pile1=DISCONNECTED,pile2=PRIMARY --detach`: the demo adopts the newer generation (visible in its log) and still sends no failover, because pile1 is already `DISCONNECTED` in the adopted map. Finish with `$MOCK cluster reset`.

## S10 Silent node on `bridge list` (documented demo hazard)

```bash
$MOCK cluster fault pile1 --op silence --on --max 40 --methods GetClusterState
sleep 60
tail -20 $RUN/logs/demo.log
$MOCK cluster events --pile pile1 --kind rpc.cancelled
```

Expect: while the fault is on, any demo `bridge list` call that reached a pile1 node hangs (the real CLI sets no deadline on it), the demo's health data goes stale for the stall, and after at most 40 s the node answers and the demo recovers. This documents the hazard rather than asserting good behavior. Note how long the demo was stalled, and whether it sent a failover meanwhile (it should not).

## S11 Three piles: the demo refuses to start

```bash
kill $(cat $RUN/demo.pid) 2>/dev/null; rm -f $RUN/demo.pid
$MOCK cluster down
$MOCK cluster up --fresh --piles pile1,pile2,pile3 --nodes-per-pile 3
$DEMO --fresh -e mock-pile1-1 > $RUN/logs/demo3.log 2>&1; echo "exit=$?"
tail -5 $RUN/logs/demo3.log
```

Expect: exit code 2 and a log line that the demo does not support more than 2 piles. This confirms that the mock serves N piles and that the demo's own limit is the only thing that stops it.

## S12 Teardown and final checks

```bash
kill $(cat $RUN/demo.pid) 2>/dev/null; rm -f $RUN/demo.pid
$MOCK cluster down
pgrep -fl ydbd_mock || echo "no stray instances"
pgrep -fl skipper.py || echo "no stray demo"
lsof -nP -iTCP:22000-22199 -sTCP:LISTEN || echo "ports free"
git -C "$REPO" diff --stat -- ydb/apps/bridge_skipper_demo
git -C "$REPO" status --short ydb/apps/bridge_skipper_demo
```

Expect: no stray processes, no listeners, and no output from the two git commands (the demo is unchanged).
