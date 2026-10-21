# ydbd_mock design

Behavior of the mock. How to build, run, and drive it is in [README.md](README.md). The manual script is [MANUAL_TEST_SCENARIO.ai.md](MANUAL_TEST_SCENARIO.ai.md). If this file and the code disagree, the code wins.

One process is one pile. N piles are N processes. Each keeps its own replica of the cluster state and they agree on commits through a small peer protocol. There is no shared snapshot: a dead, partitioned, or `DISCONNECTED` pile simply keeps the last state it learned.

Out of scope: tablets, Hive, the data path, real storage, interconnect, and distconf itself. The peer protocol only imitates what a client can observe.

## What the demo and the CLI require

The unmodified demo (`ydb/apps/bridge_skipper_demo`) shells out to the `ydb` CLI. The CLI talks to the mock. The demo supports exactly two piles and exits 2 on three or more; the mock still serves N. The demo issues only `list`, `monitoring healthcheck`, and `failover`. The mock also serves `switchover`, `takedown`, `rejoin`, and hidden `update`, because the real CLI sends them.

Scheme and port, on every call except the help check: the demo builds `grpc://<host>:2135` (`grpcs` when its auth options contain `--ca-file` or `--client-cert-file`). `<host>` is the `host` string from config fetch. The shim (`scripts/ydb_shim.py`) rewrites a mock label to `127.0.0.1:<port>` and refuses every other endpoint (exit 3). `monitoring healthcheck --help` is a property of the real CLI, not of the mock; the demo's capability check still needs that binary, and its stdout must contain `--no-merge`.

Config fetch is one YAML document. The demo reads `config.hosts[].host` and `location.bridge_pile_name` and ignores the rest. Pile order is first-seen host order. The CLI prints `metadata.version` plus one, so a body with `version: 1` shows up as `2`. A second document, or a second config entry concatenated without `---`, makes resolve fail.

Health, one process per sampled host, no cross-host retry:

```text
ydb <auth...> -e grpc://<host>:2135 -d  monitoring healthcheck -v --format=json --no-merge --no-cache --timeout 9000
```

The demo keeps an issue only when `type` contains `GROUP`, `status` is `RED`, and `location.storage.pool.group.pile.name` is set. Two successful replies that agree are a quorum (threshold 2). A pile with fewer than two successes is silent for that round. `self_check_result` may be `GOOD` together with a RED issue; the demo accepts that. It does not rank `GOOD` / `DEGRADED` / `MAINTENANCE_REQUIRED` / `EMERGENCY`.

Admin list:

```text
ydb <auth...> -e grpc://<host>:2135 admin cluster bridge list --format=json
```

```json
{"generation": 1, "piles": [{"pile_name": "pile1", "state": "PRIMARY"}, {"pile_name": "pile2", "state": "SYNCHRONIZED"}]}
```

`generation` must be a positive JSON number. The demo treats `0` as missing and replaces a stored snapshot only when the new generation is strictly greater. Hosts of one pile agree unless that pile's replica was overridden. State strings the CLI emits: `UNSPECIFIED`, `DISCONNECTED`, `NOT_SYNCHRONIZED`, `SYNCHRONIZED`, `PROMOTED`, `PRIMARY`, `SUSPENDED`.

Failover, stdout ignored:

```text
ydb <auth...> -e grpc://<host>:2135 --assume-yes admin cluster bridge failover --pile <name> [--new-primary <name>]
```

`--assume-yes` is required. Without it the CLI prompts and sends no RPC. `--pile` becomes `DISCONNECTED`. `--new-primary` is present only when the bad pile is the current primary, and that pile becomes `PRIMARY` in the same commit unless another pile is already `PROMOTED`. Every accepted commit increments `generation` by 1, including a repeated failover of an already-disconnected pile. The demo then waits until list reports `generation + succeeded_count` and sends nothing until then.

The CLI calls `ListEndpoints` before every other RPC, with about a 10 s discovery deadline. A mock that does not answer makes every call cost that long, which is the demo's health TTL. `list`, `failover`, and `config fetch` set no client deadline. `healthcheck --timeout` is the only one, 9 s.

## Topology

Piles are an ordered list. The index is the position and does not change. Pile `p` (0-based) listens for control and peer HTTP on `22000 + p`. Node `n` (0-based) of that pile listens for gRPC on `22100 + 10 * p + n`. Labels are `mock-<pile>-<n>` with `n` 1-based, and config fetch returns those labels as `host`. The bind address is `127.0.0.1` unless the topology sets `allow_remote: true`. The control API has no authentication.

`cluster up` writes `topology.yaml` and `endpoint_map.json` into the run directory. Timings (seconds) and policies live in that file and can be changed on a live instance. A pile with fewer than 3 nodes is a warning, not an error: the demo logs the same warning and still starts.

## Cluster state

Stored states mirror `NKikimrBridge.TClusterState.EPileState`. They are not a proto the mock depends on. They appear in `/state`, the event log, and the peer protocol, never on the gRPC wire.

| Stored | Data quorum | Config quorum | Connects |
| --- | --- | --- | --- |
| `SYNCHRONIZED` | yes | yes | yes |
| `NOT_SYNCHRONIZED_2` | yes | yes | yes |
| `SUSPENDED` | no | yes | yes |
| `NOT_SYNCHRONIZED_1` | no | no | yes |
| `DISCONNECTED` | no | no | no |

Public state: the primary index is `PRIMARY`, the promoted index is `PROMOTED` when it differs, and the rest map directly (`_1` and `_2` are both public `NOT_SYNCHRONIZED`). Initial snapshot: every pile `SYNCHRONIZED`, primary and promoted are pile 0, generation 1.

Sync stage is local to each instance, not replicated. One stage per pile, not per storage group. A commit to `DISCONNECTED` or `SUSPENDED` resets that pile's stage to `BLOCKS`. A commit to `NOT_SYNCHRONIZED_1` does not. While a pile is `_2` and this instance sees it connected, the stage advances one step per `sync_step_delay`, then `bsc_cleared` is set, then a follow-up commits `SYNCHRONIZED`.

### Validator

`validate_update` follows the real server's order, because the order decides which error the client sees. Messages are verbatim.

1. Empty `updates`: `BAD_REQUEST`, `Updates list cannot be empty`.
2. Duplicate name: `BAD_REQUEST`, `duplicate update for pile name# X`.
3. Two `PRIMARY` or two `PROMOTED` in the request: `BAD_REQUEST`, `multiple primary piles are not allowed in a single request` (or `promoted`).
4. Unknown name: `BAD_REQUEST`, `Invalid pile name# X`.
5. Named pile is stored `SUSPENDED`: `BAD_REQUEST`, `cannot update suspended pile: <id>`. `<id>` is the pile index plus one.
6. Rebuild every pile. A named pile takes the requested public state. An unnamed pile is re-derived from its current public state, so a pile in `_2` becomes `_1`. That rewrite counts as a stored change.
7. Map public to stored: `PRIMARY`, `PROMOTED`, and `SYNCHRONIZED` become `SYNCHRONIZED`; `NOT_SYNCHRONIZED` becomes `_1`; `DISCONNECTED` and `SUSPENDED` stay. Anything else, including `UNSPECIFIED`: `INTERNAL_ERROR`, `Unsupported pile state`.
8. The result must contain exactly one `PRIMARY`. Otherwise `BAD_REQUEST`, `Request must result in a state with one primary pile`. This rejects takedown of the current primary and switchover to the current primary.
9. With no `PROMOTED` left, promoted becomes the primary. A pile that was already `PROMOTED` and was not named stays `PROMOTED`.
10. `quorum_piles`: unknown name or a suspended witness is `BAD_REQUEST`. The list is stored and not used to narrow the commit set.
11. More than one stored slot changed: `INTERNAL_ERROR`, `Too many state changes in new configuration`. Direct switches to stored `SYNCHRONIZED` or to `_2`, and `SYNCHRONIZED` to `_1`, are `INTERNAL_ERROR` with the distconf text (`Can't switch to SYNCHRONIZED directly`, `Can't switch to NOT_SYNCHRONIZED_2 directly`, `Invalid transition from SYNCHRONIZED to NOT_SYNCHRONIZED_1`).

Consequences: while any pile is `_2`, failing over a different pile is two stored changes and is rejected; a switchover in that window succeeds and knocks the resyncing pile back to `_1`. A no-op failover of an already `DISCONNECTED` pile still commits at generation + 1. Failover of the primary while a switchover to R is in flight leaves R promoted, and the promote follow-up then makes R primary.

If `admin_token` is set, `UpdateClusterState` without a matching `x-ydb-auth-ticket` is `UNAUTHORIZED` (`Bridge operations require administrator privileges`) before the validator. `GetClusterState` does not check it.

### Follow-ups

Each of these is its own commit, so generation steps by one. The one-slot rule does not apply. The lowest pile index normally wins; a lost race is settled by the generation check.

| Follow-up | When | Commit |
| --- | --- | --- |
| Promote | promoted ≠ primary, and the promoted pile is stored `SYNCHRONIZED` | primary becomes the promoted pile. Does not wait for tablets. |
| Suspend | some pile is `SUSPENDED` | that pile becomes `DISCONNECTED` |
| Connect | pile is `_1` and this instance sees it connected | `_2` |
| Synchronized | pile is `_2`, stage is `SYNCED`, and `bsc_cleared` | `SYNCHRONIZED` |

Stage advances and `bsc_cleared` are local and do not bump generation. An instance runs follow-ups only when it is not detached, not itself `DISCONNECTED`, not paused for reset, and a majority of its nodes are up.

## Commits

Every admin update and every follow-up goes through one two-phase commit. The required set is every pile whose new stored state needs a config quorum (`SYNCHRONIZED`, `_2`, `SUSPENDED`). `enforce_config_quorum: false` shrinks that set to the proposer.

The proposer also asks every observer: itself and every pile whose old or new state allows connection. A pile outside the required set may be committing something else, and two proposals with disjoint required sets must still exclude each other. An observer blocks the proposal only when it answers `busy`, or `generation` at or above the proposed generation.

A pile accepts a prepare when it is not detached, a strict majority of its nodes are not `down`, the sender is not link-blocked, `new.generation == local + 1`, and its prepared slot is empty or holds the same proposal. On refusal the proposer aborts every slot it took, including prepares whose reply was lost, then retries until `quorum_fail_delay` (admin) or rearms (follow-up). A higher generation on a peer that allows connection is pulled and the admin request is validated again. After the deadline the error is `INTERNAL_ERROR`, `mock: no config quorum for piles [...]`.

After every required pile has accepted and no observer conflicted, the proposer installs locally and sends commit. The commit phase is not tied to the client's handler: a client deadline does not abandon a proposal. A commit for a snapshot this instance never prepared does not evict a different prepared proposal.

Push, after commit, goes to piles that did not take part and whose old or new state allows connection. Anti-entropy pulls a higher generation from a peer only when that peer's snapshot still allows this pile to connect. A `DISCONNECTED` pile is never pulled forward; it stays stale until a `rejoin` pushes to it, and records `pull.refused`.

Each install writes `replica.json` by temp file, fsync, and rename. Startup loads it. An unreadable or mismatched file stops the process instead of bootstrapping generation 1. The initial snapshot is written at first start, so a restart reloads it. A peer that has never answered a heartbeat counts as reachable for a short grace after start, so the first commit does not race the first ping.

A pile whose own stored state is `DISCONNECTED` or `NOT_SYNCHRONIZED_1` closes its gRPC listeners after the request that caused it has been answered. The control and peer API stay up. `policies.close_grpc_when_disconnected: false` turns this off.

## gRPC

Each node serves Discovery `ListEndpoints` (itself only, plus `pile_states`), Config `FetchConfig` (one main-config entry), Monitoring `SelfCheck`, and Bridge `GetClusterState` / `UpdateClusterState`. Success and application errors are ready `Operation`s. gRPC status codes are used only for transport faults and for methods the mock does not implement.

`SelfCheck` on a healthy node is `GOOD` with an empty issue log. A blamed pile adds a `BRIDGE_GROUP` issue, status `RED`, message containing `storage` or `connectivity`, with the pile name at `location.storage.pool.group.pile.name`. Automatic blame (on by default) is every other pile this instance sees as not connected, whose stored state still requires data quorum. A pile already `DISCONNECTED` is not blamed, and a node does not automatically blame its own pile. `issue_style: realistic` also emits the parent `STORAGE_GROUP` issue.

Any other method returns gRPC `UNIMPLEMENTED` and an `rpc.unimplemented` event, whether or not the service is registered (`WhoAmI` versus `scheme ls`).

## Faults

Three layers.

Per node, on that node's RPCs, in order: record `rpc.start`; `unavailable` aborts with gRPC `UNAVAILABLE`; `delay` sleeps; `silence` waits until cleared, until `max` seconds, or until the client cancels (`rpc.cancelled`); then the method runs. `delay` and `silence` can name methods, so health checks can keep answering while `GetClusterState` stalls. `down` closes that node's listener (connection refused) and counts against the pile's majority.

Process: `kill` is `SIGKILL` (connections refused, replica frozen on disk); `stop` is `SIGSTOP` (the kernel accepts connections and nobody answers). `revive` restarts with the persisted replica and reapplies recorded partitions and keeper-splits.

Links: a block list drops peer traffic in both directions (the receiver answers 503). `partition "a|b"` blocks across the groups. Nothing is client-visible, so the keeper can still reach both sides and they blame each other. `keeper-split P` sets `unavailable` on every node of P and does not touch links. `heal` clears block lists and macro faults; faults set by hand stay.

`silence` with no cap freezes the demo's `bridge list`, because the CLI sets no deadline and the demo waits for every target. Always pass a max, or clear it. Measured timings for kill versus stop are in the README.

## HTTP API

JSON on the control port. Errors are `{"error": "..."}` with status 400 or 404. A successful control call returns `/state`.

| Method and path | Body | Effect |
| --- | --- | --- |
| `GET /state` | | This instance's view, including `pid`, `generation`, `max_generation`, `reset_pending`, public and stored state, sync stage, peers, blocked piles, prepared slot, nodes and their faults, timings, policies. `admin_token` is omitted. |
| `GET /events` | query `since`, `limit`, `after_ts`, `kind`, `grep` | This instance's event log. |
| `POST /control` | `node` (id) or `label` or `"all": true`, plus `op` | One fault op on each targeted node of this instance. |
| `POST /link` | `{"block": [...]}`, `{"unblock": [...]}`, or `{"set": [...]}` | Peer-link block list. |
| `POST /replica` | `{"generation", "states": {"pile1": "PRIMARY", ...}, "detach": true}` or `{"clear": true}` | Replace this replica, optionally detached so it ignores pushes, pulls, and prepares. |
| `POST /timings` | timing names to seconds | Change timings. Durations that must be positive are rejected otherwise. |
| `POST /policies` | policy names | `auto_blame_dead_piles`, `enforce_config_quorum`, `anti_entropy`, `admin_token`, `issue_style` (`minimal` or `realistic`), `close_grpc_when_disconnected`. |
| `POST /reset-prepare` | `{}` | Pause new proposals and follow-ups. Used by `cluster reset`. |
| `POST /reset-cancel` | `{}` | Release that pause. |
| `POST /reset-local` | `{"generation": G}` with G ≥ 1 | Install the initial snapshot at G, clear faults, links, detach, and timers. The caller chooses G. |
| `POST /scenario` | `{"name", "target", ...}` | Record a `scenario` event. Does not change state. |

`/control` ops: `storage` and `connectivity` take `piles` (replace the blame set; empty clears it); `down`, `unavailable`, and `silence` take `on` and optional `max`; `delay` takes `seconds` and optional `methods`; `self_check` takes `value` (`GOOD`, `DEGRADED`, `MAINTENANCE_REQUIRED`, `EMERGENCY`, or null); `clear` clears the node.

`cluster reset` pauses every pile, reads each persisted `max_generation`, installs the initial snapshot at the maximum plus one, then cancels the pause. A lower `replica` override does not lower the watermark. The sequence is in the README.

Peer API, same port, used by other instances. `GET /peer/ping?from=<pile>` returns `{pile, generation, ready}`. `GET /peer/state` returns the snapshot. `POST /peer/prepare`, `/commit`, `/abort`, and `/push` carry `{from, ...}`. A blocked sender gets 503. A stopped or killed instance does not answer.

## Events

Each instance appends to a ring buffer and to `logs/<pile>.events.jsonl`, so the log survives a kill. `cluster up` without `--fresh` keeps the file and continues `seq`. Kinds: `rpc.start`, `rpc.end`, `rpc.cancelled`, `rpc.unimplemented`, `commit` (cause `admin`, `promote`, `suspend`, `connect`, `synchronized`, `install`), `reject`, `propose.retry`, `propose.fail`, `fault`, `scenario`, `link`, `peer.up`, `peer.down`, `followup.armed`, `followup.cancelled`, `pull.refused`. The keys `seq`, `ts`, `pile`, and `kind` are reserved.

## Fidelity

| Real behavior | Mock |
| --- | --- |
| Public mapping, validator order and texts, `_2` → `_1` rewrite, one stored change per admin commit, generation + 1 including no-ops | Faithful |
| `quorum_piles` stored and not applied; suspended pile and suspended witness rejected | Faithful |
| Promote, suspend → disconnect, `_1` → `_2` → `SYNCHRONIZED` | Separate commits, with the configured delays. Real `UpdateBridgeConfig` can combine a promotion and several `_1` → `_2` moves. |
| Config quorum | Two-phase commit over the piles that need it, plus observers. The quorum-failure text is the mock's own. |
| Per-group sync stages | One stage per pile, local, not replicated |
| gRPC of a pile that lost the config-quorum trait | Listeners close after the causing reply, as in `run.cpp` |
| Distconf consensus | Generation CAS. No leader election. A crash between phases is covered by `prepare_ttl` and anti-entropy. |
| Data-path stall, bridge-proxy timeouts | Not modeled. A dead data-quorum pile shows up only as health blame. |
| Discovery | The answering node only |
| Auth, TLS | Optional admin token on `UpdateClusterState`. Optional server certificate; covered by an in-process test, not by the demo. |
