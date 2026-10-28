# ydbd_mock

A Python mock of the public YDB Bridge API, for testing Skipper controllers, the unmodified bridge skipper demo (`ydb/apps/bridge_skipper_demo`), and the `ydb` CLI without running real YDB.

**One `ydbd_mock` process is one pile.** It serves K nodes (default 3), each on its own gRPC port on `127.0.0.1`, plus one HTTP port for the control API and the peer API. Processes exchange snapshots using a simplified commit protocol with heartbeats and anti-entropy. This tests client-visible behavior; it does not simulate real storage durability or distconf consensus.

Design, including what the demo requires and the HTTP API: [README.ai.md](README.ai.md). Manual test script: [MANUAL_TEST_SCENARIO.ai.md](MANUAL_TEST_SCENARIO.ai.md).

## What it serves

| Service | Methods |
| --- | --- |
| Discovery | `ListEndpoints` (the answering node only, with `pile_states`) |
| Config | `FetchConfig` (one YAML document with `config.hosts[]`) |
| Monitoring | `SelfCheck` (`BRIDGE_GROUP` RED issues for blamed piles) |
| Bridge | `GetClusterState`, `UpdateClusterState`, `ConvertToBridge`, `AddPile`, `RemovePile` |

Every other method returns `UNIMPLEMENTED` and an `rpc.unimplemented` event.

The mock supports inactive piles, persistent named registry slots, deployed-config changes, conversion and pending add/remove operations. Omitted states retain `_2` and `INACTIVE`; batched disconnects are allowed. Deterministic hooks can hold snapshots or accepted mutations, lose committed replies, inject per-node observations, and hold/release BSC work. The versioned supervisor HTTP API is intended for a Skipper Lab UI. See [the behavior and conformance contract](README.ai.md) for exact controls and limits.

## Quick start

Once, from the repo root. This creates `~/ydbd_mock_work` (virtualenv, generated protos, run directory):

```bash
ydb/apps/bridge_skipper/ydbd_mock/scripts/setup_venv.sh
```

Start a fresh two-pile cluster. `up` returns when both processes answer:

```bash
S=ydb/apps/bridge_skipper/ydbd_mock/scripts
$S/mock.sh cluster up --fresh
```

That is two processes, `pile1` and `pile2`, three nodes each, all on `127.0.0.1`. `pile1` is `PRIMARY`, `pile2` is `SYNCHRONIZED`, generation 1.

| Pile | Control (HTTP) | Nodes (gRPC) |
| --- | --- | --- |
| pile1 | 22000 | `mock-pile1-1` 22100, `mock-pile1-2` 22101, `mock-pile1-3` 22102 |
| pile2 | 22001 | `mock-pile2-1` 22110, `mock-pile2-2` 22111, `mock-pile2-3` 22112 |

Then start the unmodified demo (TUI). It needs the real `ydb` CLI at `~/ydb/bin/ydb` (or `YDBD_MOCK_REAL_YDB`). The first decision is about 10 s after start:

```bash
$S/demo.sh --fresh --tui
```

Quit the TUI to stop the demo; the mock keeps running. `$S/mock.sh cluster down` stops it.

## Layout

```
ydbd_mock/
  main.py            entry point: run | cluster | selfcheck
  lib/               the implementation (PY3_LIBRARY)
  tests/             unit and in-process multi-instance tests (PY3TEST, unittest)
  e2e/               real processes, the real ydb CLI and the real demo (manual; venv only)
  scripts/           wrappers: setup_venv.sh gen_protos.sh mock.sh cli.sh ydb_shim.py demo.sh test.sh
  README.ai.md
  MANUAL_TEST_SCENARIO.ai.md
```

## Build and test

```bash
ydb/apps/bridge_skipper/ydbd_mock/ydbd_mock selfcheck        # imports everything, prints versions

YDBD_MOCK_IMPL=ya ydb/apps/bridge_skipper/ydbd_mock/scripts/mock.sh cluster up --fresh
```

Build target: `ydb/apps/bridge_skipper/ydbd_mock`. Canonical test target: `ydb/apps/bridge_skipper/ydbd_mock/tests`. Use the checkout’s build and test instructions. The commands above assume the binary is already built; platform support must be determined by the actual build result.

## Virtualenv runtime and compatibility scripts

```bash
S=ydb/apps/bridge_skipper/ydbd_mock/scripts
$S/setup_venv.sh        # ~/ydbd_mock_work/venv with grpcio, protobuf, aiohttp, PyYAML, requests
$S/gen_protos.sh        # generates the *_pb2 modules into ~/ydbd_mock_work/gen
# tests/ is the canonical suite; follow the checkout build instructions
# scripts/test.sh retains an optional venv CLI/demo compatibility harness
```

## Run a cluster by hand

```bash
S=ydb/apps/bridge_skipper/ydbd_mock/scripts
$S/mock.sh cluster up --fresh --piles pile1,pile2 --nodes-per-pile 3     # ports: control 22000+p, nodes 22100+10p+n
$S/mock.sh cluster status
$S/cli.sh -e grpc://mock-pile1-1:2135 admin cluster bridge list          # a label works as the host
$S/demo.sh --fresh                                                       # the unmodified demo
$S/mock.sh cluster down
```

State lives in `$YDBD_MOCK_RUN_DIR` (default `~/ydbd_mock_work/run`): `topology.yaml`, `cluster.json`, `macros.json`, `endpoint_map.json`, `endpoint_manifest.json`, `pids/`, `state/<pile>/replica.json`, `logs/<pile>.log`, `logs/<pile>.events.jsonl`.

### Why a shim

The demo hardcodes `grpc://<host>:2135` and takes hosts from `config fetch`. The mock serves every node on `127.0.0.1` with distinct ports, and `config fetch` returns labels (`mock-pile1-1`). The demo is started with `--ydb scripts/ydb_shim.py`. The shim maps a label to `127.0.0.1:<port>` and execs the real CLI. It **refuses every endpoint that is not a mock endpoint** (exit code 3), refuses profiles, and allows only `--help` without `-e`. So the CLI can never reach a real cluster through it. Use `cli.sh`, never `ydb` directly.

## Supervisor commands (`mock.sh cluster ...`)

| Command | Effect |
| --- | --- |
| `up [--fresh] [--piles a,b] [--nodes-per-pile N] [--topology F] [--set timings.X=V]` | start one process per pile |
| `down`, `status [--json]`, `state [pile]`, `endpoints` | lifecycle and views |
| `events [--pile] [--after-ts T] [--kind PREFIX] [--grep S] [--limit N]` | merged event log (survives a kill) |
| `now` | epoch time for `--after-ts` |
| `kill P`, `revive P`, `stop P`, `cont P` | SIGKILL, restart with the persisted replica, SIGSTOP, SIGCONT |
| `fault TARGET --op down\|unavailable\|silence\|delay\|storage\|connectivity\|self_check\|clear [--off] ...` | per node (`mock-pile1-2`) or per pile |
| `node-down L`, `node-up L` | close or reopen one node's listener |
| `partition "a\|b"`, `heal`, `keeper-split P` | cut peer links; the keeper loses a pile while the cluster is fine |
| `replica P [--generation G] [--states a=PRIMARY,b=...] [--detach] [--clear]` | override or detach one replica |
| `timings k=v ...`, `policies k=v ...` | change on every live instance |
| `reset` | revive and un-fault everything, install a fresh snapshot above every previously persisted generation |
| `manifest` | versioned node identities and direct SDK dial endpoints |
| `serve [--host 127.0.0.1] [--port 21999] [--allow-remote]` | foreground cluster HTTP API under `/api/v1` |

The supervisor serves cluster controls under `/api/v1`. Each pile separately serves `/state`, `/events`, `/control`, `/link`, `/replica`, `/topology`, `/followups`, `/timings`, `/policies`, `/failpoints`, `/response-override`, `/rpc-override`, `/reset-prepare`, `/reset-cancel`, `/reset-local`, `/scenario`, and internal `/peer/*` routes. Both APIs are documented in [README.ai.md](README.ai.md).

`reset` first pauses new proposals and follow-ups on every pile through `/reset-prepare`. It then reads the
persisted `max_generation` watermarks and installs the initial snapshot at their maximum plus one. A lower
`replica` override does not lower this watermark. Previously committed messages can finish normally: their
generations are below the reset snapshot. `/reset-cancel` resumes proposals after all snapshots are installed,
and also releases the pause if reset fails. If a pile cannot be resumed, reset reports the error; retry reset
once the pile is reachable. These two endpoints coordinate the supervisor; a direct `/reset-local` uses the
generation supplied by its caller. Run one `cluster reset` at a time; concurrent resets are not coordinated.

## Hazards and measured numbers (macOS arm64, CLI 2.33.0)

- A CLI call to a live mock instance takes 0.12-0.15 s. `--help` takes about 0.9 s.
- A CLI call to a **closed port** (`kill`, `node-down`) is refused in about 0.14 s.
- A CLI call to a **frozen process** (`stop`, SIGSTOP) hangs for 19-20 s (the CLI's discovery deadline is about 10 s and it retries). The demo's health data has a 10 s TTL, so a frozen pile makes the demo go stale and report "unresponsive" for that long. Prefer `kill` for "dead" and `stop` only to study this.
- `fault ... --op silence` makes calls hang until the client gives up (add `--methods GetClusterState` to hold only that method). The CLI sets no deadline on `bridge list`, so the demo's `bridge list` blocks until the fault clears. Always pass `--max SECONDS` or clear it.
- The demo waits 10 s at start and polls every second. After `kill` of a pile it sends the failover 1-3 s later (the survivor blames the dead pile after about 1 s of missed heartbeats, and the CLI is refused at once). After `stop` (frozen) expect 10-25 s, because every call hangs first.
- Every successful `cluster up`, with or without `--fresh`, mints a new `cluster_id`; a pinned Skipper Lab binding must treat down/up as a new cluster and use a new Skipper state directory. Restarting `cluster serve` preserves `cluster_id` and changes only `supervisor_id`.
- `cluster up --fresh` also deletes the event logs. A plain `cluster up` keeps them and continues their `seq`. Save `cluster events` output first if you need it.
- Every pile without the config-quorum trait (`DISCONNECTED`, `NOT_SYNCHRONIZED_1`, or `INACTIVE`) has closed gRPC listeners, as a real node does (`run.cpp`). Its control and peer API stay up: use `cluster state PILE` to see its replica. `--set policies.close_grpc_when_disconnected=false` turns this off.
- Two commits proposed at the same moment on different piles exclude each other (the proposer asks every pile that could be committing, not only the ones that must accept). A failover while the old pile is frozen (`stop`) waits one `peer_timeout` (0.8 s) for it.
- An admin request is finished by the instance even if the client's deadline expires first.
- The topology `host` must be a loopback address (`allow_remote: true` opts into remote binding for pile gRPC/control/peer listeners). The supervisor requires its own `cluster serve --allow-remote` for a remote `--host`. Both HTTP APIs are unauthenticated.
- An unreadable `replica.json` stops the instance with an error instead of starting from generation 1. Delete the file (or use `cluster up --fresh`) to start over.
- Public operation timeouts return TIMEOUT envelopes; client deadlines return gRPC DEADLINE_EXCEEDED. Accepted mutations can still complete after either timeout.
- Anti-entropy normally refuses snapshots that leave the local pile DISCONNECTED or INACTIVE. It still pulls when the registry or `configured_piles` differ, to propagate deployed-config changes. An ordinary revived DISCONNECTED pile with the same configuration stays stale until `rejoin`.
- TLS and mTLS use `tls: {cert_file: ..., key_file: ..., root_cert_file: ..., require_client_auth: true}`. Optional `policies.admin_certificate_cn` models an explicit administrator CN allowlist; it does not model YDB SID/ACL mapping. Direct gRPC certificate tests cover trusted and untrusted peers.
- The demo supports exactly 2 piles (it exits with code 2 on three). The mock itself serves N piles.

## Health and replication policies

All three policies below default to `true`. Configure them under `policies` in the topology or with `cluster policies key=value`; per-pile `/policies` changes only that process.

| Policy | Default behavior | Effect of `false` |
| --- | --- | --- |
| `auto_blame_dead_piles` | Emit RED BRIDGE_GROUP issues for unreachable peer piles whose local stored state is SYNCHRONIZED | Only explicit storage/connectivity fault injections produce pile issues |
| `enforce_config_quorum` | Require every pile with the config-quorum trait in the proposed state to prepare | Bypass that simulated commit requirement; for deliberately unrealistic lab scenarios |
| `anti_entropy` | Periodically pull newer snapshots from peers, subject to state/configuration rules above | Disable periodic pulling; proposal replication still occurs |

SelfCheck evaluates only SYNCHRONIZED pile copies, including PRIMARY/PROMOTED. A failed NOT_SYNCHRONIZED_2 pile produces no BRIDGE_GROUP issue even though it still blocks the data quorum. Manual storage/connectivity blame follows the same state filter. A representative group is ORANGE (`MAINTENANCE_REQUIRED`) when some synchronized copies fail, RED (`EMERGENCY`) only when all fail, and GREEN (`GOOD`) otherwise. `issue_style: realistic` adds the parent STORAGE_GROUP issue; the default `minimal` style keeps the child issues and the same aggregate statuses.

SelfCheck always returns synthetic verbose coverage. It ignores diagnostic request settings for minimum status, maximum level, verbose output, merging, and caching. It does not collect tablets/whiteboards or generate BSC-unavailable, missing-data, or HEALTHCHECK_STATUS truncation messages itself. Use response overrides for captured real-YDB fixtures; mock health alone cannot certify production failover.

An exact empty pile name is supported: create it with `cluster up --topology FILE` and YAML `piles: [named, ""]`, then select it with quoted CLI arguments such as `state ""` or JSON `{"pile":""}` / `{"piles":[""]}`. It differs from an omitted selector. The `cluster up --piles` CSV shorthand rejects empty segments, including leading/trailing commas and repeated commas, before changing the run directory or starting processes. In a `partition` spec the empty name is a whole empty group, e.g. `partition "|named"`; with an empty-named pile present, blank tokens inside a group (`"named, |other"`) are rejected as ambiguous. This covers empty names on the public wire and UI; real conversion's ImplicitPileId and hosts with an omitted `bridge_pile_name` still require real-YDB fixtures. The unmodified demo does not support an empty name.
