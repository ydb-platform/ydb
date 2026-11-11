# bridge_skipper controller facts (developer reference)

Checked at `skipper_initial_wip` HEAD `c33440067d60ba18b814dd92df9f1be37ed50df4` (2026-11-05). Paths are
relative to `ydb/apps/bridge_skipper/`. Re-verify line numbers before citing; they drift.

## Layout and targets

| Directory | Contents | Tests |
| --- | --- | --- |
| `main.cpp`, `ya.make` | `PROGRAM(bridge_skipper)`; startup, signal handling, actor system, HTTP listener | — |
| `config/` | YAML config parsing and validation (`config.cpp`), `example.yaml` | `config/ut` (`UNITTEST`, `config_ut.cpp`) |
| `domain/` | Pure decision code: `types.*` (enums, cluster view, state record), `health.*` (SelfCheck classification, pile verdicts), `policy.*` (`Decide`, `BuildFailoverUpdate`, `PlanLifecycle`, confirmations), `mutation.*` (outcomes, resend, endpoint selection), `view.*` (view selection, transition explanation) | `domain/ut` (`UNITTEST_FOR`, `policy_ut.cpp`, `health_ut.cpp`, `admin_ut.cpp`; `fixtures/*.pbtxt` synthetic SelfCheck replies pinned to server `7596b0fcb34`) |
| `runtime/` | `skipper.cpp` (actor: scheduling, evaluation, persistence, dispatch, HTTP commands), `ydb_clients.cpp` (SDK calls, deadlines, token reload, outcome classification), `state_file.cpp` (`state.json`, lock), `diagnostics.cpp` (history/log stores, `boot_id`), `events.h`, `commands.h` | `runtime/ut` (`UNITTEST`, `skipper_ut.cpp`, `ydb_clients_ut.cpp`, `state_file_ut.cpp`, `diagnostics_ut.cpp`) |
| `http/` | `actor.cpp` (routes, Host/Origin guard, command parsing), `state_json.cpp` (`/api/state` schema 3), `page.cpp` (HTML UI), `metrics.cpp` (Prometheus) | `http/ut` (`UNITTEST_FOR`, `http_ut.cpp`, `commands_ut.cpp`) |
| `docs/` | `operations.md` (runbook, command API, recovery constraints), `monitoring.md` (state schema, metrics, alerts, log events), `alerts.yaml` (10 example rules) | — |
| `ydbd_mock/` | Python mock of Bridge, Monitoring, Discovery `ListEndpoints` and Config `FetchConfig` with per-pile control HTTP, fault injection and a cluster lab (`scripts/lab.sh`, `supervisor.py`) | `ydbd_mock/tests` (`PY3TEST`, 24 files) |
| `acceptance/` | Python harness driving the real binary against the mock (`lib/harness.py`, `tests/test_e2e.py`, `test_standalone.py`, `test_ports.py`); browser test target is blocked until resource IDs are registered | `acceptance/tests` (`PY3TEST`, `DEPENDS(ydbd_mock, bridge_skipper)`) |
| `README.md` | Safety summary, required server/SDK commits, release checklist (10 recording cases, M1–M4, all open) | — |
| `IMPLEMENTATION-index.md` | Source map, actors/threads, observation and policy invariants, validation map, web-administration contracts | — |

Build and test through the personal `yamake` wrapper per the `ydb-build-test` skill, e.g. the unit
targets `ydb/apps/bridge_skipper/{config,domain,runtime,http}/ut` and `ydb/apps/bridge_skipper`.
Mock lab: `YDBD_MOCK_IMPL=ya ydb/apps/bridge_skipper/ydbd_mock/scripts/lab.sh --fresh` (2 piles × 3 hosts,
skipper on `:8080`, control UI on `:21999`). Mock and acceptance passes are not qualification.

## CLI and startup (`config/config.cpp:339-392`, `main.cpp`)

- Flags: `--config FILE` (required), `--state-dir DIR` (required to run, ignored by `--check-config`),
  `--check-config`, `--clear-pending-mutation`, `--version`, `--help`. No flags for mode, HTTP port or
  log level: mode lives in `state.json`, listen address and level in YAML.
- `--clear-pending-mutation` drops the pending record, keeps `cluster_id` and `generation_high_water`,
  and **forces mode Observe** (`runtime/state_file.cpp:292-297`).
- Startup: parse args → load config and credentials → `TStateFile::Open` (flock `skipper.lock`,
  load/create `state.json`, check `cluster_id`) → actor system (pool "skipper" + IO pool
  "skipper-controller-io") → SDK clients → HTTP listener → Skipper actor ready handshake. Startup
  failures print a raw `startup_failed` JSON line with category `arguments|config|credentials|state|runtime`.

## Config (`config/config.h`, `config.cpp`)

- Keys (strict allowlist, unknown/duplicate rejected): `cluster_id` (required), `endpoints` (pile →
  nonempty list of `grpc://`/`grpcs://host:port`; no mixed schemes, no duplicate host:port),
  `auth.{ca_file,cert_file,key_file,token_file}` (cert+key together; TLS files need `grpcs`; files
  ≤16 MiB; token single line, re-read before every Update and after UNAUTHORIZED; CA/cert/key read
  once, change needs restart), `http.listen` (default `127.0.0.1:8080`), `http.trusted_authorities`,
  `timing.*_seconds`, `health_endpoints_per_pile` (default 2, no upper bound), `logging.level`.
- Timing defaults (`config.h:48-61`): `state_poll_interval` 1, `read_timeout` 1 (GetClusterState op =
  client deadline), `round_interval` 1, `selfcheck_operation_timeout` 4 (client deadline op+2),
  `evidence_ttl` 8, `confirm` 2, `bsc_warmup` 30, `mutation_operation_timeout` 25 (client op+2).
- Validation invariants (`config.cpp:158-166`): `state_poll + read_timeout < evidence_ttl`;
  `selfcheck_op + 2 + round_interval < evidence_ttl`; `bsc_warmup >= 20`. Nothing ties `confirm` or
  the mutation timeouts to the TTL. The SelfCheck half-budget rule (BSC gets half the operation
  timeout) is documented in the configuration example. The default budget targets
  a committed steady-state failover within 20 s with usable failure evidence and
  a surviving commit quorum; role warm-up and safety blockers can take longer.

## Controller loop (`runtime/skipper.cpp`)

- 250 ms tick. Plain `GetClusterState` on every endpoint each `state_poll_interval`; health rounds
  every `round_interval` sample `health_endpoints_per_pile` endpoints per pile (SelfCheck plus the
  connectivity variant of GetClusterState) with rotation. One outstanding call per (endpoint,
  method); a busy slot defers the sample. Slots are released only by the completion event (SDK
  deadline); no actor watchdog.
- `Evaluate`: `SelectView` → persist raised high-water mark / clear resolved pending →
  `EvaluatePiles` → `Confirmations.Update` → `Decide` → `STATE_WRITE_FAILED` if Dirty →
  `REJECTED_BY_SERVER` latch → Auto `MaybeSend` → `decision` log on transition.
- `state.json` is rewritten on every observed generation increase (HWM), before each send, on
  Success/GenerationMoved, on mode change; any write failure blocks all dispatch until a retry succeeds.

## Modes and HTTP API (`http/actor.cpp`, `runtime/skipper.cpp:777-1062`)

- `EMode {Observe, Manual, Auto}`; fresh state starts in Observe. Auto dispatches only a Ready
  failover. Manual dispatches only reviewed plans (`failover`, `switchover`, `take_offline`,
  `rejoin`). Add/Remove/Convert are never called.
- Routes: `GET /`, `GET /api/state` (schema 3), `GET /metrics`, `GET /api/history?after=`,
  `GET /api/logs?after=&level=&search=&pile=`, `GET /api/logs/export` (NDJSON),
  `GET /api/operations/{id}`, `POST /api/mode`, `POST /api/plans`, `POST /api/operations`.
  All requests pass a Host/Origin guard (403 `HOST_NOT_ALLOWED`/`ORIGIN_NOT_ALLOWED`); bodies ≤64 KiB
  JSON (413 `BODY_TOO_LARGE`, 400 `JSON_REQUIRED`, 400 `INVALID_COMMAND`).
- `POST /api/mode {mode, request_id, expected_boot_id, expected_mode_revision[, reviewed_snapshot_revision for auto]}`:
  409 `MODE_REVISION_CHANGED` (a matching request consumes the revision even when refused),
  409 `SNAPSHOT_CHANGED`, 409 `PENDING_MUTATION` (Auto over pending lifecycle work), 503 `STATE_WRITE_FAILED`.
- `POST /api/plans {action, snapshot_revision, target_pile}`: returns plan id `<boot>:plan:N`
  (120 s TTL, ≤256 retained), before/after vectors, `cli_command`, evidence; 400 `TARGET_REQUIRED`,
  409 `SNAPSHOT_CHANGED|MODE_NOT_MANUAL|ATTEMPT_IN_FLIGHT|REJECTED_AT_GENERATION|PENDING_MUTATION|PLAN_CHANGED`.
  No receipt is kept for plan reviews.
- `POST /api/operations {request_id, plan_id}`: 202 `{operation_id}`; the plan is consumed even on
  refusal; 409 `PLAN_UNKNOWN|PLAN_CONSUMED|PLAN_CHANGED|EVIDENCE_EXPIRED|...`. Mode and execute
  commands keep receipts by `request_id` (identical replay returns the stored result, different body
  → 409 `REQUEST_ID_CONFLICT`). Delivery failures: 503 `CONTROLLER_UNAVAILABLE`, 504 `COMMAND_OUTCOME_UNKNOWN` (15 s).
- `snapshot_revision` is `<boot>:snapshot:N` and changes whenever the semantic signature (mode,
  decision, plan, dirty, inflight, latches, pending, view, actions, health, participation) changes.

## State file (`runtime/state_file.cpp`)

- Directory 0700; `skipper.lock` with `flock(LOCK_EX|LOCK_NB)` (local exclusion only).
- `state.json` ≤1 MiB, `state_schema` must be 3, fields exactly `state_schema, mode, cluster_id,
  generation_high_water, pending_mutation` (null or `{expected_generation, update{pile:STATE}, kind
  failover|switchover|take_offline|rejoin, origin manual|auto}`); pending generation ≤ HWM; file
  `cluster_id` must match config. Writes go to `state.json.tmp`, fsync, rename, directory fsync; a
  directory-fsync failure adopts the state in memory and reports "persistence uncertain".

## Domain invariants (`domain/`)

- Public states `EPileState {Unspecified, PRIMARY, PROMOTED, SYNCHRONIZED, NOT_SYNCHRONIZED, SUSPENDED,
  DISCONNECTED, INACTIVE}`; unknown wire values fail the whole reply. A valid view has exactly one
  PRIMARY and at most one PROMOTED (`types.cpp:46-61`).
- `EPileVerdict {Healthy, Failed, Lost, Replicating, Unknown}`, default Unknown; Down = Failed or Lost;
  Replicating is never Down. Precedence in `EvaluatePiles` (`health.cpp:366-389`): participation
  unknown → Unknown; Failed (every fresh BSC-mode reply attributes a RED part); Lost (settled outside
  reports all show config quorum without node quorum, no inside self-quorum); Replicating; Healthy
  (continuous clean BSC window ≥ `bsc_warmup`); else Unknown.
- `EPart {RedPart, ReplicatingPart, FallbackRed, Incomplete}`; merge rank Red > Incomplete >
  Replicating. Fallback marker is the exact RED `STORAGE` issue "System tablet BSC didn't provide
  information". In fallback replies group parts become `FallbackRed` (RED) or `Incomplete` (GREY);
  both set the pile's `FallbackRed` flag, which only vetoes a no-BSC candidate and is displayed. A
  fallback reply never makes a pile Failed or Healthy.
- Participation (`health.cpp:185-205,296-313`): PRIMARY/PROMOTED/SYNCHRONIZED always;
  NOT_SYNCHRONIZED when any valid report shows `in_config_quorum` (a binding report counts only
  positively) or a fresh BSC part names it; unknown participation keeps the pile in `Participating`
  with verdict Unknown. `in_data_quorum` is decoded and displayed, never used. SUSPENDED gets a verdict
  but never participates. BSC host = PROMOTED else PRIMARY; a fresh fallback reply from it resets the
  clean windows.
- `TConfirmations`: all Down piles must confirm; the span is over evidence send times ≥ `confirm`;
  entries clear on a role change or on a pile-state change across generations (unchanged states keep
  their confirmation across a no-op generation bump; the plan always carries the new generation).
- Decision kinds `{Stable, Unknown, Confirming, Blocked, Ready}`. Blockers (`types.h:95-104`):
  `NO_VIEW`, `VIEW_CONFLICT`, `STALE_HISTORY` (generation < HWM), `PENDING_MUTATION`, `NO_CANDIDATE`,
  `SUSPENDED_PILE_LOST` (checked first), `NO_MUTATION_ENDPOINT`, `STATE_WRITE_FAILED`. Runtime-only
  reasons: `REJECTED_BY_SERVER` (kind stays Ready), `EVIDENCE_EXPIRED` (kind Unknown).
- Survivor: a non-Down primary stays primary whatever its verdict. Else PROMOTED first, then
  SYNCHRONIZED by name; eligible = Healthy, or (`!BscAvailable && SelfQuorum && !FallbackRed`) with no
  candidate confirmation window (`policy.cpp:224-250`).
- Failover update: every Down pile → DISCONNECTED; if the primary is Down, survivor → PRIMARY and any
  other PROMOTED → SYNCHRONIZED. Superset replacement at the same G keeps every pending entry and adds
  only DISCONNECTED targets.
- `PlanLifecycle` (`policy.cpp:290-370`): switchover needs a Healthy SYNCHRONIZED target (old PROMOTED
  → SYNCHRONIZED); take_offline only a SYNCHRONIZED non-primary → SUSPENDED; rejoin only DISCONNECTED →
  NOT_SYNCHRONIZED; all refused while failover is Ready/Confirming, a SUSPENDED pile is Lost, or the
  target has a pending Add/Remove or conversion.
- Outcome mapping (`ydb_clients.cpp:328-345`): SUCCESS → Success; ABORTED → GenerationMoved;
  UNAVAILABLE, PRECONDITION_FAILED → NotAppliedHere; BAD_REQUEST, UNAUTHORIZED → Rejected; anything
  else (INTERNAL_ERROR, TIMEOUT, transport, unknown) → Ambiguous. Success and GenerationMoved raise
  HWM to G+1 and clear pending; others keep pending. NotAppliedHere retries at once elsewhere;
  Ambiguous backs off `mutation_client_timeout · 2ⁿ` capped at 5 min; Rejected latches
  `RejectedGeneration` (process-local, cleared by a view > G).
- Mutation endpoint (`mutation.cpp:83-136`): Verified + Current endpoint whose own valid report shows
  node quorum for every required survivor (PRIMARY, PROMOTED/SYNCHRONIZED, NOT_SYNCHRONIZED with
  participation `true`, SUSPENDED); unknown-participation NOT_SYNCHRONIZED piles are not required
  (`mutation.cpp:107`, open decision). Rank by post-update state PRIMARY 0, synchronized 1, stage-2 2,
  SUSPENDED 3, tie by URI. Launch is authorized only until `ValidUntil` = min admitted view/health/
  connectivity send time + TTL, re-checked after persist and after token reload.

## Observability

- Metrics: `skipper_loop_age_seconds`, `skipper_cluster_state_age_seconds`,
  `skipper_pile_verdict{pile,verdict}`, `skipper_decision{kind}`, `skipper_blocked{reason}`,
  `skipper_mode{mode}`, `skipper_pending_mutation`, `skipper_mutation_total{outcome}`,
  `skipper_unexplained_state_change_total`, `skipper_rpc_total{method,status}`. No metric for endpoint
  identity MISMATCH (visible only in `/api/state` `endpoints[].identity`).
- Alerts (`docs/alerts.yaml`): SkipperScrapeUnavailable, SkipperLoopStalled, SkipperClusterStateStale,
  SkipperPileDown, SkipperPileUnknown, SkipperBlocked, SkipperPendingMutation, SkipperMutationRejected,
  SkipperMutationAmbiguous, SkipperUnexplainedStateChange.
- Log events (NActors JSON): `startup`, `plaintext_token`, `state_write_failed`, `pending_mutation`,
  `view_change` (Initial/Explained/Unexplained; Unexplained is WARN), `configured_endpoint_ignored`,
  `decision`, `mutation_sent`, `mutation_outcome`, `mutation_rejected`; raw stderr lines
  `startup_failed`, `pending_mutation_override`, `shutdown_failed`, `token_reloaded`. The redacted
  mutation error text is log-only, not in `/api/state`.
- `ExplainTransition` whitelist: promotion completing, NOT_SYNCHRONIZED → SYNCHRONIZED, SUSPENDED →
  DISCONNECTED, absent == INACTIVE, own pending/last update. Operator Add/Remove completion, CLI
  switchover and rejoin are unexplained and raise the counter and alert.
- History types `view, decision, operation_sent, operation_outcome, mode` (retention 1000 rows, 5000
  logs, 1000 operations).

## Known doc/code drift at this snapshot

- `monitoring.md` lists a "pending" history type the code never writes.
- Most command error codes above are undocumented in `operations.md`.
- `/api/state` has keys `monitoring.md` omits (`schema`, `started_at`, `evidence_ttl_ms`,
  `rejected_generation`, `lifecycle_rejected_generation`, `operation_message`, `failover_action`, `incident`).
- `--help` does not say that `--clear-pending-mutation` forces Observe.
