# Operator runbook

Skipper observes one cluster and offers reviewed failover, switchover, take
offline and rejoin through its web UI. A fresh state directory starts Observe;
Manual enables one reviewed handoff per approval; Auto sends policy-approved
failover and guarded retries. Use exactly one active controller and exclusive
administration. Observe stops new dispatch, including retries, when acknowledged;
it does not cancel remote work or provide a server lease.

## Prepare and launch

Pin the required server/SDK/CLI/Skipper builds and complete the
[README release checklist](../README.md#required-builds-and-release-checklist)
before production Auto. Older RPC nodes can silently ignore the generation
guard. There is no runtime capability probe or numeric released minimum.

Copy [config/example.yaml](../config/example.yaml); its comments state the
timing constraints and the derived client deadlines (operation timeout + 2 s
for SelfCheck and mutations; `read_timeout_seconds` is both the operation and
client deadline of `GetClusterState`). Endpoints must be unique,
have one common explicit `grpc://` or `grpcs://` scheme and a port, and be
keyed by the pile name returned by their SelfCheck. For a converted cluster use
its resolved implicit pile name. Each endpoint must address exactly one YDB
node: identity is verified only from that node's SelfCheck reply, so a balancer
or a multi-node DNS name breaks attribution. Every SelfCheck rechecks the
endpoint's pile; a mismatch excludes the endpoint's evidence and its use for
mutations until a matching reply. A
pile without configured endpoints remains visible and may still be assessed
from other piles' evidence. Configuration, CA, certificate and key are read
once; any change to them requires a restart. The token file is re-read as
described below.

Independently verify that all configured nodes belong to the intended cluster.
`VERIFIED` means the SelfCheck pile matches its configured name; it is not a
remote cluster identity check. `cluster_id` is a configured local label binding
the state directory to this configuration, not an identifier read from YDB.
Fresh views can influence selection and the durable high-water mark before
SelfCheck verifies the pile. A wrong-cluster endpoint with matching pile names
can also pass that check; correcting the endpoint does not lower a persisted
high-water mark. Trusted endpoint inventory is a deployment requirement.

The YAML parser rejects unknown/duplicate keys and files above 1 MiB. Credential
files are bounded to 16 MiB each, read at startup (the token file again on every
re-read) and never exposed as contents
in the snapshot. Certificate and key must be paired. CA, certificate and key
require `grpcs://` endpoints. A token over plaintext
`grpc://` logs a `plaintext_token` warning at startup; use TLS for deployment.
Verify certificate
mapping to an administrator SID while healthy: getting a new password/login
token can depend on SchemeShard, which may be unavailable during a stall.
Rotate a token by replacing the file in place: Skipper re-reads it before each
mutation and after any UNAUTHORIZED reply and logs `token_reloaded` once per
change; a failed re-read keeps the previous token.
If a mutation has already returned `UNAUTHORIZED`, Auto remains stopped at that
generation after rotation. Fix the credential problem and restart, or observe a
newer generation; the persisted pending overlay still constrains any resend.
The service account must protect its configuration and credential files.

Use local durable storage supporting `flock`, atomic rename and file/directory
fsync. Startup creates the state directory's final component (mode 0700) if
its parent exists; keep the directory private to the service account, Skipper
does not check its owner or mode. `skipper.lock` excludes another process using
that directory only. A corrupt state file (including a pending generation above
`generation_high_water`), lock failure or mismatched `cluster_id` fails
startup; preserve the files for diagnosis. The `startup_failed` record on stderr
carries the exact reason.

```text
bridge_skipper --config /etc/skipper/config.yaml --check-config
bridge_skipper --config /etc/skipper/config.yaml --state-dir /var/lib/skipper/cluster-a
```

`--check-config` validates configuration and bounded credential-file reads. It
opens no listener, contacts no YDB endpoint and ignores `--state-dir` if given.
It does not establish authorization or deployment readiness. Errors are
reported as [`startup_failed`](monitoring.md#structured-stderr-logs) records.
Start under a service manager and capture stderr and `--version`. Skipper binds
its HTTP listener before the controller starts, so a bind failure exits before
any YDB call. The listener has no login; keep it on loopback or behind a
network boundary. HTTP Host checks and framing denial protect the browser path;
they do not authenticate clients that can already reach the listener.

Every route checks the Host authority before serving state or admitting commands.
The configured concrete listen host and the loopback aliases `localhost`,
`127.0.0.1` and `[::1]` are accepted on the listen port. A wildcard listen host
(`0.0.0.0` or `[::]`) also accepts numeric IP authorities on that port; it does
not trust arbitrary DNS names. The port may be omitted for listen ports 80 or
443. Add exact additional browser authorities under `http.trusted_authorities`,
for example `[skipper.example, 'skipper.example:8443']`. Names are case-insensitive,
IPv6 literals require brackets and numeric addresses are normalized. Explicit
ports are normalized as decimal integers; omitted ports remain distinct from
explicit ports for configured additions. No DNS lookup is made for these checks.

A reverse proxy must preserve an approved public Host, adding that authority to
`trusted_authorities` when it differs from the defaults. HTTPS termination is
supported: an Origin, when present, must use HTTP or HTTPS and match the request's
approved host and effective port. Missing Origin is accepted for CLI clients;
null, duplicate or malformed Origin and duplicate Host are rejected. Forwarded
headers do not establish trust. Every response sends `X-Frame-Options: DENY` and
`Content-Security-Policy: frame-ancestors 'none'`.

The default [failover timing budget](../README.md#failover-timing) uses an
8-second evidence TTL and 2-second confirmation. Explicit settings retain their
values; an old configuration with TTL 30, confirmation 20 and 10-second rounds
continues to use that slower profile. Review timing overrides when upgrading.
Partial overrides with longer RPC budgets or rounds may require an explicit
larger TTL to satisfy validation; larger values also extend failover latency.

## Observe, Manual and Auto

Open `/` or read `/api/state`. Confirm the cluster ID, acknowledged mode,
controller readiness, state/evidence freshness and endpoint identity. A working
HTTP connection does not prove healthy storage. The page retains last observed
roles and unresolved incidents through expired evidence; requested roles appear
only in a plan or pending summary until observed.

Enable Manual for reviewed commands. Every cluster mutation opens a concrete
review showing expected generation, complete exact overlay, before/after states,
actual endpoint, reasons, pending interaction and alternate synchronized-pile
availability. All failed piles are disconnected in one failover request;
abandoned PROMOTED roles are reset explicitly. Unlisted piles retain their exact
internal state. Failover preserves zero data loss; verify application
availability after the observed transition.

The plan response supplies `alternate_synchronized_pile_names` and the matching
`alternate_synchronized_piles` count. These exclude the effective primary after
the action. For switchover that is the proposed target, so the old primary can
remain an alternate. Other actions exclude the PRIMARY in the after vector.
A two-pile takedown leaves no alternate; rejoining a pile does not count it as
synchronized before YDB observes that transition.

A review expires after 120 seconds. Execute freshly evaluates the inputs in the
controller turn, consumes the plan even on refusal, persists pending and then
hands off. `202` acknowledges handoff only. Supporting evidence must meet TTL
rules at evaluation and immediately before SDK launch. The actor rechecks after
persistence, and the SDK adapter rechecks after token reload. Local expiry
returns `409 EVIDENCE_EXPIRED`, keeps pending and records no invocation. A pause
after the final launch check or network delay can still outlast the evidence TTL.
After `409`, read current state and review again. A timeout or
`504 COMMAND_OUTCOME_UNKNOWN` never means cancelled and must never trigger an
automatic repost.

Auto activation has a review showing current readiness and any plan that could
send immediately. It performs failover only. Auto cannot activate over pending
lifecycle work. Turning the active mode off is immediate and needs no review;
both switches off means Observe. A changed mode is shared by all browsers.

Mode lives in `state.json`, not YAML. Restart restores it. Restored Auto may send
one guarded identical failover or compatible superset when Ready. Before a
restart that must not dispatch, acknowledge Observe and confirm its save. If
saving Off fails, Observe still applies in memory and all dispatch is blocked;
a restart before successful persistence resumes the file's mode, possibly the
previous one. The UI displays this warning and persistence is retried.

SIGINT/SIGTERM closes command admission and stops new sends before draining
the SDK driver while the actor system remains alive for callbacks. Outcomes
processed during the drain may be persisted; any remaining pending record is
reconciled after restart. A normal stop exits with status 0. A stop acknowledgement
failure emits `shutdown_failed` and exits with status 1. Shutdown cannot cancel
work already handed off or establish that a remote request cannot commit.

## Command API

POST bodies are JSON, limited to 64 KiB. There is no login or browser auth setup;
anyone reaching the configured host/port can administer it. No CORS headers are
emitted. GET/HEAD are read-only and every response uses `Cache-Control: no-store`.
Generations and revisions are decimal strings; do not convert them through a
JavaScript floating-point number.

| Route | Contract |
| --- | --- |
| `GET /api/state` | Schema 3 canonical controller, eligibility, evidence and pending projection. |
| `POST /api/mode` | `{mode, expected_boot_id, expected_mode_revision, request_id}`; Auto also supplies `reviewed_snapshot_revision`. |
| `POST /api/plans` | `{action, snapshot_revision, target_pile?}`; action is `failover`, `switchover`, `take_offline` or `rejoin`. |
| `POST /api/operations` | `{plan_id, request_id}`; `202 {operation_id}` means one handoff. |
| `GET /api/operations/{id}` | Current-boot attempt status. Missing/evicted records do not prove cancellation or that nothing was sent. |
| `GET /api/history?after=…` | Typed bounded history with coverage-gap metadata. |
| `GET /api/logs?after=…` | Retained redacted logs with filters and coverage metadata. |
| `GET /api/logs/export` | Retained-log export with the same filters and UTC/cursor coverage metadata. |

Use a fresh unique `request_id` for an intentional mode or execute command. A
retained identical receipt replays the response; the same ID with a different
body conflicts. After eviction, consumed plans and mode revisions still prevent
reuse. Old boot tokens fail after restart. A POST reply triggers a fresh state
read; it is not a replacement for canonical mode/pending state.

`503 CONTROLLER_UNAVAILABLE` means local admission/nondelivery failed.
`504 COMMAND_OUTCOME_UNKNOWN` means no reply arrived within 15 seconds; the
controller may still process the command. Reconcile state and operation status.
Unrecovered persistence failures return `503 STATE_WRITE_FAILED` with no SDK
handoff. A mode request acknowledges success after an immediate retry only when
the requested mode is durably saved; restoring the previous mode keeps the
request failed.

## Planned lifecycle and membership operations

Use the corresponding Manual UI review when available. Lifecycle requires
fresh coherent observations, known participation and Healthy/Replicating
participants; it is disabled during failure confirmation, unresolved participation,
a Lost SUSPENDED blocker or a competing Ready failover. Switchover additionally
requires a Healthy SYNCHRONIZED target. Take offline submits SUSPENDED for a
non-primary SYNCHRONIZED target, excluding that pile's endpoints for dispatch.
Rejoin submits NOT_SYNCHRONIZED only from DISCONNECTED. A pile named by a pending
Add/Remove cannot be targeted.

For external CLI administration switch Skipper to Observe or stop it, using a
fresh coherent generation and an endpoint in a pile that stays in config quorum. The named `switchover`,
`failover`, `takedown` and `rejoin` commands currently do **not** support
`--expected-generation`; use hidden `update` for guarded overlays. Examples
assume dc-a is PRIMARY, dc-b SYNCHRONIZED and there is no other PROMOTED pile.
Replace each illustrative generation from a newly reviewed state.

| Goal | Guarded overlay and completion |
| --- | --- |
| Switchover to dc-b | `update --set 'dc-b:PROMOTED' --expected-generation '42'`; wait for dc-b PRIMARY and dc-a SYNCHRONIZED. Explicitly reset any abandoned PROMOTED role in the same overlay. |
| Takedown dc-a after that switchover | From a new view, `update --set 'dc-a:SUSPENDED' --expected-generation '44'`; wait for DISCONNECTED. A primary cannot be suspended in the switchover request. |
| Rejoin repaired dc-a | From a new view showing DISCONNECTED, `update --set 'dc-a:NOT_SYNCHRONIZED' --expected-generation '46'`; wait for SYNCHRONIZED. |
| Emergency failover | Use the complete reviewed `decision.cli_command` above, including all failed piles and explicit role resets. |

Prefix the table's `update` commands with, for example,
`ydb -e 'grpcs://ydb-b1.example:2135' admin cluster bridge` and your own auth.
Unlisted piles retain their exact internal state. Never repeatedly issue rejoin
as a status refresh: explicit NOT_SYNCHRONIZED can reset stage 2 to stage 1 and
restart resynchronization. No public resync percentage or ETA exists. A lost
SUSPENDED pile yields `SUSPENDED_PILE_LOST`; public update cannot disconnect it,
so restore its quorum or follow a separate server recovery procedure.

When BSC is unavailable and outside connectivity stays binding, Auto can
confirm failure of the BSC host (PROMOTED, otherwise PRIMARY) from fresh
agreeing fallback RED static parts with complete VDisk reasons, provided
the host has no own node-quorum claim. A separate storage-failure proof
permits a live own quorum only when every current outside fallback reply names
a RED static group whose complete unique reason graph contains only RED
VDisks caused by an explicitly reported same-node, same-pile PDisk
`DeviceIoError`. A hardware error mixed with transport loss or replication
does not suffice. GREY, missing and truncated data remain insufficient.
An I/O hang without an explicit device error remains unproven.
After a role change this evidence
starts only after BSC warm-up, then spans `confirm_seconds` within one
generation. Switching between the generic fallback rule and the storage proof
restarts confirmation; earlier samples cannot confirm the new proof.
A replacement needs own node quorum and an own fresh
non-truncated fallback reply without any attributed fallback RED/GREY veto;
it has no separate candidate confirmation timer. A `NO_CANDIDATE` decision
can therefore persist while connectivity works but own SelfCheck is missing.
These observations do not fence an isolated primary or prove full disk health;
keep exactly one active controller and qualify the policy for the server build.

Membership and conversion use separate existing RPCs. Their CLI commands have
no `expected_generation` guard. Keep exclusive operator control and Skipper
Observe/stopped; the mode is not a server fence:

```text
ydb -e grpcs://ydb-b1.example:2135 admin cluster bridge add-pile --pile dc-c
ydb -e grpcs://ydb-b1.example:2135 admin cluster bridge remove-pile --pile dc-c
ydb -e grpcs://ydb-a1.example:2135 admin cluster bridge convert
```

Provision configuration/hosts/storage externally before Add. The target must
be INACTIVE; Add finishes DISCONNECTED and rejoin is separate. Remove requires
DISCONNECTED and **deletes the pile's data**; verify the exact target and
retention decision before invoking it. Remove configuration/hosts only after
completion at INACTIVE. One Add/Remove may be pending at a time; neither starts
during conversion. Add/Remove errors can leave YDB retrying background work:
inspect `view.pending_operation`, its error and fresh state before acting again.

Convert requires compatible single-pile bridge configuration already deployed.
Its return does not finish BSC conversion; wait for
`view.conversion_in_progress: false`. The optional
`convert --skip-storage-pool-conversion` requires
`bridge_config.enable_non_uniform_storage: true` and leaves existing dynamic
storage pools and their groups non-bridged. Static storage groups are still
converted to bridge mode; prepare that topology explicitly. Skipper
does not call Add, Remove or Convert, and exposes no synthetic prebridge view.

## Pending mutations and state overrides

The directory contains `skipper.lock` and `state.json`. The latter is atomically
replaced: `state.json.tmp` is written and fsynced, renamed over it, and the
directory is fsynced:

```json
{"state_schema":3,"cluster_id":"production-bridge-01","mode":"observe","generation_high_water":"42","pending_mutation":{"expected_generation":"42","update":{"dc-a":"DISCONNECTED","dc-b":"PRIMARY"},"kind":"failover","origin":"manual"}}
```

`--version` advertises `state_schema=3`; `/api/state` also uses schema 3.
Unversioned and other state formats fail startup explicitly. No migration or
automatic state deletion is provided; preserve incompatible files for diagnosis.

Selecting a coherent view at G raises and persists `generation_high_water` to
at least G, even when Skipper sends no mutation. Success or ABORTED at G raises
it to at least G+1. After restart, a view below the persisted floor is rejected
as `STALE_HISTORY`, including when the controller had only observed the newer
generation. After an ABORTED outcome, `STALE_HISTORY` clears on the next fresh
view at or above the raised mark.

If ABORTED came from a lagging distconf root while the actual coherent history
remains at G, this conservative floor can block both Auto and generated Manual
plans indefinitely. Restart and `--clear-pending-mutation` do not lower it.
Check the actual generation and history directly through independently trusted
cluster endpoints and investigate the lagging component before incident
recovery. ABORTED alone is not proof that G+1 exists. Do not lower the floor or
remove the generation guard to force an old plan; state overrides retain the
recovery constraints described below.

A pending record is persisted before sending and survives a crash even if the
RPC was never sent. A failover may be replaced only by a Ready failover superset
preserving every pending entry and the common primary, adding only DISCONNECTED
targets. Pending lifecycle admits only its identical overlay after new Manual
review and settlement of previous local attempts. Auto never retries lifecycle.
Manual performs no automatic retry/backoff; every resend needs another review.
All updates remain guarded at their decision generation.

| Outcome | Controller behavior |
| --- | --- |
| Local `EVIDENCE_EXPIRED` (no RPC) | Preparation outlasted the supporting evidence. Keep pending and wait for fresh evidence; no invocation or `mutation_sent` is recorded. No restart is required. |
| `SUCCESS` / `SUCCESS` outcome | Raise high-water to at least G+1 and clear pending. Success does not establish unique invocation identity. |
| `ABORTED` / `GENERATION_MOVED` | Raise high-water to at least G+1 and clear pending. Views still at G are stale. distconf also answers ABORTED from a root that lags behind G; the controller then reports `STALE_HISTORY` at G until a fresh view at G+1 or above arrives, which is fail-safe. |
| Server `UNAVAILABLE` or `PRECONDITION_FAILED` / `NOT_APPLIED_HERE` | Keep pending. Auto may resend the identical failover update at once to another eligible endpoint; Manual needs a new review after local settlement and never resends automatically. Every endpoint that answered this way stays excluded (exclusions accumulate) until the next state poll that follows an evaluation with no selectable endpoint (`NO_MUTATION_ENDPOINT`); then all are eligible again, so Auto resends are bounded to about one per endpoint per state poll. Earlier invocations may remain in flight. |
| `BAD_REQUEST` or `UNAUTHORIZED` / `REJECTED` | Keep pending: only this invocation was refused before distconf; earlier ambiguous or superseded attempts may still commit. Rejected failover latches all sends at G; rejected lifecycle latches lifecycle sends only, and the retained lifecycle record blocks incompatible failover work. The latch is process-local and resolves at a fresh coherent newer generation. |
| `INTERNAL_ERROR`, timeout, transport/client errors or other status / `AMBIGUOUS` | Keep pending. Auto may resend an identical failover no earlier than the mutation client deadline (`mutation_operation_timeout_seconds` + 2 s) after the last send, doubling after each ambiguous outcome up to 5 minutes; `last_mutation.ambiguous_resends` counts them. Manual never retries automatically or applies this backoff; another handoff needs local settlement and a new review. |

A coherent selected view above pending G clears it automatically. After restart,
Auto permits one immediate identical failover resend, subject to a current valid
Ready plan; restored Manual still needs a new review. A failed
state write blocks mutations with `STATE_WRITE_FAILED`; observation continues.
If the directory fsync after the rename fails, the renamed file and memory
already hold the new record; the next successful write makes it durable.

If a pending plan can never commit because its intended survivors lack quorum,
first stop the old process, prevent restart and reconcile every possible delayed
request and both sides of the partition. Record why replacing that plan at G
cannot create competing histories. Then start with:

```text
bridge_skipper --config /etc/skipper/config.yaml --state-dir /var/lib/skipper/cluster-a --clear-pending-mutation
```

Confirm `mode: "observe"` in `GET /api/state` before reviewing another mutation.
This logged startup override atomically stores Observe while clearing pending
and **preserves the high-water mark**. It does not cancel remote
work or fence an old controller.
Do not use it merely because a request timed out or a different plan is desired.
Inspect fresh coherent state before any further mutation.

Deleting `state.json` is a separate, more dangerous override: the next start
creates fresh state and forgets **both** pending uncertainty and the generation
high-water mark. It can allow failover from the older side of a partition and
create split brain. Treat it only as an explicit split-brain recovery decision,
after choosing the surviving history and ensuring the losing side and all old
controllers cannot act. Preserve the original file and incident evidence.
Do not edit generations or delete state to clear an ordinary blocker.

## Backup and restore

Stop Skipper, prevent restart and preserve the state directory,
config and exact build IDs together. Keep credentials under their own secret
policy. This is controller state backup, not YDB data backup. A stale backup
forgets later high-water advancement and pending mutations; reconcile all
activity since it was taken before dispatch. Never run original and restored
copies concurrently. Keep restored operation manual until history and exclusive
control have been established.
