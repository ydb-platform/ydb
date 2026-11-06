# Operator runbook

Skipper performs failover only. Start in manual mode, inspect the read-only
snapshot and run reviewed commands with your own YDB administrator identity.
Use exactly one active controller and one exclusive administrator at a time.
Manual mode stops Skipper from sending mutations; it provides no server fencing
and does not cancel a request that was already sent.

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
network boundary.

## Manual failover

1. Read `GET /api/state`. Verify `mode: "manual"`, `cluster_id`, endpoint
   `identity`, fresh view and evidence ages, pile verdicts, confirmations
   and any pending mutation. Unknown or absent evidence is not health.
   `confirm_seconds` is measured over the send times of the evidence that
   supports the Down verdict, so `confirmed` can turn true (and Auto can send)
   less than `confirm_seconds` after `down_since` when RED evidence sent
   before that observation already spans part of the window.
2. When `decision.kind` is `READY`, review `decision.plan`: exact generation,
   endpoint and complete update. It disconnects all confirmed Down piles in
   one request and includes explicit role changes. Do not split the batch.
3. Copy `decision.cli_command`, add your own CLI authentication configuration
   and run it promptly. Preserve its exact update and `--expected-generation`.
   For example, a primary loss at generation 42 may produce:

   ```sh
   ydb -e 'grpcs://ydb-b1.example:2135' admin cluster bridge update --set 'dc-a:DISCONNECTED' --set 'dc-b:PRIMARY' --expected-generation '42'
   ```

   This hidden `update` command is supported by the required CLI build.
   Skipper does not include credential flags. If a pile name is empty or
   contains a colon, the rendered command uses JSON through `--file` instead
   of `--set`; preserve that command and exact name.
4. On `ABORTED`, obtain a new view and review a new plan. Never remove the guard
   to force an old plan. A timeout, transport error or `INTERNAL_ERROR` can mean
   the request is still able to commit at G; retain its exact update and inspect
   coherent state before issuing another command.
5. Observe the newer generation and resulting pile states. Success or a matching
   vector does not prove which invocation caused the change. Role completion
   alone does not establish application readiness; check the application too.

CLI invocations are external to Skipper: they do not create Skipper's persistent
pending record or change `last_mutation`. Keep an incident/change record of
the command and outcome. Skipper may report the resulting transition as
unexplained; investigate against that operator record.

## Auto, stopping and handover

After release validation and exclusive-control preparation, set `mode: auto`
in the configuration and restart with the same state directory. Verify the
published mode and current snapshot. Auto sends `READY` plans using the guarded
pending-record protocol below; the plan computation is identical in both modes.
No HTTP route changes mode.

To stop new dispatch immediately, stop the process and inhibit automatic restart
if needed. SIGINT/SIGTERM stops scheduling, drains the SDK driver while the actor
system remains alive for callbacks, then stops the actors. Outcomes processed
during the drain may be persisted; any remaining pending record is reconciled
after restart. A normal stop exits
with status 0. If the actor does not acknowledge the stop within 5 s, Skipper
writes a raw `shutdown_failed` JSON line (`category` `stop_ack`), still drains
and stops, and exits with status 1. Shutdown does not
undo accepted YDB work or establish that a remote request cannot commit.

Before planned external changes, set `mode: manual` and restart, or stop Skipper.
Inspect and reconcile any pending/in-flight mutation first. Keep exclusive
operator control through the whole procedure; manual mode is not a lease.
Afterwards inspect fresh state and restore Auto only by configuration/restart.

## Planned lifecycle and membership operations

Run these with Skipper manual or stopped, using a fresh coherent generation
and an endpoint in a pile that stays in config quorum. The named `switchover`,
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

Membership and conversion use separate existing RPCs. Their CLI commands have
no `expected_generation` guard. Keep exclusive operator control and Skipper
manual/stopped; this limitation is not repaired by manual mode:

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
{"cluster_id":"production-bridge-01","generation_high_water":"42","pending_mutation":{"expected_generation":"42","update":{"dc-a":"DISCONNECTED","dc-b":"PRIMARY"}}}
```

`--version` prints the format as `state_schema=2`. A `state.json` written by a
build that printed `state_schema=1` is rejected at startup; the operator must
remove it after confirming that no mutation is pending on the cluster.

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
RPC was never sent. While its generation remains current, Auto can resend only
the identical overlay; it never substitutes an opposite failover at G. A plan
that keeps the pending primary and only adds DISCONNECTED piles replaces the
record and is sent at G: both plans commit only in a component holding the
primary's quorum, so the server's generation guard lets exactly one of them
commit.

| Outcome | Controller behavior |
| --- | --- |
| Local `EVIDENCE_EXPIRED` (no RPC) | Preparation outlasted the supporting evidence. Keep pending and wait for fresh evidence; no invocation or `mutation_sent` is recorded. No restart is required. |
| `SUCCESS` / `SUCCESS` outcome | Raise high-water to at least G+1 and clear pending. Success does not establish unique invocation identity. |
| `ABORTED` / `GENERATION_MOVED` | Raise high-water to at least G+1 and clear pending. Views still at G are stale. distconf also answers ABORTED from a root that lags behind G; the controller then reports `STALE_HISTORY` at G until a fresh view at G+1 or above arrives, which is fail-safe. |
| Server `UNAVAILABLE` or `PRECONDITION_FAILED` / `NOT_APPLIED_HERE` | Keep pending; resend the identical update at once to another eligible endpoint. Every endpoint that answered this way stays excluded (exclusions accumulate) until the next state poll that follows an evaluation with no selectable endpoint (`NO_MUTATION_ENDPOINT`); then all are retried, so resends are bounded to about one per endpoint per state poll. Earlier invocations may remain in flight. |
| `BAD_REQUEST` or `UNAUTHORIZED` / `REJECTED` | Keep pending: only this invocation was refused before distconf; earlier ambiguous or superseded invocations may still commit. Auto sends nothing more at this generation until a restart or a new generation; the decision stays READY with reason `REJECTED_BY_SERVER` and the `mutation_rejected` error is logged. Fix the cause before restarting. The persisted overlay still restricts the plan after restart. |
| `INTERNAL_ERROR`, timeout, transport/client errors or other status / `AMBIGUOUS` | Keep pending; identical resend no earlier than the mutation client deadline (`mutation_operation_timeout_seconds` + 2 s) after the last send, doubling after each ambiguous outcome up to 5 minutes; `last_mutation.ambiguous_resends` counts them. |

A coherent selected view above pending G clears it automatically. Restart allows
one immediate identical resend, subject to a current valid Ready plan. A failed
state write blocks mutations with `STATE_WRITE_FAILED`; observation continues.
If the directory fsync after the rename fails, the renamed file and memory
already hold the new record; the next successful write makes it durable.

If a pending plan can never commit because its intended survivors lack quorum,
first stop the old process, prevent restart and reconcile every possible delayed
request and both sides of the partition. Record why replacing that plan at G
cannot create competing histories. Set `mode: manual` in the YAML file used by
`--config`; `--clear-pending-mutation` does not change mode. Then start with:

```text
bridge_skipper --config /etc/skipper/config.yaml --state-dir /var/lib/skipper/cluster-a --clear-pending-mutation
```

Confirm `mode: "manual"` in `GET /api/state` before leaving the process running
or reviewing another mutation. This logged startup override clears only the
pending record and **preserves the high-water mark**. It does not cancel remote
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
