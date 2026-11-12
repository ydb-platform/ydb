# [EPIC] Make VDisk recovery safe across rolling wipes and temporary unavailability

**Ticket type:** umbrella ticket draft. **Investigation and GitHub research date:** 2026-11-12

## Problem

When one VDisk is temporarily unavailable, sequential wipes of its peers can erase both incremental synchronization history and peer copies of its eternal GUID. On return, the VDisk can either become operational while missing committed metadata or reject its intact local state as inconsistent. If it also lost its local state, the same witness erosion can cause an existing slot to be classified as `FirstRun`.

Two failures have been reproduced; the conditional `FirstRun` scenario is established from code and still needs its own regression. Preserving GUID witnesses and restoring metadata freshness are separate requirements: fixing either one alone does not resolve this epic.

## Goal

Make recovery across rolling wipes preserve VDisk identity and recover the metadata needed to resume synchronization safely. Recovery and readiness decisions must be backed by the required durable evidence, including when a peer's original SyncLog history no longer exists.

## Child tickets

| Ticket | Finding | Evidence |
| --- | --- | --- |
| [01 — Missing metadata after `Good`](01-stale-metadata-after-rolling-wipes.md) | A returning VDisk reports `Good` and `Replicated` but lacks a successfully committed `Block`. | Reproduced on `mr006`; a recovered peer still returns the blocked generation. |
| [02 — Intact VDisk rejected after witness loss](02-guid-witness-loss-after-rolling-wipes.md) | An all-`Empty` first survey quorum produces `Inconsistency` despite local `Final` state. | Reproduced on `mr006` using finite delays of actual GUID replies. |
| [03 — Existing VDisk mistaken for `FirstRun`](03-false-first-run-after-witness-loss.md) | If the unavailable VDisk also has local `Empty` state, erased witnesses can make an existing slot appear new. | Decision path verified in code; no dedicated end-to-end test was run. |

Tickets 02 and 03 share the witness-erasure root cause and may be implemented together; ticket 01 requires a separate metadata-history guarantee.

## Work to complete

- [ ] Prevent synchronization cursors from advancing past historical metadata that has not been applied locally when a peer's incarnation changes.
- [ ] Preserve or reconstruct durable eternal-GUID witnesses across peer wipe recovery, while retaining detection of genuinely conflicting `Final` GUIDs.
- [ ] Validate the conditional `FirstRun` scenario with the smallest focused regression and resolve the wrong classification if confirmed.
- [ ] Define which successful synchronization evidence backs recovery/readiness decisions. Cover both startup and live actors returning from a partition.
- [ ] Reconcile the 4+2 behavior with the existing mirror-3-dc recovery proposals and their compatibility/fallback rules.
- [ ] Determine whether already-advanced cursors require an explicit full-synchronization procedure to repair existing metadata gaps.

## Acceptance criteria

- [ ] `RollingWipeWithPreservedLocalState` passes: the returned VDisk reads generation 42 for the committed `Block` after the tested operation/replication checkpoint, with the recovered-peer control still passing.
- [ ] `RollingWipeWithDelayedSelfGuid` passes: finite delays of valid witnesses do not turn preserved local `Final` state into `Inconsistency` solely because peers lost their witness copies.
- [ ] A focused test distinguishes genuine initialization from an existing locally empty VDisk after witness erosion; the existing slot retains its identity and takes the required lost-data recovery path.
- [ ] The chosen protocol establishes metadata coverage rather than treating a changed incarnation or a completed synchronization attempt as proof of transferred history.
- [ ] Scope and limitations for `Erasure4Plus2Block`, mirror-3-dc realm recovery, legacy peers, and already-affected VDisks are documented with the implementation.

## Related existing work

The strongest existing proposal is open draft [PR #57055](https://github.com/ydb-platform/ydb/pull/57055), which already addresses GUID-history erosion for mirror-3-dc realm recovery. Open [issue #58640](https://github.com/ydb-platform/ydb/issues/58640) describes a related missing-index outcome after interrupted full recovery, and open [PR #57945](https://github.com/ydb-platform/ydb/pull/57945) adds a metadata freshness check before repeated mirror-3-dc Osiris. The [related GitHub issues and PRs](04-related-github-work.md) file records the scope differences and search limitations as of 2026-11-12.

## Reproduced revision and test evidence

- Reproduced: **2026-11-12**, on `mr006`. Captured test events are around **00:29:21 UTC+3**; the log timestamps are `2026-11-11T21:29:21–22Z`, not a different reproduction date.
- Tested Git SHA: **`c28196d089ffdd61c365f15abc76cee36af8cbde`**, committed **2026-11-12T00:27:29+03:00**, available in the `mr006` repository on branch `codex/sync-guid-recovery-investigation`.
- Production baseline: [`6e3629b18daf4a2441d2b129fa3b1ae84564610f`](https://github.com/ydb-platform/ydb/commit/6e3629b18daf4a2441d2b129fa3b1ae84564610f), committed **2026-11-11T17:52:44+03:00**.
- Test changes: two cases and one shared scenario in [recover_lost_data_restart.cpp](../core/blobstorage/ut_blobstorage/recover_lost_data_restart.cpp), 111 added lines. No production changes are included in that revision.
- Target: `ydb/core/blobstorage/ut_blobstorage/ut_replication`.
- Final result: **exactly two tests executed, both failed at the intended assertions; build succeeded; `ya make` exited with code 10**.
- [Captured final console log](evidence/mr006-c28196d089f.log).
- Remote checkout: `mr006:/tmp/sync-guid-recovery-d73ac283d28`. The directory name reflects the first test commit; the final checked-out revision was verified as `c28196d089f`.

Command executed on `mr006`, from the repository root:

```sh
./ya make --build relwithdebinfo -j64 --link-threads=2 -R -tA \
  ydb/core/blobstorage/ut_blobstorage/ut_replication \
  -F 'RecoverLostDataRestart::RollingWipe*'
```

The parallelism above is for `mr006`. It is not a laptop build command.

| Test | Result |
| --- | --- |
| `RecoverLostDataRestart::RollingWipeWithDelayedSelfGuid` | Expected `Good`; observed `Inconsistency`. |
| `RecoverLostDataRestart::RollingWipeWithPreservedLocalState` | Expected direct `TEvVGetBlockResult(OK, Generation=42)`; observed `NODATA` after `Good` and replication completion. |

Both cases first verify a successful `TEvBlock` and, after all peer wipes, a direct `TEvVGetBlockResult(OK, Generation=42)` from a recovered peer. Each wipe is followed by a new `TEvSyncGuidRecoveryDone(OK)` and `WaitForVDiskRepl`; this avoids treating the pre-wipe actor's `Replicated` status as recovery completion.

## Interpretation limits

The executed configuration is `Erasure4Plus2Block` with eight VDisks. The tests preserve the unavailable VDisk's local state and wipe the other seven VDisks one at a time. At most the unavailable VDisk and the currently wiped peer are unavailable during this sequence. The fixture's `Wipe` helper explicitly bypasses BSC degraded-group policy checks, so this is a storage-protocol scenario rather than a claim that every normal administration workflow permits the operation.

The direct `Block` result proves metadata divergence. It does not prove permanent loss of acknowledged blob payloads or successful stale writes at group level. Effects on `LogoBlob` and `Barrier` metadata are code-derived extensions, not separately executed regressions. The conditional `FirstRun` scenario must not be presented as an outcome of temporary unavailability alone.

Source links in the issue drafts are pinned to the production baseline. The regression commits are on the remote testing repository, not published as a GitHub PR. Current upstream work and the scope of the GitHub search are documented separately in [related work](04-related-github-work.md).

At the inspected upstream `main` revision [`8e3614b1517d54c60db9a8fe5bc3e39566560791`](https://github.com/ydb-platform/ydb/commit/8e3614b1517d54c60db9a8fe5bc3e39566560791), the Git blob IDs for `guid_recovery.cpp`, `guid_firstrun.cpp`, `syncer_job_task.cpp`, `blobstorage_syncer.cpp`, `blobstorage_synclog.cpp`, and `blobstorage_synclogreader.cpp` exactly match the tested baseline. This is a bounded source comparison, not a test run of current upstream `main`.
