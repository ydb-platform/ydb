# Related GitHub issues and pull requests

Repository: [`ydb-platform/ydb`](https://github.com/ydb-platform/ydb). Research date: **2026-11-12**. States and merge dates below were read from the GitHub API; dates in the table are UTC.

## Most relevant existing proposal

**[PR #57055 — [BlobStorage] Preserve VDisk GUID history across realm recovery](https://github.com/ydb-platform/ydb/pull/57055)** is an **open draft**, created on 2026-10-27 and not merged at the time of inspection. The inspected head was `3b76db37c8aab4c253113c19efa6a880b308102b`.

This is a direct conceptual overlap with [issue 02](02-guid-witness-loss-after-rolling-wipes.md) and especially [issue 03](03-false-first-run-after-witness-loss.md). Its description identifies the same failure chain: copying blob indexes after a realm wipe does not retain peer GUID observations; a later wiped realm can appear to be `FirstRun` and skip `LostData` recovery.

The inspected changes add durable GUID history, collect `Final` witnesses, persist them through Syncer Committer, and keep historical evidence separate from current quorum votes. `TDecisionMaker::ReachAVerdict(historicalGuid)` can republish a historical GUID before choosing `Good` or `LostData`. See the [PR changes](https://github.com/ydb-platform/ydb/pull/57055/files).

The scope matters:

- `TVDiskGuidRecoveryActor::StartSurvey` enables history collection only for `ErasureMirror3dc` when the local fail realm is `R`.
- The reproduced regressions use `Erasure4Plus2Block`. The draft is not evidence that these exact cases are fixed; no test was run with the PR applied.
- The PR does not change `TSyncerJobTask::HandleRestart` or the `RESTART`/`DbBirthLsn` history-coverage mechanism in [issue 01](01-stale-metadata-after-rolling-wipes.md).
- Legacy peers can trigger a fallback without a history-completeness claim, according to the proposed protocol.

Reference this PR before opening another GUID-history proposal. The 4+2 regression and preserved-local-state availability case are useful additional coverage to discuss with it.

## Existing issues worth mentioning

The closest existing symptom report is **[issue #58640 — VDisk syncer commits the peer's position after a cut full recovery, so the rest of the peer's index never arrives](https://github.com/ydb-platform/ydb/issues/58640)**, **open**, created 2026-11-09. Its body describes an interrupted full-recovery job leaving `Current.SyncState` advanced and the scheduler committing that position despite failure. It includes a `SyncFullRecovery::CutFullRecoveryIsRedone` reproducer.

This shares issue 01's invariant: persisting a peer cursor must not skip metadata that was never applied. Its documented trigger is different: full recovery starts and is interrupted, whereas issue 01 adopts `DbBirthLsn` through `RESTART` without first transferring the historical index. It is worth cross-linking or discussing combined tracking, but the existing report does not by itself cover this reproduction. The report and its stated reproduction were read; this investigation did not execute that other test.

**[Issue #43217 — SyncLogDataCutting enabled may lead to the failure of syncer protocol](https://github.com/ydb-platform/ydb/issues/43217)** was **open** at inspection, created on 2026-06-11. Its body is empty and the comments API returned no comments.

The issue is explicitly linked from [PR #43159](https://github.com/ydb-platform/ydb/pull/43159). That PR describes premature persistence of `SyncState` with `SyncDataCutting` enabled. This is relevant background for cursor/durability invariants, but the available issue content does not describe the rolling-wipe/GUID-witness sequence. Treat it as an adjacent issue, not a confirmed duplicate of issue 01. Its open state should not be interpreted as proof that the merged PR failed to fix its original race.

Additional open reports provide narrower context rather than an exact duplicate:

| Issue | Verified scope | Relationship |
| --- | --- | --- |
| [#58680 — VDisk sync log can drop the first record it still needs when it frees memory pages](https://github.com/ydb-platform/ydb/issues/58680) | Memory-page removal can discard the first needed LSN. Created 2026-11-09. | Another silent synchronization-record gap; its trigger is an eviction boundary, not `RESTART` after rolling wipes. |
| [#58699 — Phantom flag storage builder skips the sync log record at LogStartLsn](https://github.com/ydb-platform/ydb/issues/58699) | Phantom-flag rebuilding skips a record at the initial boundary. Created 2026-11-09. | Relevant to lost synchronization history and flags, but a different off-by-one path. |
| [#58560 — Bridge: a pile becomes SYNCHRONIZED while ordinary VDisks in it are still catching up](https://github.com/ydb-platform/ydb/issues/58560) | Pile-level synchronization status can precede ordinary VDisk replication. Created 2026-11-08. | Related readiness concern at another layer; issue 01 observes the affected VDisk itself reporting `Replicated` while missing `Block` metadata. |

The bodies of these reports were inspected. Their own reproduction or code-review claims are not additional tests performed by this investigation.

## Other open recovery proposals

| PR | State at inspection | Scope and limits |
| --- | --- | --- |
| [#57945 — Synchronize metadata before repeating mirror3dc recovery](https://github.com/ydb-platform/ydb/pull/57945) | Open, not draft; inspected head `4cb9b965c64aff714f8014d43ece7f0a9c4a8b97` | Adds a fixed metadata synchronization boundary before repeated Osiris after mirror-3-dc durability changes. Relevant to issue 01's freshness requirement. The inspected `HandleRestart` change resets a target and still adopts `newSyncState`; it does not replace that operation with historical full synchronization. It is not an established fix for the 4+2 regression. |
| [#57796 — [BlobStorage] Validate DC transitions and wait for configuration convergence](https://github.com/ydb-platform/ydb/pull/57796) | Open draft | Durable configuration acknowledgements and DC-transition quorums. Its description explicitly excludes incarnation-GUID handling; configuration convergence is a separate prerequisite from complete metadata history. |
| [#55778 — Support pool-scoped emergency VDisk recovery](https://github.com/ydb-platform/ydb/pull/55778) | Open draft | Opt-in mirror-3-dc recovery when one DC is offline and another was erased. Proposes provisional full-sync snapshots and later resynchronization after the final `DbBirthLsn`. Operationally related; not a demonstrated fix for GUID-witness erosion or the tested 4+2 case. |
| [#59181 — Add file-backed emergency recovery group selector](https://github.com/ydb-platform/ydb/pull/59181) | Open draft, created 2026-11-12 | Extends #55778 for the 26.3.1.17 codebase using group selection. It is recovery tooling, not evidence that the default synchronization protocol is fixed. |

#57945's relevant cursor-handling patch was inspected. The other three entries in this table are classified from their PR descriptions; none was applied or tested here.

## Relevant merged PRs

| PR | State and merge date | Relationship to these drafts |
| --- | --- | --- |
| [#56555 — VDisk: fix possible LostData mishandling](https://github.com/ydb-platform/ydb/pull/56555) | Merged 2026-10-23 | Protects GUID republication from prematurely writing local `Final` before Osiris finishes. Adds `EFirstRunMode::PublishOnly` and the restart-test fixture extended by this investigation. It does not preserve absent peers' witnesses or change all-`Empty` classification. |
| [#57091 — [BlobStorage] Advance and persist the SyncLog recovery base](https://github.com/ydb-platform/ydb/pull/57091) | Merged 2026-10-28 | Advances and persists the post-recovery SyncLog base, including an empty journal, to prevent repeated full-sync cycles. Directly relevant to `DbBirthLsn`, but does not force historical metadata transfer when an old peer receives `RESTART`. |
| [#57047 — [BlobStorage] Use state-aware Osiris and full-sync quorums for mirror-3-dc](https://github.com/ydb-platform/ydb/pull/57047) | Merged 2026-10-28 | Changes mirror-3-dc recovery quorums and forces full sync on `F` to `R` transitions. Its description explicitly keeps GUID history in #57055. The 4+2 tests do not exercise these realm-state rules. |
| [#55471 — Fix SyncLog record loss after a recovery log cut by delivering committer reports after earlier puts](https://github.com/ydb-platform/ydb/pull/55471) | Merged 2026-10-07 | Fixes ordering between puts and committer reports so recovery-log cutting cannot overtake an unreceived SyncLog put. Adjacent durability bug; not the loss of historical coverage after sequential peer wipes. |
| [#43159 — Get rid of premature updates of the SyncState in Skeleton](https://github.com/ydb-platform/ydb/pull/43159) | Merged 2026-06-11 | Addresses premature `SyncState` persistence and links issue #43217. Its diff does not replace `HandleRestart`'s adoption of the remote `DbBirthLsn` cursor. |

#56555 and #57091 are already ancestors of the production baseline `6e3629b18daf4a2441d2b129fa3b1ae84564610f` where the regressions fail. Their presence therefore does not resolve the tested failures. This investigation did not bisect the introduction of the metadata gap; #57091 must not be labeled its introducing change merely because it touches the recovery base. The existing `Max(LogStartLsn, DbBirthLsn + 1)` lower bound predates that PR.

## Current upstream source comparison

The inspected upstream `main` revision was [`8e3614b1517d54c60db9a8fe5bc3e39566560791`](https://github.com/ydb-platform/ydb/commit/8e3614b1517d54c60db9a8fe5bc3e39566560791), with commit time 2026-11-12T10:33:38Z. GitHub's contents API returned Git blob IDs identical to the tested baseline for all six checked files:

- `ydb/core/blobstorage/vdisk/syncer/guid_recovery.cpp`
- `ydb/core/blobstorage/vdisk/syncer/guid_firstrun.cpp`
- `ydb/core/blobstorage/vdisk/syncer/syncer_job_task.cpp`
- `ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer.cpp`
- `ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog.cpp`
- `ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogreader.cpp`

This confirms that the inspected decision and cursor-handling code has not changed at that upstream snapshot. It is not a fresh execution of the regressions on current `main`, nor a review of every surrounding dependency.

## Search scope

The investigation uses public GitHub issues and PRs, including open and closed entries, plus file history and references in relevant PR descriptions. The following GitHub REST search queries completed. Every query below has the common prefix `repo:ydb-platform/ydb`; counts overlap and must not be added together.

| Remaining query | Reported matches | Coverage |
| --- | --- | --- |
| `is:issue SyncGuidRecovery` | 0 | Complete returned result set. |
| `is:issue DbBirthLsn` | 2 | Test-muting reports #57767 and #56725; not duplicate evidence by themselves. |
| `is:issue "rolling" "wipe"` | 0 | Complete returned result set. |
| `is:issue "sync" "guid"` | 5 | Includes #58640 and test-muting/other reports. |
| `is:issue SyncLog` | 11 | Substantive matches selected for body review; test-muting and unrelated assertion reports distinguished. |
| `is:issue VDisk` | 282 | Broad discovery query; only the first 100 entries were returned, not an exhaustive review. |
| `is:pr GUID` | 61 | All result metadata returned; relevant recovery proposals selected for description/diff review. |
| `is:pr DbBirthLsn` | 4 | Includes #57091, #55778, #59181, and older full-sync protocol PR #19444. |
| `is:pr LostData` | 3 | #57055, #56555, and #57047. |
| `is:pr SyncState` | 1 | #43159. |

Exact-symbol results were checked against changed files where necessary; merely mentioning `SyncGuidRecovery` in a status filter is not a protocol fix. Substantive issue bodies and the strongest PRs are discussed above. Not every body in the broad result sets was reviewed.

The directly related open draft #57055 is an existing proposal for the GUID-history problem. An exact GitHub issue duplicate has not been established from the evidence recorded here. Search is not proof of absence, and private/internal reports are outside this search.
