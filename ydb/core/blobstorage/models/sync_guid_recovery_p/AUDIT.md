# SyncGuidRecovery audit and correspondence

## Scope and evidence rules

Audit date: 2026-11-17. Original series: `dc1cd2af38a..0379afa1f46`.
Reconstructed branch: `fix-vdisk-sync-guid-recovery-2`, same baseline.
PChecker/PEx execute P code. They are not a proof that C++ refines the model.
C++ executions use exact remote checkouts on mr006 with normal dependencies.

Scope is `Erasure4Plus2Block` (8 VDisks) and `ErasureMirror3dc` (9 VDisks),
`NORMAL`, all realms `INCLUDED`, one VDisk per fail domain. The returning
VDisk is stopped and restarted with retained or explicitly erased local state;
these checks do not cover a live TSyncerScheduler reconnecting without restart
or metadata writes concurrent with the final catch-up round.

## Historical checkpoints

| Commit | Observation / deviation |
|---|---|
| `ab8ab5e963f` Add regression tests for rolling wipes during VDisk outage | Test-only reproduction; original assertion queried group behavior rather than the returning VDisk. |
| `6a80553dddf` Check Block metadata after rolling VDisk wipes | Queries Block metadata directly on the returned VDisk after Good and replication checkpoint. |
| `85d2b7b25a5` Verify Block metadata survives on a recovered peer | Adds recovered-peer Block control, distinguishing absent metadata from failed group commit. |
| `282097db31d` Add sync-guid-recovery-issues | Historical issue investigation; local evidence references required repair after relocation. |
| `8a98d1bc08b` Add sync_guid_recovery_p verification | Baseline 4+2 P model; intentional failing monitors and passing controls. |
| `ef13485ee20` Improve sync_guid_recovery_p with mirror-3-dc support | Adds NORMAL mirror-3-dc topology and topology-aware quorum predicate. |
| `0596d5c242e` Introduce sync_guid_recovery_p protocol fixes on model | First repaired P model: full-sync on RESTART, durable GUID facts, catch-up. Deviations: atomic all-online publication; history copied during LostData instead of committed survey; no degraded two-peer scenario. |
| `20eba867180` Preserve syncer GUID history and full-sync changed incarnations | First C++ implementation collects/commits GuidHistory before verdict, unlike that P model. HasGuidHistoryQuorum excludes self and introduces an unmodeled extra startup gate. It also masks minority conflicting legacy Final evidence that the corrected P function rejects. |
| `70e024a809f` Require successful metadata catch-up before reporting replicated | Successful-only StartupDataSyncPeers accounting also controls broker completion and status; original model has no broker or failed attempts. |
| `e9fba16e68c` Reject GUID publication conflicting with committed identity | Incoming publication rejects a different committed GUID; model already asserted immutable identity. |
| `89337c36d68` Retain old synchronization cursor when a full snapshot fails | Rollback OldSyncState after unsuccessful unordered full-sync; old model has no interrupted full-sync result. |
| `f1f1bd6ed3a` Keep replication status false until the sync scheduler is ready | Explicitly returns Replicated=false and DurabilityReplicated=false before scheduler creation; not represented as a separate model phase. |
| `c210c9a4f10` Reduce SyncGuidRecovery model state space | State-space reduction: canonical sets, cleared terminal fields. Does not address previous semantic gaps. |
| `2f070276041` Fixup update issue mds | Updates historical evidence claims; no protocol change. |
| `ea971e3b79c` Check closed GUID survey noninterference | Closed-survey component harness verifies P actor behavior, not execution of C++ actor. |
| `7ddce91498e` Move files around | Intermediate relocation omitted from rebuilt series. |
| `f5f2b82955e` Remove excessive logs | Logging cleanup incorporated into final C++ source. |
| `31a67d6abcc` Move sync_guid_recovery_p to core/blobstorage/models | Final model location adopted from first reconstructed model commit. |
| `7155fbaa8de` Remove excess md file | Removes superseded correspondence document; new AUDIT.md provides explicit mapping and limits. |
| `0379afa1f46` Fix SyncGuidRecovery history coverage and catch-up quorum in P model | Adds degraded scenarios, HasGuidHistoryCoverage, MetadataSynced/MetadataSyncComplete, history-commit wait and minority-Final check. Remaining gaps were partial publication, extra LostData history import, fresh outcomes and interrupted snapshots. |

Relevant sources (syncer, recovery test, final model path) are identical for
`6f0bf0923cc` → `0379afa1f46`, `7605b01dcff` → `f1f1bd6ed3a`, and
`cb5d537d248` → `20eba867180`. This is a scoped content comparison, not a
claim that whole repository trees or all test dependencies are identical.
The complete 20-pair mapping in Evidence/rewritten-checkpoints.json also compares model files after normalizing their three historical directory prefixes; every pair matches in those scoped sources. Whole-repository trees and other protobuf definitions are not claimed identical.

## What the regression means

With retained `LocalFinal`, all modern responders, and two unavailable peers,
4+2 can obtain six survey responses including self. The old history gate removes
self and requires six remaining history sources: only five can answer. No legacy
response activates fallback. Consequently `TEvVDiskGuidRecovered` cannot complete.
This is a startup-liveness regression, not proof that every group operation stops.
For mirror-3-dc this depends on placement: self 0 and unavailable 5/8 leave
(3,2,2) responses, but excluding self leaves nonquorum (2,2,2).

The old scheduler conflates the successful peer-only quorum with
`TEvStartupDataSyncDone`. With too few successful peers it retains its startup
broker token and forces `Replicated`/`DurabilityReplicated` false. Conversely,
if a quorum succeeds before one initial due attempt is applied, it releases the
token too early. The existing
`StartupDataSyncBrokerKeepsTokenUntilInitialDueSyncAttemptCompletes` test
checks this latter contract. Historical CI logs also show DSProxy and balancing
failures; they are context, not fresh execution evidence for this audit. A test that stalls earlier in GUID
recovery cannot independently demonstrate this scheduler consequence.
`RestartWithLegacyGuidRepliesAndTwoUnavailablePeers` strips history extensions
from otherwise unchanged GUID replies to exercise legacy fallback and isolate
`Replicated`/`DurabilityReplicated` after the GUID verdict.

## A concrete model-to-C++ transfer deviation

For retained local Final(1), one legacy RemoteFinal(2), and otherwise Empty
responses forming a survey quorum:

| Checkpoint | Decision |
|---|---|
| Baseline C++ dc1cd2af38a | Inconsistency |
| Corrected P function 0596d5c242e | Inconsistency |
| First C++ repair 20eba867180 | Good(1), ACTION_WriteFinalToQuorum |
| Last C++ correction 0379afa1f46 | Inconsistency |

VDiskQuorumDecision encodes minority Final evidence as InProgress with
ACTION_WriteFinalToQuorum. The first repair checked only State == Final before
replacing that decision's GUID with local history, losing a baseline consistency
check. The last correction checks the encoded minority-Final case too.
Modern history-aware replies can independently expose the conflict through
GuidHistory, so the legacy/no-history qualifier matters. This is defensive
handling of inconsistent identity evidence, not a claim that ordinary rolling
wipes spontaneously create conflicting GUIDs or lose payloads. Tentative
InProgress(2) must instead preserve local GUID 1.

## Transition correspondence

| C++ | P | Boundary / abstraction |
|---|---|---|
| `TDecisionMaker::ReachAVerdict`, `VDiskQuorumDecision` | `ReachAVerdict` | Historical identity adds no Final responder IDs; a minority conflicting RemoteFinal rejects; tentative InProgress does not replace identity. |
| `HasGuidHistoryCoverage` | Same function | Complement must not contain a publication quorum; exclude erased self; retain self only with LocalFinal. |
| `TEvVSyncGuidResult::GuidHistory`, `EGuidHistory`, `TEvSyncerCommitDone` | `ReadHistory`, `eCommitGuidHistory`, CommittingHistory | History is acquired before verdict, not invented during LostData. Witness provenance is projected to owner/GUID because it supplies no votes. |
| `ACTION_WriteInProgressToQuorum`, `ACTION_WriteFinalToQuorum` | `TGuidPublication`, per-peer durable writes and acknowledgements | First quorum completes once; any prefix of remaining writes may complete or be canceled, interleaved with later phases. The single-store model chooses write order and serializes write/ack pairs; it does not model arbitrary network/session failures. |
| `TSyncerRecoverLostDataActor` | LostData branch in TRecoveryStorage | Full-sync sources exclude self; payload transfer and Osiris are atomic harness operations. No extra GUID-history import. |
| `TSyncerJobTask::HandleRestart` | RequestFullSync on RESTART/NODATA | Successful local import acknowledgement precedes SyncDone. Blob/Barrier records are abstracted to one committed Block generation. |
| `ReplyAndDie`, `OldSyncState`, `FullSyncRequested` | Failed full-sync rollback and FullSyncRetryProof | Partial snapshot followed by interruption preserves old cursor; the retry harness represents only the relevant scheduler contract, not scheduler implementation. |
| `MetadataSynced`, `MetadataSyncComplete` | HasCatchupQuorum and roundDone | Startup checkpoint: self counts after retained/recovered metadata; failed attempts cannot count as successful sources. This is not a continuously updated freshness proof. |
| `TEvStartupDataSyncDone` | Not modeled | Releases broker token after first attempts; independent of MetadataSyncComplete. Reviewed in C++, not formally verified here. |

`eSurvey` samples peer records atomically; TVDiskGuidProxy and SurveyTransport
reorder delivery, not read execution. This relies on serialized recoveries,
the eRunPropagators settle barrier, and compatible identities. Remaining writes
from the preceding recovered owner concern a different owner: they cannot
change the current owner's verdict, the unavailable owner's witnesses, or Block
metadata. Without that settle barrier, or with overlapping same-owner recovery
or concurrent initialization, snapshot creation must move into per-peer request
handlers. Those scenarios are not covered by the present model.

The harness field Ready is not an exact copy of every C++ Replicated or
DurabilityReplicated transition. After LostData/FirstRun it permits use as a
valid metadata source; the returning Good owner's eRoundFinished represents
its checked catch-up checkpoint. FreshGroup checks FirstRun, not concurrent
whole-group initialization or public status flags. The separate C++ startup
controls check Good, Replicated and DurabilityReplicated directly.

The first corrected model's extra LostData copying was a modeling mismatch,
not by itself a proven C++ witness-loss defect: C++ can obtain the required
history during the preceding survey. The corrected model removes this shortcut.

FreshGroup/DegradedFreshGroup now require FirstRun explicitly; erased existing
VDisk scenarios require LostData; retained scenarios require Good. Previously
returning Inconsistency unconditionally still passed all four fresh controls.
The new assertions reject that mutation.

## Reviewed, outside the checked model

- Mixed-version peers: absent GuidHistory is tracked separately as LegacySources;
  fallback does not claim complete history. Full preservation requires support.
- Read-only: incoming publication does not update the in-memory cache; history
  commit is skipped. Progress requiring publication is not guaranteed here.
- TEvVGenerationChange: GUID survey cancels old proxies and restarts; scheduler
  clears MetadataSynced to self and rejects stale-generation success for coverage.
- Realm transitions use TMirror3dcWriteQuorumChecker::CheckQuorumForFullSync and
  FullSyncRequested; the NORMAL P predicate does not verify those transitions.
- Existing metadata gaps with unchanged peer incarnation are not automatically
  repaired by handling a future RESTART. Crash/reconfiguration during history
  commit, arbitrary disk failures, and general timeout/lifetime correctness are
  outside these bounded scenarios.

History contains at most one GUID per owner in a fixed group; merge and read scans
are bounded by group size (8/9 here). No cluster-wide state or new background scan
is introduced. Protobuf fields are additive and existing enum values unchanged.

## Verification

Detailed machine-readable results and exact commands are in `Evidence/`.
Historical console logs under issues/evidence are preserved historical evidence,
not results for the reconstructed branch. Unavailable old untracked P traces
are marked as unavailable rather than linked as current artifacts.

Current model results: all 18 main scenarios passed 50 schedules with seed 1;
FreshGroup, DegradedRestart and DegradedFreshGroup also passed seeds 2/3 for both
topologies (1,500 main schedules). Four compact harnesses passed seeds 1/2/3
(600 schedules). PEx completed ClosedSurvey, FullSyncRetry, GuidConflict and
GuidPublication; see their logs for exact state/schedule counts.

The expanded full FreshGroup searches exhausted the 8 GB checker limit, with
no bug found and unexplored choices remaining. They are explicitly incomplete.
Do not reuse the older model's completed FreshGroup searches as proof of this
expanded publication model.

### Fresh C++ executions on mr006

| Production checkpoint / test snapshot | Observed result |
|---|---|
| Baseline dc1cd2af38a / b29e4c989c0 | Two rolling-wipe assertions fail as intended; RestartWithTwoUnavailablePeers passes. Direct returning-VDisk Block result is NODATA after the recovered-peer generation-42 control passes. |
| Pre-correction 7155fbaa8de / 2a56734e4bf | Modern replies fail decision.has_value(); legacy replies pass Good and fail replicated. Both are intended assertions, not timeouts. |
| Reconstructed ec26ddef811 | All 13 RecoverLostDataRestart and five TSyncNeighborsTests pass, including GUID-history entry-point round trip. |
| Reconstructed production sources / 8f509cd04e8 | All six additionally approved DSProxy, balancing and startup-broker checks pass. Production sources are identical to ec26ddef811. |
| Pre-correction 7155fbaa8de / ea17cba86ce | Additional CI matrix: one pass, three assertions fail, two tests time out. The broker assertion observes seven grants instead of one before the held initial job result is applied. |

The initial historical startup probe blocked waiting for TEvVStatusResult and
aborted in the test runtime. That diagnostic attempt is retained but excluded
from intended-assertion evidence. The final helper captures replies asynchronously
and fails at TEvVDiskGuidRecovered or Replicated; identical corrected probes run
on pre-correction and repaired production code. No production change was needed
for that harness correction.

The reconstructed production fix is the reviewed final correction from the
original series, with the unused GuidHistoryComplete completion-event field
removed. Additional work closes model/test gaps and reconstructs reviewable
commits; it does not claim an additional newly discovered C++ data-loss fix.

All commands, revision-specific results, source hashes and receipts are under
[Evidence](Evidence/commands.txt). Canceled cold/redundant builds are not test
failures. Normal dependencies were built, no driver binary was substituted, and
no canonical output was regenerated.
