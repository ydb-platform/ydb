# Testing and Observability

This guide maps the implemented behavior to tests and diagnostics. See [Architecture](architecture.md) and [Workload](workload.md) for the code paths, and the [usage guide](../../../../docs/en/core/contributor/load-actors-nbs-dbg-like.md) for running workloads on a test cluster.

## Test Targets

| Target | Coverage |
| --- | --- |
| `ydb/core/load_test/ut` | Allocation request construction, BSC response validation, routing parameters, and slot coordination |
| `ydb/core/blobstorage/ut_blobstorage/ut_ddisk` | End-to-end load-tablet lifecycle and I/O against the BlobStorage test environment |

The helper tests are in [nbs_dbg_like_alloc_helper_ut.cpp](../../ut/nbs_dbg_like_alloc_helper_ut.cpp), suite `NbsDbgLikeAllocHelper`. They cover response status/count/peer-count errors, all/subset DBG selection, and invalid I/O geometry.

The [range coordinator tests](../../ut/nbs_dbg_like_range_coordinator_ut.cpp), suite `NbsDbgLikeRangeCoordinator`, cover acceptance ordering, monotonic visibility, pending overwrites, PB/DDisk read pins, independent ranges, ordered erase retirement, and 64-bit wire VChunk IDs. The same suite also covers these cohort and slot contracts. They are deterministic scheduler examinations and do not measure production throughput.

| Test | Contract |
| --- | --- |
| `FailedVersionLeavesTheUnflushedListWithoutWalkingPredecessors` | Failed-version unlink without walking predecessors |
| `ColdReadsReuseSlotTableCapacity` | Reusable slot-table capacity across cold reads |
| `SameIndexVChunksMixProbeBitsAndFingerprints` | Diverse probe bits and fingerprints for same-index keys across 1,024 vChunks, plus independent visible slot state |
| `NormalAndSelectiveAdmissionRetainCapacityAndOrder` | Retained flush/erase vector capacity over repeated normal and selective admission; only selected members become admitted, with membership order preserved |
| `SelectiveAdmissionDoesNotRescanPinnedMembers` | A selectively admitted pinned member is not examined again; last-reader release wakes its slot |
| `LastReaderWakesOnlyItsSlot` | Last-reader wakeup of one slot |
| `CohortsExcludeLaterAndLowerLsnCompletions` | Cohorts that exclude a later lower LSN |
| `AdmissionSurvivesPartialPopAndRetryDedup` | Admission kept across a partial pop and deduplicated retry |
| `WakingOneSlotDoesNotRevisitTheBlockedPopulation` | A one-slot wakeup that does not revisit the blocked population |
| `EraseCohortWaitsForSyncCompletionAndKeepsBypass` | Erase-gate bypass that still waits for the older version |

Hash-distribution assertions use diversity bounds rather than fixed hash values or timing measurements.

The integration tests are in [nbs_dbg_like_load_tablet_ut.cpp](../../../blobstorage/ut_blobstorage/ut_ddisk/nbs_dbg_like_load_tablet_ut.cpp), suite `NbsDbgLikeLoadTablet`:

| Tests | Contract Exercised |
| --- | --- |
| `BasicSingleDbg`, `MultiDbg`, `SubsetOneOfTwoDbgs` | Allocation, run, and selected DBG prefixes |
| `GetSummaryReadyCounts` | Registered-PB readiness prefix |
| `RunBeforeCreate`, `DoubleCreate` | Lifecycle error responses |
| `CreateRestartRunDelete` | Recovery of a completed allocation after tablet restart |
| `WriteRead1000Blocks`, `WriteReadMultiDbg` | Direct request/response data-integrity checks |
| `MultiDbgSharedDDiskNoLsnCollision`, `SharedDDiskVChunksKeepDifferentDBGData` | Unique LSNs and distinct payloads at equal offsets in multiple shared-DDisk VChunks |
| `RunRunDelete` | Reusing one allocation for consecutive runs |
| `MultiTablet`, `MultiTabletSharedBscTabletId` | Multiple load tablets and forced unique storage-owner IDs |
| `HeldSyncAllowsPBWritesAndReadsAndOrdersSuccessors` | Concurrent PB writes/reads with acceptance-ordered Sync |
| `PBReadPinsEraseWhileNextSyncContinues`, `DDiskReadPinsNextSyncWithoutBlockingPBWrite` | Different PB and DDisk read lifetimes |
| `ReorderedPBRepliesPreserveVisibilityAndFlushOrder`, `PendingOverwriteReadsPreviousAcknowledgedPBVersion` | Monotonic acknowledged visibility and pending overwrites |
| `MalformedAndWrongSenderSyncRepliesKeepReservations`, `SyncFailureRetainsAllDestinationsUntilRetryCompletes` | Reply validation and complete destination-set retirement |
| `OlderEraseFailureBlocksNewerEraseButAllowsSync` | Separate Sync and Erase ordering |
| `AdmittedFlushAndEraseCohortsSurviveFallingBelowGate` | Deferred admitted records stay eligible after the unadmitted ready count falls below the normal threshold |
| `LaterCompletionsWaitForTheNextFlushCohort` | Individual PB completions after one cohort produce no Sync until the next threshold, including a late lower LSN; batch payload size is the cohort size |
| `EraseCohortsBatchOnlyAfterSyncCompletions` | Erase batches form only after controlled Sync completions, across two cohorts |
| `DDiskReadPinDefersOnlyItsAdmittedSlot` | A pinned cohort member waits while an independent slot syncs, then proceeds when the last DDisk reader clears, without another write |
| `IdleCleanupUnblocksReorderedHeadAtLsnCap` | Gate `2`, LSN cap `5`, and five writes to one slot with PB replies `2–5` before `1`: initial backpressure and no Sync, followed by ordered Sync/erase, correct data, and another accepted write without reconfiguration |
| `IdleCleanupErasesWithoutReplication` | A below-gate erase tail finishes without Sync when replication is disabled and releases LSN capacity |
| `IdleCleanupSkipsVChunkWithIncompletePBWrite` | An idle vChunk progresses while another holds a PB result; completion later allows the busy vChunk to progress |
| `IdleCleanupAccountsVChunkSyncBatchesRetriesAndDuplicates` | Per-vChunk accounting using real single-vChunk Sync replies, with deliberate failure, malformed/unknown, and duplicate injections; shared flush/erase activity snapshot and readback of initial and tail data |
| `MultiVChunkBatchesCompleteAndDelete` | Six interleaved writes across three vChunks at flush limits `2` and `16`: successful bounded Sync batches, two records per request at limit `16`, all-slot readback, LSN-cap reclamation, and deletion below the gate |
| `IdleCleanupPreservesDDiskAndPBReadPins` | Below-gate admission preserves both read-pin types; releasing each pin resumes the corresponding admitted work |
| `ForcedCleanupDoesNotCountGateBlocks` | Idle cleanup and lifecycle drain leave normal gate-blocked counters unchanged |
| `StaleIdleCleanupCannotAdmitOrClearNewTimer` | A stale timer cannot admit new work or clear the newer timer, even when legacy configuration ID zero is reused |
| `PartialDuplicateAndLatePBRepliesCleanFailedWriteOnce` | Subquorum invisibility, late PB replies, and failed-copy cleanup |
| `ReconfigureFlushesBelowGatesBeforeChangingIoGeometry`, `ReconfigureWaitsForAcceptedPBReadAndErase` | Accepted data survives configuration, independent of normal gates |
| `ConfigurationSupersessionRejectsStaleAndWrongSenderAcks`, `SupersessionDuringDrainInstallsOnlyLatestConfiguration` | Latest pending configuration and acknowledgement filtering |
| `LegacyConfigurationIdZeroStillDrainsAndReopensAdmission` | Legacy configuration without an external acknowledgement |
| `DeleteWaitsForFlushEraseAndDisconnectAcknowledgements`, `RepeatedPoisonDrainsAndWaitsForFinalDisconnect` | Orderly lifecycle drain and final disconnect barrier |
| `AmbiguousPBFailureCannotAuthorizeDeleteUntilLateResults` | Ambiguous replies cannot authorize cleanup or deletion |
| `RejectsAddressesOutsideConfiguredIoSlots` | Client address alignment to the configured I/O slot |
| `WriteChecksumsFollowRunConfiguration`, `AutomationWaitsForEntirePrefixAndConfiguration`, `AutomationControlAcrossNodes` | Existing checksum and acknowledged automation contracts |

Use the repository's current build/test instructions with these targets and a suite or test filter. The tests do not require a separately provisioned production cluster.

The ordering and drain tests use the BlobStorage fixture with mock PDisk. They hold and reorder protocol requests and replies. `LaterCompletionsWaitForTheNextFlushCohort`, `EraseCohortsBatchOnlyAfterSyncCompletions`, `DDiskReadPinDefersOnlyItsAdmittedSlot`, and `AdmittedFlushAndEraseCohortsSurviveFallingBelowGate` check deterministic request counts and payloads. The `NbsDbgLikeRangeCoordinator` tests check scheduler examinations, including that waking one slot does not revisit blocked slots. Ordinary cohort request-count assertions are made before the idle-cleanup timer fires; idle-cleanup cases advance simulated time explicitly. They verify observable payloads, admission, and operation retirement rather than timing alone. They do not establish native io_uring coverage and do not measure production throughput. Retain the independent `DDiskLoadRangeAdmission` suite in `ydb/core/load_test/ut` when validating this extraction.

When changing behavior, select tests by the contract affected. Remaining gaps include allocation interruption before DBG persistence and random-worker fanout above 512 concurrent operations. Disabled-replication coverage exercises erase cleanup; it does not establish production performance in that mode. Allocation restart coverage does not establish recovery of unfinished writes or full NBS repair/recovery.

## Results and Counters

The load service enriches `TEvLoadTestFinished` from `TNbsDbgLikeFinishStats`. Final results expose separate write/read rates and latency percentiles; multi-tablet results carry a per-tablet breakdown. Local and remote histogram merges retain observations rather than averaging percentiles. Despite internal names such as `ReadPbUs` and `ReadsPbOk`, worker-level read statistics aggregate both PB and DDisk replies.

Two counter scopes describe different lifetimes:

| Scope | Labels and Useful Values |
| --- | --- |
| Load worker | Under the supplied run counter group, `worker=<index>/load=actor/op=Writes` or `Reads`; requests, OK/error replies, bytes, bytes in flight, and `ResponseTimeUs` |
| Persistent tablet and DBG actors | `counters=load_actor/load=tablet`; lifecycle and operation counters, plus `dbg=<tablet-id>:<logical-dbg-id>` groups |

Multi-tablet local child counters have an additional `tablet=<target-index>` parent. The persistent counter root can be shared by several tablets on a node, so root gauges must not be mistaken for isolated per-tablet values. Use DBG labels for separation where available.

Tablet/DBG subgroups include:

- `subsystem=lifecycle` and `lifecycle_worker`: BSC allocation/deallocation results and peer connection activity. `DbgsAllocated` is published with lifecycle phase transitions: the allocated count after allocation and zero after deletion.
- `subsystem=lsns`: tracked states, configured budget/threshold, backpressure hits, and `SyncGateFlushBlocked` / `SyncGateEraseBlocked`. Both the root counters and the per-DBG counters increment only when a nonempty normal unadmitted ready set is below the threshold and that pass schedules no admitted work. They do not increment while admitted work is sent, during forced lifecycle or idle cleanup, or from a read-pin wakeup.
- `subsystem=op/operation=Write|Flush|Erase|ReadPB|ReadDDisk`: request/reply counts, pending work, latency, and batch or byte counts where applicable.
- `subsystem=request`: quorum and end-to-end LSN lifecycle observations.

Per-peer groups `subsystem=peers/peer=PB<n>|DD<n>` are enabled only when the allocated DBG count is below ten. Their connection, request, reply, and latency counters help diagnose imbalance. PB free-space-derived gauges should be checked against their update expressions when interpreting their direction.

The tablet HTML page shows phase, storage owner, DBG count, vChunk size, BSC retry information, and the peer roster. The load worker's last HTML report shows run totals and latency percentiles. The former RFC's dynamic runtime controls, detailed per-run live HTML, request-size histograms, speed-series UI, and persisted run history are not implemented by these actors.

The load proxies have no `TEvHttpInfo` handler. The general service's live HTML results path nevertheless forwards that event to running load actors, which can hit the strict-handler assertion in debug builds or leave an HTML request pending in release builds. Poll `mode=results` with `Accept: application/json` to query the service's completed-result cache without sending live HTML requests to the NBS actors. After completion, the stored HTML report is available.

## Failure Investigation

Use `BS_LOAD_TEST` logs with tablet/DBG identity, cookie, and LSN to correlate requests. Trace spans exist for load-worker write/read latency and per-DBG operations. A client write can complete successfully before DDisk copy or PB erasure, so compare request latency with the background operation and LSN counters.

| Symptom | Check |
| --- | --- |
| Create fails on node-count mismatch | `HostsPerDbg` versus the actual BSC pool allocation geometry |
| "Peers not ready" after Create | PB readiness prefix, DDisk connections, and the zero-ready fallback in the load proxy |
| Backpressure despite few client requests | Per-DBG share of the LSN budget, pending flush/erase state, and sync threshold |
| Count-limited run stops issuing but does not finish | Failed writes consumed the issued-write cap; use a duration bound |
| Allocation remains busy after restart | Persisted config without DBG rows restores `Allocating` without resuming it |
| Delete appears successful after BSC errors | The current explicit-error path can clear local state; inspect BSC allocation separately |
| Reconfiguration or deletion remains pending | Outstanding PB writes/reads, Sync/Erase retries, ambiguous replies, and final disconnect acknowledgements; the worker drain has no deadline |

Run one workload at a time per tablet. Client run completion does not force or wait for background flush/erase. The coalesced one-second idle cleanup can finish small tails naturally; subsequent configuration and deletion force ready admission and drain accepted work. An operation timeout cannot establish that storage I/O has retired. Do not infer full NBS crash recovery, repair, or handoff policy from this load model.
