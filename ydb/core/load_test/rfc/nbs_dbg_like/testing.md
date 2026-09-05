# Testing and Observability

This guide maps the implemented behavior to tests and diagnostics. See [Architecture](architecture.md) and [Workload](workload.md) for the code paths, and the [usage guide](../../../../docs/en/core/contributor/load-actors-nbs-dbg-like.md) for running workloads on a test cluster.

## Test Targets

| Target | Coverage |
| --- | --- |
| `ydb/core/load_test/ut` | Allocation request construction, deallocation with zero target vChunks, BSC response validation, and shared routing parameters |
| `ydb/core/blobstorage/ut_blobstorage/ut_ddisk` | End-to-end load-tablet lifecycle and I/O against the BlobStorage test environment |

The helper tests are in [nbs_dbg_like_alloc_helper_ut.cpp](../../ut/nbs_dbg_like_alloc_helper_ut.cpp), suite `NbsDbgLikeAllocHelper`. They cover response status/count/peer-count errors, all/subset DBG selection, and invalid I/O geometry.

The integration tests are in [nbs_dbg_like_load_tablet_ut.cpp](../../../blobstorage/ut_blobstorage/ut_ddisk/nbs_dbg_like_load_tablet_ut.cpp), suite `NbsDbgLikeLoadTablet`:

| Tests | Contract Exercised |
| --- | --- |
| `BasicSingleDbg`, `MultiDbg`, `SubsetOneOfTwoDbgs` | Allocation, run, and selected DBG prefixes |
| `GetSummaryReadyCounts` | Connected-PB readiness prefix |
| `RunBeforeCreate`, `DoubleCreate` | Lifecycle error responses |
| `CreateRestartRunDelete` | Recovery of a completed allocation after tablet restart |
| `WriteRead1000Blocks`, `WriteReadMultiDbg` | Direct request/response data-integrity checks |
| `MultiDbgSharedDDiskNoLsnCollision` | Multiple DBGs sharing storage without colliding LSNs |
| `RunRunDelete` | Reusing one allocation for consecutive runs |
| `MultiTablet`, `MultiTabletSharedBscTabletId` | Multiple load tablets and forced unique storage-owner IDs |

Use the repository's current build/test instructions with these targets and a suite or test filter. The tests do not require a separately provisioned production cluster.

When changing behavior, select tests by the contract affected. Coverage gaps include allocation interruption before DBG persistence, overlapping run/delete, remote multi-tablet dispatch, random-worker fanout above 512 concurrent operations, `TargetNumVChunks > 1`, disabled replication, and flush/erase tails under a closed sync gate. Do not treat the existence of the broad integration suite as coverage of those cases.

## Results and Counters

The load service enriches `TEvLoadTestFinished` from `TNbsDbgLikeFinishStats`. Final results expose separate write/read rates and latency percentiles; multi-tablet results carry a per-tablet breakdown. Local and remote histogram merges retain observations rather than averaging percentiles. Despite internal names such as `ReadPbUs` and `ReadsPbOk`, worker-level read statistics aggregate both PB and DDisk replies.

Two counter scopes describe different lifetimes:

| Scope | Labels and Useful Values |
| --- | --- |
| Load worker | Under the supplied run counter group, `worker=<index>/load=actor/op=Writes` or `Reads`; requests, OK/error replies, bytes, bytes in flight, and `ResponseTimeUs` |
| Persistent tablet and DBG actors | `counters=load_actor/load=tablet`; lifecycle and operation counters, plus `dbg=<tablet-id>:<logical-dbg-id>` groups |

Multi-tablet local child counters have an additional `tablet=<target-index>` parent. The persistent counter root can be shared by several tablets on a node, so root gauges must not be mistaken for isolated per-tablet values. Use DBG labels for separation where available.

Tablet/DBG subgroups include:

- `subsystem=lifecycle` and `lifecycle_worker`: BSC allocation/deallocation results and peer connection activity.
- `subsystem=lsns`: tracked states, configured budget/threshold, backpressure hits, and flush/erase gate-blocked counts.
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

Current operational constraints follow from these implementations: avoid overlapping runs or deletion during a run; do not assume client drain finishes PB cleanup; do not infer full NBS recovery or read consistency from this load model. Correctness changes to these behaviors require focused tests and corresponding updates to the owning local guide.
