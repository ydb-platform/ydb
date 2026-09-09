# Load Actor Architecture

See the [usage guide](../../../../docs/en/core/contributor/load-actors-nbs-dbg-like.md) for configuration and HTTP examples. This page describes ownership and persistence in the current implementation.

## Actor Ownership

```text
Load service
└── TNbsDbgLikeMultiLoadActor (only when Targets is nonempty)
    └── TNbsDbgLikeLoadActorProxy (one per target tablet)
        └── TNbsDbgLikeLoadActor (one or more load workers)
            └── tablet pipe → TNbsDbgLikeLoadTablet
                             └── TNbsDbgLikeActor (one per allocated DBG)
                                 ├── PB peers
                                 └── DDisk peers
```

The tree shows ownership down to load workers and from the tablet to per-DBG actors. The tablet-pipe arrow and the peer edges show communication: both the persistent tablet and the PB/DDisk services have independent lifetimes. Without `Targets`, the factory directly creates the per-tablet proxy. The factory, proxy, workers, and multi-tablet coordinator are in [nbs_dbg_like_load.cpp](../../nbs_dbg_like_load.cpp).

The per-tablet proxy queries allocation/readiness, validates workload geometry, sends one `TEvConfigureTablet`, and spawns load workers. It retains its configuration pipe until completion. Each worker opens its own pipe for I/O, owns its request cookies and latency measurements, and reports statistics back to the proxy.

For random load, the initial worker count is `ceil(MaxInFlight / 512)`, with a minimum of one. It is capped by the write-count target when nonzero and by the number of addressable I/O units. Sequential load uses one worker. The proxy divides concurrent-operation and successful-write budgets among workers with quotient/remainder splitting, so each per-tablet budget is preserved.

The persistent `TNbsDbgLikeLoadTablet` is a KeyValue-flat-derived tablet. It stores allocation metadata, creates the per-DBG actors, and routes I/O by flat address. Each `TNbsDbgLikeActor` owns one DBG's peer tokens, LSN map, queues, and request/response state. The per-DBG actors survive individual runs but are recreated when their tablet restarts.

The tablet forwards I/O while preserving the original sender, cookie, and payload. The per-DBG actor replies directly to that sender. `TEvNbsWrite`, `TEvNbsRead`, and their results support serialization and can cross nodes. The `Payload` rope on `TEvNbsWrite` is the sender-facing interface; the wrapper supplies `PayloadId` during serialization.

## Allocation and Identity

The HTTP helper resolves the tenant domain and Hive, creates a tablet with type `NbsLoadTablet`, and sends `TEvNbsLoadTabletAllocateGroups` over a pipe. The fixed Hive owner is `0xB1610AD`; `owner_idx` selects a tablet within that owner namespace. It is different from the Hive-assigned tablet ID used for I/O.

The tablet always sets `AllocConfig.TabletId = TabletID()`. User-supplied storage owner IDs cannot make two load tablets share a storage namespace. The current tablet generation supplies the session generation.

[BuildAllocateRequest](../../nbs_dbg_like_alloc_helper.cpp) sends one BSC query per DBG, with logical group IDs `0 .. NumDirectBlockGroups-1` and the requested vChunk count. Delete sends the same queries with `TargetNumVChunks = 0`. `HostsPerDbg` is checked against each response's node count, rather than transmitted as a pool-geometry setting. `ParseAllocateResult` accepts overall `OK` or `ALREADY` and requires the expected number of groups and peer pairs.

The shared meaning of BSC allocation, DDisk/PB pairing, capacity claims, and physical reclamation is documented in [Direct Block Groups](../../../../docs/en/core/contributor/distributed-storage/direct-block-groups.md). Do not implement a second allocator description here.

## Persistent State and Lifecycle

The load tablet's schema contains two tables:

| Table | Persisted Data |
| --- | --- |
| `State` | Serialized allocation configuration under one fixed key |
| `Dbgs` | DBG index, logical group ID, and protobuf-encoded DDisk/PB IDs |

There is no persisted `Runs` table. Workload configuration, LSN state, in-flight requests, peer tokens, and latency histograms are memory state.

```text
Uninitialized → Allocating → Ready → Deleting → Uninitialized
                                ↑
                         workload runs here
```

Create validates input, persists `State`, sends BSC allocation, then persists `Dbgs`. Only after the DBG transaction completes does it enter `Ready`, acknowledge Create, and spawn the per-DBG actors. Peer readiness therefore follows allocation readiness.

Create while `Ready` returns `NBSLT_ALREADY_INITIALIZED`. Create or Delete while allocating/deleting returns `NBSLT_BUSY`. Delete while uninitialized returns success. Runs do not create a separate tablet phase, and neither a run nor Delete acquires an exclusive-run guard.

Delete requests BSC deallocation, clears local configuration and DBG rows, and poisons per-DBG actors. The HTTP helper then asks Hive to delete the tablet. A direct `TEvNbsLoadTabletDelete` only performs the tablet's allocation cleanup; it does not delete the Hive object itself.

On BSC pipe failure the tablet retries with bounded exponential delay: initially 500 ms, capped at 10 seconds, with a retry limit of five. Allocation errors, malformed responses, and allocation retry exhaustion reset the in-memory phase to uninitialized without clearing the persisted configuration. Deletion retry exhaustion restores `Ready` so Delete can be retried. In contrast, an explicit BSC deallocation error is logged but still leads to local clearing. These paths do not establish that external allocation state has been removed.

## Boot and Peer Sessions

The tablet loads allocation and DBG rows in one transaction. It parses peer IDs as `TPersistedDbgIds`, drops unreadable rows, and resets to uninitialized on a gap in the DBG indices. Contiguous indices are required because routing and readiness use vector positions. Valid config plus nonempty DBG rows restores `Ready` and creates per-DBG actors.

Config with no DBG rows restores `Allocating`, but `OnLoadComplete` does not resume allocation. This interrupted-allocation case is a current recovery limitation. A ready tablet restart restores allocation, not unfinished workload requests or the PB-to-DDisk pipeline.

Each per-DBG actor connects to both services for each of its 3–5 peer pairs. Connection credentials include the tablet ID, tablet generation, and DBG index; DDisk also uses a session sequence number. Successful connects yield opaque connection tokens and instance GUIDs. Subsequent requests use the tokens; flush source descriptors also identify the PB instance. See the shared [DDisk contract](../../../../docs/en/core/contributor/distributed-storage/ddisk.md).

Readiness notifications report connected PB count to the tablet. `GetSummary` computes the longest prefix with at least three PB connections in each DBG. The load proxy uses a positive prefix, otherwise falls back to total allocation for compatibility. It then clamps the requested subset to this count. A summary response is a snapshot and does not fence later peer failures.

## Multi-Tablet Runs

`TNbsDbgLikeMultiLoadActor` creates one child run per target. A target on the coordinator's node, or with `NodeId = 0`, gets a directly registered child. A remote target is submitted to that node's load service using `TEvLoadTestRequest`. The coordinator tracks local children by actor ID and remote children by UUID; child tags also encode coordinator identity.

Each target receives the full workload configuration. Budgets are divided among workers within a tablet, but not among target tablets. Node placement is taken from the request and is not refreshed from Hive.

Local children return typed finish statistics. Remote children send `TEvNodeFinishResponse` with serialized latency histograms. The coordinator sums counts and merges histograms, using the maximum child measurement duration for combined rate calculation, and adds a per-tablet result array. Thus runs are approximately concurrent, not synchronized by a cross-node start barrier.

The coordinator has a timeout of workload duration plus initialization, drain, and 60 seconds of slack. A count-only run still has this finite safety timeout. Stopping the coordinator propagates stop requests to its children.

See [Workload](workload.md) for data flow and [Testing and Observability](testing.md) for validation and limitations.
