# VDisk space usage report

The [VDisk](../concepts/glossary.md#vdisk) space usage report estimates how the space allocated to one VDisk is distributed between data, metadata, garbage, fragmentation, and auxiliary subsystems. A chunk is the fixed unit of space allocation on a [PDisk](../concepts/glossary.md#pdisk).

The report is available through the VDisk monitoring page, an internal actor API, and dynamic counters.

{% note warning %}

The report is not a consistent snapshot. Its statistics sources and indexes are read at different times and can describe different VDisk states. Treat every value as a monitoring estimate: do not expect totals from different sections or repeated requests to match. A nonzero `ReconciliationDeltaBytes` and unclassified bytes are valid results. Do not use the report for correctness decisions, data placement, or operation execution.

{% endnote %}

## Operational guidance {#operational-guidance}

Returning a cached report is inexpensive, but recalculating it scans the VDisk indexes. To limit the impact on user workloads, do not force concurrent recalculations for more than one VDisk in a group or ten VDisks on one node. Prefer cached reports or configure periodic collection when reports are needed regularly.

Only one recalculation runs on a VDisk at a time. Concurrent forced requests join the active recalculation and receive the same result.

## Monitoring page {#monitoring-page}

Append one of the following query strings to the target VDisk monitoring-page URL:

- `?type=spacereportvisual` displays the cached report as tables and space-distribution bars;
- `?type=spacereport` returns the protobuf text representation;
- add `&force=1` to either form to wait for a newly calculated report, for example `?type=spacereportvisual&force=1`.

The visual page also provides **Raw Proto** and **Recalculate** links. The monitoring request has a one-minute timeout. Recalculation continues on the VDisk if the HTTP client disconnects or the monitoring request times out.

Without `force=1`, the page reads the cache and does not start a recalculation. Before the first successful collection, the response has the `NOTREADY` status and no report.

## Collection and cache {#collection-cache}

The `VDiskControls.SpaceReportPeriodSeconds` immediate control sets the periodic collection interval in seconds. Its range is 0 through 86400, and its default value is 0. Zero disables periodic collection; reports can still be produced by forced requests. The initial collection is randomly distributed across the configured interval, and subsequent collections use jitter to avoid synchronized scans by all VDisks on a node.

A successful collection replaces the cached report and publishes its counters. A failed collection leaves the previous successful report and its data counters intact. Operational counters still record the failed attempt.

## Actor API {#actor-api}

The request and response events are declared in [vdisk_events.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/vdisk/common/vdisk_events.h):

- `TEvGetVDiskSpaceReportRequest` carries a `TGetVDiskSpaceReportRequest` protobuf message;
- `TEvGetVDiskSpaceReportResponse` carries a `TGetVDiskSpaceReportResponse` protobuf message;
- the message schema is defined in [space_report.proto](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/vdisk/protos/space_report.proto).

Send the request directly to the service [ActorId](../concepts/glossary.md#actorid) of the target VDisk.

`TGetVDiskSpaceReportRequest` has one field:

| Field | Meaning |
|---|---|
| `ForceRecalculation` | If `false`, return the latest cached report immediately or `NOTREADY` if the cache is empty. This does not start collection. If `true`, start a collection or join the one already running and reply when that attempt finishes. |

`TGetVDiskSpaceReportResponse` contains:

| Field | Meaning |
|---|---|
| `Status` | String name of an `NKikimrProto::EReplyStatus` value. |
| `Report` | Optional structured report. Check its presence independently of `Status`. |
| `ErrorReason` | Diagnostic text. Its wording is not part of the API contract. |

A successful response has the `OK` status and a report. If an auxiliary statistics source fails or times out, a forced request can receive `ERROR` with a partial report. If the mandatory PDisk statistics are unavailable, the response does not contain a report. Failed or partial results are not cached.

The caller must set its own deadline because the actor request has no cancellation protocol. A collection has a 30-minute watchdog. Restarting a VDisk does not restore an in-progress request; retry after the VDisk becomes ready.

## Report structure {#report-structure}

`TVDiskSpaceReport` contains a global balance and a breakdown by subsystem. All sizes are in bytes.

### Global balance {#top-level-fields}

| Field | Meaning |
|---|---|
| `ChunkSizeBytes` | Size of one chunk on the current PDisk. |
| `PDiskAllocatedChunks` | Number of chunks allocated to the VDisk owner according to PDisk. |
| `PDiskAllocatedBytes` | Product of `PDiskAllocatedChunks` and `ChunkSizeBytes`. |
| `AccountedBytes` | Sum of all fields in `Total`. |
| `ReconciliationDeltaBytes` | Signed difference between `PDiskAllocatedBytes` and `AccountedBytes`. A positive value is unaccounted space; a negative value is space counted in excess. Because the inputs are sampled separately, a nonzero value is expected and zero does not make the report a consistent snapshot. |
| `Total` | Combined byte classification of all report components. |
| `CollectionStartedAtUnixMs` | Collection start time as Unix time in milliseconds. |
| `CollectionCompletedAtUnixMs` | Collection completion time as Unix time in milliseconds. Use it to determine cache age. |

Use the following relationships when interpreting a result:

```text
PDiskAllocatedBytes = PDiskAllocatedChunks * ChunkSizeBytes
AccountedBytes = sum of the Total fields
ReconciliationDeltaBytes = PDiskAllocatedBytes - AccountedBytes
component AllocatedBytes = component ChunkCount * ChunkSizeBytes + component StripedBytes
```

### Components {#components}

Each regular component contains `ChunkCount`, `StripedBytes`, `AllocatedBytes`, and `Breakdown`. `ChunkCount` includes only chunks dedicated to that component. `StripedBytes` contains the component's extents in shared stripe chunks.

In the table below, [LogoBlob](../concepts/glossary.md#logoblob) is a Hull blob record, [SST](../concepts/glossary.md#sst) is an immutable sorted index segment, `Huge` is the large-blob allocator, `SyncLog` is the synchronization log, and `ChunkKeeper` owns chunks for auxiliary subsystems.

| Component | Accounted content |
|---|---|
| `LogoBlobs` | LogoBlob SSTs and indexes, plus blob data stored in Hull. Huge extents are accounted separately in `Huge`. |
| `Blocks` | SSTs and indexes for tablet-generation block records. |
| `Barriers` | Garbage collection barrier SSTs and indexes. |
| `Huge` | Dedicated Huge allocator chunks, its extents in shared stripe chunks, free reserve, and per-size-class slot statistics. |
| `SyncLog` | Active synchronization log chunks. |
| `ChunkKeeper` | Repeated entries grouped by ChunkKeeper subsystem identifier. Only committed chunk counts are known, so all their bytes are assigned to `UnclassifiedBytes`. |
| `Unattributed` | Remaining PDisk chunks that were not matched to any named component. All these bytes are assigned to `UnclassifiedBytes`. |
| `StripeHeap` | Allocator summary for the shared stripe chunks: `ChunkCount`, `AllocatedBytes`, `UsedBytes`, `FreeBytes`, and `LockedFreeBytes`. This is not an additional component: the same capacity is distributed through regular components' `StripedBytes` and breakdowns. |

Every `ChunkKeeper` entry identifies its owner in `SubsystemId` and stores the corresponding regular component in `Total`.

### Byte categories {#breakdown}

`TVDiskSpaceBreakdown` classifies physical space by semantic purpose.

| Field | Meaning | Suggested high-level category |
|---|---|---|
| `UsefulBlobDataBytes` | Payload of the selected current physical blob representation. | Useful data |
| `LiveMetadataBytes` | Metadata for current records and structural SST metadata. | Metadata |
| `LiveAuxiliaryDataBytes` | Current auxiliary data outside blobs. SyncLog used bytes are assigned here. | System data |
| `GcDeadBlobDataBytes` | Blob data that garbage collection barriers allow the system to remove. | Garbage |
| `GcDeadMetadataBytes` | Metadata that garbage collection barriers allow the system to remove. | Garbage |
| `MergeRedundantBlobDataBytes` | Old or duplicate physical data not required to preserve the logical value after a merge. | Garbage |
| `MergeRedundantMetadataBytes` | Old or duplicate metadata not required after a merge. | Garbage |
| `WritePaddingBytes` | Padding written for data alignment inside Hull or for aligning a Huge write to a PDisk write block. | Fragmentation |
| `SlotInternalFragmentationBytes` | Unused suffix of an occupied Huge slot after the aligned write. | Fragmentation |
| `FreeSlotBytes` | Free and unlocked Huge slots. | Fragmentation |
| `FreeStripeBytes` | Free capacity in shared stripe chunks. | Fragmentation |
| `ChunkTailBytes` | Remaining capacity in dedicated chunks not assigned to another category. SyncLog reported free bytes are included here. | Other |
| `FreeChunkReserveBytes` | Free chunks held in the Huge allocator reserve. | Other |
| `LockedOrQuarantinedBytes` | Locked free Huge slots and locked free stripe capacity. | Other |
| `UnclassifiedBytes` | Bytes for which a safe semantic classification is unavailable. | Other |

### Shared stripe chunks {#stripe-heap}

Hull can place SST extents and Huge data in the same physical stripe chunk. Such a chunk appears once in `StripeHeap.ChunkCount`; it is not included in the `ChunkCount` of `LogoBlobs`, `Blocks`, `Barriers`, or `Huge`.

The report attributes used extents to the owning component through `StripedBytes`. Free stripe space is assigned to `Huge.Breakdown.FreeStripeBytes`, locked free space to `Huge.Breakdown.LockedOrQuarantinedBytes`, and any remainder that cannot be classified to `Huge.Breakdown.UnclassifiedBytes`. When stripe statistics are available, the builder assigns the entire stripe capacity, so the sum of `StripedBytes` for `LogoBlobs`, `Blocks`, `Barriers`, and `Huge.Total` equals `StripeHeap.AllocatedBytes` within that report.

`StripeHeap.UsedBytes`, `FreeBytes`, and `LockedFreeBytes` are an allocator summary. Do not add them to `AccountedBytes`: the stripe-heap capacity is already represented in the component breakdowns.

### Huge size classes {#huge-size-classes}

`Huge.SizeClasses` describes dedicated Huge allocator chunks. Striped Huge data is not part of these size classes. Each entry contains the slot size, slots per chunk, chunk count, slot-state counters, and a `Breakdown` of the complete class capacity.

| Field | Meaning |
|---|---|
| `SlotSizeBytes` | Physical size of one slot in the class. |
| `SlotsPerChunk` | Number of class slots in one chunk. |
| `ChunkCount` | Number of dedicated chunks assigned to the class. |
| `LiveSlotCount` | Number of slots containing the selected current blob representation. |
| `GcDeadSlotCount` | Number of slots containing data removable by garbage collection. |
| `MergeRedundantSlotCount` | Number of slots containing redundant physical representations. |
| `UnclassifiedSlotCount` | Number of slots without a safe semantic classification. |
| `Breakdown` | Classification of the complete class chunk capacity by byte category. |

`Huge.FreeReserveChunks` is the current number of free Huge allocator chunks. These chunks are also included in `Huge.Total` through `FreeChunkReserveBytes`.

If the allocator counters and the index scan happened to describe the same VDisk state, the sum of `LiveSlotCount`, `GcDeadSlotCount`, `MergeRedundantSlotCount`, and `UnclassifiedSlotCount` would equal the number of non-free slots. The sources are sampled separately, so do not expect this relationship in every report. Free and locked-free slots appear only as bytes in `Breakdown`. Slots that the allocator does not describe are added to `UnclassifiedSlotCount`. If allocator counters contradict semantic classification, the entire size class is assigned to `UnclassifiedBytes` to preserve physical capacity without publishing an unreliable split.

## Dynamic counters {#dynamic-counters}

The latest successful report is exported under the `subsystem=vdisk_space_report` subgroup of each VDisk's counters.

| Counter group | Contents |
|---|---|
| Root | `ChunkSizeBytes`, `PDiskAllocatedChunks`, `PDiskAllocatedBytes`, `AccountedBytes`, and signed `ReconciliationDeltaBytes`. `UnaccountedBytes` and `OveraccountedBytes` expose the positive and negative sides of the delta as nonnegative values. |
| `scope=total` | All `TVDiskSpaceBreakdown` fields for the complete report. |
| `component=inplace_blobs`, `blocks`, `barriers`, `huge_blobs`, `sync_log`, `unattributed` | `ChunkCount`, `AllocatedBytes`, `StripedBytes`, `AccountedBytes`, and all breakdown fields. |
| `scope=stripe_heap` | Stripe-heap chunk, allocated, used, free, and locked-free byte counters. |
| `slot_size_bytes=<size>` | Dedicated Huge size-class counters and breakdown. |
| `chunk_keeper_subsystem=<id>` | ChunkKeeper subsystem counters and breakdown. |

Operational counters at the root include `RefreshInProgress`, `LastAttemptSuccessful`, `RefreshSuccesses`, `RefreshFailures`, `PeriodicTicksSkipped`, `ColdCacheRequests`, `LastRefreshDurationMs`, `LastRefreshCpuTimeUs`, `LastRefreshQuanta`, `LastRefreshVisitedKeys`, and `LastRefreshPhysicalRecords`.

## How the report is built {#scan-algorithm}

The collection worker runs in the VDisk batch pool and releases Hull snapshots between short scan quanta:

1. It requests allocated chunk counts from PDisk, allocator and stripe-chunk statistics from HugeKeeper, and compact statistics from SyncLog and ChunkKeeper.
2. It waits up to 10 seconds for the sources. PDisk statistics are mandatory. Missing auxiliary statistics produce an `ERROR` result with a partial report.
3. It scans the `LogoBlobs`, `Blocks`, and `Barriers` metabases sequentially. A new Hull snapshot is acquired before every quantum.
4. For each key, it visits every physical record, merges the logical value, and applies garbage collection barriers. The selected physical representation is useful, removable records are classified as `GcDead*`, and remaining versions are `MergeRedundant*`.
5. At the end of a quantum, it saves the traversal position, destroys the snapshot, and continues after a delay when necessary.
6. It combines Hull estimates with dedicated Huge slots, shared stripe chunks, SyncLog, ChunkKeeper, and the PDisk allocation.
7. On success, the manager caches the report and publishes dynamic counters.

The target scan quantum is 5 milliseconds, followed by a scheduled 10 millisecond delay. Time is checked after processing a complete key, so one large key can extend a quantum. The manager aborts an attempt that exceeds the 30-minute watchdog.

The scanner traverses metabases in descending key order and resumes below the last processed key between quanta. Keys inserted above the saved boundary do not extend the current scan, but they are omitted from the report. Keys inserted below the boundary may still be observed. The report is therefore not consistent: values and totals from different sections can describe different VDisk states and are not required to match.
