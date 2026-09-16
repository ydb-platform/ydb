# VDisk space usage report

The [VDisk](../concepts/glossary.md#vdisk) space usage report is an internal actor API for estimating the amount and distribution of space in chunks allocated to a single VDisk. A chunk is the fixed unit of space allocation on a [PDisk](../concepts/glossary.md#pdisk).

{% note warning %}

The API provides a weakly consistent monitoring view. Do not use it for correctness decisions, data placement, or operation execution. Statistics sources and indexes are sampled at different times, so a nonzero `ReconciliationDeltaBytes` and unclassified bytes are valid results.

{% endnote %}

## Important notes for API users {#important-notes}

Building the report is designed to have minimal impact on VDisk performance, but it still consumes actor-system resources. To reduce potential impact on user-workload processing, we recommend scanning no more than one VDisk in a group or ten VDisks on one node concurrently.

## Actor API {#actor-api}

The request and response events are declared in [vdisk_events.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/vdisk/common/vdisk_events.h):

- `TEvGetVDiskSpaceReportRequest` carries an empty protobuf message named `TGetVDiskSpaceReportRequest`;
- `TEvGetVDiskSpaceReportResponse` carries a `TGetVDiskSpaceReportResponse` protobuf message;
- the result schema is defined in [space_report.proto](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/vdisk/protos/space_report.proto).

Send the request directly to the service [ActorId](../concepts/glossary.md#actorid) of the target VDisk.

The `TEvGetVDiskSpaceReportResponse` response contains:

- `Status`: the request result, represented by the string name of an `NKikimrProto::EReplyStatus` value;
- `Report`: an optional structured report;
- `ErrorReason`: diagnostic error text.

Only one scan may run on a VDisk at a time. A concurrent request receives `TRYLATER` without a report. Retry such a request after a delay.

`Report` is optional, so check its presence independently of `Status`. An `ERROR` response may contain a partial report. The wording of `ErrorReason` is intended only for diagnostics and is not part of the API contract.

The request does not support cancellation, so the caller must set its own deadline. A VDisk restart does not restore an in-progress request. After the deadline expires, send a new request when the VDisk is ready again.

## Report structure {#report-structure}

`TVDiskSpaceReport` contains a global balance and a breakdown by subsystem. All sizes are measured in bytes.

### Global balance {#top-level-fields}

| Field | Meaning |
|---|---|
| `ChunkSizeBytes` | Size of one chunk on the current PDisk. |
| `PDiskAllocatedChunks` | Number of chunks allocated to the VDisk owner according to PDisk. |
| `PDiskAllocatedBytes` | Product of `PDiskAllocatedChunks` and `ChunkSizeBytes`. |
| `AccountedBytes` | Sum of all fields in `Total`. |
| `ReconciliationDeltaBytes` | Signed difference between `PDiskAllocatedBytes` and `AccountedBytes`. In the current implementation, the expected value is zero or negative. |
| `Total` | Combined classification of all report components. |

Use the following equalities when interpreting a result:

```text
PDiskAllocatedBytes = PDiskAllocatedChunks * ChunkSizeBytes
AccountedBytes = sum of the Total fields
ReconciliationDeltaBytes = PDiskAllocatedBytes - AccountedBytes
component AllocatedBytes = component ChunkCount * ChunkSizeBytes
```

### Components {#components}

Each regular component contains `ChunkCount`, `AllocatedBytes`, and `Breakdown`. In the table below, [LogoBlob](../concepts/glossary.md#logoblob) is a Hull blob record, [SST](../concepts/glossary.md#sst) (sorted string table) is an immutable sorted index segment, `Huge` is the large blob allocator, `SyncLog` is the synchronization log, and `ChunkKeeper` owns chunks for auxiliary subsystems.

| Component | Accounted content |
|---|---|
| `LogoBlobs` | LogoBlob SSTs and indexes, plus data stored inside Hull. Huge extents are accounted separately in `Huge`. |
| `Blocks` | SSTs and indexes for tablet-generation block records. |
| `Barriers` | Garbage collection barrier SSTs and indexes. |
| `Huge` | Huge allocator chunks, free reserve, and per-size-class slot statistics. |
| `SyncLog` | Active synchronization log chunks. |
| `ChunkKeeper` | Repeated entries grouped by ChunkKeeper subsystem identifier. Only committed chunk counts are currently known, so all their bytes are assigned to `UnclassifiedBytes`. |
| `Unattributed` | Remaining PDisk chunks that were not matched to any named component. All these bytes are assigned to `UnclassifiedBytes`. |

### Byte categories {#breakdown}

`TVDiskSpaceBreakdown` classifies physical space by semantic purpose.

At a high level, all allocated space can be grouped into the following categories:

- Useful data: current blob content that is not behind a garbage collection barrier;
- Metadata: current index and structural metadata;
- Garbage: data and metadata that can be reclaimed by garbage collection, merging, or compaction;
- Fragmentation: unused space inside allocated chunks;
- System data: current auxiliary-subsystem data that the scanner can classify; currently this is the used portion of SyncLog;
- Other: free reserves and capacity without a more specific classification.

| Field | Meaning | Suggested high-level category |
|---|---|---|
| `UsefulBlobDataBytes` | Payload of the selected current physical blob representation. | Useful data |
| `LiveMetadataBytes` | Metadata for current records and structural SST metadata. | Metadata |
| `LiveAuxiliaryDataBytes` | Current auxiliary data outside blobs. SyncLog used bytes are currently assigned here. | System data |
| `GcDeadBlobDataBytes` | Blob data that garbage collection barriers allow the system to remove. | Garbage |
| `GcDeadMetadataBytes` | Metadata that garbage collection barriers allow the system to remove. | Garbage |
| `MergeRedundantBlobDataBytes` | Old or duplicate physical data not required to preserve the logical value after a merge. | Garbage |
| `MergeRedundantMetadataBytes` | Old or duplicate metadata not required after a merge. | Garbage |
| `WritePaddingBytes` | Padding written for data alignment inside Hull or for aligning a Huge write to a PDisk write block. | Fragmentation |
| `SlotInternalFragmentationBytes` | Unused suffix of an occupied Huge slot after the aligned write. | Fragmentation |
| `FreeSlotBytes` | Free and unlocked Huge slots. | Fragmentation |
| `ChunkTailBytes` | Remaining capacity in allocated chunks not assigned to other categories. SyncLog reported free bytes are included here. | Other |
| `FreeChunkReserveBytes` | Free chunks held in the Huge allocator reserve. | Other |
| `LockedOrQuarantinedBytes` | Locked free Huge slots. | Other |
| `UnclassifiedBytes` | Bytes for which a safe semantic classification is unavailable. | Other |

### Huge size classes {#huge-size-classes}

`Huge.SizeClasses` describes the Huge allocator in more detail. Each entry contains the slot size, slots per chunk, chunk count, and counts of current, garbage collection dead, merge redundant, and unclassified slots. Its nested `Breakdown` classifies the full chunk capacity of the class into payload and metadata, alignment padding, internal fragmentation, free and locked slots, unclassified capacity, and chunk tails.

| Field | Meaning |
|---|---|
| `SlotSizeBytes` | Physical size of one slot in the class. |
| `SlotsPerChunk` | Number of class slots in one chunk. |
| `ChunkCount` | Number of chunks assigned to the class. |
| `LiveSlotCount` | Number of slots containing the selected current blob representation. |
| `GcDeadSlotCount` | Number of slots containing data removable by garbage collection. |
| `MergeRedundantSlotCount` | Number of slots containing redundant physical representations. |
| `UnclassifiedSlotCount` | Number of slots without a safe semantic classification. |
| `Breakdown` | Classification of the complete class chunk capacity by byte category. |

`Huge.FreeReserveChunks` is the current number of free Huge allocator chunks. These chunks are also included in `Huge.Total` through `FreeChunkReserveBytes`.

With complete counters, the sum of `LiveSlotCount`, `GcDeadSlotCount`, `MergeRedundantSlotCount`, and `UnclassifiedSlotCount` equals the number of allocated slots. Free and locked-free slots appear only as bytes in `Breakdown`. If the allocator does not describe every slot, the missing slots are added to `UnclassifiedSlotCount`. If size-class validation fails, the other three counters are reset to zero and `UnclassifiedSlotCount` contains the total number of slots in the class.

If allocator counters contradict semantic classification, the entire size class is assigned to `UnclassifiedBytes`. This preserves its physical capacity without publishing an unreliable category split.

## How the report is built {#scan-algorithm}

The scanner actor performs the work in stages so that it does not retain one Hull snapshot or block an actor-system thread for a long time.

1. It requests allocated chunk counts from PDisk and compact statistics from HugeKeeper, SyncLog, and ChunkKeeper.
2. It waits for source responses for up to 10 seconds. PDisk statistics are mandatory, so a missing PDisk response produces no report. An auxiliary source timeout allows scanning to continue with a partial result.
3. It scans the `LogoBlobs`, `Blocks`, and `Barriers` metabases sequentially. A new Hull snapshot is acquired before every quantum.
4. For each key, it visits every physical record, merges the logical value, and applies garbage collection barriers. The selected physical representation is useful, removable records are classified as `GcDead*`, and the remaining versions are `MergeRedundant*`.
5. At the end of a quantum, it stores the traversal position, destroys the snapshot, and continues in another quantum when necessary.
6. It combines the Hull estimates with allocator statistics and calculates the global balance.

The target scan quantum is 5 milliseconds, followed by a scheduled 10 millisecond delay. Time is checked after processing a complete key, so one large key may extend a quantum. Total scan duration is not bounded and depends on index size.

The scanner traverses metabases in descending key order and resumes below the last processed key between quanta. Consequently, keys inserted above the saved boundary do not extend the current scan, but they are omitted from the report and reduce its accuracy. Keys inserted below the boundary may still be observed.
