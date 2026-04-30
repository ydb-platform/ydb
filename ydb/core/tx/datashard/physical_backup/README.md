# Physical SST Backup/Restore for YDB DataShards

## Problem

The current S3 export has two major bottlenecks for large tables (e.g. 400TB, 200K shards):

1. **CopyTables snapshot** -- requires global coordination across all shards via
   `ESchemeOpCreateConsistentCopyTables`. Each source datashard must take a snapshot
   (`MakeSnapshotUnit`) and send data to a newly created destination datashard. This
   triggers a memtable compaction task (`compaction_gen0`) per shard, all competing
   for a per-node resource broker queue (default CPU limit: 10). With 800 shards per
   node, the queue becomes a severe bottleneck. Additionally, the borrow mechanism
   prevents GC of old SST parts on source shards, effectively doubling storage usage
   for the duration of the export.

2. **CSV restore** -- the current export produces `data_NN.csv` files (one per shard)
   containing comma-separated row data. Restore requires parsing CSV, re-inserting
   rows through the full datashard write pipeline (WAL, replication, compaction).
   This is orders of magnitude slower than raw download speed.

## Design Goals

- **No global snapshot** -- each shard independently exports its own SST parts
- **No CopyTables step** -- eliminates borrow mechanism and storage doubling
- **Optional memtable flush** -- with `--snapshot=false`, export reads only existing
  SST parts on disk with zero compaction cost (may miss unflushed memtable data).
  With `--snapshot=true`, flush memtable first for complete data capture.
- **Restore at near-download speed** -- load raw SST parts directly into the tablet
  executor via `LoanTable`/`Database->Merge()`, bypassing the row-level write pipeline
- **Per-shard consistency** -- each shard's export is a point-in-time view; different
  shards may be at different timestamps
- **Disaster recovery** -- full table restore to the same or different YDB cluster

## Key Insight

The `BorrowSnapshot` mechanism (`flat_executor.cpp`) already serializes SST parts as
`TDatabaseBorrowPart` protos containing `TBundle` entries with `LargeGlobId`s,
`Opaque` (slices), and `Epoch`. The receiving side (`LoanTable` in
`flat_executor_tx_env.h`) deserializes and merges parts via `Database->Merge()`.

This design reuses the same wire format but routes it through S3 instead of
inter-tablet ReadSets.

## Architecture

### Export Flow (Per-Shard, Independent)

```
SchemeShard initiates export
  -> For each shard (independently, no CopyTables):
     1. DataShard receives ExportPhysical command
     2. If snapshot=true: DataShard takes a LOCAL snapshot (TxSnapTable, advances epoch)
        - Single tablet, no cross-shard coordination
        - Resource broker: 1 compaction task in queue_compaction_gen0
        - No borrow mechanism, no storage doubling
        If snapshot=false: Skip memtable flush entirely (zero compaction cost)
        - Only existing SST parts on disk are exported
        - Unflushed memtable data is not included
     3. DataShard enumerates parts via txc.DB.EnumerateTableParts(localTid)
     4. For each TPartView:
        a. Serialize TBundle proto (PageCollections + Opaque/Slices + Epoch)
        b. Collect all blob IDs via SaveAllBlobIdsTo()
        c. Fetch raw blob contents from blobstorage (TEvBlobStorage::TEvGet)
        d. Upload to S3: metadata + blob data
     5. Upload shard manifest (schema, key range, part list, MVCC edges)
     6. Drop local snapshot
     7. Report completion to SchemeShard
```

### S3 File Structure

```
<prefix>/
+-- export_metadata.pb             # Global: table schema, partition config, timestamp
+-- shard_000000/
|   +-- manifest.pb                # Shard metadata: key range, MVCC edges, part list
|   +-- parts/
|   |   +-- part_0.bundle.pb       # TBundle proto (LargeGlobIds, Slices, Epoch)
|   |   +-- part_0.blobs.bin       # Concatenated raw blob data
|   |   +-- part_0.blobs.index     # Blob ID -> offset/size index
|   |   +-- part_1.bundle.pb
|   |   +-- part_1.blobs.bin
|   |   +-- ...
|   +-- txstatus/
|       +-- txstatus_0.pb          # TxStatus parts
+-- shard_000001/
|   +-- ...
+-- shard_199999/
    +-- ...
```

### Restore Flow (Per-Shard, Near-Download Speed)

```
1. Read export_metadata.pb -> recreate table with schema + partition config
2. For each shard (independently, in parallel):
   a. Download manifest.pb -> get key range, MVCC edges, part list
   b. For each part:
      - Download part_N.bundle.pb + part_N.blobs.bin
      - Write blobs to blobstorage (TEvBlobStorage::TEvPut to shard's groups)
      - Reconstruct TPartComponents from bundle proto
   c. Inside a datashard transaction:
      - Call txc.Env.LoanTable(localTid, reconstructedBorrowPart)
      - This triggers the existing part switch pipeline:
        PrepareExternalPart -> LoadMeta -> TLoader -> ApplyExternalPartSwitch
      - Parts are merged into the table via Database->Merge()
   d. Set MVCC edges from manifest
   e. Shard is ready to serve
```

### Why Restore is Fast

The existing `LoanTable` -> `PrepareExternalPart` -> `ApplyExternalPartSwitch` path:

1. **No row parsing** -- parts are loaded as opaque page collections
2. **No WAL** -- parts are added via `Database->Merge()`, not the write pipeline
3. **No compaction during load** -- parts are already compacted SST files
4. **No replication overhead** -- parts are directly placed, followers catch up via log
5. **Parallel per-shard** -- each shard loads independently, no coordination

### Restore Bottleneck: Blobstorage Writes

The restore bottleneck is writing blobs into blobstorage via `TEvBlobStorage::TEvPut`.
Unlike export (which just reads existing blobs), restore must create new blobs in
the distributed storage layer. With block-4-2 erasure, each blob is written to 8
VDisks across 8 storage nodes. This is the same replication cost as any normal write
to YDB -- it is the unavoidable price of fault-tolerant distributed storage.

Export is asymmetrically fast because blobs already exist -- `TEvGet` reads from a
single VDisk. Restore must replicate to 8 VDisks per blob.

### Performance Comparison: Physical vs CSV Restore

Assuming a 400TB table where downloading from S3 takes 1 hour:

| Step | Physical Restore | CSV Restore (current) |
|------|------------------|-----------------------|
| Download from S3 | 1h | 1h |
| Parse data | 0 (raw blobs) | 10-20h (CSV text parsing for billions of rows) |
| Write to storage | 2-4h (TEvPut, 8x replication) | 2-4h (same replication, via WAL + compaction) |
| WAL / transaction overhead | 0 (LoanTable bypasses WAL) | 20-40h (per-row transaction, WAL, MVCC) |
| Compaction | 0 (parts already compacted) | 10-30h (compaction storm from bulk inserts) |
| Index rebuilding | 0 (indexes in SST parts) | Hours (rebuilt from scratch) |
| **Total (pipelined)** | **~3-5h** | **~50-100h+ (days)** |

Physical restore achieves ~10-20x speedup over CSV restore. The remaining time
(2-4h for blobstorage writes) is the irreducible cost of erasure-coded distributed
storage. The only way to reduce it further would be to lower the replication factor
(e.g., mirror-3-dc with 3 copies instead of block-4-2 with 8), which is a
cluster-level durability tradeoff.

## Components

### 1. Physical Export Unit (`physical_export_unit.cpp`)

New datashard execution unit replacing `TBackupUnit` for physical exports:

- Runs inside the datashard's tablet executor
- Takes a local snapshot via `txc.Env.MakeSnapshot()` (single tablet, not coordinated)
- Enumerates parts: `txc.DB.EnumerateTableParts(localTid, callback)`
- For each `TPartView`:
  - Serializes `TBundle` via `TPageCollectionProtoHelper::Do()`
  - Collects blob IDs via `partStore->Packet(room)->SaveAllBlobIdsTo()`
  - Fetches raw blob data from blobstorage
  - Streams to S3 uploader

Key references:
- `flat_executor.cpp:4159-4199` -- BorrowSnapshot serialization pattern
- `flat_store_hotdog.cpp:54-77` -- TBundle serialization
- `export_s3_uploader.cpp` -- reusable S3 multipart upload logic

### 2. Physical Restore Unit (`physical_restore_unit.cpp`)

New actor that downloads from S3 and injects parts:

- Downloads manifest + bundle protos + blob data from S3
- Writes blob data to the shard's blobstorage groups via `TEvBlobStorage::TEvPut`
- Constructs `TDatabaseBorrowPart` proto (same format as BorrowSnapshot output)
- Calls `txc.Env.LoanTable()` inside a datashard transaction
- The existing executor machinery handles the rest

Key references:
- `flat_executor_tx_env.h:242-263` -- LoanTable mechanism
- `build_scheme_tx_out_rs_unit.cpp` -- BorrowSnapshot data handling
- `flat_executor.cpp:1581-1677` -- ApplyExternalPartSwitch

### 3. SchemeShard State Machine

Simplified export state machine (no CopyTables):

```
Export: CreateExportDir -> Transferring (per-shard physical export) -> Done
Import: CreateTable -> Transferring (per-shard physical restore) -> Done
```

### 4. Metadata

Per-shard manifest includes:
- `TUserTable` metadata: `Range` (key boundaries), `Columns`, `KeyColumnTypes`, `Families`
- Per-part: `TBundle` proto, blob ID list, `Epoch`, `Slices`
- MVCC state: `MinWriteVersion`, `CompleteEdge`, `IncompleteEdge`, `ImmediateWriteEdge`
- `TxStatus` parts
- Shard-level stats (row count, data size)

## Trade-offs

### What You Gain
- No CopyTables bottleneck (no 200K-shard coordinated snapshot)
- Restore at near-download speed (~10-100x faster than CSV re-ingestion)
- Per-shard parallelism (no coordinator involvement)
- No storage doubling during export

### What You Lose
- No cross-shard consistency (different shards may be at different timestamps)
- YDB-internal format (only YDB can restore; not human-readable)
- Version coupling (SST format may change between YDB versions)
- Blob placement dependency (restore must match destination storage group layout)

## Open Questions

1. **Memtable data** -- `EnumerateTableParts` only returns flushed SST parts. With
   `--snapshot=true`, the memtable is flushed first. With `--snapshot=false`, unflushed
   data is skipped -- acceptable for DR of mostly-static or recently-compacted tables.
2. **Active writes during export** -- snapshot epoch boundary ensures we only export
   pre-snapshot parts, but new parts may appear during upload.
3. **Cold parts** -- `TColdPartStore` has metadata only; blob data must be fetched on
   demand during export.
4. **Cross-version restore** -- `TPartScheme` includes column types and layout. Parts
   from a different YDB version may need conversion.
