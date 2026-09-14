# PersistentBuffer

A PersistentBuffer (PB) stores durable staging records for direct block clients. The client can read a record, replicate it to other PBs, ask a DDisk to pull it into final block storage, and erase it once its own durability policy permits. PB implements these primitives; a partition or load actor supplies the group policy.

PB is the child of a [DDisk](ddisk.md), implemented by `TDDiskActor` with a separate state function. Its service ID is independently addressable. Its storage chunks and PDisk owner are managed through the parent; see [{#T}](node-warden.md).

## Record Identity

A PB record is identified by `(TabletId, Generation, Lsn, DirectBlockGroupIndex)`. Its metadata records the destination virtual chunk, byte offset, size, sector locations, and optional payload checksums. `DirectBlockGroupIndex` is stored as `ui8` and must be in `0..255`; omitted indexes use namespace zero.

The log sequence number (LSN) is assigned by the client. It is a record identifier within the namespace, not a physical PB offset or a PDisk log LSN. An identical same-key write is idempotent and returns `OK`. A conflicting selector returns `INCORRECT_REQUEST`; conflicting payload data also returns `INCORRECT_REQUEST` when the stored checksums allow PB to detect the difference. Writes at or behind a persisted erase barrier return `OUTDATED`. A client must not use one LSN for different writes and expect overwrite semantics.

{% note info %}

Duplicate-payload conflict detection is under development. For a record written in the checksum-free on-disk format without payload checksums, PB currently compares only the selector metadata (`VChunkIndex`, `OffsetInBytes`, and `Size`). A same-key write with the same selector but different payload bytes can therefore be accepted as a duplicate and return `OK`. For any task that relies on or changes this behavior, inspect the latest `PreprocessPersistentBufferWrite` implementation and its focused tests before drawing conclusions.

{% endnote %}

The PB namespace contains a generation, while its erase barrier spans generations for one `(TabletId, DirectBlockGroupIndex)`. A client recovering an older generation can read or erase its records using the operation's record-generation field where available, while authenticating through its current session.

## Write, Read, and Replicate

`TEvWritePersistentBuffer` writes one record to one PB. The payload helpers in [ddisk.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk.h) construct event payload references and checksum fields. With checksum validation enabled, the PB rejects a mismatching payload before allocation or I/O.

The current record format allows at most 128 data sectors, or 512 KiB at 4 KiB per sector. Small writes of up to eight sectors can be batched under one header sector, subject to metadata capacity. Acknowledgment follows the corresponding disk operation, not merely insertion into the in-memory map. A record's data may stay in the bounded memory cache; a cold read issues I/O against the recorded sector locations and reconstructs the original payload. `TEvReadPersistentBuffer` can return the whole record or a requested part with the corresponding checksums.

`TEvWritePersistentBuffers` delegates fan-out to a coordinator PB. Its `TWritePersistentBuffersRequestActor` sends a single-record write to each explicit destination and aggregates their statuses. The caller remains responsible for quorum and retries.

The reply timeout is an aggregation deadline, not cancellation. At that deadline the coordinator reports the destinations that have replied so far, potentially none. Outstanding operations remain tracked; once their results arrive, a later response can carry additional destination results. Clients must merge results by destination and request identity and must not assume exactly one response per plural-write request.

`TEvReadThenWritePersistentBuffers` first reads a record from the coordinator's PB and then sends its payload and persisted checksums to the destination list. This supports client-directed re-replication without routing the data through the client.

For PB-to-DDisk transfer, send the destination DDisk a `TEvSync` with PB source segments. The PB read is part of that DDisk's sync operation; no PB write acknowledgment or sync result automatically advances the erase barrier.

## Erase and Barriers {#erase}

| Mechanism | Contract and Implementation |
|---|---|
| `TEvBatchErasePersistentBuffer` | Erases explicitly named `(Generation, Lsn)` records in the caller's DBG namespace. It can use a compact fast-erase record when generation and capacity constraints permit, or fall back to per-record invalidation. |
| Fast erase | Persists a compact LSN set, then reclaims the selected records. This avoids one data-sector write per erased record but needs room for its own metadata. |
| Per-record invalidation | Invalidates the first data sector of each record. A batched header may be shared by live records, so erasing one record must not invalidate that shared header. |
| `TEvErasePersistentBuffer` | Advances a durable cutoff barrier for `(TabletId, DirectBlockGroupIndex)`. The barrier orders `(Generation, Lsn)`: older generations and records through the cutoff in the barrier generation become obsolete. |

Barrier and fast-erase updates write replacement metadata before releasing the old metadata and data sectors. `MinFreeSectorsReserve` keeps space for that progress; admitting writes until every sector is occupied can prevent reclamation itself.

Erase is persistent state, including for records that are no longer present in the in-memory map. Recovery must apply barriers and fast erases before publishing the live record set so that old disk contents do not reappear. Exact erases can be idempotent when their named records are already absent.

The client decides which records are safe to erase. For example, a partition may wait for all required data replicas and replacement PB copies; those conditions are outside the generic PB handler. A cutoff is particularly strong because it retires a prefix and older generations, so it must not be derived from an arbitrary completed write if earlier writes are still needed.

## Integrity Formats {#integrity}

There are two related settings with different responsibilities:

- `TDDiskConfig::EnableChecksums` controls payload checksum validation and forwarding through the DDisk/PB interface. Wire checksums are pure XXH3-64 values per 4 KiB block.
- `TPersistentBufferFormat::EnableChecksums` controls the PB on-disk integrity format. Checksummed records store sector/header integrity information. The checksum-free format stores a record header unique ID in data sectors and preserves their original prefix bytes in metadata.

The on-disk header flags identify the format of each record, so recovery and reads interpret an existing record according to its persisted format rather than the current setting alone. Sender payload checksums are an additional optional metadata array identified by `HAS_PAYLOAD_CHECKSUMS`; do not confuse them with the PB sector checksums or with DDisk's separate integrity chunks.

Header layout, signature correction, checksummed content length, and packed-record capacity are defined in [persistent_buffer_header.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/persistent_buffer_header.h). A format change must preserve how replay distinguishes valid headers, data sectors, barriers, and erase records.

## Recovery, Listing, and Limits

The DDisk parent restores PB chunk ownership and the PB unique ID before creating the child. The PB scans the owned chunks, validates headers and data references, applies barriers and erases, rebuilds allocation metadata, and publishes recovered records. Requests that need the recovered state can wait in the bounded pending queue; exceeding that queue returns an overload result.

`TEvListPersistentBuffer` is the client's recovery view of PB records. Listing is deferred while relevant disk operations are in flight for the tablet, so it does not deliberately expose a partially applied write or erase. `ListPersistentBufferMaxRetries` and `ListPersistentBufferRetryPeriodMilliseconds` bound this wait; exhaustion returns an overload result rather than an authoritative empty record list.

PB limits include allocated chunks, per-tablet storage, cache memory, pending requests, and the reserve needed for erases. Proactive allocation and deallocation thresholds control chunk growth and release. These are separate from [BSC's PB reference count](direct-block-groups.md), which measures sharing and placement rather than live record bytes.

`TEvGetPersistentBufferInfo` and the monitoring page expose local records, barriers, capacity, cache, and operation statistics. A client must still combine information from all relevant peers to recover its logical group state.

## Source and Test Map

- [persistent_buffer.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/persistent_buffer.h): namespace keys, record metadata, and cached data.
- [ddisk_actor_persistent_buffer.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk_actor_persistent_buffer.cpp): write/read, batching, recovery, listing, and erase execution.
- [persistent_buffer_barriers_manager.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/persistent_buffer_barriers_manager.cpp): cutoff barriers and compact erase state.
- [persistent_buffer_space_allocator.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/persistent_buffer_space_allocator.cpp): sector ownership and allocation.
- [write_persistent_buffers_request_actor.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/write_persistent_buffers_request_actor.cpp): fan-out, partial replies, and re-replication.
- [persistent_buffer_mon.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/persistent_buffer_mon.cpp): monitoring.

Focused tests live in `ydb/core/blobstorage/ddisk/ut`: `ddisk_actor_ut.cpp` covers sessions, namespace separation, listing, late coordinator replies, and PB lifecycle; `ddisk_actor_batch_write_ut.cpp` covers packed writes; `ddisk_actor_checksum_ut.cpp` covers payload integrity. The allocator and barriers manager have their own unit-test files. PDisk-backed recovery scenarios are in `ddisk_actor_pdisk_ut.cpp` and `../ut_large`.

## See Also

- [{#T}](ddisk.md)
- [{#T}](direct-block-groups.md)
- [{#T}](../load-actors-nbs-dbg-like.md)
