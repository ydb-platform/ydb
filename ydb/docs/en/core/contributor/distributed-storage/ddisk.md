# DDisk

DDisk provides block-addressed storage for direct block clients. A request identifies a tablet, a virtual chunk, and a byte range. DDisk manages the mapping to local PDisk chunks, integrity metadata, and I/O completion. Group replication and user-visible quorum are chosen by the client.

DDisk shares PDisk and slot-management infrastructure with VDisk, but implements a different interface. VDisk stores blob parts for the DS proxy; DDisk serves the events in [ddisk.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk.h) and [blobstorage_ddisk.proto](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/blobstorage_ddisk.proto).

## Ownership and Startup

[NodeWarden](node-warden.md) creates a DDisk actor for a slot configured as DDisk. The actor initializes its PDisk owner, restores chunk-map snapshots and log increments, restores integrity mappings, and creates its [PersistentBuffer](persistent-buffer.md) child. The child has its own service ID and event handlers but shares the parent's PDisk ownership and PB resource lifecycle.

Unless `ForcePDiskFallback` is set, DDisk asks for a submit-only io_uring client in `TEvYardInit` and passes its configured `IdleSpinUs`. On the first such request, PDisk duplicates its device handle and creates and starts one `TUringRouter` shared by the DDisk slots and their PB children. The first requester therefore selects `IdleSpinUs` for the shared router for that PDisk incarnation; later requesters use the existing setting. DDisk and PB hold shared `IUringRouterClient` references and cannot control the router lifecycle.

`ForcePDiskFallback` opts out of the shared router and selects the PDisk raw-event path. An unavailable device handle, unsupported platform, or failed io_uring probe also falls back. In that path, `TEvChunkReadRaw` and `TEvChunkWriteRaw` carry PDisk owner and owner round. Logging and chunk management continue to use PDisk services with either data-I/O backend.

## Sessions {#sessions}

A client establishes a session with `TEvConnect` for each DDisk or PB recipient. The connection metadata includes tablet ID, tablet generation, direct block group index, and the recipient kind. DDisk sessions additionally use `DDiskSessionSeqNo`; PB sessions do not use that sequence number to distinguish sessions.

`TEvConnectResult` returns the instance GUID and an opaque `TConnectionToken`. Normal requests carry the token, and the receiver resolves it to server-side connection metadata. Clients should use the token constructors in `TQueryCredentials` rather than synthesizing the token's fields or serializing the initial metadata on every request.

Connections are keyed by `(TabletId, DirectBlockGroupIndex)`. An older generation, or an older DDisk session sequence within the same generation, cannot supersede an active newer session: such a connect returns `BLOCKED`. An ordinary request with stale or invalid session credentials returns `SESSION_MISMATCH`. The instance GUID is generated for each actor incarnation, not only after detected data loss. Connecting, disconnecting, replacing a session, and restarting the service affect token validity. A client must restore a valid connection before retrying operations under its retry policy.

Internal DDisk/PB forwarding uses `TQueryCredentials::ForInternal`. This has different validation from an ordinary client request, including support for a peer without an existing client connection. It is a server-to-server mechanism, not a replacement for client session establishment.

## Addressing and Writes

`TBlockSelector` contains `VChunkIndex`, `OffsetInBytes`, and `Size`. Data chunk mappings are keyed by `(TabletId, VChunkIndex)`. The direct block group index separates sessions and PB namespaces; it does not add another dimension to the DDisk data chunk map. Clients sharing a DDisk under one tablet must therefore assign virtual chunk indexes consistently across their DBGs.

Reads and writes operate on nonempty ranges aligned to the 4 KiB integrity unit and contained within a PDisk chunk. Write payloads may be fragmented or have unaligned buffer addresses: direct I/O preparation copies them into aligned storage when needed, while suitably aligned rope chunks can use scatter/gather I/O. Use the event payload helpers; the payload ID is an event-local reference, not a persistent data identifier.

The `interface/UnalignedWritePayloads` counter counts incoming writes with fragmented payloads or buffer addresses not aligned to the device sector size. Each request is counted once, including when chunk allocation or write serialization delays its execution.

The first write to a virtual chunk can park while data and integrity resources are allocated. With checksums enabled, a write acknowledgment waits for both the data write and the integrity update. Writes to the same integrity extent are serialized through that path; independent extents can proceed concurrently.

An unallocated virtual chunk reads as zeroes. Chunk allocation and restored integrity state affect how the implementation recognizes never-written blocks within an allocated chunk; do not treat a successful read as evidence that the range has previously been written.

## Integrity

Wire checksums are unsalted XXH3-64 values, one per 4 KiB payload block. When `TDDiskConfig::EnableChecksums` is enabled, write handlers reject missing, incorrectly counted, or mismatching checksums before allocating resources or issuing I/O.

DDisk stores integrity metadata separately from data. Stored checksums are sealed with logical and physical identity information, while the wire protocol and checksum cache use the pure payload checksum. Each integrity metadata block uses a pair of slots, self-checksums, identity/generation checks, and a sequence number to select a valid durable version after recovery. The implementation supports layouts for different device atomic-write properties; their exact format belongs to [ddisk_checksums.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk_checksums.h).

Changing checksum modes is a format and recovery concern, not only a performance setting. DDisk validates checksum-mode compatibility against restored state. PB has a separate on-disk checksum setting, described in [{#T}](persistent-buffer.md#integrity).

## Synchronization

`TEvSync` is the unified pull-and-write operation used for both PB-to-DDisk flush and DDisk-to-DDisk repair. It contains source identities and segments; each segment selects either a PB record or a DDisk source range. The destination issues reads to those services, validates the returned data, and writes the destination range.

All destination segments in one request must belong to one virtual chunk. The sync handler checks nonempty aligned ranges and chunk bounds. `TSegmentManager` tracks overlapping synchronization ranges so that a delayed source read cannot blindly overwrite a newer synchronization request. Changes to this path need coverage for both request ordering and late replies.

The operation reports `TEvSyncResult` after processing its destination work. It does not erase source PB records. The client decides when enough replicas have been flushed, whether repair is required, and when [PB erase](persistent-buffer.md#erase) is safe. A PB source can be remote even when it occupies the same logical DBG index as the destination DDisk.

## Failure and Recovery

Chunk-map snapshots and PDisk log increments restore ownership and integrity-extent mappings. PB then performs its own chunk scan and record recovery. Connection state must be re-established by clients after service replacement.

Fatal PDisk or integrity failures can put the actor into its broken/termination path and fail parked work. Review failure handling alongside normal completions: pending chunk allocation, serialized writes, sync reads, and PB operations can all outlive the event that initiated shutdown.

On stop, DDisk and PB reject new requests with `SESSION_MISMATCH` and independently
wait for their own submitted router I/O. Existing completions may finish requests,
but shutdown does not start further I/O or retries, and a write still requires its
integrity and allocation-log durability conditions before success. PDisk fallback
I/O does not delay actor shutdown. Callback cleanup and queued completion events
finish before the actor dies. After one minute of outstanding direct I/O, the actor
marks itself stalled and contributes one to the non-derivative `ddisks/io_stalled`
gauge; completing the drain removes that contribution.

Router callback ownership and the stopping flag share one atomic state. A single
completion guard recycles the operation or transfers it to a retry event before
`OnDirectIODone` retires the running count and router callback ownership. Rejected
submissions retire through the same method after error handling and destruction;
PDisk fallback only retires the running count. `OnComplete` and `OnDrop` use the
supplied actor system, including outside actor activation. Only shutdown publishes
`TEvFinishStopping`: stop sends it if no callbacks remain, otherwise the last
callback sends it. This final mailbox turn processes already-published results
before destruction. A timeout observing zero callbacks does nothing.

PDisk does not unconditionally stop the shared router when PDisk itself stops.
If PDisk is its sole owner, it closes admission with `StopAsync()` and releases
the router. If DDisk or PB clients remain, PDisk leaves admission open and does
not wait; the last shared-owner release drops work not submitted to the kernel,
drains submitted I/O, joins the I/O thread, and closes the duplicated device
handle.

`TEvDeleteTabletChunks` retires a tablet's data chunk mappings. While deletion is in flight, writes and sync requests for that tablet can return `BUSY`. Controller claim removal and local chunk deletion are separate operations; callers must arrange their order and retire outstanding client work.

## Source and Test Map

| Area | Source | Focused Tests |
|---|---|---|
| Actor state and sessions | [ddisk_actor.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk_actor.cpp), [ddisk_actor_connect.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk_actor_connect.cpp) | `ut/ddisk_actor_ut.cpp` |
| Boot, log, and chunks | [ddisk_actor_boot.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk_actor_boot.cpp), [ddisk_actor_chunks.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk_actor_chunks.cpp) | `ut/ddisk_actor_pdisk_ut.cpp` |
| Read/write and I/O adapters | [ddisk_actor_read_write.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk_actor_read_write.cpp), [direct_io_op.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/direct_io_op.cpp) | `ut/ddisk_actor_checksum_ut.cpp`, `ut/ddisk_actor_ut.cpp` |
| Integrity state | [integrity_manager.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/integrity_manager.cpp) | `ut/integrity_manager_ut.cpp` |
| Synchronization | [ddisk_actor_sync.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk_actor_sync.cpp), [segment_manager.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/segment_manager.cpp) | `ut/ddisk_sync_ut.cpp`, `ut/segment_manager_ut.cpp` |

Paths in the test column are relative to `ydb/core/blobstorage/ddisk`. `ut_large` contains longer PDisk-backed I/O and synchronization scenarios. Select the relevant target and test cases rather than running the entire distributed storage test tree for a local change.

## See Also

- [{#T}](../distributed-storage.md)
- [{#T}](direct-block-groups.md)
- [{#T}](persistent-buffer.md)
- [DDisk source map](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/README.md)
