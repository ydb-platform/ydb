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

## Shutdown and Restart {#shutdown-and-restart}

DDisk and PB must always wait for their accepted asynchronous router I/O and
callbacks before acknowledging shutdown. Neither actor may publish Gone while
those callbacks still own its state. The PDisk fallback path instead cancels
actor-owned requests and processes their terminal results before Gone; the
PDisk stop barrier below drains the underlying device I/O.

PDisk session loss starts the same idempotent Stopping state as poison, but
only poison authorizes actor death and the shutdown acknowledgement. Stopping
rejects new requests with `SESSION_MISMATCH`, cancels actor-owned fallback I/O
and parked retries, and continues processing submitted I/O results. Cancellation
balances counters and publishes results before the final mailbox barrier.
Existing completions may finish writes only when their integrity and allocation
log durability conditions are satisfied; shutdown starts no further I/O.

DDisk and PB drain their own I/O concurrently. PB releases its router reference
before sending `TEvGone` to its concrete parent actor. The parent waits for both
drains, releases its router reference, then notifies Warden. Tracked child poison
handles an already absent PB; duplicate poison and notifications are harmless.
After 60 seconds each actor with outstanding callbacks contributes one to
`ddisks/io_stalled`, cleared as soon as its own drain finishes. Waiting for PB is
reported separately. Normal shutdown waits indefinitely for stalled I/O.

The callback retirement count and stopping flag share one atomic state. Callback
cleanup and result publication precede retirement; completion and retry
cancellation events finish before actor destruction. Forced actor destruction
retains all members while waiting up to 10 seconds using monotonic time, then
aborts if callbacks still own actor state. This forced-destruction deadline is
separate from the 60-second stalled-I/O diagnostic and does not bound normal
shutdown waits.

Critical integrity/formatting overload errors permit 20 resubmissions, delayed
by `min(1 ms × 2^(retry−1), 100 ms)`. The immediate callback event transfers the
operation to an actor-owned map; timers contain only IDs. Broken/Stopping cancels
parked operations and stale timers do nothing. Exhaustion returns `ERROR` with
attempt count and last errno; ordinary-I/O error mapping is unchanged.

PB restore consumes payloads only after successful reads. The first failed read
enters Broken and fails queued requests with `ERROR`; late completions only
retire accounting and cannot parse data, resume recovery or publish readiness.
The restore set continues to mean chunks already scheduled.

For a NodeWarden-requested PDisk restart, NodeWarden first requests shutdown of
all affected DDisk actor incarnations. Each DDisk requests shutdown of its PB
and waits for its own drain and the concrete child's `TEvGone`. Only after the
last DDisk's Gone does NodeWarden send PDisk restart permission. Replacement
DDisk/PB startup remains fenced while waiting for these actors and while the
PDisk restart is in flight. The order is therefore PB drain/Gone and DDisk drain,
then DDisk Gone, then PDisk restart permission; the two drains may finish in
either order.

A replacement PDisk cannot begin device I/O until the previous PDisk's I/O has
retired. `TPDisk::Stop()` always calls the shared router's `StopSync()`, even if
DDisk/PB or retained initialization results still hold router clients. This
closes admission, waits for publishers and terminal callbacks, joins the issuer,
retires the ring, and closes the router's duplicated device descriptor. PDisk
then stops its source block device before replacement bootstrap. An old client
can outlive this barrier, but it rejects new work and no longer owns an active
ring or device descriptor. Healthy draining has no timeout; failure to retire
kernel ownership on a broken ring aborts the process instead of permitting
replacement startup with old I/O still live.

The same synchronous I/O barrier applies when PDisk stops or restarts
independently of the NodeWarden-requested sequence. DDisk and PB observe the
stopped router on a rejected submission and enter Stopping; PDisk session loss
also enters Stopping. There is no unsolicited router notification to an idle
actor. PDisk waits for accepted router operations and their callbacks to retire,
which protects actor state while those callbacks run. This barrier does not wait
for the actors' final mailbox processing or Gone notifications. Those actors
still drain their results, and poison remains required before actor death and
notification to Warden. Do not equate this independent I/O barrier with the
additional actor-Gone ordering of a requested restart.

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
