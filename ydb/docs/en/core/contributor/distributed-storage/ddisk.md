# DDisk

DDisk provides block-addressed storage for direct block clients. A request identifies a tablet, a virtual chunk, and a byte range. DDisk manages the mapping to local PDisk chunks, integrity metadata, and I/O completion. Group replication and user-visible quorum are chosen by the client.

DDisk shares PDisk and slot-management infrastructure with VDisk, but implements a different interface. VDisk stores blob parts for the DS proxy; DDisk serves the events in [ddisk.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk.h) and [blobstorage_ddisk.proto](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/blobstorage_ddisk.proto).

## Ownership and Startup

[NodeWarden](node-warden.md) creates a DDisk actor for a slot configured as DDisk. The actor initializes its PDisk owner, restores chunk-map snapshots and log increments, restores integrity mappings, reconciles orphan reservations, and creates its [PersistentBuffer](persistent-buffer.md) child. The child has its own service ID and event handlers but shares the parent's PDisk ownership and PB resource lifecycle.

Unless `ForcePDiskFallback` is set, DDisk asks for a submit-only io_uring client in `TEvYardInit` and passes its configured `IdleSpinUs`. On the first such request, PDisk duplicates its device handle and creates and starts one `TUringRouter` shared by the DDisk slots and their PB children. The first requester therefore selects `IdleSpinUs` for the shared router for that PDisk incarnation; later requesters use the existing setting. DDisk and PB hold shared `IUringRouterClient` references and cannot control the router lifecycle.

`ForcePDiskFallback` opts out of the shared router and selects the PDisk raw-event path. An unavailable device handle, unsupported platform, or failed io_uring probe also falls back. In that path, `TEvChunkReadRaw` and `TEvChunkWriteRaw` carry PDisk owner and owner round. Logging and chunk management continue to use PDisk services with either data-I/O backend.

`TDDiskConfig::DevNullMode` (default `false`) changes the shared router's data-I/O semantics for disposable tests: accepted writes complete without changing the device, and reads return zero-filled buffers. It requires a working io_uring router, cannot be combined with `ForcePDiskFallback`, and fails initialization if the router is unavailable. All DDisk slots attaching to the shared router on one PDisk must request the same mode; a conflicting slot is rejected, and changing mode requires a PDisk restart. The NBS flat `DevNullMode` field overrides `GlobalDDiskConfig.DevNullMode` when explicitly set, including `false`. The stress tool exposes this mode as `--ddisk-devnull`.

PDisk formatting, chunk management, and mapping logs still perform real I/O. DevNull data and integrity images are not persistent: after metadata eviction or restart, zero-filled integrity reads do not constitute valid checksum metadata. Checksummed DevNull benchmarks must first write zero-valued used data and keep its checksum metadata resident. Set `IntegrityChecksumCacheBytes` large enough to retain all used integrity pairs in the working set; zero disables caching. Nonzero writes cannot be read back successfully with checksum verification because data reads return zeroes. Use normal device I/O for cold-cache, recovery, and persistence validation.

## Sessions {#sessions}

A client establishes a session with `TEvConnect` for each DDisk or PB recipient. The connection metadata includes tablet ID, tablet generation, direct block group index, and the recipient kind. DDisk sessions additionally use `DDiskSessionSeqNo`; PB sessions do not use that sequence number to distinguish sessions.

`TEvConnectResult` returns the instance GUID and an opaque `TConnectionToken`. Normal requests carry the token, and the receiver resolves it to server-side connection metadata. Clients should use the token constructors in `TQueryCredentials` rather than synthesizing the token's fields or serializing the initial metadata on every request.

Connections are keyed by `(TabletId, DirectBlockGroupIndex)`. An older generation, or an older DDisk session sequence within the same generation, cannot supersede an active newer session: such a connect returns `BLOCKED`. An ordinary request with stale or invalid session credentials returns `SESSION_MISMATCH`. The instance GUID is generated for each actor incarnation, not only after detected data loss. Connecting, disconnecting, replacing a session, and restarting the service affect token validity. A client must restore a valid connection before retrying operations under its retry policy.

Read, write, and Sync validation resolve credentials into native `TQueryCredentials` without rewriting the request protobuf. Write and Sync retain the original credentials across allocation and metadata-ownership waits. With checksums enabled, their shared `ExecuteDataWrite` coroutine acquires every affected metadata pair and then revalidates the session before loading cold metadata or submitting data. This final check is the admission point; a request whose token was replaced while parked does not load metadata or submit its data or metadata writes. With checksums disabled, credentials are revalidated after allocation, immediately before data submission. A read validates credentials once, before chunk lookup, and does not wait for an in-flight allocation. Disconnecting and reconnecting invalidates an old parked token even when generation and session numbers are unchanged. A write rejected at admission reports `SESSION_MISMATCH`; Sync reports it for the affected input and returns an aggregate failure. After admission, a later client-token change, including during a cold metadata load, does not cancel the write and its integrity update. Device errors and actor shutdown retain their separate failure and drain rules. Read validation describes a recognized old token as “stale” and an unrecognized token as “invalid”. Repeating connect without changing the token remains valid.

Internal DDisk/PB forwarding uses `TQueryCredentials::ForInternal`. This has different validation from an ordinary client request, including support for a peer without an existing client connection. It is a server-to-server mechanism, not a replacement for client session establishment.

## Addressing and Writes

`TBlockSelector` contains `VChunkIndex`, `OffsetInBytes`, and `Size`. Data chunk mappings are keyed by `(TabletId, VChunkIndex)`. The direct block group index separates sessions and PB namespaces; it does not add another dimension to the DDisk data chunk map. Clients sharing a DDisk under one tablet must therefore assign virtual chunk indexes consistently across their DBGs.

Reads and writes operate on nonempty ranges aligned to the 4 KiB integrity unit and contained within a PDisk chunk. Write payloads may be fragmented or have unaligned buffer addresses: direct I/O preparation copies them into aligned storage when needed, while suitably aligned rope chunks can use scatter/gather I/O. Use the event payload helpers; the payload ID is an event-local reference, not a persistent data identifier.

Public `TEvWrite` requests are limited to 1 MiB, whether checksums are enabled or disabled. Larger requests receive `INCORRECT_REQUEST` before allocation or I/O. Exactly 1 MiB remains valid, including the data copier's current request size. Clients with a configurable larger write size must split those requests; large reads and Sync segments are not subject to this public-write limit.

Callers guarantee at most one active Sync for any given block set, disjoint active data ranges, and exclusion of ordinary writes from reads and Sync. Syncs over disjoint block sets may run concurrently. DDisk relies on these guarantees without implementing their admission protocol. Disjoint writers may share integrity metadata pairs; DDisk serializes their metadata modifications.

The `interface/UnalignedWritePayloads` counter counts incoming writes with fragmented payloads or buffer addresses not aligned to the device sector size. Each request is counted once, including when chunk allocation delays its execution.

The first write to a virtual chunk can wait while data and integrity resources are allocated. Concurrent requests for that virtual chunk share one allocation. With checksums enabled, a write acknowledgment waits for the data write, integrity update, and durable allocation mapping. The write handler moves its reply route, original credentials, payload, and checksums into a frame-owned record and releases the incoming event.

`ExecuteDataWrite` is the one flat coroutine for the destination data and metadata of both Write and Sync; a Write is a single piece whose payload came with the request. With checksums enabled, after allocation it acquires metadata pairs in ascending order, waits for occupied pairs, and revalidates the session at admission. A cold metadata image is read in a batch of its own and transformed on the actor thread; a warm image needs no read. Only then are data and metadata writes prepared and submitted as one batch. Both operations complete before actor-side metadata publication and writer release. Failure still drains every accepted sibling, and a metadata load or transformation failure submits no data. The mapping-log durability gate precedes a successful reply. A client-token change after admission does not interrupt the request, including its cold load. A chunk pin protects the mapping and physical chunk through accepted I/O and mapping commits.

DDisk coroutine frames use the actor runtime's shared [TLS allocator](../actor-system/coroutine-actors.md), without a private actor-owned cache or allocator override. Cache occupancy statistics measure idle frames retained by executor threads, not live DDisk frames; request registries, chunk pins, and completion drain determine whether an actor's work has retired.

A read is validated by an ordinary handler, so a misaligned range, a tablet whose chunks are being deleted, and a virtual chunk with no published physical chunk are answered without allocating a coroutine frame. For an allocated, formatted chunk the handler moves the reply route, resolved credentials, selector, and span into an owned record and starts `ExecuteDataRead`. With checksums disabled it submits and awaits data. With checksums enabled, the coroutine uses four paths:

| Path | Processing |
| --- | --- |
| Warm metadata | Capture response-owned checksums and hole information; submit and await data only, unless the range is entirely zero. |
| Small read initiating cold loads | Submit the owned metadata read and data in one batch. Publish completed metadata and finish the initiating request before notifying its metadata waiters, when all dependencies are complete. |
| Cold read of at least 32 KiB (`MetadataFirstReadThreshold`) | Await metadata, capture the result, and release metadata waiters before any subsequent data wait. Skip data I/O for an entirely zero range. |
| Follower of existing metadata work | Start data immediately, await it, then inspect the retained metadata result. Await the metadata event only if that result is still incomplete. |

A request can own some metadata loads and join others. It publishes completed loads and notifies their waiters before suspending on another dependency. Followers retain a completion result independently of cache residency: a reference to the non-sticky `TAsyncEvent` alone would miss a completion followed by cache eviction while data is outstanding. If speculative data turns out to cover only never-written blocks, its completion still retires before the zero reply.

`TBatchedIOAwaiter` joins a frame's device I/O in two phases. Preparation reserves stationary result slots and adds operations without submitting them. Awaiting the batch publishes the actor runtime's generic callback bridge and submits all prepared operations under an extra pending-count guard. The guard prevents inline callbacks from completing a partially submitted batch. Each operation completes once, including across retries. The frame and submitted operations retain the batch through `std::shared_ptr`; callbacks write separate result slots and decrement the atomic pending count. The last completion resumes the bridge, which schedules continuation on the actor's mailbox. If all operations complete during submission, the awaiter returns the bridge handle and the runtime continues the frame without suspending. Device callbacks never resume actor-local continuations directly.

The batch supports inline completion and reuse after all results have been consumed. Callbacks acquire shared batch ownership only at submission, so prepared operations do not create an ownership cycle. Cooperative shutdown cannot unwind a frame still holding accepted device buffers. During forced frame destruction, the waiter atomically withdraws and destroys the bridge if no completion has taken it. A bridge already taken by a callback relies on the runtime's dead-actor handling and cannot re-enter the destroyed frame. Accepted operations retain shared callback storage through retirement.

Before I/O submission, a read stores its requester, interconnect session, client cookie, tablet/chunk identity, and start time in an inline coroutine reply context and releases the incoming event. The frame retains the trace span, while its shared `TBatchedIOAwaiter` owns the `TDDiskReadResult` and completed payload buffers, without a completion registry or a heap-allocated I/O result event. Each `TDDiskIoOp` retains the class callback until recycling or destruction. Callbacks write disjoint slots, which is what makes concurrent io_uring completions into one batch safe. Integrity loads additionally mark their operation critical, which keeps the session-loss rule for metadata reads. The span is retained until terminal processing, including cancellation.

A logical read uses an ordinary scalar router operation for its data range, when needed, and at most one newly claimed metadata read. Metadata for several claimed pairs is read as one contiguous range and split by the integrity manager. A small initiating read joins data and metadata in one `TBatchedIOAwaiter` batch; a large initiating read awaits metadata before deciding whether to read data. Stable buffers and result slots are allocated before submission, and callbacks may complete in any order, including before a submission call returns. The router handles queue pressure and short reads separately for each accepted scalar operation.

If the router rejects a submission, DDisk completes that operation locally through the same callback path and schedules a transition to Stopping. Each remaining prepared operation also receives a terminal outcome. The batch still waits for every accepted sibling, including error and drop callbacks. PDisk fallback sends one raw-read message per part and joins the same batch. The read also joins metadata loads already owned by another operation; it can finish its own I/O while still waiting for a shared load. Chunk pins and the frame itself remain alive until both I/O and shared metadata settle. Accepted buffers remain owned through callback retirement and actor-side terminal-result processing.

`TReadPayload` owns either no data, a native `TRcBuf`, or a fallback `TRope`. I/O callbacks move this ownership into the shared batch's result, keeping native buffers native until reply preparation. `FinishDDiskRead` zeroes holes in mixed ranges through the payload helper, then converts native client data to rope once before optional payload checksum validation and attachment. This transfers successful data without copying the payload. Broken-state handling, status mapping, byte accounting, short-I/O counters, and tracing still apply; error replies carry neither payload nor checksums. `TEvReadResult` consumes a `TConstArrayRef<ui64>` immediately into protobuf, with no retained view or intermediate checksum-vector copy.

Reads capture only their required checksums and hole information in owned immutable results. Once captured, these results do not pin metadata images and remain valid across neighboring metadata changes and eviction. A warm pair remains readable during a metadata write using its existing immutable image. A reader encountering a cold metadata read/modify/write waits for the final write completion. These cache properties do not enforce the caller's data-range and operation-exclusion guarantees.

An unallocated virtual chunk reads as zeroes. An allocation that has not yet published its physical chunk leaves the virtual chunk unallocated, so a concurrent read returns zeroes instead of waiting. Chunk allocation and restored integrity state affect how the implementation recognizes never-written blocks within an allocated chunk; do not treat a successful read as evidence that the range has previously been written.

## Integrity

Wire checksums are unsalted XXH3-64 values, one per 4 KiB payload block. When `TDDiskConfig::EnableChecksums` is enabled, write handlers reject missing, incorrectly counted, or mismatching checksums before allocating resources or issuing I/O.

DDisk stores integrity metadata separately from data. Stored checksums are sealed with logical and physical identity information, while the wire protocol and checksum cache use the pure payload checksum. Each integrity metadata block uses a pair of slots, self-checksums, identity/generation checks, and a sequence number to select a valid durable version after recovery. The implementation supports layouts for different device atomic-write properties; their exact format belongs to [ddisk_checksums.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk_checksums.h).

`TIntegrityManager` performs no physical I/O. It keeps stable cache entries in a node-based hash map with `Missing`, `WaitingRead`, `Data`, and `WaitingWrite` states. Preparation looks up each relevant pair once and retains stable handles while an operation requires ownership. Compact per-pair used-block bitmaps, expected digests, and current-slot information are retained independently of evictable checksum images. Slot selection, identity and digest checks, and lost-write detection therefore remain effective after eviction.

`PrepareRead` constructs an immediate result in the caller's empty optional or returns a pending handle and at most one descriptor for newly claimed metadata loads. Existing loads are joined without duplication. `TReadChecksums` stores a singleton checksum inline and owns storage for larger snapshots. A single-block result selects zero or passthrough without a mask; multi-block results retain a mask only for mixed ranges. Read results own their checksums and hole information rather than retaining cached metadata. Pending handles retain the final result even after notification and eviction.

Read preparation exposes `MetadataReads` as `TMetadataReads` (`absl::InlinedVector<TMetadataRead, 1>`). Its optional descriptor identifies a contiguous metadata read with an opaque ID; DDisk supplies the `TMetadataReadResult` to `CompleteMetadataReads`. The read covers the range from the first newly claimed pair through the last, and the manager extracts each claimed pair's two integrity slots from that image. In the base format a pair covers 492 data blocks, so an 8 KiB data read at offset `491 * 4096` loads one contiguous 16 KiB metadata image if both pairs are cold. A pair already owned by another operation is joined instead of claimed again. Callback result storage is sized before submission and stays stationary until callbacks retire.

`CompleteMetadataReads` validates and publishes images, captures affected read results, and defers dependency notifications to `NotifyCompleted()`. The small initiating reader can finish before its waiters are notified. When other dependencies remain, and for large metadata-first reads, DDisk notifies completed-load waiters before its next suspension. Active loads and writes retain their entries. `WaitingWrite` keeps the previous immutable image when warm; a cold RMW has no readable image until its final write succeeds.

`TWriteOperation` owns exclusive pair claims. Writers acquire multiple pairs in ascending order. Queued writers and the next writer selected for resumption keep stable entries alive. Successful release wakes one next writer; cancellation releases its claims, and terminal failure or shutdown resolves all affected waits. This explicit ownership replaces mutation/durability versions, write tickets, dirty coalescing, and checksum-flush runners.

`TMetadataWrite` is an owned operation context prepared by the manager. A fully warm operation constructs replacement images without metadata reads. A cold operation reads one complete pair, or the contiguous two-pair region when either affected pair is cold: at most four 4 KiB blocks. Transformation validates images and expected digests, selects current slots, applies the new checksums, and updates sequences and digests. One pair writes its replacement slot. Two pairs write the contiguous middle slots when those are the replacements; otherwise they write all four blocks while preserving each unchanged current image. The 1 MiB public-write limit bounds a write to at most two pairs. The disk format and recovery rules remain unchanged.

The coroutine prepares a cold metadata load with `PrepareMetadataRead` as an ordinary critical read and awaits it separately. After successful completion, it transforms the owned `TMetadataWrite` context on the actor thread, then prepares and batches the data and metadata image writes. A failed load, a failed transformation, Broken, or Stopping prevents further data submission. Successful metadata is published into the cache on the actor thread after the write batch completes.

PDisk fallback follows the same coroutine flow through actor-side raw I/O. Data and critical metadata errors retain separate results. Metadata overloads retain the existing retry limits and delays for the cold read and metadata write independently. Completion processing continues during Stopping, and every accepted sibling retains buffer and chunk ownership until retirement. `TDataRequestGuard` counts client, allocation, and formatting coroutines through submission, actor-side processing, notification, and cleanup. Reservation release and Gone wait for these operations as well as callback retirement. With checksums disabled, one coroutine zero-formats a chunk in sequential slices.

New extent placement and readiness are separate milestones. One allocation coroutine starts the extent, awaits placement, publishes the physical data chunk, awaits formatting readiness, submits the mapping log, awaits durability, and publishes commit. Readiness requires both extent formatting and all three integrity-chunk headers. The allocation ownership registry retains its token, physical chunk, and whether a mapping log was submitted; a zero physical chunk denotes a pending reservation. Token identity is checked after suspensions, and submitted commits retain conservative physical ownership during failure and shutdown. Deleted extents retain their slots until deletion is durable and outstanding formatting has retired.

PDisk log records use optional completion tickets. Allocation, PB allocation/deallocation, deletion, and reclamation apply completion-dependent effects after awaiting successful completion; snapshot/map construction and quarantine happen before submission. Background reclamation can submit a snapshot without a waiter when it has no completion-dependent effect. Every LSN and delivery cookie remains tracked, including records without waiters; background snapshots need no ticket allocation. Completion handling detaches the entire matching batch before waking any waiter.

Changing checksum modes is a format and recovery concern, not only a performance setting. DDisk validates checksum-mode compatibility against restored state. PB has a separate on-disk checksum setting, described in [{#T}](persistent-buffer.md#integrity).

## Synchronization

`TEvSync` is the unified pull-and-write operation used for both PB-to-DDisk flush and DDisk-to-DDisk repair. It contains source identities and segments; each segment selects either a PB record or a DDisk source range. The destination issues reads to those services, validates the returned data, and writes the destination range.

All destination segments in one request must belong to one virtual chunk. The handler validates the complete request, including nonempty aligned ranges, exactly one segment kind per input, and chunk bounds, before issuing any work. The caller's guarantees of one active Sync per block set, disjoint ranges, and ordinary-write exclusion apply. The wire `OUTDATED` status remains available for source-service results; DDisk treats such a source reply as a failed input.

The handler releases its incoming event and launches `ExecuteDataWrite`, which runs one coroutine loop. It processes original segments in input order and aligned pieces of at most 512 KiB in increasing offset order. For each piece it awaits one source reply, validates status, exact payload size, checksum count and configured checksum hashes, executes allocation as needed, and performs the destination write. With checksums enabled, destination admission follows pair ownership and session revalidation; any cold metadata load and transformation precede the data-plus-metadata write batch. A missing destination is allocated only after a valid source reply. The next source read starts only after the current destination operations retire. DDisk and PB sources both support subrange reads. There is no slot scheduler, admission queue, metadata prefetch, or per-slot subscription; bounded cooperative yielding limits uninterrupted synchronous work.

Every source request receives a fresh cookie identifying the owning Sync and expected event kind. Source handlers retain a result and wake the root coroutine; the root makes workflow decisions. Stale replies are ignored, and wrong-kind replies preserve the valid route. Accepted local callbacks retain their shared batch storage through forced frame destruction.

The reply contains one result per original segment in input order. A piece failure skips the rest of that segment, preserves completed writes, drains accepted local branches, and continues later segments while the actor remains healthy. Data, metadata, and session outcomes retain their error precedence, including metadata corruption overriding a session mismatch for the failed piece. Broken or Stopping abandons pending source replies, prevents new destination work, and drains accepted local work. An already accepted native piece may succeed during Stopping after both branches retire and its mapping is committed. Before replying, Sync also waits for the final mapping-commit gate, including an error behind an unrelated allocation already in progress.

The operation reports `TEvSyncResult` after processing its destination work. It does not erase source PB records. The client decides when enough replicas have been flushed, whether repair is required, and when [PB erase](persistent-buffer.md#erase) is safe. A PB source can be remote even when it occupies the same logical DBG index as the destination DDisk.

## Failure and Recovery

Chunk-map snapshots and PDisk log increments restore ownership and integrity-extent mappings. After successful, complete log replay, DDisk computes orphan candidates as `OwnedChunksOnBoot` minus the union of restored data chunks, listed integrity chunks, every integrity chunk referenced by a restored extent, and PB chunks. Extent references protect chunks even when they are absent from the integrity-chunk list. DDisk computes this union before boot-time reclamation changes the mappings, so references recovered from both snapshots and later log increments are protected. This conservative protection does not make an otherwise invalid recovery mapping valid; existing recovery validation still applies. Failed or incomplete recovery does not attempt reconciliation.

DDisk submits one orphan at a time through `TEvChunkForget`, tracks delivery, and waits for its reply before continuing. PB creation, new allocations, and client readiness remain blocked until reconciliation completes. DDisk explicitly sets `IsDDisk = true` on its reserve and forget requests; the field defaults to `false` for other callers. For flagged forget requests, PDisk preserves the request cookie in all replies and validates the owner and owner round when executing the request, so a delayed request from an older incarnation cannot release reused chunks. Existing VDisk behavior, including error responses, cookie handling, and logging severity, is unchanged.

Successful cleanup and chunk-validation rejections both allow reconciliation to continue. DDisk preserves rejected chunks and logs their IDs and rejection reasons at WARN. For flagged requests, PDisk also logs BPD91 at WARN for expected transitional states: `DATA_ON_QUARANTINE`, `DATA_RESERVED_DELETE_IN_PROGRESS`, `DATA_COMMITTED_DELETE_IN_PROGRESS`, `DATA_RESERVED_DELETE_ON_QUARANTINE`, and `DATA_COMMITTED_DELETE_ON_QUARANTINE`. PDisk's existing I/O and log completion reclaim these chunks; reconciliation adds no retry or forced deletion. Rejection of a committed orphan (`DATA_COMMITTED`) remains an ERROR in PDisk, and other unexpected validation failures retain their existing severity. A PDisk session or device error, or request nondelivery, enters Stopping. Poison cancels remaining reconciliation, and a late reply cannot resume bootstrap. PDisk returns terminal `CORRUPTED` replies when shutdown aborts flagged queued reserve or forget requests, including requests in the dedicated forget queue. Unflagged queued requests are silently discarded as before.

PB then performs its own chunk scan and record recovery. Connection state must be re-established by clients after service replacement.

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

Broken and Stopping wake logical waits that cannot progress, but do not cancel
accepted router-I/O waits. A failed request joins already-submitted sibling I/O
before replying; its buffers and physical chunk pins remain owned until terminal
results are consumed. Logical operations and outstanding physical producers
both protect chunks from deletion. Remaining logical waits and request replies
finish before the final mailbox barrier and Gone. Batch completions are published
in both running and Stopping states. `TDataRequestGuard` counts client, allocation, and formatting coroutines in the same
`DataRequestsInFlight` counter through submission, result processing, notifications,
and cleanup. Reservation release and shutdown completion wait for that counter to
reach zero as well as callback drain, including when callbacks have retired but a
batch result is still queued. Forced destruction drains callbacks before releasing any remaining
registry pins.

Chunk-map `TEvLog` requests track delivery; a nondelivery notification correlated
with an outstanding request by its cookie enters Stopping.

After its own I/O drain and terminal-result processing, DDisk requests release
of its known reservations through `TEvChunkForget`, provided PDisk initialization
and log replay have completed. Candidates include unused reserved chunks,
abandoned formatting and data allocations, and integrity chunks, but only when
their commit log has never been submitted. A submitted commit excludes a chunk
even if its acknowledgement is still pending; PB allocations are also excluded.
The request carries the current PDisk owner and owner round. Accepted router
I/O must have retired before release; PDisk quarantines chunks with remaining
fallback device I/O until that I/O finishes.

Broken retains abandoned formatting and unlogged data allocations separately
from unused reservations, so PB cannot reuse a chunk while an old DDisk write
may still target it. Fresh reservations that have not been used for DDisk I/O
remain available to PB. Successful reserve replies received during Stopping
are collected without starting allocation or formatting. After DDisk's own drain,
late replies can trigger further forget requests while the actor remains alive.
Each chunk is submitted for release at most once per actor incarnation, so
follow-up requests contain only new IDs, regardless of earlier forget replies.

An outstanding reserve request (`ChunkManager.IsReservationInFlight()`) prevents DDisk from publishing
Gone, even after its own drain and PB shutdown finish. Every terminal reserve
reply clears this barrier; successful replies contribute their chunks to the
release set, and release waits for the existing I/O barrier. Reserve delivery is
tracked: nondelivery clears the pending request and enters Stopping. PDisk returns
a terminal `CORRUPTED` reply when shutdown aborts a queued reserve request marked
`IsDDisk`. There is no
timeout that abandons an outstanding reservation.

Shutdown does not wait for forget replies and does not retry forget requests.
These acknowledgements remain best effort: lost or rejected forget requests can
leave reservations in a running PDisk across DDisk-only restarts. Startup
reconciliation repairs unreferenced reservations left by earlier incarnations,
subject to the validation and error handling above. A PDisk restart discards
never-committed reservations. This cleanup requires no new event types, durable
format, or log schema and does not change the protocol for deleting committed
chunks.

DDisk and PB drain their own I/O concurrently. PB releases its router reference
before sending `TEvGone` to its concrete parent actor. The parent waits for its
own drain, the concrete child's `TEvGone`, and resolution of its reserve request,
releases its router reference, then notifies Warden. Tracked child poison
handles an already absent PB; duplicate poison and notifications are harmless.
After 60 seconds each actor with outstanding callbacks contributes one to
`ddisks/io_stalled`, cleared as soon as its own drain finishes. Shutdown diagnostics
also report waiting for PB and an outstanding reserve request. Normal shutdown
waits indefinitely for stalled I/O or an unresolved reservation.

The callback retirement count and stopping flag share one atomic state. Callback
cleanup and result publication precede retirement; completion and retry
cancellation events finish before actor destruction. Forced mailbox cleanup
destroys coroutine frames before the actor destructor runs. This clears their
active batch continuations and releases the frames'
shared owners; outstanding operations retain each `TBatchedIOAwaiter`, so late
callbacks can safely write
their results during the destructor's drain. The destructor retains all actor
members while waiting up to 10 seconds using monotonic time, then
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
and waits for its own drain, the concrete child's `TEvGone`, and resolution of any
outstanding reserve request. Only after the last DDisk's Gone does NodeWarden
send PDisk restart permission. Replacement DDisk/PB startup remains fenced while waiting for these actors and while the
PDisk restart is in flight. PB drain/Gone, DDisk drain, and reserve resolution
must all precede DDisk Gone, which precedes PDisk restart permission. The drains
and reserve resolution may finish in any order.

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
| Reservation bookkeeping and allocation ordering | [chunk_manager.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/chunk_manager.h) | `ut/chunk_manager_ut.cpp` |
| Read/write and I/O adapters | [ddisk_actor_read_write.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk_actor_read_write.cpp), [direct_io_op.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/direct_io_op.cpp) | `ut/ddisk_actor_checksum_ut.cpp`, `ut/ddisk_actor_ut.cpp` |
| Integrity state | [integrity_manager.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/integrity_manager.cpp) | `ut/integrity_manager_ut.cpp` |
| Synchronization | [ddisk_actor_sync.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/ddisk_actor_sync.cpp) | `ut/ddisk_sync_ut.cpp`, `ut/ddisk_actor_ut.cpp` |

Paths in the test column are relative to `ydb/core/blobstorage/ddisk`. `ut_large` contains longer PDisk-backed I/O and synchronization scenarios. Select the relevant target and test cases rather than running the entire distributed storage test tree for a local change.

## See Also

- [{#T}](../distributed-storage.md)
- [{#T}](direct-block-groups.md)
- [{#T}](persistent-buffer.md)
- [DDisk source map](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ddisk/README.md)
