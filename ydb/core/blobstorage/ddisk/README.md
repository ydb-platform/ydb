# DDisk and PersistentBuffer Source Map

The canonical developer contracts are in the [DDisk](../../../docs/en/core/contributor/distributed-storage/ddisk.md), [PersistentBuffer](../../../docs/en/core/contributor/distributed-storage/persistent-buffer.md), and [direct block group](../../../docs/en/core/contributor/distributed-storage/direct-block-groups.md) contributor pages.

| Task | Entry Points |
|---|---|
| Public requests and payload helpers | `ddisk.h`, `ddisk.cpp`, `ydb/core/protos/blobstorage_ddisk.proto` |
| Actor state, completions, and shutdown | `ddisk_actor.h`, `ddisk_actor.cpp` |
| Session validation | `ddisk_actor_connect.cpp` |
| PDisk owner recovery and PB child creation | `ddisk_actor_boot.cpp` |
| Reservation bookkeeping and allocation ordering | `chunk_manager.h` |
| Chunk allocation, formatting, commit, and deletion | `ddisk_actor_chunks.cpp` |
| Data read/write and owned read values | `ddisk_actor_read_write.cpp`, `direct_io_op.{h,cpp}`, `read_result.h` |
| Integrity format and state | `ddisk_checksums.{h,cpp}`, `integrity_manager.{h,cpp}` |
| Sequential Sync and source replies | `ddisk_actor_sync.cpp` |
| PB record layout and execution | `persistent_buffer{,_header}.h`, `ddisk_actor_persistent_buffer.cpp` |
| PB allocation and erase state | `persistent_buffer_space_allocator.*`, `persistent_buffer_barriers_manager.*` |
| PB fan-out and partial replies | `write_persistent_buffers_request_actor.*` |
| Monitoring | `ddisk_actor_mon.cpp`, `persistent_buffer_mon.*` |

`ut/` contains focused actor, integrity, sync, batching, barrier, and allocator tests. `ut_large/` contains longer PDisk-backed scenarios. Cross-component allocation and load-actor scenarios live in `../ut_blobstorage/` and `../ut_blobstorage/ut_ddisk/`.

`TChunkManager` owns allocation ordering, reusable reservations, and refill
accounting. One allocation coroutine per virtual chunk starts its integrity
extent, awaits placement, publishes the physical chunk, awaits formatting
readiness, submits the mapping log, awaits durability, and publishes commit.
The allocation ownership registry retains `{token, physical chunk, log submitted}`;
a zero chunk is a pending reservation. The coroutine rechecks token identity
after waits. Submitted commits conservatively retain physical ownership through
failure and shutdown. Formatting slots stay quarantined until accepted I/O retires.
Checksum-disabled zero formatting uses one coroutine over sequential slices.
Allocation, PB allocation/deallocation, deletion, and reclamation await optional
log completion tickets before applying completion-dependent effects. Snapshot/map
construction and quarantine happen before submission; background reclamation can
submit a snapshot without a waiter. All LSNs and delivery
cookies remain tracked; matching log batches are detached before any waiter is
woken. Background snapshots need no ticket. Direct-I/O completions retain buffer
ownership through retirement.

Read, write, and Sync requests run as root coroutines that own their workflow
decisions. Ordinary read/write handlers validate and answer rejections before
starting a frame. Public `TEvWrite` requests are limited to 1 MiB, independently
of checksum mode; larger requests receive `INCORRECT_REQUEST` before allocation
or I/O. The existing 1 MiB data-copier request fits this limit. Configurable
clients must split larger writes. Frames use the actor runtime's shared TLS
allocator, without a DDisk-owned frame cache.

Callers guarantee at most one active Sync for any given block set, disjoint active
data ranges, and exclusion of ordinary writes from reads and Sync. Syncs over
disjoint block sets may run concurrently. DDisk does not implement an admission
protocol for these guarantees. Disjoint writers can share an integrity
pair; metadata ownership serializes their modifications. A read's captured
checksums and hole information remain valid across neighboring metadata changes.

Read, Write, and Sync move their reply route, credentials, payload, and checksums
into owned records and release incoming events. `ExecuteDataWrite` is the one
flat coroutine for the destination data-plus-metadata write of both Write and
Sync; a Write is a single piece whose payload came with the request.
After validation, allocation, and session checks, it acquires the metadata pairs
in ascending order, waiting for occupied entries, and validates the session once
more as soon as it owns them. That is the admission point: nothing is submitted
before it, and a request replaced while parked receives `SESSION_MISMATCH` having
written nothing. A cold pair is loaded by the coroutine itself: one metadata read
is awaited alone and transformed on the actor thread. A warm pair, or one whose
load another request completed while this one waited, is modified without any
read. Past admission, the data write and the metadata image write are prepared
and submitted as one batch, and both retire before actor-side metadata
publication and writer release. Accepted siblings drain even after failure.
The mapping-log durability gate still precedes a successful reply. A client-token
change after admission does not interrupt the request, including its cold load:
accepted data must receive matching checksums. A metadata load or transformation
failure admits no data. `ChunkRefPins` protects the mapping and physical chunk
through accepted I/O and mapping commits; `TDataRequestGuard` counts client,
allocation, and formatting coroutines in `DataRequestsInFlight`.

`TBatchedIOAwaiter` joins a frame's device operations in two phases. The frame
prepares operations with `Prepare*` and adds them to the batch; nothing is
submitted and nothing runs concurrently at this point, so the batch knows its
operation count. `await_ready` is a pure check: it is true only when nothing was
prepared and nothing is pending. `await_suspend` adds the count plus one
submission guard to `Pending`, publishes the bridge, binds each operation's
callback to the batch and submits them all, then releases the guard. If every
operation, even an inline one, already completed, it returns the bridge and the
frame continues without suspending. Otherwise the callback whose decrement brings
`Pending` to zero exchanges the bridge out and resumes it, so `Pending` alone
decides which thread resumes the frame. There is no active-waiter or rearm state;
the batch is reused by clearing its results. Callbacks are bound at submission,
so a prepared but unsubmitted operation never owns its batch. Each operation
completes once, including across retries. The library bridge posts the mailbox
resume (or schedules it on the actor's own mailbox); callbacks do not touch the
actor continuation. Result slots remain stationary until callbacks retire.
Cooperative shutdown cannot cancel an accepted batch wait. Forced frame
destruction destroys an unpublished bridge; an undelivered resume runs with a null
actor and does not enter the frame. Accepted operations retain the shared
callback storage through that drain.

`ExecuteDataRead` handles four checksum paths explicitly:

- Warm metadata: capture response-owned checksums and hole information, then
  submit and await data only when the range is not entirely zero.
- A small read initiating cold loads: submit its one metadata read and its data
  in one batch. Publish metadata and finish the initiating read before notifying
  its metadata waiters, when all its dependencies are complete.
- A cold read of at least `MetadataFirstReadThreshold` (32 KiB): await metadata,
  capture the result, and notify metadata waiters before subsequent data I/O.
  Entirely zero ranges omit the data read.
- A follower of existing metadata work: start data immediately, await data,
  then inspect its retained metadata result and await its event only if that
  result is still incomplete.

A request can own some loads and join others. It publishes its completed loads
and notifies their waiters before suspending on another dependency. Pending read
handles retain completion independently of cache residency: `TAsyncEvent` is
non-sticky, so an event reference alone is insufficient if metadata completes
and is evicted before the follower's data. Final read results own only the
required checksums and hole information, and do not pin cached images.
`TReadChecksums` keeps a singleton checksum inline and owns storage for larger
ranges. One preparation claims at most one metadata read. That read is a single
ping-pong pair, or one contiguous image from the first claimed pair through the
last when the range crosses a pair boundary; the manager splits the image. A
request joins a pair another request already owns instead of reading it again.
An unpublished physical chunk reads as zeroes without waiting for allocation. Checksums-disabled reads await data
only. `TReadPayload` retains native `TRcBuf` or fallback `TRope` ownership;
`FinishDDiskRead` zeroes mixed-range holes and prepares the validated reply.

The integrity manager keeps stable entries in a node-based hash map with
`Missing`, `WaitingRead`, `Data`, and `WaitingWrite` states. Preparation resolves
each pair once and retains stable handles while ownership requires them.
Compact used-block bitmaps, expected digests, and current-slot information live
separately from evictable immutable checksum images, preserving lost-write
detection after eviction. Active loads/writes, queued writers, and a writer
selected for resumption keep their entries alive. Successful writer release
wakes one next writer; terminal failure or shutdown resolves all affected waits.
`WaitingWrite` retains a readable old image for a warm pair. A reader encountering
a cold RMW waits for its final write completion before capturing metadata.

`TWriteOperation` owns exclusive pair claims; `TMetadataWrite` owns the images,
checksums, identities, and expected digests needed for transformation. Fully warm
operations build replacements without reading metadata. Otherwise the coroutine reads
one complete pair, or the contiguous two-pair region if either pair is cold:
at most four 4 KiB blocks. Transformation validates images and expected digests,
selects current slots, applies checksums, and updates sequences and digests.
One pair writes its replacement slot. Two pairs write the contiguous middle
slots when both replacements occupy them; otherwise they write all four blocks,
preserving the unchanged current images. The disk format and recovery selection
rules are unchanged.

The cold metadata load is an ordinary critical read in a batch of its own,
prepared by `PrepareMetadataRead`. After it completes, the coroutine transforms
the owned context on the actor thread, and only then prepares the data write and
the metadata image write together. Successful metadata is published into the
cache only on the actor thread. Broken, Stopping, or a failed load or
transformation end the request before any data is admitted; the PDisk fallback
uses the same flow through actor-side raw I/O. Critical overload retries and
accepted-I/O drain apply to the load and to both writes.

Sync validates the complete request before I/O and launches `ExecuteDataWrite`,
whose loop runs over segments in input order and pieces in increasing offset
order. Each piece is at most 512 KiB. The loop awaits and validates one source
payload, allocates only for a valid source reply, performs the destination write,
whose pair ownership is the admission point, and starts the next source only after the current
destination operations retire. No slot scheduler, admission queue, prefetch,
per-slot subscription, or checksum-flush coroutine is needed. Bounded
cooperative yielding prevents inline progress from monopolizing the actor.
Fresh source cookies identify the owning Sync and expected event kind; stale
replies are ignored, and wrong-kind replies preserve the valid route.

Results remain one per original segment in input order. A piece failure skips
the rest of that segment and preserves completed writes; later segments proceed
while the actor remains healthy. Data, metadata, and session failures retain
their error precedence. Broken and Stopping abandon pending remote replies,
prevent new destination work, and drain accepted local branches. The final reply
also preserves the mapping-commit gate, including errors behind an unrelated
allocation already in progress.

Broken and Stopping resolve logical waits without canceling accepted router
I/O waits. Failed branches still drain submitted siblings, retaining buffers
and physical ownership. Batch completions are handled during Stopping too.
Reservation release and Gone wait for `DataRequestsInFlight` to reach zero and
callbacks to retire, including queued results whose callbacks have already
retired. Forced destruction destroys coroutine frames before the actor
destructor drains callbacks; shared callback storage and remaining registry
pins survive that drain.

Shutdown tests must distinguish the indefinite normal actor drain from the
60-second `io_stalled` diagnostic and the 10-second forced-destructor deadline.
Use explicit callback and mailbox barriers to check intermediate ordering;
wall-clock deadlines are hang watchdogs. Router callbacks retain actor state
until retirement, while canceled fallback operations must publish one result
per request before Gone. Validate native io_uring and PDisk fallback separately.

Review session identity, delayed replies, physical ownership, and replay together when changing a persistent operation. NBS quorum, role rotation, dirty-map routing, and flush scheduling are owned by the [partition implementation](../../nbs/cloud/blockstore/libs/storage/partition_direct/README.md); the [load actor](../../load_test/rfc/nbs_dbg_like/README.md) has its own workload policy.
