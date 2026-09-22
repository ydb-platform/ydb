# In-memory metrics ownership

`TInMemoryMetricsBackend` has one owner: the metrics manager actor. Only that
owner registers lines, edits labels, builds snapshots, collects statistics,
maintains line history and chooses chunks for reuse. There are no backend or
per-line mutexes, and no synchronous allocation path in `Append`.

The registry is a producer-facing adapter. Registration, label updates and
snapshot requests enter a lock-free command queue, limited by
`MaxPendingRequests` (default 1024). Rejection is immediate if the queue is full
or shutdown has started. At most 64 commands are handled per activation before
requesting another activation. Requests accepted before shutdown can be
cancelled without a response.

`CreateLine` returns a pending, move-only writer. `Append` returns false until
the actor publishes `Ready` and supplies chunks. Duplicate canonical keys,
filters and `MaxLines` are checked by the actor; a rejected handle subsequently
converts to false. Close publishes `Closed`, including before registration.
Registration uses a Pending-to-Ready CAS and cannot revive a closed handle.
Backend lifetime must exceed writer lifetime. A writer is single-threaded:
Append, Close and moving its handle must not overlap each other.

Each line has a preallocated SPSC ring of ready chunks. `ReserveChunks` defaults
to one and is capped by the pool's physical chunk count. The active chunk is
outside the ring. Every pop requests refill through a shared acq_rel exchange;
the actor resets that flag before draining commands and line requests. Writers
never access the free deque or victim heap and never grow line metadata.
Spare chunks count against `MemoryBytes`. Reserve size is a replenishment target,
not a guarantee that an arbitrarily fast writer never runs dry.

`RequestSnapshot(recipient, cookie)` replies with `TEvInMemoryMetricsSnapshot`.
`RequestLineSnapshot(recipient, lineId, cookie)` and
`RequestLineSnapshot(recipient, name, labels, cookie)` use the same reply type,
but look up and pin only the selected line. The backend provides corresponding
`CaptureSnapshot(lineId)` and `CaptureSnapshot(name, labels)` overloads.
Key lookup canonicalizes labels. A missing line yields an empty snapshot; an
open line without captured chunks is also omitted.
Its `Snapshot.Read(callback)` exposes immutable metadata and captured committed
prefixes; the callback's borrowed views are valid only for the call. The owning
snapshot can be moved or copied across threads and can outlive the actor system.
It retains a shared physical pool, so last-pin release never calls a destroyed
backend or actor. Statistics are sampled by the same actor and are approximate
with respect to concurrently committing writers. Pool counts and committed bytes
of sealed/retiring chunks are maintained incrementally. Sampling reads only
reserved/writable/pending-seal chunks and checks owner-tracked open lines for
writer-published closes; it does not scan the full pool or closed-line history.

Writer-sealed chunks arrive through an intrusive funnel queue, removing the
history scan for PendingSeal. Each queued chunk holds an internal pin so admission
cannot reuse its node before the queue consumer has detached it. Last-pin release
of a retiring chunk publishes it to a second funnel queue and notifies the manager.
The manager alone drains returns, resets chunks and owns the free deque. Both
queues share one hook per chunk, reused only after full detachment. TryPop Retry
reschedules work without spinning inside the actor.

FreeChunkReservePercent defaults to 5 (integer-rounded down by chunk count; zero
disables proactive eviction). Below half the target, the manager retires oldest
sealed chunks until free + retiring reaches the target, at most 64 retirements per
pass. Retiring chunks count as pending supply. Demand eviction is also capped by
pending supply (at least one chunk) to avoid stripping pinned history on retries.
Victims are ordered by their last record timestamp, then chunk id.
Each sealed chunk records its heap position, allowing direct O(log ChunkCount)
removal without stale entries or rebuilding the heap when a line is evicted.

Shutdown closes the callback admission gate and waits for callbacks already in
flight before destroying the endpoint. This wait is shutdown-only; writers and
snapshot releasers do not wait on a mutex. Late snapshot releases can still queue
returns in the shared pool but cannot notify a destroyed backend. Refill also has
a one-second periodic retry. `MaintenanceBatchSize` (64) bounds each maintenance
stage, including refill, sealed/returned chunks and normal close processing.
Full snapshots still traverse all line history; a selected-line snapshot visits
only that line's chunks. Statistics still traverse the mutable subset described
above. Removing mutexes is not a wall-clock or
wait-free guarantee: command/event allocation, transport and CAS retries remain.

During shutdown the registry stops admission and writers stop accepting values.
After executor threads join, the shutdown hook cancels pending commands and
finishes closes published by pre-stop callbacks. No event delivery is required
for this final cleanup. Snapshots already delivered remain readable.

Per-line history uses intrusive links in the fixed physical chunk pool. Removing
a chunk takes constant time and retains no per-line peak-capacity array; list
metadata is O(ChunkCount + MaxLines). This excludes snapshot metadata and the
configured per-line reserve queues. Admission drains close notifications before
checking the line limit and selects an eviction candidate from the owner's
closed-line set. Replacing a known closed key removes that line directly.
Neither path searches all registered lines. At the line limit, registration may
process up to max(MaxLines, MaintenanceBatchSize) close notifications.

Line refill requests use a third intrusive funnel queue. A per-writer
atomic flag coalesces requests. The queue owns a shared reference in the node;
the consumer moves it out before clearing the flag, allowing safe re-enqueue.
An owner-only weak reader reference handles requests for already-removed lines.
Registration queues initial delivery; each reserve pop (or empty retry)
queues that specific line. Close of a Ready writer uses a separate lock-free
`CloseRequests` queue holding shared references; closing a Pending writer is
observed by registration. No registry scan or sorting occurs in maintenance.
The manager drains at most 64 requests and services at most 64 refill-list entries
per pass. `WaitingForRefill` stays set while a line is being serviced or requeued,
and clears when refill finishes or the line closes.
Lines waiting for pinned capacity stay in an owner-only intrusive list;
chunk returns, new requests or the periodic tick retry it, without self-spinning
on unavailable memory. Shutdown drains all remaining line requests after writers
stop, including the separate close queue. ClosedLines reports the writer-published
status, matching snapshot metadata.
