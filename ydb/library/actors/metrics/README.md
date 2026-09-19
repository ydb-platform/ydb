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
the actor resets that flag before draining commands and scanning lines. Writers
never search the free bitmap or victim heap and never grow line metadata.
Spare chunks count against `MemoryBytes`. Reserve size is a replenishment target,
not a guarantee that an arbitrarily fast writer never runs dry.

`RequestSnapshot(recipient, cookie)` replies with `TEvInMemoryMetricsSnapshot`.
Its `Snapshot.Read(callback)` exposes immutable metadata and captured committed
prefixes; the callback's borrowed views are valid only for the call. The owning
snapshot can be moved or copied across threads and can outlive the actor system.
It retains a shared physical pool, so last-pin release never calls a destroyed
backend or actor. Statistics are sampled by the same actor and are approximate
with respect to concurrently committing writers.

A last-pin release returns capacity atomically. Refill is retried by the next
writer request or a one-second manager tick. The actor allocates at most 64
chunks per maintenance pass, but scanning/sorting lines and scanning retained
history are not bounded by that budget. Removing mutexes is not a wall-clock or
wait-free guarantee: command/event allocation, transport and CAS retries remain.

During shutdown the registry stops admission and writers stop accepting values.
After executor threads join, the shutdown hook cancels pending commands and
finishes closes published by pre-stop callbacks. No event delivery is required
for this final cleanup. Snapshots already delivered remain readable.

Per-line history uses intrusive links in the fixed physical chunk pool. Removing
a chunk takes constant time and retains no per-line peak-capacity array; list
metadata is O(ChunkCount + MaxLines). This excludes snapshot metadata and the
configured per-line reserve queues. Admission observes the writer-published
Closed status without waiting for a maintenance pass.
