# Generic batching and resources (D3)

`TScheduler` wraps the D1 lifecycle. `TExecutionServices` uses it for the D2
MiniKQL graph. No WASM, HTTP, authorization or engine-specific code is involved.

## Submission and batching

Register an `IProvider` for scalar dispatch or an `IBatchProvider` for explicit
batch support. Submission admits the operation and puts it in a bounded queue;
it never invokes the backend. Each operation keeps its D1 ID and completion
sink. Batch responses may be partial/out of order. Cancelling one position does
not cancel its neighbours. A thrown StartBatch fails the affected positions;
physical reservations remain until the backend acknowledges cleanup.

The provider supplies an opaque compatibility key (at most 1024 bytes). It must
encode any tenant/security/codec/cancellation incompatibility. The scheduler
also separates registration, operation contract and exact deadline. It never
examines credentials, endpoints or application fields. Compatibility does not
permit deduplication: equal arguments remain independent IDs.

`MaxBatchSize`, provider batch capacity, frame window, `MaxInFlight` and
`TLimits::MaxOperations` have distinct meanings. MaxOperations includes queued,
ready and cleanup-pending entries. MaxInFlight counts dispatched items until
physical Release, including cancelled items. Each provider owns its transport
pooling and any physical-send metrics.

At the boundary of available work call `Flush(tryStep)`. MiniKQL does this on
Yield; the owner also flushes on BeginTurn. A partial batch is sent immediately
at this boundary, without waiting for more input or a timer. Flush returns true
only when dispatchable work remains and CPU budget prevented it. Capacity
exhaustion returns false and waits for cleanup. Queue/dispatch operations and
all hooks except wakeup/cleanup run on the owner.

Operation deadlines still use `NextDeadline()` and owner `ExpireDeadlines()`.
The engine must arm that one deadline event; there is no background polling.
Flush expires queued work before dispatch. The boundary-flush policy does not
need a separate batch-filling timer.

## Resource ownership

`TMemoryQuota` is a thread-safe shared reservation pool. By default each
scheduler owns its pool; a host can supply a shared pool. A waiter is identified
by the D1 execution generation. Releasing bytes wakes only generations whose smallest blocked reservation now
fits. Callbacks are moved out of the lock and enqueued once per registration;
rolling back a rejected submission cannot wake an unsatisfied waiter. Shutdown removes the waiter and detaches the original wakeup capture after
cancellation, outside locks; callbacks already in flight retain their own copy;
a notification already in flight is rejected by the generation/closed gate.
Wakeup callbacks must be nonthrowing and only enqueue work.

One operation reservation covers its encoded request, maximum response, bounded
compatibility key and conservative entry metadata allowance. It is shared by
request, lifecycle entry and returned TResult. Taking a result out of D1 does
not release its memory while a slow consumer still owns it. Cleanup and result
consumption must both release their references. Provider code may borrow this
lease for the same buffers; passing a lease into another logical Submit is
rejected. New logical operations and additional private buffers need their own
reservations. The quota never grows to accommodate an impossible request.

The graph additionally reserves continuation vectors/metadata and reconciles
its allocator's retained-page usage on Fetch and instruction boundaries. These
are distinct allocations from the encoded provider buffers. Ready/reorder
frames keep their reservations until consumed or cancelled. Input admission
reserves the destination frame before starting Source.Fetch and retains this
reservation across Yield. Nested sources can therefore consume their ready
results without requiring new outer-frame credits. The reservation transfers
into the returned row frame, or is released on EOF, failure or destruction.
Admission stops when frame memory is unavailable; a local pool that cannot fit
the suspended frame path and has no outstanding work fails explicitly. Temporary occupancy waits for a real
credit notification; an impossible continuation with no outstanding work fails.
A shared external pool may instead wait for another execution to release bytes.

Reservation bounds do not preempt arbitrary C++ allocation inside a synchronous
fragment or provider. The existing engine allocator guard must enforce transient
allocation peaks; its task/node manager integration is D6. Hosts must also
account for their provider-private buffers using the shared quota. The public
mechanism provides owned leases and wakeups, not an estimate that permits
unaccounted buffers. `MaxMemoryBytes` creates the default pool; supplying `Quota`
replaces it with that pool's bound (it is not a second independent limit).

## Fairness and CPU

Dispatch is round-robin across registered providers. Within a provider the oldest
queued item selects the next compatibility group; compatible positions preserve
queue order. A hot producer cannot insert new work ahead of an older incompatible
item. Each dispatch consumes a cooperative step, and batch size is bounded.

MiniKQL rotates the next frame after every local step, preserving ordered output.
It finishes available local work before admitting more source rows. Nested maps
retain D2's source-fetch charge across Yield, so a one-step quantum still makes
progress. Per-row dependent calls remain sequential; batching combines only
already submitted independent operations.

Each owner event calls `BeginTurn()` once, then fetches, then `EndTurn()`.
Thread CPU time (microseconds) and a step bound limit local work. Each turn
permits at least one cooperative step, even when bookkeeping exhausted its CPU
budget; subsequent steps enforce the bound. This prevents zero-progress reruns. EndTurn includes
the final local work in metrics; BeginTurn starts a fresh CPU sample so other
tasks and callback waiting are not charged. A runtime rerun is coalesced and
posted before budget Yield. Provider/input/quota/output waits do not cause a
self-wakeup loop. Other ready tasks can run between owner events on one worker.
The engine must preserve this protocol; actor/runner bridges remain D6.

## Observability

Scheduler snapshots expose queued/in-flight items, reserved memory, submissions,
dispatches/items, drained terminal outcomes and accumulated queue/execution wait.
`MemoryBytes` is the supplied pool's gauge: export a shared node pool once,
rather than sum its value across tasks. Compute snapshots include turns, steps,
measured CPU time and CPU-budget yields. Cancellation/timeout outcomes are
recorded on owner result drain; call TakeReady after Shutdown (execution services
do so). Cleanup can outlive the logical result and retains its capacity charge.

The optional owner trace hook reports Submitted/Dispatched/Completed events with
operation IDs and timings. It is nonthrowing and carries no payload, key, secret
or network address. The host can correlate IDs with its execution context; real
SQL tracing integration and transport facts are separate adapter concerns.

## Tests and delivery boundary

Core tests cover scalar fallback; batches of 1/8/64 and partial tails; security,
contract/deadline/registration separation; partial/out-of-order results; per-item
cancel; queued cancel/expiry; cleanup-retained capacity; slow consumers; a shared
quota; shutdown; callback-thread release; callback-capture destruction and metrics.
Graph tests cover actual 8+8+3 batches, one-worker CPU fairness after the last
external completion, frame round-robin, slow downstream, memory failure and all
D2 branch/continuation/nested-map regressions.

D4 still owns checkpoint/replay and graph checkpoint remains rejected. D5 owns
additional operator/UDAF lowering. D6 owns production engine services, quota
manager/allocator wiring, SQL discovery and actor/local/pipe acceptance. The
WASM stack is not migrated by D3.
