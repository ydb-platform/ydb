# Target concept: reusable compute workers

This design supersedes dispatcher-issued permits as the target architecture.
The source now includes the concurrent queue, persistent workers, cooperative
slice budgets and a KQP integration for pure synchronous v2 channel tasks. These
changes have not been compiled or executed yet. The dispatcher remains an
explicit comparison mode. See the README for the current scope and configuration.

The following sections describe the full target. In the current integration the
endpoint still owns compute context and memory-quota lifetime under the shared
execution lease; generalized readiness predicates, full state extraction,
additional executor pools and source/sink/checkpoint migration remain unfinished.

## Ownership

Each participating YDB node has a shared work pool for each compatible actor
executor pool. Persistent anonymous compute actors pull work from it. A worker
does not own a query, task runner, allocator, channel identity, or query quota.
It temporarily owns an exclusive execution lease for one task slice.

The pool contains a ready-query queue. Each query has its own ready-task queue,
immutable full topology, local task registry, resource-scheduler identity,
accounting and running-task count. Blocked tasks are registered but absent from
the ready-task queue. A query is selectable only when it has ready local work,
available concurrency and CPU quota. Its last task completing does not destroy
any worker.

The existing graph identity is the KQP executer actor ID. A user SQL request may
run several successive execution graphs. Grouping those graphs under a longer
lived SQL-request identity would be a separate change.

`TTaskExecution` owns the runner, allocator, computation context, memory quota,
operator state, input/output adapters and asynchronous-operation handles. A
worker binds that task's allocator, memory-quota scope, query resource identity
and tracing context for a slice, then restores its own context on exit.

A stable task endpoint remains the address for remote channels, source events,
errors and cancellation. Initially this can be a thin task-bound actor. It
receives events and queues task-local commands; it never runs the MiniKQL graph.
Workers never borrow an existing actor object and invoke its methods from a
different mailbox. An actor's `SelfId()` is the worker's identity while the worker
runs; channel protocols and task callbacks must use the explicit task endpoint.

All accesses to mutable runner state, including polling sources, ingesting
non-thread-safe buffers, checkpoint state and output processing, use the same
execution lease. The endpoint cannot mutate these concurrently with a worker.
Thread-safe channel buffers and dependency counters can still receive data
concurrently. Other events go through the pending-command inbox. Initialization,
errors and output-finish acknowledgements are schedulable control work even
when no data rows can be produced. Only fully retired tasks release their state;
runner `Finished` alone is insufficient while channels still need closing.

## What ready means

Keep the MiniKQL `One/Yield/Finish` protocol and the optional context observer.
Extend the side channel to describe the next executable step, its prerequisites
and whether readiness is known or conservative. Examples:

* input has data or an end marker, with enough output space when required;
* both required inputs are available for the current operator phase;
* a specific spill-read or spill-write future completed, including an error;
* an internal continuation is runnable without new input;
* a control command needs processing.

Dependency versions prevent lost notifications; a changed version is not itself
proof that the predicate is true. Waking a candidate requires checking its
current level predicate. After acquiring the exclusive task lease, a worker
validates the predicate and cancellation epoch again before running it.
Resources which another consumer can take need a reservation, not just a check.

“Guaranteed progress” means that the declared next step can execute. It does
not guarantee an output row: joins can build a table, filters can discard every
row, and completed I/O can report an error. Record both internal/control work
and data progress when possible; absence of input/output rows is not proof of
wasted execution.

The current probe-based waits do not provide this stronger contract. Adapted
operators can use the known-ready path. During migration, unadapted operators
must retain a separately labelled conservative path or the legacy executor.
They cannot be silently classified as guaranteed-ready or permanently excluded
from execution. Initial creation and explicit continuations are executable work
without an input arrival.

Notifications only update task state and publish ready work. They do not send
execution events to a specific task actor. Publish ready work and register an
idle worker using a check/register/recheck protocol, so a notification between
the empty check and parking cannot strand the pool. Duplicate notifications
coalesce. A worker awakened to drain the pool is not reserved for the task that
caused the notification.

## Choosing work

The selection order is:

1. Pick a ready query permitted by its resource pool and concurrency limit.
2. Obtain a query-specific CPU reservation and task-slice allowance.
3. Pick a known-ready local task nearest a result endpoint.
4. Acquire its exclusive lease and revalidate its prerequisites.
5. Execute a bounded slice, publish progress and the next readiness condition,
   charge actual CPU, and release both reservations.
6. Reconsider query priority before taking another task.

Query selection uses configured weights and accumulated CPU service, for example
weighted virtual runtime within the applicable priority class. Reserve estimated
service for in-flight slices and reconcile it with actual service on completion,
so concurrent workers cannot all exploit an uncharged query. Bound per-query
concurrency and age waiting work. A permanently ready query must not win forever
merely because its initial priority was highest. The next selection can choose
the same query if it is still entitled to service; requiring a different query
would strand workers when only one query is runnable.

For task selection, compute distance to a result over the full graph using a
reverse BFS from external result channels, sinks and effect endpoints. A result
task has distance zero; upstream tasks have increasing distance. Remote edges
contribute to distance but only local tasks can be claimed. Tasks without a
reachable endpoint receive an explicit unknown distance and remain eligible.
Use readiness first, distance second, then waiting age/FIFO. Add an ageing or
bounded-bypass rule so continuously ready downstream work cannot starve upstream
work. Keep query fairness separate from within-query task priority.

This policy does not solve deadlocked diamonds whose buffers require spilling.
It chooses among runnable tasks; it cannot make an unsatisfiable prerequisite
true. Pure producers run when downstream ready work has drained, subject to
ageing and the query's share.

## CPU grants and actor execution

Two budgets have different ownership:

* A worker activation has a short occupancy limit before returning control to
  the actor executor. It is independent of which query will be chosen next.
* A task slice has a query-specific CPU allowance admitted by the existing KQP
  resource scheduler after selecting the query. Release and charge it before
  switching query identity.

The current `TSchedulableBase::TryStartExecution()` returns admission or a retry
delay for query-bound work. It does not return a remaining time quantum. Add an
explicit reservation/allowance interface, retaining the existing resource-tree
accounting and wakeup policy. Do not reserve quota for one query and execute a
different query under it. Asynchronous source work also keeps the task's query
identity through its work factory.

Use a CPU budget and a wall deadline for a slice, bounded by the worker's
remaining activation allowance. Check them cooperatively between Fetch calls
and keep the row budget as another limit. Long Fetch calls can overrun these
limits; they require additional operator safe points for tighter preemption.
No claim of hard preemption is made by the PoC.

Workers can run several slices during one activation. At the occupancy limit
they enqueue one continuation for themselves and return from the handler. With
no selectable work they park, rather than spin or immediately reschedule
themselves. An actor stays alive while yielding its executor thread. A worker
does not keep a task lease while waiting for quota, input, output space or I/O.
Quota-denied queries wait for a scheduler signal or deadline, not busy polling.

## Concurrency without a dispatcher actor

The work pool is a node-local concurrent C++ object, not a master actor and not a
cluster-wide service. Workers call queue APIs directly. Task state transitions
are `Blocked -> Ready -> Running -> Blocked/Ready/Retiring/Finished`; an atomic
lease/generation prevents two workers from running one task. Cancellation marks
the epoch immediately and retires state only after its active lease is returned.
Late callbacks hold weak references and cannot resurrect a retired task.

For the first PoC use one short queue lock per compatible executor pool plus a
per-query lock. Never hold a queue lock during graph evaluation, I/O, allocator
binding, callback subscription, destruction or runner execution. Specify one
lock order and release queue locks before acquiring an execution lease. Recheck
and retry on stale queue entries. This gives exact local query ordering at each
successful claim, with a measurable shared metadata critical section.

No dispatcher actor does not mean no coordination cost. Measure lock contention
and selection CPU before claiming scalability. If selection becomes limiting,
partition ready queries among queue shards and add work stealing or multi-queue
selection. Queries retain one task-queue owner; several workers can lease
different tasks of a query. This removes a single hot query-queue lock across all
queries, but changes exact global priority to approximate priority across shards.
That tradeoff must be explicit. Neither variant has one owner for the cluster.

## Implementation sequence

1. Implement actor-independent query/task queues, graph distance, task leases,
   idle-worker notification and a resource-grant interface. Test deterministic
   selection and wakeup/cancellation races before KQP integration.
2. Replace the synthetic comparison's task execution actors with persistent
   workers over several query instances. Exercise the actual queue and lease
   implementation. Keep the dispatcher comparison as a labelled baseline.
3. Extract `TTaskExecution` from `TDqComputeActorBase` and
   `TDqSyncComputeActorBase`. Move the full execution envelope, not just
   `TaskRunner->Run()`. Keep a thin endpoint for the existing network protocol.
4. Add CPU-time/deadline checks to the runner and adapt KQP resource admission
   to per-slice query grants. Register worker actors per compatible executor
   pool through the node service/factory. Opt in synchronous v2 tasks first;
   checkpointing and unsupported sources retain their legacy execution path.
5. Reuse `IExecutionWaitObserver`, explicit spill futures and the full task graph.
   Add non-consuming readiness predicates/reservations for supported inputs and
   outputs; preserve an explicit conservative category for unadapted operators.
6. Compare legacy execution, dispatcher PoC and worker pool with identical
   workloads, slice limits and resource limits. Keep all three modes clearly
   labelled until the replacement is validated.

Relevant code boundaries are `task_scheduler.{h,cpp}` and `task_graph.{h,cpp}`;
`dq_execution_wait.{h,cpp}` and `mkql_execution_wait.h`;
`dq_compute_actor_impl.h`, `dq_sync_compute_actor_base.h` and `dq_tasks_runner.cpp`;
KQP compute-actor creation and query control plane; and
`kqp_schedulable_base.{h,cpp}` / `kqp_compute_actor.h` for quota integration.

## Validation and observability

Required scenarios include two workers contending for one task; multiple tasks
from one query running concurrently; weighted service across several queries;
closest-ready-to-result selection; upstream ageing; producer/consumer pressure;
I/O completing before parking; all queries blocked or quota-denied; cancellation
during execution; and finishing output channels after runner completion. Verify
that one persistent worker executes tasks from different queries and that query
CPU and memory attribution never follows a previous task.

Measure query latency/throughput and CPU fairness, task slices and work per
slice, readiness recheck failures, conservative attempts without observed
progress, useful/control work, queue waiting time, peak buffered and task-state
memory, selection/locking CPU, worker idle/active time and budget overrun. Keep
the synthetic benchmark's serialized payload limitation visible; add independent
query payloads to measure worker parallelism. No hard timing threshold belongs
in correctness tests.

For later visualization, record worker ID, query/task IDs, lease generation,
readiness evidence, distance, query priority/service at selection, CPU allowance,
actual CPU, progress and next wait. Use bounded per-worker recording with an
explicit overflow count. The prior trace proposal remains unimplemented.
