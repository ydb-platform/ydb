# Experimental task scheduling

This PoC has a [pool of reusable anonymous compute workers](worker_pool_design.md)
with ready-query and per-query ready-task queues. The older dispatcher mode is
retained as a comparison baseline. Task scheduling is disabled by default and
requires DQ channels v2. The trace and worker paths have been built and exercised
with a 16-node kqprun Q5 run; see [trace validation](trace/README.md#validation).

Configure `TTableServiceConfig` before starting the node:

```protobuf
DqChannelVersion: 2
DqChannelConfig {
    EnableTaskScheduling: true
    TaskSchedulerObserveOnly: false
    TaskSchedulerUseWorkerPool: true
    TaskSchedulerShards: 4
    TaskSchedulerMaxInFlightPerShard: 2
    TaskSchedulerMaxRowsPerRun: 0
    TaskSchedulerWorkerSliceCpuUs: 1000
    TaskSchedulerWorkerActivationUs: 2000
}
```

`TaskSchedulerMaxRowsPerRun: 0` disables voluntary runner yields by row count,
CPU slice and activation deadline (the default). A runner continues until its
normal input/output wait or completion. A positive value opts into cooperative
runner budgets. Worker activation limits still apply between task executions;
CPU quota admission and accounting remain enabled.

With `TaskSchedulerObserveOnly: true`, the same per-run instrumentation is active,
but original notifications, execution order, and unlimited runner slices are
preserved. Use this mode as the baseline for scheduling experiments.

## Worker execution

With scheduling enabled, `TaskSchedulerUseWorkerPool` defaults to true. There is
no dispatcher actor in this mode. The initial worker pool uses the channel
service's user executor pool. It creates `Shards * MaxInFlightPerShard` persistent
workers; `MaxInFlightPerShard` becomes the per-query worker concurrency limit.
These existing configuration names are retained for PoC compatibility.

Each worker pulls a query, then a task. Query ordering uses accumulated worker
CPU service divided by weight, including reservations for in-flight slices.
Weights default to one; KQP refreshes them from the resource scheduler's query
fair share. Task ordering prefers explicit readiness over conservative waits,
then minimum graph distance to a result/sink/effects endpoint, then FIFO.
A task bypassed for 64 selections receives an ageing boost. Graph distance
includes remote edges, but only local tasks are executable. Unknown distances
remain eligible. Queue selection currently scans ready query/task sets under
one short metadata lock; runner execution never holds it. This is an initial
implementation, not a lock-free or proven scalable queue.

`TDqTaskExecution` holds the runner and a task-exclusive lease. It binds the task
allocator and operator memory quota, obtains a query-specific CPU grant through
`IDqSchedulableWork::TryAcquireSlice`, and runs MiniKQL directly in the anonymous
worker's handler. Quota denial parks the whole query until its retry deadline.
Workers can switch queries after every slice and park when no work is selectable.
At the activation deadline they yield the executor thread, retaining the actor.
The CPU grant currently admits the configured cooperative slice cap through the
existing quota gate; it is not an exact remaining-CPU token balance.

The first KQP integration supports pure synchronous channel tasks. Scan metadata,
external sources, sinks/effects, transforms, checkpoints, watermarks, WASM TLS and
separate batch executor pools retain the original task-bound execution path.
The full graph still includes those tasks. For opted-in tasks the stable actor
is an endpoint for channel/control events and output finalization. It defers
events while a worker owns the runner; allocator, compute context and quota
lifetime remain protected by the same lease. The worker returns a slice result
to the endpoint and immediately becomes available for other work. The handle is
completed after output/control processing, so the same task cannot be scheduled
again prematurely. Complete extraction of all task state from the endpoint is
still future work.

Input wait descriptions and concrete spill futures are preserved. Version
changes determine candidate readiness; the existing conservative probe fallback
does not guarantee a useful operator step. Such candidates remain explicitly
lower priority, rather than being labelled as proven-ready. General level
predicates and shared-resource reservations remain part of the target design.
CPU, wall and output-row limits are checked after successful Fetch calls. A long
Fetch can overrun them and needs operator safe points for tighter preemption.

Worker counters use `component=TaskSchedulerWorkers`: `WorkerSlices`,
`WorkerRuns`, `WorkerCpuUs`, `QuotaRejected`, `Selections`, `SelectionCpuUs`,
`SelectionLockWaitUs`, and `LeaseRetries`. Per-run progress metrics below still
apply. Query fairness uses worker CPU; endpoint/control overhead and legacy
source work are not included in that virtual runtime, although existing KQP
resource accounting remains active for legacy work.

## Dispatcher baseline (`TaskSchedulerUseWorkerPool: false`)

Each query execution graph hashes to one dispatcher on each participating node,
using its executer actor ID. All of that graph's local tasks share this dispatcher;
different queries can share a dispatcher too. No run permits cross node boundaries.
An SQL request that runs several execution graphs is grouped per executer, not
across successive graphs. The task handle retains the query identity for future
query-level policies. Query fair sharing is not implemented yet.

### Query topology and local state

When scheduling is enabled on the coordinator, KQP planner builds an immutable
snapshot of all tasks, stages and channels in its execution graph. It includes
input/output port indexes, routing kinds, planned node IDs and source/sink/effects
roles, but no programs, parameters or credentials. Remote start requests carry
this metadata once per task batch; local starts use the same snapshot directly.
Nodes keep actual local task IDs separately from placement hints. The dispatcher
knows the whole topology, including edges to remote tasks and external result
endpoints, while it controls only the locally enrolled compute actors.

`RegisterGraph()` returns a lifetime registration shared by the planner or local
query manager, compute actors and their handles. The last owner removes the graph
from the dispatcher. Metadata therefore has a cleanup path even when compute
actor creation fails.
`DescribeQuery()` asynchronously replies with `TEvQueryGraph`: full topology,
actual local task IDs and the latest states of locally managed tasks. State
revisions reject stale reports; updates from a replaced actor cannot overwrite
its successor. This registry does not yet implement demand propagation or change
the weighted queue policy below.

### Explicit operator waits

The task runner installs an optional `IExecutionWaitObserver` in the runtime
MiniKQL computation context. Operators keep the existing Fetch statuses and use
the observer to describe an input wait, a concrete async future, a callback wait,
or an explicit runnable continuation. The report includes operator identity
(currently a process-local state address), input side, dependency condition and
the source of the reason, and separately whether the dependency condition is
exact, collected from input probes, or an opaque fallback.
Nested input scopes retain a child's spill reason and add the enclosing input
path instead of replacing it with a generic input wait.

Packed hash joins report their build/probe input phase and the actual spill read
or write futures. Grace join reports left/right input waits and spill phases;
its older storage path still uses a spill-only callback dependency, marked as an
opaque fallback. Unadapted operators retain conservative channel-probe waits
with callback and spill fallbacks; an input scope without observed dependencies
accepts any notification.
Async future completion signals its own dependency. The legacy spiller wakeup
signals a separate spill dependency instead of forcing every task wait runnable.
The await token is installed before subscription, so completion before parking
is observed as well. Typed waits do not infer an ALL condition from two failed
probes.

Run completion now reports progress, wait descriptions and state to the dispatcher
before returning its slot. Ready, running, blocked, finished and cancelled states
are distinct. Budget exhaustion and CPU-quota rejection are recorded separately
from dependency waits. Normal completion disables the handle without labelling it
as a cancelled query.

### Dispatch policy

A dispatcher keeps three FIFO queues and a bounded number of outstanding permits.
The bound applies to all queries assigned to that dispatcher together. With the
defaults, one query can therefore have at most two outstanding permits per node;
query affinity can leave other shards idle. Tasks do not migrate between
dispatchers. Weighted round robin gives
draining, normal, and producing tasks 4:2:1 opportunities when all classes have
work. Tasks consuming input have preference over tasks only producing output;
channel pressure can promote a waiting consumer. Empty classes do not delay the
others. This is an initial heuristic, not a fair-share policy between queries.
Existing KQP CPU-quota checks and accounting still apply, including their retry
timers. Work stealing and quota-driven redispatch are not implemented yet.

The compute actor continues to own the runner, allocator, and operator state.
Only a matching run permit may execute it in scheduling mode. Returning a permit
or cancelling a dispatched task releases the dispatcher slot exactly once.
Errors, cancellation, timeouts, and incoming data remain actor messages.

Channels publish task-local dependency versions instead of sending execution
wakeups directly to enrolled compute actors. Failed channel probes record the
version observed before examining the buffer. After execution the task waits for
any observed dependency, an opaque async callback or a spill callback to change.
This also catches
changes between the empty probe and publishing the wait. Notifications while the
task is queued or running are coalesced. A ready-set union republishes the cached
empty-probe versions for its idle inputs when it yields, including inputs it
skipped on that Fetch. If no precise wait is available, the task conservatively
waits for any notification.

`TTaskWaitCondition` also supports OR-of-AND conditions for operators that can
provide an explicit wait set. Current channel integration deliberately uses
conservative ANY waits: it does not infer that a join needs both inputs from the
fact that both were probed. Adapted joins provide the explicit phase and future
waits described above; other operators still use the conservative fallback.
KQP streaming table aggregation records opaque callback waits to
avoid the legacy self-poll fallback while waiting for table I/O.

The runner checks an output-row budget after a completed Fetch. Reaching the
budget flushes output and explicitly marks the task runnable. It is not an
ordinary input wait. The legacy `ERunStatus` stays unchanged; scheduling metadata
records budget exhaustion separately. A single long Fetch is not preempted.

The scheduler does not fix cycles of blocked dependencies, such as a build/probe
join reading two branches of one source when the unread branch fills its channel.
Such plans still need spilling or a different execution strategy.

## Metrics

Metrics are under KQP `subsystem=compute_channels/component=TaskSchedulerPoC`.

* `Attempts`, `QuotaRejected`, `Dispatched`, `Enqueued`, `Notifications`, `Coalesced`
  distinguish dispatcher activity from actual runner execution.
* `Runs`, `InputItems`, `InputRows`, `OutputRows`, `InputBytes`, `OutputBytes`,
  `CpuUs`, and `BudgetYields` describe runner work. Quota rejections are not runs.
* Histograms: `InputRowsPerRun`, `OutputRowsPerRun`, `CpuUsPerRun`,
  `ElapsedUsPerRun`, and `ReadyWaitUs`.
* `NoObservedProgressRuns` and `NoObservedProgressCpuUs` count runs with no
  instrumented input/output or completion progress. They are candidates for
  analysis, not proof of useless execution: internal operator work is not fully
  instrumented.

Input rows come from input-stream metering, including logical rows inside Arrow
blocks. Input items additionally cover opaque embedded table readers whose row
counts are not available through those streams. Output rows count logical rows,
not just Fetch calls. Bytes count transferred channel chunks, including control
markers and copies sent to multiple channels; buffered rows may be consumed in a
later run than the channel read. Source I/O and operator-state memory are not
included. CPU uses thread CPU time; elapsed time is measured separately.

Useful comparisons include input rows / runs, no-observed-progress CPU / total
runner CPU, output rows / CPU, query latency, and peak channel memory. Increasing
the slice budget alone raises rows/run, so keep the budget visible in comparisons.
Per-row input-size metering and dependency locks add overhead in this PoC; measure
that overhead with observation mode before drawing throughput conclusions.

## Execution timeline

Optional bounded per-query capture records task contents/topology, state changes,
worker selection criteria, runner/worker intervals, progress and passive channel
snapshots. Captures survive local graph cleanup and can be downloaded from
`/actors/kqp_channels?task_traces=1`. A stdlib Python utility merges node captures
into a Perfetto timeline with waiting intervals and a per-task CSV summary.
See [capture configuration, download commands and semantics](trace/README.md).
Trace-only mode observes synchronous channels-v2 tasks without enabling queues.

## Validation

A [fast actor-runtime comparison](compare/README.md) runs direct notifications,
observation and scheduling in one process without starting YDB. It produces CSV
measurements and checks a bounded producer/aggregation/consumer graph.

```bash
./ya make --build relwithdebinfo -tA ydb/library/yql/dq/task_scheduler/ut 2>&1 | tail
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/task_scheduler 2>&1 | tail
./ya make --build relwithdebinfo -tA ydb/library/yql/dq/runtime/ut -F 'DqInputReadySet::*' 2>&1 | tail
./ya make --build relwithdebinfo -tA ydb/library/yql/dq/runtime/ut -F 'DqExecutionWait::*' 2>&1 | tail
./ya make --build relwithdebinfo -tA ydb/library/yql/dq/actors/compute/ut -F 'DqDetachedTaskExecution::*' 2>&1 | tail
./ya make --build relwithdebinfo -tA ydb/library/yql/dq/comp_nodes/ut -F 'TDqHashJoinBasicTest::TestJoinReportsConcreteSpillFutureThroughContext' 2>&1 | tail
```

The scheduler tests exercise wakeup races, ANY/ALL waits, concurrent notifications,
permit cancellation, explicit continuations, dispatch bounds, run metrics, graph
registration and local ownership. Runtime tests cover concrete future waits,
completion before parking, nested input context and cancellation. The hash-join
test checks the actual operator's reported spill future using controlled writes.
Worker-pool tests cover graph distance, weighted query service, in-flight CPU
reservations, exclusive claims, idle-worker wakeups, query-wide quota waits,
upstream ageing and one persistent worker serving multiple queries. The KQP
comparison asserts that the worker mode actually records worker runner calls.
The KQP tests compare query results with legacy and observation modes and exercise
a small slice budget and execution on two nodes.
