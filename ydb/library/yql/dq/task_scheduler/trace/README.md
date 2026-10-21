# Task execution timeline

Opt-in diagnostic capture for synchronous KQP runners using channels v2. It works
with the worker pool, the dispatcher baseline and the original task-bound path.
Enabling capture alone does not enable the scheduling queues.

Configure all participating nodes before running the query:

```protobuf
DqChannelVersion: 2
DqChannelConfig {
    EnableTaskExecutionTrace: true
    TaskExecutionTraceMaxQueries: 16
    TaskExecutionTraceMaxEvents: 100000
    TaskExecutionTraceMaxBytes: 8388608
    TaskExecutionTraceRetentionMs: 600000
}
```

Keep the existing scheduling options when comparing scheduler modes. These
settings are read when the channel service is created; runtime toggling is not
implemented. Capture is off by default.

## Download after execution

The existing channel monitoring page serves JSON without holding the channel
service lock. Use the monitoring address of each participating node:

```bash
curl --get 'http://localhost:8765/actors/kqp_channels' \
  --data-urlencode 'task_traces=1' > traces.json
```

The list contains query executer actor IDs, capture sizes, completion and loss
markers. An execution graph is identified by its executer, not by SQL text or
transaction ID; one SQL operation may have several execution graphs. Download
one snapshot per query and node, selecting a `query` value from the list:

```bash
curl --get 'http://localhost:8765/actors/kqp_channels' \
  --data-urlencode 'task_traces=1' \
  --data-urlencode 'query=<executer actor id>' > node-1.json

python3 ydb/library/yql/dq/task_scheduler/trace/prepare_timeline.py \
  node-1.json node-2.json -o timeline.json --summary tasks.csv
```

Open `timeline.json` in [Perfetto](https://ui.perfetto.dev/) with **Open trace file**.
Each query/node is a process, each task/actor a named track. Select a runner
interval to inspect its duration, CPU, progress and channel snapshots. Select
`task`, `selected`, `state` and `signal` markers to inspect contents, selection
criteria, stop reasons and dependency notifications. `queue_wait` and
`dependency_wait` intervals are derived from state changes, not sampled CPU use.
An interval without a closing transition is explicitly marked `open_interval`.
No synthetic execution duration is assigned to tasks that never ran.

The CSV summarizes captured runner calls per task/actor. It includes runs, CPU,
wall time, rows and rows/run. No-observed-progress runs are not proof of useless
work: an operator can do useful internal work without consuming or emitting rows.
Truncated captures produce partial totals and explicit warnings.

## Recorded data and semantics

* `graph`: complete task/channel topology, stages, routing and source/sink/effects
  flags. `local_tasks_registered` records ownership for every local start batch.
* `task`: counts of callable names in the task program and source/sink types.
  This describes program contents, not a per-operator execution trace. It excludes
  literals, parameter values, query text and connector settings. Wait operator
  IDs remain process-local state addresses; they are not indexes in this list.
* `state`: ready, dispatched, running, blocked and terminal transitions with
  revisions/generations, last-run progress, stop reason, typed input/spill waits,
  ANY/ALL dependency versions and the precision of each wait description.
* `signal` / `continuation`: dependency/version and whether the notification made
  a blocked task ready. Repeated notifications remain distinguishable from runs.
* `selected`: worker ID, query service score/weight, reserved CPU, concurrent query
  work, exact/conservative readiness, graph distance, FIFO sequence and ageing.
  Selection can be followed by a lease retry or cancellation without a run.
* `runner`: actual `Run()` interval, thread CPU time, input/output logical rows,
  items, completion/budget flags and passive channel samples before and after it.
  `runner_status`: 0=Finished, 1=PendingInput, 2=PendingOutput; null on exception.
* `worker_slice`: worker execution envelope, including quota admission, lease
  release and diagnostics around the runner. CPU and elapsed time include this
  envelope's instrumentation; the requested budget is not a hard preemption bound.

Channel samples expose available transport-buffer bytes/chunks, in-flight and
spilled bytes, the current window threshold, fill level, empty/finish/early-finish/
abort flags and checkpoint pause state. Fill levels are 0=NoLimit, 1=SoftLimit,
2=HardLimit. Unknown/unsupported fields are **null**. The remote sender's
`buffered_bytes` counts its local wait queue; `inflight_bytes` counts sent bytes
not yet acknowledged as consumed. A remote receiver counts its receiving queue.
The local channel counts shared resident bytes and separately spilled bytes.
`capacity_bytes` is the current flow-control window, not an allocation limit.
These numbers exclude serializer staging, deserialized values and operator state.

Snapshots use passive APIs: they neither arm channel notifications nor append
scheduler waits. Each buffer synchronizes its own reads, but a task's collection
of channel samples is not one atomic cut of the distributed graph. Per-channel
`sampled_monotonic_us` identifies the local sample time. The after sample is taken
at the end of `Run()`, before endpoint output finalization. State `running` can
continue until the endpoint handles the result; it does not mean continuous CPU
execution. Source/sink asynchronous work outside `Run()` has no runner interval.

## Bounds and coverage

Each node admits at most `MaxQueries` captures, each with at most `MaxEvents` and
`MaxBytes` serialized event payload. In-memory container overhead and temporary
export/serialization allocations are additional. Event overflow increments
`dropped_events` and sets `truncated`. Completed captures are removed lazily after
retention, or evicted earlier to admit another query. Active captures are never
evicted for admission. `rejected_admissions` in the list counts refused creation
attempts, potentially several attempts for one query. Missing captures are an
explicit `trace_not_found` response. Storage is volatile across node restarts.

`completed` means the local graph registration's last owner was released, not
that the entire distributed query succeeded. Channel/handle cleanup can lag
behind SQL completion, so a finished query can still have `completed=false`.
Downloads work before and after that marker. Cancellation is retained as a
terminal task state.

Current coverage excludes asynchronous runners and channels v1. Compare graph
local ownership with `task` records before assuming every task was observed.
Topology can exist for an unsupported task without execution intervals. Remote
nodes must independently enable capture; the utility cannot detect an omitted
node capture. Local order/durations use monotonic clocks, with a wall-clock anchor
for approximate cross-node alignment. Cross-node ordering depends on clock
synchronization. Capturing samples, decoding program names once at preparation
and serializing events have diagnostic overhead; benchmark with capture off too.

Source tests cover recorder bounds, concurrent writes, retention, terminal graph
cleanup, precise waits, worker selections and passive channel reads. Converter checks can be run with:

```bash
python3 -m unittest discover -s ydb/library/yql/dq/task_scheduler/trace -p 'test_*.py'
```

## Validation

Built kqprun and the math UDF with `relwithdebinfo` on the Linux build host.
The selected `DqTaskTrace`, `DqChannelTrace` and `DqTaskWorkerPool` suites passed
(18 C++ tests); all four Python converter tests passed.

A 16-node kqprun TPC-H Q5 run with parquet sources, block hash joins and hash
shuffle completed in both original and worker-pool modes. Both captures contain
all 196 task programs on all 16 nodes, with zero dropped events. Both five-row
results match the prepared reference with floating-point tolerance. Sources still
use task-bound compute actors; eligible pure tasks use the worker pool.

For this diagnostic run, capture limits were increased to 1,000,000 events and
512 MiB per node/query. Smaller limits truncated some earlier captures. These are
capture limits, not a recommendation for production defaults.

The end-to-end run exposed a lost-wakeup race at remote channel EOF. An EOF chunk
can be popped before finish confirmation arrives. The input channel now registers
that dependency even though it returns no row, so a stale wait from an already
finished input cannot hide the confirmation. Regression tests cover confirmation
both before and after the runner reports completion.

One complete traced pair measured 10.334184 s for the original path and
11.139947 s for the worker pool. Captured runner calls were 23,436 and 20,609;
no-observed-progress fractions were 64.78% and 9.34%. This is diagnostic evidence,
not a stable performance benchmark: repeat with tracing disabled before drawing
latency or CPU conclusions.
