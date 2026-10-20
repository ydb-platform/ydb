# Fast scheduling comparison

This target needs no YDB server, tables, SDK clients, or cluster restarts. All
four modes run in one process and reuse one real actor runtime:

* `direct_model`: task actors receive execution messages directly.
* `observe_only`: the same messages, plus the production handle's instrumentation.
* `scheduled`: the production sharded dispatcher issues execution permits.
* `worker_pool`: persistent workers pull queries/tasks and invoke ordinary stage
  operation objects directly; endpoint actors handle only setup and barriers.

The three stages share a query identity. The dispatcher mode uses one dispatcher;
the worker mode uses one query queue in the shared pool. Separate trials use
separate query identities; they run sequentially and do not measure query fair
sharing. Dedicated worker-pool unit tests exercise query weights, in-flight
reservations and reuse of a worker across queries.

It is a **synthetic scheduler microbenchmark**, not an implementation of the
legacy KQP compute actor. Its operators and buffers are deliberately small:
producer -> bounded input -> group sums -> bounded output -> consumer. The
consumer reads one group per slice by default. All modes use the same work,
slice sizes and buffer capacities; channel-like notifications are coalesced per
dependency in every mode. A mutex protects the model's payload state, so this
does not measure parallel SQL operator throughput or real channel serialization.
All modes collect the same benchmark-local counters and step timings; the
observation mode adds the production handle's bookkeeping. The direct model is
not an uninstrumented KQP baseline.

Two scenarios run: a normal pipeline and a pipeline with independent irrelevant
notifications delivered while the aggregate and consumer are parked. These model
stale callbacks or previously probed inputs; they are not repeated pushes that
bypass one channel's notification flag. The deterministic regression checks that
the dispatcher and worker modes perform zero executions during this irrelevant-notification
phase. Every trial verifies the input count, group count, checksum, and buffer
bound. No elapsed-time or CPU-speedup threshold is asserted in CI.

Run the target:

```bash
./ya make --build relwithdebinfo -tA ydb/library/yql/dq/task_scheduler/compare 2>&1 | tail -80
```

Run just the comparison, with parameters forwarded to the test process:

```bash
./ya make --build relwithdebinfo -tA ydb/library/yql/dq/task_scheduler/compare \
    -F 'DqTaskSchedulingComparison::ReportComparison' \
    --test-param rows=131072 \
    --test-param repeats=7 \
    --test-param noise=1024 \
    --test-param quantum=64 \
    --test-param consumer_quantum=1 2>&1 | tail -80
```

After building once, rerun the binary directly with the default parameters:

```bash
./ydb/library/yql/dq/task_scheduler/compare/ydb-library-yql-dq-task_scheduler-compare \
    --print-times +DqTaskSchedulingComparison::ReportComparison 2>&1 | tail -80
```

This skips the build tool entirely. Direct execution writes the CSV under
`testing_out_stuff` in the current directory; use an absolute binary path to run
from a scratch directory. Use `ya make --test-param` above to change parameters.

Each comparison warms up all modes, then rotates their order across repetitions.
CSV is saved as `comparison.csv` in the `ReportComparison` test's output
directory under `test-results`, and also printed to its stderr.
The table contains run counts, runs without observed progress, executions during
the irrelevant-notification phase, rows per run, delivered run events,
dependency notifications, execution CPU, process CPU, elapsed time, peak buffered
payload bytes, and a checksum. Row counts sum work across stages; they are not
unique result rows. Payload bytes exclude allocator and scheduler metadata.

Process CPU includes the driver, task actors, dispatchers and workers, measured from the
first execution request until completion barriers on the task mailboxes. Actor
creation and runtime startup are excluded. A few final scheduling cleanup events
may finish after the barriers, so use sufficiently long trials and repeated
medians; very short timings are diagnostic only. Compare the normal case as well
as the notification stress case: suppressing wakeups may still cost more overall
CPU. This test does not replace end-to-end SQL latency, throughput, memory and
fairness measurements.
