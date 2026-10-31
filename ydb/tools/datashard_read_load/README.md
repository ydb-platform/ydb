# DataShard read/cancel load

A C++ SDK workload for investigating DataShard mailbox congestion and preserving
the cancellation behavior introduced by [#15398](https://github.com/ydb-platform/ydb/pull/15398)
for [#14936](https://github.com/ydb-platform/ydb/issues/14936).
The first target is a local single-node YDB. The tool requires no server patches,
pauses, failpoints, or client access to internal actor events.

## Build and initialize

```bash
./ya make --build relwithdebinfo ydb/tools/datashard_read_load
./ya make --build relwithdebinfo -tA ydb/tools/datashard_read_load 2>&1 | tail -80

ydb/tools/datashard_read_load/datashard_read_load init \
  --endpoint localhost:2135 --database /Root --table /Root/mailbox_load
```

`init` creates a dedicated row table with primary key `(Bucket, Item)`,
all keys under `Bucket = 0`, and a nullable String `Payload`. Defaults are 4,096 rows
and 64 payload bytes per row. Override them with `--rows` and `--payload-bytes`.
Size and load splitting are disabled, and the table is limited to one partition.
An existing table causes failure; the tool never drops tables. Interrupted initialization
can leave a partial table.

## Load controls

Each `--phase NAME:DURATION:LONG_RPS:SHORT_RPS` specifies independent arrival
rates. Repeat this argument to compose an experiment. Rates are whole requests
per second; zero disables a stream in that phase. Durations accept units such as
`ms`, `s`, and `m`. Names must be unique. An independent probe stream defaults to
one request per second (`--probe-rps`).

All three in-flight caps are explicit: `--long-inflight`, `--short-inflight`,
and `--probe-inflight`. A disabled stream can have a zero cap. Each slot reserves
one SDK session before measurement, overriding the SDK's default limit of 50.
Session creation and warmup happen outside the measured phases. Session capacity
on the server must also accommodate the chosen caps.

`--threads N` selects the number of dispatch threads (default 1, range 1–64).
All rates and in-flight caps are **totals for the process**: keep the same values
when changing the thread count. The tool divides each population's sessions
among the threads and interleaves its scheduled arrivals. For example, two
threads with a 50,000/s long rate and a 40,000 long cap target 50,000/s combined
and reserve 40,000 long sessions in total. Low rates, including the default
one-per-second probe, are preserved without rounding or multiplication. Sessions
stay with their assigned thread, so idle capacity is not borrowed between threads.
All threads use the same phase timeline and preserve outstanding work across
phase changes. One coordinator emits aggregate counts and merged latency
histograms in the existing JSONL format.

Reads execute cached, parameterized `OnlineRO` SQL ranges over the primary key.
`--rows-per-read` defaults to one and can be increased to vary execution cost.
Pass the initialization row count again with `--rows` when overriding it. Warmup
covers the configured keyspace and checks that reads return the expected count.
Random range positions use `--seed` (default 1).

Short requests set server-side `CancelAfter` to `--cancel-after` (default 100ms).
All requests use `--long-timeout` as their operation timeout (default 60s);
the RPC timeout adds `--rpc-grace` (default 5s). Short cancellation deadlines
must be less than the operation timeout. First measure healthy latency and choose
a cancellation deadline above it, so requests normally reach the shard before
becoming obsolete under congestion.

Arrivals follow a monotonic clock independently of completions. Each scheduling
pass dispatches up to `--dispatch-batch` elapsed arrivals per stream per thread
(default 64, range 1–1,024). Batching absorbs ordinary timer wakeup jitter: for example, 50,000
requests/s requires four launches per 80us wakeup. A limit of one would drop three
of those arrivals and impose an artificial rate ceiling.

When more arrivals have elapsed than the batch limit, only the most recent batch
is eligible and older arrivals are counted as scheduler skips. Skipped arrivals
are never retained for later dispatch. An occupied or unavailable session also
causes a counted skip. Batch dispatch stops at phase end or interruption. SDK query
retries are disabled; normal KQP iterator retries remain part of server behavior.
Broken sessions are replaced asynchronously, with failures and replacements
reported separately. Failed replacements are retried at most once per second,
independently of the reporting interval.

Phase changes preserve outstanding requests, including their original deadlines.
Each phase starts its arrival clocks from a shared timestamp after its dispatch
threads are ready and its `phase_start` snapshot has been collected. Workers are
released before that record is serialized and written, using the record's
`elapsed_s` as their common start. Each phase receives its full configured
duration. Joining workers and writing phase-end reports creates arrival-free
gaps between phases and can extend total wall time beyond the sum of phase
durations.

After the final phase, arrivals stop and existing requests drain for up to
`--drain-timeout` (default 70s). SIGINT/SIGTERM stop arrivals and use the same bounded
drain. Probe arrivals continue during zero-rate phases, but stop during final
draining. Exit codes are 0 for a completed run, 1 for setup errors or unexpected
successful row counts, 2 for an incomplete drain, and 130 for a drained interrupted
run. Ordinary query error statuses are measurements, not process failures.

## Output and interpreting results

Stdout is JSON Lines; diagnostics and bounded error samples go to stderr.
`run_start` records effective settings, including `threads`, query text, and the
explained query plan.
Then `phase_start`, approximately one-second `interval`, `phase_end`, and `final`
records report populations separately as `long`, `short`, and `probe`.

`totals` are cumulative since measurement began. `interval` contains deltas and
interval histograms with the actual `interval_s`; derive rates by dividing counts
by that duration. Completion counters include requests launched in earlier phases.
`origin` in phase records describes requests scheduled in that phase; `origins`
in the final record includes later completions attributed to their launch phase.

For each population, cumulative accounting obeys:

```text
scheduled = launched + skipped_scheduler + skipped_capacity
launched = completed + outstanding
```

Also reported: completion status counts, successful rows, unexpected row counts,
session failures/replacements, all-completion and success-only latency percentiles,
and launch scheduling delay, in microseconds. A zero histogram has no samples.
At phase end, origin `outstanding` may be positive; later final origin records
show what eventually completed. `final.drained` indicates whether any measured
requests remained when the bounded drain ended.
