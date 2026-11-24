# DQ worker pool runs

Read this before changing `ydb/library/yql/dq/task_scheduler` or repeating the kqprun comparisons. The design note is `ydb/library/yql/dq/task_scheduler/README.md`. This file is the measured state.

## What the code does

1. With `EnableTaskScheduling` and `TaskSchedulerUseWorkerPool`, anonymous workers take ready compute tasks from one node-local queue. The task reports when it is ready.
2. Worker count is `TaskSchedulerShards * TaskSchedulerMaxInFlightPerShard`. That product is also how many tasks of one query may run at once. Workers do not add cores. `kqprun --threads 1` gives each node one User thread, and the workers share it.
3. `YDB_DQ_TASK_FIFO=1` selects from a FIFO deque. `YDB_DQ_TASK_QUERY_ORDER` is on unless it is exactly `0`: the claimable task from the smallest query tx id wins, and equal tx ids keep queue order. A consumer unblocked by data this worker just pushed is taken before that order when `TaskSchedulerPreferLocalConsumer` is true.
4. `TaskSchedulerMaxRowsPerRun: 0` does not stop a run after a row count. A run continues until the output channel reaches its inflight limit, the input yields, or the task finishes. `TaskSchedulerWorkerSliceCpuUs` and `TaskSchedulerWorkerActivationUs` still bound how long one worker activation holds the thread.
5. Datashard range reads run on workers. S3 sources run on workers only when `YDB_DQ_TASK_S3_SOURCES=1`. The TPC-H harness sets that variable. Sysview scans, rate-limited scans, watermarks, and transforms stay on the task actor.
6. Scheduling is off unless the config sets `EnableTaskScheduling: true`. Compare on and off with the same binary.

## What the runs showed

Reordering ready tasks did not shorten a CPU-bound local query or a suite of them. Both sides did the same CPU work on the same cores. A several-times speedup is not in this queue.

A wall-clock win needs one of these, and neither was present in the clean runs:

1. The thread is busy with a task that is not making progress, while another ready task on that same thread can run. If the first task yields, the actor mailbox already switches. The pool helps when that mailbox keeps running the empty task.
2. The order changes the amount of work, for example by building fewer hash tables at once so the query spills less. These runs kept a high memory limit. Process user time matched, so the order did not remove work.

Do not turn spilling off to chase a speedup. If a run hits the memory guard, raise the limit.

## How to repeat a run

Build from this repo, with no `-j` and no forced rebuild. Do not edit sources while `ya` is compiling.

```bash
./ya make --build relwithdebinfo ydb/tests/tools/kqprun
```

The launchers in `sched-runs/` are copies of the scripts that produced the numbers below. They still contain absolute paths under `/home/mfilitov/`. Data, `libmath_udf.so` (43 MB), and the `kqprun` binary are not in git.

- TPC-H SF10, all 22 queries: `sched-runs/tpch10-all-sched/run.py`. Data `/home/mfilitov/tpch10-kqprun/data32`. Queries and `libmath_udf.so` in `/home/mfilitov/tpch10-suite-ab`.
- TPC-H SF100 Q5: `sched-runs/tpch100-q5-sched/run.py`. Data `/home/mfilitov/tpch100-q5-ab`.
- TPC-C: `sched-runs/tpcc-scheduler-readsrc2-ab/run.py` is the clean comparison. Older launchers are the other `sched-runs/tpcc-scheduler-*` directories.

`run.py` sets `YDB_DQ_TASK_FIFO=1` when scheduling is on. It does not set `YDB_DQ_TASK_QUERY_ORDER`, so query order stays on unless the shell already exported `0`.

Host of these runs: 16 vCPU, about 20% steal, no swap. Treat single samples as noisy.

## TPC-C

Clean pair, read ranges on workers, about 120 s, consistency check passed. Scheduler 77.9 and 77.1 tx/s, off 80.4 and 78.5 tx/s. NewOrder and Payment are short datashard point operations, so there is no pile of heavy ready compute tasks.

An earlier 61 s pair on the same host was worse: scheduler 76.5 and 70.7 tx/s, off 81.0 and 77.2 tx/s.

| dir | scheduler | seconds | tx/s | failed | server cores |
| --- | --- | --- | --- | --- | --- |
| tpcc-scheduler-readsrc2-ab/off-1 | off | 122 | 78.5 | 459 | 6.73 |
| tpcc-scheduler-readsrc2-ab/off-2 | off | 121 | 80.4 | 490 | 7.23 |
| tpcc-scheduler-readsrc2-ab/fast-1 | on | 121 | 77.9 | 449 | 7.14 |
| tpcc-scheduler-readsrc2-ab/fast-2 | on | 121 | 77.1 | 464 | 7.45 |
| tpcc-scheduler-ab/off-1 | off | 61 | 81.0 | 243 | 7.05 |
| tpcc-scheduler-ab/on-1 | on | 61 | 76.5 | 255 | 7.33 |
| tpcc-scheduler-ab/off-2 | off | 61 | 77.2 | 226 | 7.20 |
| tpcc-scheduler-ab/on-2 | on | 61 | 70.7 | 220 | 7.28 |

## TPC-H SF100 Q5, one query

Clean cached runs are about 53–55 s both on and off. `loc-off-a` 53.7 s and `loc-1x1-a` 53.9 s. `loc-off-b` 52.6 s and `loc-1x1-b` 55.4 s. `rs-off-b` 52.1 s and `rs-on-b` 58.6 s. User CPU stays on the same side of the comparison.

Runs named `on-1`, `on-2`, `on-3`, `bare-a` (58–238 s) are earlier builds, including the S3-on-workers busy loop and other handoff bugs. Do not quote them as the current pool.

`prefetch-new1` 108.4 s, `prefetch-new2` 82.6 s, `prefetch-old` 78.9 s. That was a hash-join probe prefetch tried on top of this tree and then reverted. It is not in the source. The warm pair did not get faster. User time was 621 s against 606 s.

`flame-q5` is scheduler off, 100.6 s, user 593 s, system 198 s, result matched. That sample had about 11% iowait and 20% steal, so it is slower than the cached 54 s runs. Profile `cycles:u`, 20 s, started 15 s after process start. The SVG is `/home/mfilitov/tpch100-q5-sched/flame-q5/cpu.svg` and is not in git.

Exclusive leaves, share of samples: `GetRowMatch` 12.4, unresolved leaf 11.8, `ZSTD_decompressSequences_bmi2` 8.6, the hybrid hash-join run lambda 6.5, actor `Spin` 5.0, a mis-resolved libc name on tuple copy and parquet 4.9, `TPackResult::AllocatedBytes` 3.6, hash-partition `WideConsume` 2.6, `DoAddMany` 2.6.

Inclusive, a sample counted once: hash join 43, tuple pack and copy 26, parquet and zstd 19, actor wait 11, allocator 6.7, channels 5.8. These groups overlap. `GetRowMatch` is two random memory loads per bucket slot (hash and index, then the matched row). Prefetching the next rows on the same thread did not hide them.

## TPC-H SF10, all 22 queries at once

16 nodes, 1 thread, unless the row says 4 nodes. `last` is the slowest query. `mean` is the mean of the 22 spend times. User time is process user CPU.

The repeated fifo-versus-off pairs do not move the suite. `off-r2` mean 150.5 last 163.1, `fifo-4f` mean 158.3 last 167.6. `off-r3` mean 152.9 last 166.0, `fifo-4g` mean 154.6 last 166.6. The first finished queries stay in the same 60–140 s band on both sides. `all-on` mean 138.2 against `all-off` 160.8 was one unpaired sample and did not repeat.

Local-consumer priority (`local-4a`/`local-4b` against `off-lc1`/`off-lc2`) did not shorten the suite. Four nodes made every query slower (last about 300 s) and still showed no scheduler win.

## Every recorded SF10 run

| label | on | nodes | mean s | last s | user s | rss GB |
| --- | --- | --- | --- | --- | --- | --- |
| all-off | off | 16 | 160.8 | 172.9 | 1564.0 | 36.8 |
| all-on | on | 16 | 138.2 | 173.0 | 1587.3 | 41.0 |
| all-fifo | on | 16 | 154.3 | 172.3 | 1618.3 | 42.5 |
| fifo-2 | on | 16 | 151.4 | 167.1 | 1605.9 | 43.7 |
| fifo-4 | on | 16 | 144.9 | 156.1 | 1500.4 | 40.1 |
| fifo-8 | on | 16 | 150.6 | 162.1 | 1582.9 | 37.9 |
| prio-2 | on | 16 | 152.7 | 177.3 | 1648.4 | 45.5 |
| prio-4 | on | 16 | 132.7 | 164.8 | 1583.3 | 38.9 |
| prio-8 | on | 16 | 167.1 | 178.0 | 1616.4 | 36.7 |
| fifo-4b | on | 16 | 153.0 | 162.8 | 1535.4 | 42.6 |
| fifo-4c | on | 16 | 150.1 | 159.6 | 1545.9 | 37.5 |
| fifo-4d | on | 16 | 143.0 | 157.0 | 1557.8 | 41.0 |
| off-r1 | off | 16 | 151.3 | 161.3 | 1490.3 | 36.2 |
| fifo-4e | on | 16 | 150.3 | 160.4 | 1569.6 | 42.5 |
| off-r2 | off | 16 | 150.5 | 163.1 | 1548.9 | 34.0 |
| fifo-4f | on | 16 | 158.3 | 167.6 | 1582.2 | 42.2 |
| off-r3 | off | 16 | 152.9 | 166.0 | 1574.1 | 36.1 |
| fifo-4g | on | 16 | 154.6 | 166.6 | 1596.5 | 36.2 |
| off-lc1 | off | 16 | 182.1 | 201.2 | 1751.6 | 35.6 |
| local-4a | on | 16 | 180.8 | 195.5 | 1753.3 | 44.8 |
| off-lc2 | off | 16 | 168.0 | 179.1 | 1695.8 | 37.3 |
| local-4b | on | 16 | 172.4 | 190.7 | 1780.8 | 42.6 |
| n4-off1 | off | 4 | 285.8 | 300.7 | 1232.1 | 33.6 |
| n4-on1 | on | 4 | 280.1 | 297.2 | 1243.7 | 36.5 |
| n4-off2 | off | 4 | 303.7 | 323.0 | 1268.2 | 30.5 |
| n4-on2 | on | 4 | 278.3 | 296.7 | 1272.9 | 35.4 |

## Every recorded SF100 Q5 run

`query_s` is the request spend time when the harness recorded it. Otherwise `wall_s` is the process wall time. Empty `query_s` on a concurrent or failed run means the harness did not store a single-query spend time.

| label | on | shards | inflight | query_s | wall_s | user s | rss GB | error |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| off-1 | off | 4 | 2 | 58.9 |  |  |  |  |
| on-1 | on | 4 | 2 | 237.7 |  |  |  |  |
| on-2 | on | 4 | 2 | 121.1 |  |  |  |  |
| off-2 | off | 4 | 2 | 111.8 |  |  |  |  |
| off-3 | off | 4 | 2 | 242.1 |  |  |  |  |
| on-3 | on | 4 | 2 | 236.6 |  |  |  |  |
| off-log | off | 4 | 2 | 55.3 |  |  |  |  |
| on-a | on | 4 | 2 | 191.9 |  |  |  |  |
| off-a | off | 4 | 2 | 63.7 |  |  |  |  |
| on-b | on | 4 | 2 | 69.5 |  |  |  |  |
| off-b | off | 4 | 2 | 58.5 |  |  |  |  |
| bare-a | on | 4 | 2 | 232.1 |  |  |  |  |
| bare-b | on | 4 | 2 | 68.9 |  |  |  |  |
| bare-c | on | 4 | 2 | 63.9 |  |  |  |  |
| mem-off-a | off | 4 | 2 | 64.8 |  |  |  |  |
| mem-on-a | on | 4 | 2 | 67.5 |  |  |  |  |
| mem-on-b | on | 4 | 2 | 71.9 |  |  |  |  |
| mem-off-b | off | 4 | 2 | 57.9 |  |  |  |  |
| mem-on-c | on | 4 | 2 | 68.9 |  |  |  |  |
| perf-off | off | 4 | 2 | 66.2 |  |  |  |  |
| perf-on | on | 4 | 2 | 81.0 |  |  |  |  |
| rs-off-a | off | 4 | 2 | 54.3 |  | 464.7 |  |  |
| rs-on-a | on | 4 | 2 | 68.7 |  | 557.7 |  |  |
| rs-on-b | on | 4 | 2 | 58.6 |  | 499.4 |  |  |
| rs-off-b | off | 4 | 2 | 52.1 |  | 458.9 |  |  |
| ps-off-a | off | 4 | 2 | 148.1 |  | 500.4 |  |  |
| ps-on-a | on | 4 | 2 | 148.4 |  | 524.1 |  |  |
| ps-on-b | on | 4 | 2 | 110.3 |  | 428.0 |  |  |
| ps-off-b | off | 4 | 2 | 143.1 |  | 489.4 |  |  |
| loc-off-a | off | 4 | 2 | 53.7 |  | 464.0 |  |  |
| loc-1x1-a | on | 1 | 1 | 53.9 |  | 469.2 |  |  |
| loc-1x2-a | on | 1 | 2 | 59.1 |  | 503.8 |  |  |
| loc-notail-a | on | 4 | 2 | 57.3 |  | 476.1 |  |  |
| loc-off-b | off | 4 | 2 | 52.6 |  | 461.8 |  |  |
| loc-1x1-b | on | 1 | 1 | 55.4 |  | 482.3 |  |  |
| c4-off-probe | off | 4 | 2 |  | 86.9 | 628.9 | 47.9 | free memory guard |
| c2-off-a | off | 4 | 2 |  | 109.8 | 920.5 | 35.8 |  |
| c2-on-a | on | 1 | 1 |  | 113.8 | 954.1 | 33.3 |  |
| c2-on-b | on | 1 | 1 |  | 109.7 | 934.3 | 34.6 |  |
| c2-off-b | off | 4 | 2 |  | 126.2 | 965.3 | 36.1 |  |
| qo-off-a | off | 4 | 2 |  | 132.3 | 998.7 | 34.2 |  |
| qo-ord-a | on | 1 | 1 |  | 117.7 | 946.5 | 26.8 |  |
| qo-fifo-a | on | 1 | 1 |  | 110.8 | 926.6 | 32.6 |  |
| qo-ord-b | on | 1 | 1 |  | 108.2 | 895.9 | 26.5 |  |
| qo-off-b | off | 4 | 2 |  | 108.7 | 928.5 | 26.7 |  |
| s3w-single | on | 1 | 1 | 58.8 |  | 522.4 | 13.6 |  |
| s3-ord-a | on | 1 | 1 |  | 150.1 | 1348.3 | 23.5 |  |
| s3-off-a | off | 4 | 2 |  | 117.7 | 973.3 | 29.3 |  |
| s3-fifo-a | on | 1 | 1 |  | 148.4 | 1290.7 | 31.7 |  |
| s3-ord-b | on | 1 | 1 |  | 127.1 | 1130.5 | 26.2 |  |
| s3w-fix2 | on | 1 | 1 | 78.1 |  | 492.9 | 13.8 |  |
| s3w-fix2b | on | 1 | 1 | 154.0 |  | 532.3 | 19.8 |  |
| s3w-bind | on | 1 | 1 | 65.0 |  | 522.3 | 16.2 |  |
| c2b-off | off | 4 | 2 |  | 133.0 | 1010.7 | 31.4 |  |
| c2b-fifo | on | 1 | 1 |  | 114.0 | 980.6 | 23.8 |  |
| c2b-ord | on | 1 | 1 |  | 114.7 | 974.3 | 26.0 |  |
| c2c-off | off | 4 | 2 |  | 168.4 | 1003.3 | 31.3 |  |
| c2c-fifo | on | 1 | 1 |  | 116.8 | 992.6 | 28.3 |  |
| c2c-ord | on | 1 | 1 |  | 119.4 | 989.2 | 25.5 |  |
| c3-off | off | 4 | 2 |  | 91.5 | 725.5 | 47.8 | free memory guard |
| c3-fifo | on | 1 | 1 |  | 201.5 | 1562.8 | 39.9 |  |
| c3-ord | on | 1 | 1 |  | 181.4 | 1511.4 | 32.3 |  |
| c4-ord | on | 1 | 1 |  | 240.0 | 2061.6 | 45.2 |  |
| c4-fifo | on | 1 | 1 |  | 59.5 | 433.7 | 47.8 | free memory guard |
| flame-q5 | off | 4 | 2 | 100.6 |  | 593.0 | 16.6 |  |
| prefetch-new1 | off | 4 | 2 | 108.4 |  | 659.5 | 16.2 |  |
| prefetch-new2 | off | 4 | 2 | 82.6 |  | 620.8 | 14.9 |  |
| prefetch-old | off | 4 | 2 | 78.9 |  | 605.9 | 15.7 |  |

## Do not do this next

1. Do not rerun the same 22 local TPC-H queries, or one local Q5, to look for a several-times speedup. Those configurations are already in the tables.
2. Do not disable spilling.
3. Do not commit the `kqprun` binary, parquet data, logs, or `libmath_udf.so`.
