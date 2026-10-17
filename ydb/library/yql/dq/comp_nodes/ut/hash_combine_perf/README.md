# dq_hash_combine_perf

Manual microbenchmark for the scalar `DqHashCombine` and `DqHashAggregate` operators.

- **Modes:** `combine` runs `DqHashCombine` with a 128 MiB limit, so bypass is possible. `aggregate` runs `DqHashAggregate` without spilling.
- **Input:** the tool generates rows of `(key, 1)` in-process. Generation is inside the measured section and identical in every build; graph construction and teardown are outside it.
- **Per-case report:**
  - thread CPU time;
  - user-mode cycles and instructions, when `perf_event_open` is permitted (`kernel.perf_event_paranoid` <= 2);
  - output rows;
  - whether the combiner switched to bypass.
- **Statistics:** each case runs once to warm up, then `--repeats` times; the best and median are printed.

## Build and run

```bash
./ya make --build relwithdebinfo ydb/library/yql/dq/comp_nodes/ut/hash_combine_perf
cd ydb/library/yql/dq/comp_nodes/ut/hash_combine_perf
taskset -c 20 ./dq_hash_combine_perf --mode combine --key int64 --state optional-uint64 --distinct 100
taskset -c 20 ./dq_hash_combine_perf --all
```

`--all` runs a fixed matrix of key and state shapes and cardinalities and takes several minutes. `--help` lists the options.

## Comparing two revisions

1. Build the tool on both revisions. If one tree lacks it, copy this directory there.
2. Run both binaries on the same idle CPU in alternating rounds.
3. Compare `mcycles`.

Cycles depend less on CPU frequency than CPU time does. On a shared machine, treat differences within about 2% as noise.
