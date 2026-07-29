# YDB platform benchmark

`platform_bench` packages benchmark executables into a single Python tool and
runs reproducible scenarios. The initial `actors-core` scenario bundles the
program variant of `ydb/library/actors/core/ut_fat` and runs only
`HeavyActorBenchmark::SendActivateReceiveCSVManual`.

Build and inspect the tool:

```bash
./ya make --build relwithdebinfo ydb/tools/platform_bench
ydb/tools/platform_bench/platform_bench list
ydb/tools/platform_bench/platform_bench describe actors-core
```

Run a short check or the fixed comparison profile:

```bash
ydb/tools/platform_bench/platform_bench run actors-core \
    --profile smoke \
    --output platform-bench-smoke

ydb/tools/platform_bench/platform_bench run actors-core \
    --profile baseline \
    --output platform-bench-baseline
```

Use `--work-dir` when the system temporary directory is mounted with `noexec`.
The tool extracts the bundled executable atomically into a unique temporary
subdirectory and removes it after the run.

The output contains:

- `run.json` with the tool revision, binary SHA-256, platform description,
  command, benchmark parameters, whitelisted environment, and status;
- `repeat-NNN/stdout.txt` and `stderr.txt` with unmodified process output;
- `repeat-NNN/metrics.csv` with CSV rows extracted from the unit-test output;
- `summary.csv` with median, minimum, and maximum throughput across external
  repetitions.

The run fails if the process exits unsuccessfully, times out, or produces an
empty or parameter-mismatched CSV result. Timeout and interruption stop the
whole process group so that scenarios can safely grow to include a server and
workload processes.
