# Using `combiner_perf -t dq-block`

## Purpose and limits

The `dq-block` mode is an isolated aggregation harness. It loads Arrow blocks from a Parquet file or generates a reproducible shuffled `Uint32` column, optionally transforms the blocks, and consumes the complete output of one aggregation graph. Use it for:

- correctness checks against a scalar `WideCombiner` reference;
- repeatable CPU and peak-RSS comparisons;
- `perf record`, `perf stat`, sanitizer, LLVM, and spilling experiments;
- direct comparisons between `DqHashAggregate` and `BlockCombineHashed` for synthesized `sum` and `count` workloads;
- custom aggregation shapes expressed as YQL AST lambdas.

For synthesized sums of narrow integers, `BlockCombineHashed` promotes the numeric slot to `Int64` or `Uint64` and makes it optional, while `DqHashAggregate` preserves the input slot. Using `Int64` or `Uint64` inputs, as in the four-key fixture below, removes the width difference; the `BlockCombineHashed` result remains optional.

This is not an end-to-end KQP query benchmark. It currently measures final `DqHashAggregate`, not early `DqHashCombine`. Its `BlockCombineHashed` path emits grouped results directly and does not include `BlockMergeFinalizeHashed`. Input loading, optional input transforms, graph construction, and correctness verification are outside the harness timer.

## Code and build targets

| Path | Role |
| --- | --- |
| `ydb/core/kqp/tools/combiner_perf/bin/main.cpp` | CLI parsing, option validation, implementation dispatch, and JSON output |
| `ydb/core/kqp/tools/combiner_perf/dq_block.cpp` | input preparation, AST loading, graph construction, timing, and verification |
| `ydb/core/kqp/tools/combiner_perf/run_params.h` | run configuration |
| `ydb/core/kqp/tools/combiner_perf/subprocess.cpp` | forked-run isolation |
| `ydb/core/kqp/tools/combiner_perf/printout.cpp` | thread CPU time, peak RSS, and attempt merging |
| `ydb/core/kqp/tools/combiner_perf/bin/` | binary target, this manual, and example ASTs |
| `ydb/core/kqp/tools/combiner_perf/bin/perf_driver.py` | repeat-and-select driver for remote `perf record` runs |
| `ydb/library/yql/dq/comp_nodes/dq_hash_combine.cpp` | measured `DqHashAggregate` implementation |
| `yql/essentials/minikql/comp_nodes/mkql_block_agg.cpp` | measured `BlockCombineHashed` implementation |

Run `ya` from the repository root. Build an assertions-enabled binary for development and correctness checks:

```bash
set -o pipefail
./ya make --build relwithdebinfo ydb/core/kqp/tools/combiner_perf/bin 2>&1 | tail -80
```

Build an optimized binary with frame pointers for measurements and profiling:

```bash
set -o pipefail
./ya make --build profile ydb/core/kqp/tools/combiner_perf/bin 2>&1 | tail -80
```

The binary is written to:

```text
ydb/core/kqp/tools/combiner_perf/bin/combiner_perf
```

Create the local results directory once if necessary:

```bash
mkdir -p ~/scratch
```

Do not edit sources while a build is running.

## First verified run

This generated workload needs no external dataset. Verification is enabled by default.

```bash
ydb/core/kqp/tools/combiner_perf/bin/combiner_perf \
  -t dq-block \
  --dq-block-generator shuffle \
  --rand-seed 42 \
  --rows-per-run 10000 \
  --num-keys 1000 \
  --dq-block-keys i \
  --dq-block-aggregations count \
  --block-size 1024
```

The process writes diagnostics to stderr, one JSON result object to stdout, and the same object to the next file under `~/.combiner_perf/json/`. A successful verified run reports `Verification passed for 1000 groups` on stderr. The important JSON fields are `resultTime` in thread CPU milliseconds, `maxRssDelta` in bytes, the effective input parameters, and the effective `randomSeed`.

## Inputs and aggregation forms

Choose exactly one input source:

- `--dq-block-file PATH` reads Parquet. Also pass `--dq-block-columns NAME,...`. Column order is preserved. `--rows-per-run 0` reads the entire file.
- `--dq-block-generator shuffle` creates column `i`, a shuffled repetition of `[0, num-keys)` stored as `Uint32`. `--rows-per-run` and `--num-keys` must be positive, and `num-keys` cannot exceed the row count. Set `--rand-seed` for reproducibility.

The file reader supports primitive integer, floating-point, Boolean, string/binary, date, and timestamp Arrow types. It rejects missing or duplicate selected columns and unsupported complex types. The complete selected input is retained in RAM before measurement. The generator also materializes its complete input and caps each generated Arrow block at 10,000 rows even if `--block-size` is larger.

Then choose exactly one aggregation form:

1. Synthesized aggregation: pass both `--dq-block-keys NAME,...` and `--dq-block-aggregations AGG,...`. Supported aggregations are `count` and `sum:column_name`. At least one key and one aggregation are required.
2. Custom aggregation: pass `--dq-block-ast PATH`. The file supplies an optional input transform and all four aggregation lambdas. This form only supports `DqHashAggregate`.

`--dq-block-generator-ast PATH` is different from `--dq-block-ast`: it supplies only an untimed input transform and is used with synthesized keys and aggregations. After a transform, data columns are named `1`, `2`, and so on in output order; use those names in `--dq-block-keys` and `sum:` specifications.

### Important controls

| Option | Default | Meaning |
| --- | ---: | --- |
| `--dq-block-impl` | `DqHashAggregate` | Select `DqHashAggregate` or `BlockCombineHashed` |
| `--rows-per-run` | `10000000` | Rows loaded or generated for one input pass |
| `--run-count` | `1` | Replay the prepared blocks this many times inside the measured graph |
| `--num-keys` | `1000` | Generator cardinality |
| `--block-size` | `8192` | Requested Parquet batch size and generated block size before the 10,000-row cap |
| `--num-attempts` | `1` | Measurement attempts; the reported `resultTime` is their minimum |
| `--no-verify` | off | Skip the separate scalar-reference correctness run |
| `--llvm` | off | Enable LLVM for `DqHashAggregate` |
| `--spilling` | off | Enable spilling support for `DqHashAggregate` |
| `--mode` | `all` | `dq-block` accepts `all` and `graph`; they currently behave identically |

`BlockCombineHashed` supports only synthesized aggregation. It rejects `--dq-block-ast`, `--llvm`, and `--spilling`. Both implementations accept an untimed `--dq-block-generator-ast` followed by synthesized aggregation.

## What is measured

The parent process prepares the data, optional transform, graph, and reusable block stream. The timer then covers graph consumption through the last output block. The result consumer counts output rows without scalarizing every result value.

- `resultTime` is current-thread CPU time, not elapsed wall time.
- Multiple attempts report the minimum nonzero time. `maxRssDelta` reports the maximum delta across attempts.
- `--run-count N` repeats the same prepared input N times during each attempt. It does not reload the Parquet file, reshuffle the generator, or rerun the input transform.
- The prepared input is already resident when the RSS delta is sampled. `maxRssDelta` is the increase in the process-lifetime `ru_maxrss` high-water mark. Allocations below the earlier high-water mark contribute zero, so this is neither total memory nor memory attributable only to aggregation.
- With more than one attempt, or whenever verification is enabled, each measurement runs in a forked child and starts from the same parent graph and input through copy-on-write.
- The special `--no-verify --num-attempts 1` case runs the measurement in the current process. This is useful for whole-process `perf record`, but the profile also contains input preparation, graph construction, and teardown.

Unless `--no-verify` is present, a separate untimed child compares the block result with a scalar `WideCombiner` graph. The verifier supports exact integer comparison and tolerant floating-point comparison, including optionals. It cannot currently compare nonnumeric aggregate outputs such as `Utf8`; keys may still be strings.

## Custom AST contract

A full `--dq-block-ast` file is an `AsTuple` or quoted six-element list:

```lisp
'(
    <input-transform lambda or ()>
    <extract-key lambda>
    <initialize-state lambda>
    <update-state lambda>
    <finalize lambda>
    (Uint64 '<leading-output-key-count>))
```

The aggregation lambda arguments follow the `DqPhyHashCombine` convention:

| Lambda | Arguments |
| --- | --- |
| extract key | input columns |
| initialize state | extracted keys, then input columns |
| update state | extracted keys, input columns, then current state columns |
| finalize | extracted keys, then state columns |

The final literal says how many leading finalize outputs are keys. The loader does not reorder them: edit the finalize lambda so it returns all key expressions first, followed by aggregate expressions, and set the literal to the number of leading key outputs. Final outputs must currently be data slots or optional data slots.

The input-transform argument is a wide stream of Arrow block columns followed by the scalar `Uint64` block-height column. Its result must have the same shape: at least one data block followed by a scalar `Uint64` height. Preserve the height when using a block-level `WideMap`. For scalar operators, the usual pattern is `WideFromBlocks`, `ToFlow`, `WideMap`, `FromFlow`, and `WideToBlocks`. The transform is compiled without LLVM, run once before measurement, and fully materialized.

`--dq-block-generator-ast` accepts a transform lambda directly, a quoted list whose first item is the transform, or a full six-element tuple. Only the first item is used. No optimizer pipeline rewrites these lambdas, so keep arities and types explicit.

## Checked-in AST examples

AST file names and links in this section are relative to `ydb/core/kqp/tools/combiner_perf/bin/`, the directory containing this manual. The command examples still use repository-root-relative paths.

| File | Intended use |
| --- | --- |
| [`simple_example.ast`](simple_example.ast) | Full custom aggregation with no transform: two leading keys and a sum of the third input column |
| [`simple_transform_example.ast`](simple_transform_example.ast) | Full custom aggregation whose block-level transform packs `URL` and `UserID` into a struct before grouping |
| [`simple_generator_example.ast`](simple_generator_example.ast) | Full custom generator case: scalarizes `i`, applies `% 100`, restores blocks, and counts by the result; its transform can also be reused with `--dq-block-generator-ast` |
| [`aggregate_some.ast`](aggregate_some.ast) | Full custom fallback-value case for `--dq-block-columns URL`: one `Utf8` key and one unchanged `Utf8` state; use `--no-verify` because aggregate-string comparison is unsupported |
| [`four_uint64_keys.ast`](four_uint64_keys.ast) | Transform-only fixture that widens generated `i` into five identical `Uint64` columns for four-key plus one-sum comparisons |
| [`fastpath_sum_uint64.ast`](fastpath_sum_uint64.ast) | Generated `Uint64` key with one nonoptional `Uint64` sum |
| [`fastpath_sum_key_int64.ast`](fastpath_sum_key_int64.ast) | Generated `Int64` key whose key value is also the sum operand |
| [`fastpath_generator_multi_uint64_sum.ast`](fastpath_generator_multi_uint64_sum.ast) | One generated `Uint64` key with two independent `Uint64` sums |
| [`fastpath_generator_optional_uint64_sum.ast`](fastpath_generator_optional_uint64_sum.ast) | One generated `Uint64` key with a present `Optional<Uint64>` sum |
| [`fastpath_optional_multi.ast`](fastpath_optional_multi.ast) | Generated key with nullable `Int64` and `Uint64` inputs, two sums, and count |
| [`fastpath_q34_final_sum.ast`](fastpath_q34_final_sum.ast) | ClickBench-style two-column key with a constant partial-count input summed in a final-aggregation shape |
| [`fastpath_q34_multi_uint64_sum.ast`](fastpath_q34_multi_uint64_sum.ast) | ClickBench-style two-column key with two `Uint64` sums |
| [`fastpath_q34_optional_uint64_sum.ast`](fastpath_q34_optional_uint64_sum.ast) | ClickBench-style two-column key with a present `Optional<Uint64>` sum |

Run all fast-path fixtures plus synthesized count and fallback cases with the separately supplied ClickBench `hits.parquet` at the working-copy root:

```bash
ROWS_PER_RUN=10000 ./run_fastpath_aggregation_test.sh
```

The script defaults to 100,000 rows for its ClickBench cases when `ROWS_PER_RUN` is unset.

### Exact four-key comparison

First verify each implementation on a small input:

```bash
ydb/core/kqp/tools/combiner_perf/bin/combiner_perf \
  -t dq-block \
  --dq-block-generator shuffle \
  --rand-seed 42 \
  --rows-per-run 10000 \
  --num-keys 1000 \
  --dq-block-generator-ast ydb/core/kqp/tools/combiner_perf/bin/four_uint64_keys.ast \
  --dq-block-keys 1,2,3,4 \
  --dq-block-aggregations sum:5 \
  --dq-block-impl DqHashAggregate

ydb/core/kqp/tools/combiner_perf/bin/combiner_perf \
  -t dq-block \
  --dq-block-generator shuffle \
  --rand-seed 42 \
  --rows-per-run 10000 \
  --num-keys 1000 \
  --dq-block-generator-ast ydb/core/kqp/tools/combiner_perf/bin/four_uint64_keys.ast \
  --dq-block-keys 1,2,3,4 \
  --dq-block-aggregations sum:5 \
  --dq-block-impl BlockCombineHashed
```

For timing, choose the target row count and cardinality, then keep every option identical between implementations. The following campaign intentionally increases both rows and cardinality from the smoke test, uses several attempts, and disables the already-completed verification:

```bash
set -o pipefail
numactl -m 0 -N 0 ydb/core/kqp/tools/combiner_perf/bin/combiner_perf \
  -t dq-block \
  --dq-block-generator shuffle \
  --rand-seed 42 \
  --rows-per-run 10000000 \
  --num-keys 1000000 \
  --block-size 8192 \
  --dq-block-generator-ast ydb/core/kqp/tools/combiner_perf/bin/four_uint64_keys.ast \
  --dq-block-keys 1,2,3,4 \
  --dq-block-aggregations sum:5 \
  --dq-block-impl DqHashAggregate \
  --no-verify \
  --num-attempts 5 \
  2>&1 | tee ~/scratch/four-uint64-dq.log
```

Repeat with `--dq-block-impl BlockCombineHashed`. Run the two implementations sequentially and alternate their order in longer campaigns. The five transformed columns are correlated copies of one generated value; this is a controlled key-width test, not a test of independently distributed composite keys.

### ClickBench Q34-style count

The working-copy-root `hits.parquet` is the local cache of the canonical ClickBench dataset used for these measurements. It is not source-controlled. From the working-copy root, download the roughly 14 GB [official ClickBench Parquet dataset](https://github.com/ClickHouse/ClickBench#data-loading) when necessary:

```bash
wget --continue https://datasets.clickhouse.com/hits_compatible/hits.parquet
```

```bash
set -o pipefail
numactl -m 0 -N 0 ydb/core/kqp/tools/combiner_perf/bin/combiner_perf \
  -t dq-block \
  --dq-block-file hits.parquet \
  --rows-per-run 10000000 \
  --dq-block-columns UserID,URL \
  --dq-block-keys UserID,URL \
  --dq-block-aggregations count \
  --block-size 8192 \
  --no-verify \
  --num-attempts 5 \
  2>&1 | tee ~/scratch/q34-dq.log
```

Before using `--no-verify`, run the same command on 10,000 rows without that flag. Parquet column order controls the custom-AST input argument order, so record it with every result.

### Full custom aggregation

This file-input example uses two keys and sums the third column:

```bash
ydb/core/kqp/tools/combiner_perf/bin/combiner_perf \
  -t dq-block \
  --dq-block-file hits.parquet \
  --rows-per-run 10000 \
  --dq-block-columns CounterID,RegionID,UserID \
  --dq-block-ast ydb/core/kqp/tools/combiner_perf/bin/ast_example.txt \
  --block-size 1024
```

## Remote measurements over SSH

Remote measurements require a dedicated testing host provided for the task. This public repository intentionally contains no default hostname. Replace the `PERFORMANCE_HOST` placeholder in every command below with the task-provided host; agents must not infer or reuse a host from another task. The expected host-side assets are `~/hits.parquet`, `~/flamegraph/stackcollapse-perf.pl`, `/usr/bin/numactl`, and `/usr/bin/perf`. Check them before a campaign because remote data and tool installations are not versioned with this checkout.

Agents must request user approval before each new SSH or SCP command prefix. Use SSH only for these performance tests and related binary, data, and profile transfers.

### Deploy a named profile binary

Build locally, then copy the binary under a unique experiment name. Do not overwrite an existing reference binary unless the task explicitly calls for it.

```bash
set -o pipefail
./ya make --build profile ydb/core/kqp/tools/combiner_perf/bin 2>&1 | tail -80

scp \
  ydb/core/kqp/tools/combiner_perf/bin/combiner_perf \
  PERFORMANCE_HOST:combiner_perf_my_change

scp \
  ydb/core/kqp/tools/combiner_perf/bin/perf_driver.py \
  PERFORMANCE_HOST:perf_driver.py
```

Record the source revision and binary checksum with the results:

```bash
git rev-parse HEAD
git status --short
sha256sum ydb/core/kqp/tools/combiner_perf/bin/combiner_perf
ssh PERFORMANCE_HOST \
  'sha256sum ./combiner_perf_my_change'
```

If the source tree is dirty, save `git diff --binary` with the results and preserve any relevant untracked source files separately.

Do not assume that old `combiner_perf_*` files on the host match the current CLI or source. Smoke-test a copied binary before a long run. In particular, the profiling driver requires exactly one JSON object on stdout:

```bash
set -o pipefail
ssh PERFORMANCE_HOST \
  'numactl -m 0 -N 0 ./combiner_perf_my_change \
    -t dq-block \
    --dq-block-generator shuffle \
    --rand-seed 42 \
    --rows-per-run 10000 \
    --num-keys 1000 \
    --dq-block-keys i \
    --dq-block-aggregations count \
    --no-verify \
    --num-attempts 1'
```

The last line on stderr names the remote `~/.combiner_perf/json/*.jsonl` file. Preserve the stdout JSON or that file together with the command, source revision, and host binding.

### Time a ClickBench workload

```bash
set -o pipefail
ssh PERFORMANCE_HOST \
  'numactl -m 0 -N 0 ./combiner_perf_my_change \
    -t dq-block \
    --dq-block-file ~/hits.parquet \
    --rows-per-run 10000000 \
    --dq-block-columns UserID,URL \
    --dq-block-keys UserID,URL \
    --dq-block-aggregations count \
    --block-size 8192 \
    --no-verify \
    --num-attempts 5' \
  2>&1 | tee ~/scratch/q34-my-change.log
```

`numactl -m 0 -N 0` binds memory and execution to NUMA node 0 but does not pin one CPU. For a CPU-pinned campaign, use a fixed CPU such as `numactl -m 0 -C 2` for every binary. Pinning does not eliminate frequency variation, so interleave baselines and experiments and keep complete per-attempt logs.

### Record a profile

The checked-in `perf_driver.py`, copied above, repeats the supplied command five times under `perf record`, selects the run with the smallest JSON `resultTime`, and writes `perf script` output for that attempt. It stores recordings under `/spilling/perfdata` by default, creates the directory when permitted, and accepts a different directory through `COMBINER_PERF_DATA_DIR`.

Put the perf-data directory on SSD or NVMe storage. On the dedicated host, `/spilling` is the known NVMe mount; `/home` and `/tmp` may be HDD-backed and must not be used for recordings. On another host, set `COMBINER_PERF_DATA_DIR` to a directory on its local SSD or NVMe device.

Give the benchmark `--no-verify --num-attempts 1` so each driver attempt profiles one in-process measurement and emits the one JSON object the driver expects:

```bash
ssh PERFORMANCE_HOST \
  './perf_driver.py q34-my-change.txt -- \
    numactl -m 0 -N 0 ./combiner_perf_my_change \
    -t dq-block \
    --dq-block-file ~/hits.parquet \
    --rows-per-run 10000000 \
    --dq-block-columns UserID,URL \
    --dq-block-keys UserID,URL \
    --dq-block-aggregations count \
    --block-size 8192 \
    --no-verify \
    --num-attempts 1'
```

All five `perf.data` files are retained for later inspection. Archive or remove them deliberately after the analysis so repeated campaigns do not exhaust the selected filesystem. On a host without writable `/spilling`, invoke the driver as `COMBINER_PERF_DATA_DIR=/path/on/ssd/perfdata ./perf_driver.py ...`.

Collapse and copy the selected profile:

```bash
ssh PERFORMANCE_HOST \
  'flamegraph/stackcollapse-perf.pl < q34-my-change.txt > q34-my-change.collapsed'

scp \
  PERFORMANCE_HOST:q34-my-change.collapsed \
  ~/scratch/q34-my-change.collapsed
```

Whole-process profiles and `perf stat` include untimed preparation. A child-only profile requires a separate PID-aware recording and analysis procedure; do not assume the driver output excludes preparation.

For hardware counters, keep in mind that the counters cover the whole process even though `resultTime` covers only graph consumption:

```bash
ssh PERFORMANCE_HOST \
  'perf stat -r 3 \
    -e cycles,instructions,branches,branch-misses,cache-references,cache-misses -- \
    numactl -m 0 -N 0 ./combiner_perf_my_change \
    -t dq-block \
    --dq-block-file ~/hits.parquet \
    --rows-per-run 10000000 \
    --dq-block-columns UserID,URL \
    --dq-block-keys UserID,URL \
    --dq-block-aggregations count \
    --block-size 8192 \
    --no-verify \
    --num-attempts 1'
```

## Measurement checklist

1. Build `relwithdebinfo` and run a small verified workload.
2. Build `profile` without changing sources during the build.
3. Record the commit, binary checksum, full command, seed, row count, cardinality, block size, run count, implementation, LLVM/spilling state, and host binding.
4. Copy the binary under a new remote name and smoke-test its single-JSON stdout.
5. Run competing binaries sequentially with identical inputs; alternate their order when the difference is small.
6. Compare `resultTime`, not wall time. Retain all attempt times rather than only the reported minimum.
7. Use `perf` or hardware counters to explain a stable timing difference, remembering that whole-process tools also see preparation.
8. Re-run the small verified workload after the experiment.
