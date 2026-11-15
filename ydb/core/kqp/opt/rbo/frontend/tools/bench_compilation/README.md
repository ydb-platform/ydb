# Native compilation comparison

`run.py` compares immutable pre-migration and native `kqprun` binaries. Run from
the repository root, with a separate output directory for each candidate:

```bash
taskset -c 2-5 python3 ydb/core/kqp/opt/rbo/frontend/tools/bench_compilation/run.py \
  --before /path/to/pre-migration-kqprun --after /path/to/native-kqprun \
  --framework /path/to/compilation_bench \
  --viewer /path/to/violinoscope/violinoscope.py \
  --out /tmp/native-comparison --warmups 5 --repeats 3
```

For an alternating control between two native checkpoints, add `--before-native`.
Both variants then require native phases and zero fallback, and both expose phase
timings. The manifest records this mode; resume rejects a change in it.

Build binaries with the workspace's clang-balancer. Use an ordinary `ya make`
build to publish `kqprun`: a test build can leave the source-tree executable link
pointing at an earlier build. Copy each published binary, check its hash, and
retain its source revision and patch before collecting measurements. Do not
replace binaries while a comparison or its controls are running.

The default corpus contains all 6,813 1C queries, 22 TPC-H queries, 99 TPC-DS
queries, 43 ClickBench queries, and the tiny and medium 1C samples. The full 1C
corpus remains external; `--framework` points to its `queries/*.yql` and
`schema.yql`. TPC-H/TPC-DS/ClickBench use the repository SQL and column-store
schemas. The existing workload lambda prefix is preserved. Tiny/medium sample
paths are normalized to `/Root/`; query logic is unchanged. The default paths
for the external corpus and viewer match the original development workspace,
and both can be overridden.

The default corpus also includes the reference TPCH performance fixture's 13
queries and its distinct Double/String schema (`tpch-perf`), for 6,992 queries
in total.

The imported `KqpNativeCompilationTime` tests preserve the reference's original
22 TPCH, 89 TPCDS and 39 ClickBench timing cases. TPCH/TPCDS use row storage;
ClickBench uses column storage. Each request checks that the SQL pragma disables
CBO, both query caches are disabled, the native-required compilation succeeds,
and exactly one new-RBO success is counted with no failures. Five warmups precede
the measured requests. These reference lists supplement the full corpus above.
Run the suites separately to avoid concurrent clusters affecting CPU samples:

```bash
~/projects/clang-balancer/balance ./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/ut/rbo -F 'KqpNativeCompilationTime::CompilationTimeBench_TPCH' \
  --test-param ENABLE_PERF_COMPILATION=1 --test-param PERF_COMPILATION_ITERATIONS=2 \
  2>&1 | tail
```

Replace `TPCH` with `TPCDS` or `CLICKBENCH` for the other workloads.
`PERF_COMPILATION_QUERIES=1,5` selects cases within that workload;
`PERF_COMPILATION_TRACE=1` exports request traces and checks native phase counts.
The fixture measures only the native pipeline. Use the immutable runners above
for revision comparisons.

Run `KqpNativeReferenceParser::ParserDfaWarmup_TPCDS66` in `frontend/ut` with
`--test-param ENABLE_PERF_PARSER=1` alone. It resets the shared grammar DFA on
iterations 1 and 7, checks reuse during each warm sequence, and checks syntax
against the handwritten parser. Its `native_parse_us` metric includes CST
construction, native AST construction and CST release inside the parser boundary.
It does not measure binding or RBO, and clearing the process-wide DFA must not
overlap other parsing jobs.

Each batch creates a local database, performs untraced warmup loops, and then
records the measured loops in the same process. The driver alternates variant
order between batches and runs one variant at a time. Both query caches, optimizer
fallback, and the optional YQL physical-stage peephole are disabled. The native
variant additionally requires `NATIVE_REQUIRED`. Run without competing builds,
tests, or profiles.
Choose an available fixed CPU set for both variants; the example uses CPUs 2–5.
The manifest records the inherited CPU affinity, and resume requires the same
set. Untraced controls restore this affinity before starting their runners.
For small changes, repeat the same comparison into another output directory
with `--reverse-pair-order`. This reverses each pair while retaining the variant
labels, query order and configuration. Record both results: a change that flips
direction with execution order does not establish a performance improvement.
The manifest records pair order, resume checks it, and the corresponding
untraced control preserves it.

Every request remains in the results, including pre-migration compilation
failures. Native failure, missing native trace phases, or fallback makes the
comparison fail. Warmup failures also fail the native comparison. Speed ratios
use per-query medians and only successful queries shared by both variants; the
coverage counts retain the rest. The preserved pre-migration binary exposes the
outer `Compile query` span, while native traces also expose individual phases.
Operator typing is included in RBO optimization and must not be added to it a
second time.

Frontend totals include parsing, table discovery, binding/scalar typing, native
plan formatting, and operator translation. Plan formatting was first measured
separately in candidate `7dfea86f68f`; earlier traces cannot isolate that cost.
Each measurement records which frontend phases were present so comparisons do
not silently treat missing instrumentation as measured zero work.

The manifest records binary, driver and layout hashes, input commands, source
snapshot, CPU affinity, observations, and trace hashes. `--resume` validates batch signatures
and trace hashes, and preserves the original source snapshot. `--render-only`
recreates reports from an existing manifest. Use `--no-render` to collect traces
without immediately launching Violinoscope. `--limit` and `--workloads` are useful
for tooling checks; limited runs do not establish full-corpus coverage.

After collection, run controls with tracing disabled:

```bash
python3 ydb/core/kqp/opt/rbo/frontend/tools/bench_compilation/untraced.py \
  --comparison /tmp/native-comparison --out /tmp/native-comparison-untraced \
  --warmups 3 --repeats 3
```

Controls retain the same binaries, queries, schemas, and compiler configuration.
Their durations are whole request durations, not the traced compiler span, so
compare before/after ratios within each measurement rather than equating the two
absolute timings. They also retain every failure, and support `--resume`.

The full reports expose coverage differences. Produce a second violin report
with identical successful query cohorts on both sides:

```bash
python3 ydb/core/kqp/opt/rbo/frontend/tools/bench_compilation/paired.py \
  --comparison /tmp/native-comparison --out /tmp/native-comparison-paired
```

This removes a query from the paired report if any measured compilation failed
in either variant. The original traces and coverage report remain intact.

## Validation snapshot: 2026-11-15

[The validation record](validation-2026-11-15.json) pins production revision
`2a5a3b0d994` against pre-migration `d11af71c6d`. All 6,992 queries compiled
natively in all three measured passes: 20,976 successes, zero fallback. Three
untraced warmups preceded measurement. Both query caches and optimizer fallback
were disabled. Speed ratios use the 6,641 queries successful in both revisions;
the 351 pre-migration failures remain in the coverage results.

| Workload | Compiler speedup | Frontend median | Frontend / compiler | Frontend / RBO |
| --- | ---: | ---: | ---: | ---: |
| 1C | 3.78x | 0.229 ms | 3.46% | 62.3% |
| TPCH | 1.82x | 0.391 ms | 1.07% | 25.1% |
| TPCDS | 1.61x | 0.603 ms | 0.77% | 16.9% |

Speedup is the geometric mean of paired per-query median ratios. Phase values
and shares are medians of per-query medians; phase medians are not additive.
Operator typing belongs to RBO. A separate untraced control used 256 evenly
spaced 1C queries and all 179 queries from the other workloads, with no success
filtering. All 435 native queries passed. Its matched request-duration speedups
were 3.61x, 1.64x and 1.47x respectively; request durations and compiler spans
have different scopes. Both runs used CPUs 32–35 and recorded no interference
from monitored builds, tests or profiles.

The record also describes frontend, operator-boundary and populated execution
tests, independent pre-PR admission oracles, the approved lossy-float correctness
exception, and reference-PR coverage. Full benchmark admission establishes
compilation coverage; populated tests establish result/error parity. Raw traces,
full and matched-cohort Violinoscope reports, manifests and test evidence are
retained in the artifact directory recorded in the JSON.

A separate 40-query 1C control compared native checkpoint `10fd4fedb43` with
this revision in both execution orders. The frontend after/before ratios were
0.818 and 1.060. Their direction changes with execution order, so this sample
does not establish a checkpoint speedup or regression. It did not reproduce
the roughly twofold frontend increase suggested by comparing absolute timings
from separate historical runs.
