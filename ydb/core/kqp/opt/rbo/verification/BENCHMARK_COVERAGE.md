# TPCH/TPCDS bounded-verification coverage

This is the reproducible coverage runbook for the new-RBO verifier. It records
what the dashboard exercises, how to rerun it, and the last complete corpus
inventory. A formula-only run is a coverage measurement, not an equivalence
proof; solver-backed results are recorded separately.

## Fixed test contract

The dashboard consumes the exact new-RBO benchmark sources under
`ydb/core/kqp/ut/rbo/data`:

- `TPCH_YQL`: `schema/tpch.sql` and `yql-tpch/q1.yql` through `q22.yql`;
- `TPCDS_YQL`: `schema/tpcds.sql` and `yql-tpcds/q1.yql` through `q99.yql`.

That is 22 TPCH queries plus 99 TPC-DS queries, 121 total. Each query is
prefixed with the benchmark compatibility definitions:

```yql
$to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
$to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
$round = ($x,$y) -> { return $x; };
```

Every schema table is created as a column-store table with
`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16`. The host enables the new RBO,
disables fallback to the YQL optimizer, permits OLAP data queries, uses the
maximum language version and all backports, and clears the result-row limit.
The query is prepared by the real KQP host; the verifier compares the initial
new-RBO snapshot with the final pre-physical StageGraph snapshot.

Every obligation has two symbolic row slots per referenced base table and a
fixed task bound of two. These bounds are part of every verdict. They do not
claim unbounded SQL equivalence or cover `ConvertToPhysical` and execution.

## Running the dashboard

Formula-only runs prepare the query, capture and strictly decode both
snapshots, and construct the SMT obligation without invoking a solver:

```bash
set -o pipefail
env -u RBO_Z3 ./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_ut \
  -F '*TPCH*' 2>&1 | tail -n 100

env -u RBO_Z3 ./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_ut \
  -F '*TPCDS*' 2>&1 | tail -n 100
```

Set an explicit Z3-compatible executable to solve supported obligations. The
focused queries currently need a 60-second budget:

```bash
set -o pipefail
RBO_Z3=/path/to/z3 \
RBO_COVERAGE_TIMEOUT_MS=60000 \
RBO_COVERAGE_QUERIES=96 \
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_ut \
  -F '*TPCDS*' 2>&1 | tail -n 100
```

`RBO_COVERAGE_QUERIES` accepts comma-separated IDs and inclusive ranges, for
example `1,4-7,96`; omitted or empty selects the whole suite.
`RBO_COVERAGE_TIMEOUT_MS` is the positive per-query solver timeout and defaults
to 10000. `RBO_Z3` is optional; when absent, supported queries stop after SMT
generation.

Each suite writes the stable report names `tpch_coverage.json` or
`tpcds_coverage.json` into the test output directory. The version-one report
contains suite, bounds, solver presence, timeout, per-query timing and status,
the status summary, and grouped unsupported and optimizer-failure inventories.
Counterexample, unknown, schema-mismatch, and solver-error rows also preserve
the two snapshots and, when one was emitted, the SMT formula as test artifacts.

## Status interpretation

| Status | Meaning |
|---|---|
| `FORMULA_EMITTED` | Both snapshots are supported and SMT was constructed, but no solver ran. This is not a proof. |
| `VERIFIED_BOUNDED` | Z3 returned UNSAT for the declared two-row/two-task model. |
| `COUNTEREXAMPLE` | Z3 returned a candidate database. Opaque or over-approximated values can make it spurious; real-YDB replay is the confirmation boundary. |
| `UNKNOWN` | Z3 timed out or could not decide the obligation. It is neither proof nor counterexample. |
| `SCHEMA_MISMATCH` | Root names, order, types, or nullability differ; this is a direct correctness failure. |
| `UNSUPPORTED` | Export or verification failed closed on unmodeled semantics. Both initial and final reasons are inventoried when present. |
| `OPTIMIZER_FAILURE` | The real host could not prepare the benchmark query, before verification. |
| `SOLVER_ERROR` | The external solver failed or violated the expected protocol. |
| `HARNESS_ERROR` | Suite setup, snapshot capture, verifier invocation, or report protocol failed. |

The current test fails on `COUNTEREXAMPLE`, `SCHEMA_MISMATCH`, `SOLVER_ERROR`,
or `HARNESS_ERROR`. `UNKNOWN`, `UNSUPPORTED`, and `OPTIMIZER_FAILURE` remain
visible coverage gaps.

## Last complete formula-only baseline

This baseline was rerun on 2026-07-21 without `RBO_Z3`; therefore its two
successful entries mean `FORMULA_EMITTED`, not `VERIFIED_BOUNDED`.

| Suite | Formula emitted | Unsupported | Optimizer failure | Total |
|---|---:|---:|---:|---:|
| TPCH_YQL | 0 | 19 | 3 | 22 |
| TPCDS_YQL | 2 (q88, q96) | 68 | 29 | 99 |

### TPCH inventory

Optimizer preparation failed for q16, q18, and q20 on unsupported PG
semantics. The remaining 19 queries failed closed as follows; a query can have
both an initial and final reason.

| Unsupported reason | Initial snapshot | Final snapshot |
|---|---|---|
| `<` integer-only ordering | q3 | - |
| `<=` integer-only ordering | q1 | - |
| `>=` integer-only ordering | q5, q6, q7, q8, q10, q14 | - |
| Catalog required for subplans | q2, q4, q11, q15, q17, q21, q22 | q2, q4, q11, q15, q17, q21, q22 |
| `Apply` | q13 | - |
| `SqlIn` | q12, q19 | - |
| `StringContains` | q9 | - |
| OLAP `string_contains` | - | q9 |
| Unsupported OLAP non-callable node | - | q1 |
| `KqpOlapApply` | - | q13 |
| `Date` literal | - | q3, q6, q7, q12, q14 |
| `DecimalMul` | - | q5, q8, q10 |
| `IfPresent` | - | q19 |

### TPC-DS inventory

The 29 optimizer-preparation failures were q9, q12, q14, q17, q20, q23, q27,
q33, q36, q39, q41, q44, q45, q47, q49, q51, q53, q56, q57, q58, q60, q63,
q67, q70, q83, q86, q89, q95, and q98.

The reason matrix below covers exactly the other 68 unsupported queries. IDs
can appear in both columns or under more than one reason because both snapshots
are audited independently.

| Unsupported reason | Initial snapshot | Final snapshot |
|---|---|---|
| `== Decimal(5,2)` vs `Int32` | q43, q61 | q61, q91 |
| `>` integer-only ordering | q31, q72 | q72 |
| `>= Decimal(7,2)` vs `Decimal(12,2)` | q13, q48, q85 | q13, q48 |
| `>= Decimal(7,2)` vs `Decimal(35,2)` | q21 | q21 |
| `>= Decimal(7,2)` vs `Int32` | q28, q37, q82 | - |
| `>= Int64` vs `Decimal(12,2)` | q65 | q65 |
| `>=` integer-only ordering | q5, q40, q77, q80, q91 | q40 |
| Catalog required for subplans | q1, q6, q10, q16, q24, q30, q32, q35, q54, q69, q81, q92, q94 | q1, q6, q10, q16, q24, q30, q32, q35, q54, q69, q81, q92, q94 |
| Unavailable physical column `__kqp_rbo_ignore_arg_100` | - | q97 |
| Unavailable physical column `year` | - | q66 |
| Opaque arithmetic `+` | q64 | q64 |
| Opaque arithmetic `-` | q11, q75 | q11, q75 |
| Opaque scalar with unordered children | q2, q59 | q2, q43, q59 |
| Scalar type is not `Data` or `Optional<Data>` | - | q28 |
| Non-integer sort | q3, q25, q42, q50, q52, q55, q71, q76 | q3, q25, q42, q50, q52, q55, q71 |
| Unsupported OLAP non-callable node | - | q37, q76, q77, q82 |
| Callable `/` | q73, q78 | q78 |
| Callable `Concat` | q84 | q84 |
| Callable `Decimal` | - | q5, q80 |
| Callable `DecimalDiv` | q4, q90 | q4, q31, q90 |
| Callable `DecimalMul` | q93 | q93 |
| Callable `IfPresent` | - | q34, q68, q73, q79 |
| Callable `SqlIn` | q8, q15, q18, q29, q46, q66, q68, q74, q79 | q8, q15, q18, q29, q46, q74, q85 |
| Callable `Substring` | q19, q62, q99 | q19, q62, q99 |
| Callable `Unwrap` | q38, q87 | q38, q87 |
| Type `Double` | q7, q22, q26, q34 | q7, q22, q26 |

## Solver-backed focused results

- TPC-DS q96 is `VERIFIED_BOUNDED` with a 60000 ms solver budget, two rows per
  referenced table, and two tasks. Its obligation covers the exact benchmark
  schema/query, passive Date/Decimal columns, `COUNT(*)`, four scans, three
  joins, split aggregation, TopSort/Merge/Limit, and
  Map/Broadcast/UnionAll StageGraph routing.
- TPC-DS q88 initially returned `COUNTEREXAMPLE`, but inspection showed a
  verifier false positive: source expressions `0 + 2`, `1 + 2`, and `3 + 2`
  were independent opaque functions while the optimized snapshot contained the
  folded literals 2, 3, and 5. The explicit scalar core now models same-type
  fixed-width integer `+`, `-`, and `*` with strict NULL propagation and exact
  modular/two's-complement overflow. The regenerated q88 obligation contains
  no opaque scalar functions and no longer produces that candidate; Z3 returns
  `UNKNOWN` at 60000 ms. q88 is therefore still an open solver-performance
  item, not a bounded proof and not a known optimizer bug.

When this inventory changes, retain the old report as a test artifact, inspect
every newly supported, unsupported, failed, or solver-changed query, and update
this document only after distinguishing exporter/model changes from optimizer
changes.
