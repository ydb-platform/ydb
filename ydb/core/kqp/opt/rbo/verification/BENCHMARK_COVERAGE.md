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
RBO_COVERAGE_USE_SOLVER=0 ./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_ut \
  -F '*::TPCH' 2>&1 | tail -n 100

RBO_COVERAGE_USE_SOLVER=0 ./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_ut \
  -F '*::TPCDS' 2>&1 | tail -n 100
```

The checked-in proof floor runs the ten curated obligations with the pinned
Z3 4.16.0 target and a fixed 60-second per-query budget. It selects TPCH q3 and
q19 plus TPC-DS q3, q42, q48, q52, q55, q90, q93, and q96 directly from the policy,
accepts only `VERIFIED_BOUNDED`, and ignores every ambient
`RBO_COVERAGE_*` variable:

```bash
set -o pipefail
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_ut \
  -F '*ProofFloor*' 2>&1 | tail -n 100
```

For non-gating experiments, explicitly enable the same hermetic solver and
choose any focused query set:

```bash
set -o pipefail
RBO_COVERAGE_USE_SOLVER=1 \
RBO_COVERAGE_TIMEOUT_MS=60000 \
RBO_COVERAGE_QUERIES=19 \
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_ut \
  -F '*::TPCH' 2>&1 | tail -n 100
```

`RBO_COVERAGE_QUERIES` accepts comma-separated IDs and inclusive ranges, for
example `1,4-7,96`; omitted or empty selects the whole suite.
`RBO_COVERAGE_TIMEOUT_MS` is the positive per-query solver timeout and defaults
to 10000. Solver use is deliberately explicit: absent, empty, or zero
`RBO_COVERAGE_USE_SOLVER` selects formula-only mode. The value `1` selects the
hermetic `contrib/tools/z3/z3` build output; every other value fails closed.

Each dashboard suite writes the stable report names `tpch_coverage.json` or
`tpcds_coverage.json`; proof-floor tests write `tpch_proof_floor.json` or
`tpcds_proof_floor.json` into their test output directories. The version-four
report contains suite, bounds, solver presence, timeout, per-query timing and
status, the status summary, grouped unsupported and optimizer-failure
inventories, and the evaluated coverage policy. Counterexample, unknown,
schema-mismatch, and solver-error rows also preserve the two snapshots, the
exact assembled query sent to KQP, the byte-for-byte verifier verdict, SHA-256
digests for those four inputs, and, when one was emitted, the SMT formula as
test artifacts. The raw verdict artifact is authoritative for counterexample
witnesses. The report's parsed verdict copy omits that witness so integers wider
than `ui64`, including Decimal cells, cannot be rounded during JSON re-encoding.
The nested policy-evaluation object has its own format identifier; it cannot be
mistaken for the strict checked-in input-policy document.

The checked-in policy has two monotonic contracts. The formula-construction
floor requires TPCH q3 and q19 plus TPC-DS q3, q42, q48, q50, q52, q55, q61,
q71, q76, q88, q90, q93, and q96; it is enforced only for a complete formula-only
suite. The proof floor requires TPCH q3 and q19 plus TPC-DS q3, q42, q48, q52,
q55, q90, q93, and q96; dedicated hermetic tests require each one to remain
`VERIFIED_BOUNDED`.
Arbitrary focused solver experiments are never mistaken for the proof floor,
even when they happen to select the same IDs. Newly supported or proven queries
are allowed without editing either floor. Policy parsing and evaluation fail
closed, and the report records both
required and observed ID sets, the explicit mode, each enforced floor, and
every violation before the test fails.

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
`HARNESS_ERROR`, or an enforced coverage-policy violation. `UNKNOWN`,
`UNSUPPORTED`, and `OPTIMIZER_FAILURE` remain visible coverage gaps in dashboard
or experimental runs, but each is a hard regression for a required proof-floor
query.

Every `COUNTEREXAMPLE` report is an input to the separate
`kqp_rbo_confirm` command documented in [README.md](README.md). That command
validates the version-four hashes for both snapshots, the query, and the
byte-exact raw verifier verdict; fixes the inspector to the witness read
directly from that authoritative raw artifact; and drives every replayable
single-result candidate through isolated real-YDB replay. Multi-result TPC-DS
q14, q23, and q39 currently fail closed as `UNRESOLVED`. A symbolic candidate
is not a confirmed real-execution divergence before that result is
`REAL_RESULT_DIVERGENCE`; attributing a reproduced divergence to the captured
StageGraph remains a separate diagnostic step.

## Current measured formula coverage

The complete post-OLAP-presence full-corpus dashboard was rerun on 2026-07-22 in
formula-only mode. It emitted the fifteen-query floor below and recorded a
current outcome for every workload entry. `FORMULA_EMITTED` is not a solver
proof. Both complete suite reports satisfied the checked-in coverage policy.

| Suite | Formula emitted | Unsupported | Optimizer failure | Total |
|---|---:|---:|---:|---:|
| TPCH_YQL | 2 (q3, q19) | 17 | 3 | 22 |
| TPCDS_YQL | 13 (q3, q42, q48, q50, q52, q55, q61, q71, q76, q88, q90, q93, q96) | 57 | 29 | 99 |

The supported formula slice is 15/121 queries (12.4%). This is a useful end-to-end
pre-physical optimizer sample, but it remains a bounded and feature-limited
slice rather than a claim about the remaining 106 workload entries or larger
inputs.

### TPCH inventory

Optimizer preparation failed for q16, q18, and q20 on unsupported PG
semantics. Seventeen queries failed closed as follows; a query can have both an
initial and final reason.

| Unsupported reason | Initial snapshot | Final snapshot |
|---|---|---|
| Catalog required for subplans | q2, q4, q11, q15, q17, q21, q22 | q2, q4, q11, q15, q17, q21, q22 |
| `Apply` | q13 | - |
| `StringContains` | q9 | - |
| Type `Interval` | q1 | - |
| Callable `Map` | q5, q6, q7, q8, q10, q12, q14 | q7, q8 |
| OLAP `string_contains` | - | q9 |
| OLAP unary operation `just` | - | q1, q5, q6, q10, q12, q14 |
| `KqpOlapApply` | - | q13 |

Exact Date literals and ordering removed the previous Date blockers and exposed
the deeper scalar and OLAP reasons above. Restricted static `IN` similarly
removed the first blocker from q12 and q19. Exact Decimal comparison,
non-null integral `SafeCast`, scoped unary `IfPresent`, and its restricted
static-membership lowering now let q19 construct a complete formula and prove
bounded equivalence.

Exact Decimal arithmetic, ordering, and SUM remove `DecimalMul`, the Decimal
sort key, and the widened partial/final aggregate as q3's first blockers.
Routing-aware row compaction and symbolic Sort ordinals then let both snapshots
construct a complete formula for q3. Together with q19, that raises TPCH to
2/22. q5 reaches initial `Map` and final OLAP `just`; q8 reaches `Map` at both
boundaries; and q10 reaches initial `Map` and final OLAP `just`. Those queries
remain unsupported.

### TPC-DS inventory

The 29 optimizer-preparation failures were q9, q12, q14, q17, q20, q23, q27,
q33, q36, q39, q41, q44, q45, q47, q49, q51, q53, q56, q57, q58, q60, q63,
q67, q70, q83, q86, q89, q95, and q98.

The exporter matrix below covers the boundary failures among 47 of the 57
currently known unsupported queries. IDs can appear in both exporter columns or
under more than one reason because both snapshots are audited independently.
The ten queries that pass export and fail closed inside the verifier are listed
after the matrix.

| Unsupported reason | Initial snapshot | Final snapshot |
|---|---|---|
| Catalog required for subplans | q1, q6, q10, q16, q24, q30, q32, q35, q54, q69, q81, q92, q94 | q1, q6, q10, q16, q24, q30, q32, q35, q54, q69, q81, q92, q94 |
| Unavailable physical column `__kqp_rbo_ignore_arg_100` | - | q97 |
| Unavailable physical column `year` | - | q66 |
| Opaque scalar with unordered children | q2, q43, q59, q66 | q2, q43, q59 |
| Nullable integral `SafeCast` to Decimal | q18 | q18 |
| Constant String-literal `SafeCast` to Decimal | q21, q40, q65 | - |
| Scalar expression is not Data or Optional&lt;Data&gt; | - | q28 |
| OLAP unary operation `just` | - | q5, q21, q37, q40, q77, q80, q82 |
| Callable `/` | q73, q78 | q73, q78 |
| Callable `Concat` | q84 | q84 |
| Callable `Substring` | q8, q15, q19, q62, q79, q99 | q8, q15, q19, q62, q79, q99 |
| Callable `Unwrap` | q38, q87 | q38, q87 |
| Type `Double` | q7, q13, q22, q26, q34, q75, q85 | q7, q13, q22, q26, q34, q75, q85 |
| Type `Interval` | q5, q37, q72, q77, q80, q82 | q72 |

After both snapshots export, q4 fails before materializing a 13,824-row join
output above the 4,096-row relation bound. q11's join matching and q25/q29's
grouped aggregates each require 65,536 candidate-row pairs above the 16,384-pair
bound. q31 likewise rejects a 32,768-pair join-matching matrix before allocation.
q46, q68, and q91 each reject 32,640 Merge candidate-row pairs above the
16,384-pair construction bound, q64 rejects an 8,192-row join output, and q74
fails closed because aggregate `max` is not modeled. The focused q68 run passed
both snapshot exports and reached that Merge audit cap after 11,175 ms of
verifier work.

Restricted static `IN` with exact types or lossless common-integer equality has
now moved all ten affected TPC-DS queries to deeper reasons. Exact Decimal
comparison removed every old Decimal-comparison blocker: q48 now emits a
formula, while q13, q21, q28, q31, q37, q40, q43, q65, q74, q82, q85, and q91
reach deeper cast, scalar, OLAP, construction, aggregate, type, or ordering
reasons.

Exact arithmetic, ordering, and SUM remove the old `+`, `-`, `DecimalMul`,
`DecimalDiv`, Decimal sort-key, and Decimal aggregate blockers.
Occurrence-aware non-Merge StageGraph gathers compact mutually exclusive
routing copies, and large Sort/Merge choices use bounded symbolic ordinals
instead of factorial outcome expansion. That moves q3, q52, q55, q61, q71,
and q93 through formula construction. Exact non-null integral `SafeCast` to
Decimal then moves q90 through formula construction and raises TPC-DS to 10/99.
Exact bounded String/Utf8 comparison and ordering then move q42 and q50 through
formula construction and raise the measured TPC-DS slice to 12/99. Every other
former String blocker now exposes a deeper reason: q4, q11, q25, q29, q46, q64,
and q91 reach the construction bounds enumerated above, while q65 retains its
initial constant String-literal `SafeCast` to Decimal. Exact
`If`, `Exists`, and scoped unary `IfPresent` move q34 to `Double` at both
boundaries, q73 to `/` at both boundaries, q79 to `Substring` at both
boundaries, and q68 through both exports to the Merge construction cap. q31
still reaches its construction cap and q74 reaches aggregate `max`. q5 now
reaches initial `Interval` and final OLAP `just`; q75 reaches `Double` in both
snapshots; and q80 reaches initial `Interval` and final OLAP `just`. Exact OLAP
unary presence lowering then moves q76 through formula construction and raises
the measured TPC-DS slice to 13/99.

## Curated proof floor and focused results

- The complete current-code proof floor returns `VERIFIED_BOUNDED` for TPCH q3,
  q19 and TPC-DS q3, q42, q48, q52, q55, q90, q93, and q96, each at two rows per
  referenced table and two tasks. These are ten bounded proofs (8.3% of the
  workload) for the modeled pre-physical semantics, not unbounded
  SQL-equivalence claims. In that run TPCH q3 spent 131 ms preparing and 11,845
  ms verifying; q42 spent 95 ms preparing and 15,210 ms verifying.

- TPCH q19 newly reaches formula construction through exact scoped unary
  `IfPresent` and restricted static-membership lowering. Its focused
  formula-only run spent 117 ms preparing and 290 ms in verification. A focused
  solver run spent 116 ms preparing and 851 ms in verification before returning
  `VERIFIED_BOUNDED`; the checked-in floor now retains that two-row/two-task
  obligation.

- TPC-DS q42 newly reaches formula construction through exact String ordering.
  The focused formula-only run spent 1,935 ms in verifier/formula construction.
  A focused 60-second solver run spent 106 ms preparing the query and 15,904 ms
  in verification before returning `VERIFIED_BOUNDED`; the checked-in floor now
  retains that two-row/two-task obligation.

- TPC-DS q50 also newly reaches formula construction through exact String
  ordering. Its focused formula-only verifier time was 62,440 ms. A separate
  solver experiment spent 299 ms preparing and 169,763 ms in verification, then
  reported `SOLVER_ERROR` because the external solver process exceeded its
  65.0-second deadline. q50 is formula-covered, but is neither a bounded proof
  nor evidence of an optimizer bug.

- TPC-DS q48 reaches the verifier after exact Decimal literal, domain,
  comparison-alignment, and integer constant-cast support. The proof-floor run
  spent 179 ms preparing the query and 2,997 ms in verification before returning
  `VERIFIED_BOUNDED`; the checked-in floor retains that proof obligation.

- TPC-DS q90 reaches the verifier after exact non-null integral `SafeCast` to
  Decimal support. Its two `Uint64` count expressions become explicit
  `cast_decimal` nodes targeting `Decimal(15,4)`, including the runtime's exact
  scale multiplication and signed-infinity saturation. The proof-floor run
  spent 227 ms preparing the query and 7,299 ms in verification before returning
  `VERIFIED_BOUNDED`; the checked-in floor retains that proof obligation.

- TPC-DS q61 constructs a 1,572,871-byte SMT formula after exact
  `DecimalDiv` support. A focused solver run spent 955 ms preparing the query
  and 63,897 ms in verification before Z3 returned `UNKNOWN` at the 60000 ms
  budget. It is formula-covered, but is neither a bounded proof nor evidence of
  an optimizer bug.

- TPC-DS q31 now fails closed before a large join allocation: 32768
  candidate-row pairs exceed the 16384 pair construction audit bound. A
  focused run reports `UNSUPPORTED` from the verifier in under one second of
  verifier work instead of exhausting memory.

- TPC-DS q71 now constructs a 118,276,852-byte SMT formula. The current complete
  run recorded 83,339 ms in its verifier/formula-emission phase. A focused solver
  attempt reached the external solver-process deadline without producing a
  verdict, so q71 is not a bounded proof.

- TPC-DS q76 now reaches formula construction through exact OLAP unary
  presence lowering. The final two-child `TKqpOlapFilterUnaryOp` form admits
  only Atom-tagged `exists(x)` and `empty(x)`, represented as `exists(x)` and
  `not(exists(x))`; malformed or unknown tags, `just`, and unavailable columns
  fail closed. `Coalesce(predicate, false)` is erased only at the filter
  boundary and through AND/OR, never beneath NOT, comparison, or a unary
  presence operation. Exporter fail-closed tests and real-host `IS NULL` and
  `IS NOT NULL` obligations cover that contract and return `VERIFIED_BOUNDED`.
  The complete dashboard recorded 360 ms of preparation and 13,561 ms of
  verification/formula construction; a focused run recorded 391 ms and 14,169
  ms. A focused 60-second solver experiment recorded 419 ms and 88,305 ms
  before returning `UNKNOWN`. q76 is formula-covered, but is not a bounded
  proof or evidence of an optimizer bug.

- TPC-DS q96 is `VERIFIED_BOUNDED` with a 60000 ms solver budget, two rows per
  referenced table, and two tasks. Its obligation covers the exact benchmark
  schema/query, exact Date and typed Decimal columns, `COUNT(*)`, four scans,
  three joins, split aggregation, TopSort/Merge/Limit, and
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

No proof-floor or focused run has confirmed an optimizer correctness bug. The
new q19 result is a bounded proof, while q68 is a construction-bound coverage
gap rather than a candidate divergence. The q88 candidate above was a
verifier-modeling false positive; replay remains the confirmation boundary for
any future symbolic counterexample.

When this inventory changes, retain the old report as a test artifact, inspect
every newly supported, unsupported, failed, or solver-changed query, and update
this document only after distinguishing exporter/model changes from optimizer
changes.
