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

The checked-in proof floor runs the twelve curated obligations with the pinned
Z3 4.16.0 target and a fixed 60-second per-query budget. It selects TPCH q3,
q6, q14, and q19 plus TPC-DS q3, q42, q48, q52, q55, q90, q93, and q96
directly from the policy, accepts only `VERIFIED_BOUNDED`, and ignores every
ambient `RBO_COVERAGE_*` variable:

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

The checked-in policy has three monotonic contracts. The verifier-entry floor
requires TPCH q1 and TPC-DS q5, q65, and q80 to keep passing both snapshot
exporters and invoke the verifier; their current verifier-side `UNSUPPORTED`
results do not count as formulas, while any later formula or proof still
satisfies this depth floor. The formula-construction floor requires TPCH q3,
q5, q6, q10, q14, and q19 plus TPC-DS q3,
q15, q19, q37, q40, q42, q48, q50, q52, q55, q61, q62, q71, q76, q79, q82,
q88, q90, q93, q96, and q99.
Both floors are enforced only for a complete formula-only suite. The proof floor
requires TPCH q3, q6, q14, and q19 plus TPC-DS q3, q42, q48, q52, q55, q90,
q93, and q96;
dedicated hermetic tests require each one to remain `VERIFIED_BOUNDED`.
Arbitrary focused solver experiments are never mistaken for the proof floor,
even when they happen to select the same IDs. Newly supported or proven queries
are allowed without editing a floor. Policy parsing and evaluation fail closed,
and the report records every required and observed ID set, the explicit mode,
each enforced floor, and every violation before the test fails.

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

The complete current-code full-corpus dashboard was rerun on 2026-07-22 in
formula-only mode. It emitted the 27-query floor below and recorded an
outcome for every workload entry. `FORMULA_EMITTED` is not a solver proof. Both
measured suites meet the updated checked-in floors: TPCH q1 and TPC-DS q5, q65,
and q80 reach verifier entry.

| Suite | Formula emitted | Unsupported | Optimizer failure | Total |
|---|---:|---:|---:|---:|
| TPCH_YQL | 6 (q3, q5, q6, q10, q14, q19) | 13 | 3 | 22 |
| TPCDS_YQL | 21 (q3, q15, q19, q37, q40, q42, q48, q50, q52, q55, q61, q62, q71, q76, q79, q82, q88, q90, q93, q96, q99) | 49 | 29 | 99 |

The supported formula slice is 27/121 queries (22.3%). This is a useful end-to-end
pre-physical optimizer sample, but it remains a bounded and feature-limited
slice rather than a claim about the remaining 94 workload entries or larger
inputs.

### TPCH inventory

Optimizer preparation failed for q16, q18, and q20 on unsupported PG
semantics. Twelve queries fail closed at a snapshot boundary as follows; a
query can have both an initial and final reason. TPCH q1 is the thirteenth
unsupported query and reaches the verifier before stopping at aggregate `avg`.

| Unsupported reason | Initial snapshot | Final snapshot |
|---|---|---|
| Catalog required for subplans | q2, q4, q11, q15, q17, q21, q22 | q2, q4, q11, q15, q17, q21, q22 |
| `Apply` | q13 | - |
| `StringContains` | q9 | - |
| Callable `Map` | q7, q8 | q7, q8 |
| Scalar node with unordered children | q12 | q12 |
| OLAP `string_contains` | - | q9 |
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
construct a complete formula for q3. Exact direct numeric Date/Interval folding
moves q1 through both snapshot
exporters to verifier-side aggregate `avg` after 109 ms of preparation and 214
ms of verifier work. Exact constant DateTime2 calendar-shift folding then moves
q5, q6, q10, and q14 through formula construction, raising TPCH to 6/22. The
complete run recorded preparation/verifier times of 170/113,378, 55/222,
108/7,886, and 53/267 ms, respectively. q12 clears the calendar shift and now
fails on unordered scalar children at both boundaries; q7 and q8 remain on
generic `Map`.

### TPC-DS inventory

The 29 optimizer-preparation failures were q9, q12, q14, q17, q20, q23, q27,
q33, q36, q39, q41, q44, q45, q47, q49, q51, q53, q56, q57, q58, q60, q63,
q67, q70, q83, q86, q89, q95, and q98.

The exporter matrix below covers the boundary failures among 35 of the 49
currently known unsupported queries. IDs can appear in both exporter columns or
under more than one reason because both snapshots are audited independently.
The fourteen queries that pass export and fail closed inside the verifier are listed
after the matrix.

| Unsupported reason | Initial snapshot | Final snapshot |
|---|---|---|
| Catalog required for subplans | q1, q6, q10, q16, q24, q30, q32, q35, q54, q69, q81, q92, q94 | q1, q6, q10, q16, q24, q30, q32, q35, q54, q69, q81, q92, q94 |
| Unavailable physical column `__kqp_rbo_ignore_arg_100` | - | q97 |
| Unavailable physical column `year` | - | q66 |
| Opaque scalar with unordered children | q2, q43, q59, q66 | q2, q43, q59 |
| Nullable integral `SafeCast` to Decimal | q18 | q18 |
| Scalar expression is not Data or Optional&lt;Data&gt; | - | q28 |
| Callable `/` | q73, q78 | q73, q78 |
| Callable `Unwrap` | q8, q38, q87 | q8, q38, q87 |
| Restricted `Concat` exceeds its allocation-totality bound | q84 | q84 |
| Type `Double` | q7, q13, q21, q22, q26, q34, q75, q85 | q7, q13, q21, q22, q26, q34, q75, q85 |
| Dynamic Date fold requires `SafeCast` with `Optional<Date>` result | q72 | q72 |

After both snapshots export, q4 fails before materializing a 13,824-row join
output above the 4,096-row relation bound. q11's join matching and q25/q29's
grouped aggregates each require 65,536 candidate-row pairs above the 16,384-pair
bound. q31 likewise rejects a 32,768-pair join-matching matrix before allocation.
q5 fails closed because its Decimal `sum` cannot establish finite accumulator
headroom. q80's grouped aggregate requires 82,944 candidate-row pairs above the
16,384-pair construction bound.
q46, q68, and q91 each reject 32,640 Merge candidate-row pairs above the
16,384-pair construction bound, q64 rejects an 8,192-row join output, and q74
rejects 65,536 join-matching pairs above that same cap. q65 passes both exports
and fails closed because aggregate `avg` is not modeled. q77 passes both
exports and fails closed because its Decimal `sum` cannot establish the finite
headroom required for order-independent partial aggregation. The focused q68 run
passed both snapshot exports and reached its Merge audit cap after 11,175 ms of
verifier work. The complete dashboard took 349,431 ms to reach q91's Merge cap;
that late preflight is a known construction-performance gap, not a correctness
candidate.

Restricted static `IN` with exact types or lossless common-integer equality has
now moved all ten affected TPC-DS queries to deeper reasons. Exact Decimal
comparison removed every old Decimal-comparison blocker: q48 now emits a
formula, while q13, q21, q28, q31, q37, q40, q43, q65, q74, q82, q85, and q91
reach deeper cast, scalar, OLAP, construction, aggregate, type, or ordering
reasons.

Exact arithmetic, ordering, SUM, and Decimal-only MAX remove the old `+`, `-`,
`DecimalMul`, `DecimalDiv`, Decimal sort-key, and modeled Decimal aggregate
blockers.
Occurrence-aware non-Merge StageGraph gathers compact mutually exclusive
routing copies, and large Sort/Merge choices use bounded symbolic ordinals
instead of factorial outcome expansion. That moves q3, q52, q55, q61, q71,
and q93 through formula construction. Exact non-null integral `SafeCast` to
Decimal then moves q90 through formula construction and raises TPC-DS to 10/99.
Exact bounded String/Utf8 comparison and ordering then move q42 and q50 through
formula construction and raise the measured TPC-DS slice to 12/99. Every other
former String blocker now exposes a deeper reason: q4, q11, q25, q29, q46, q64,
and q91 reach the construction bounds enumerated above. Exact direct
String/Utf8-literal `SafeCast` to optional Decimal then moves q21 and q40 to
initial `Interval` and final OLAP `just`, while q65 passes both exports and
reaches verifier aggregate `avg`. Those were intermediate blockers before the
constant Date/Interval fold described below. Exact `If`, `Exists`, and scoped
unary `IfPresent` move q34 to `Double` at both
boundaries, q73 to `/` at both boundaries, q79 to the formerly opaque
`Substring` at both boundaries, and q68 through both exports to the Merge
construction cap. q31 still reaches its construction cap, and exact Decimal MAX
moves q74 to its 65,536-pair join-matching cap. Before the Date fold, q5 and q80
likewise reached `Interval`/OLAP `just`, while q75 reached `Double`. Exact OLAP
unary presence lowering then moves q76 through formula construction and raises
the measured TPC-DS slice to 13/99. Restricted `Substring` then moves q15, q19,
q62, q79, and q99 through formula construction and raises that slice to 18/99.
Exact ordinary integral `DataCompare` then admits equality, null-safe equality,
and ordering across every signed/unsigned 8/16/32/64-bit pair. It uses
MiniKQL's sign-aware mathematical comparison over the existing exact typed
domains, with SQL NULL propagation and two-valued null-safe equality. This is
deliberately broader than static `IN`, which retains its lossless-common-type
gate. q8 passes its former `Uint64 > Int32` blocker and now fails closed on
unsupported scalar callable `Unwrap` at both snapshot boundaries. Exact partial
integral `SafeCast` removes q79's subsequent false-positive witness. Neither
that change nor direct text-literal Decimal-cast normalization altered the
then-current formula-construction count.

The exact constant Date/Interval normalization admits only a direct non-null
String/Utf8 literal `SafeCast` to exactly `Optional<Date>`, added to or
subtracted from the strict normalized eight-child
`DateTime2.IntervalFromDays` UDF applied to a direct non-null `Int32` day count
in `[-49672, 49672]`. MiniKQL parses the Date; an invalid input or arithmetic
result outside `[0, 49673)` becomes typed Date NULL. The corresponding OLAP
`just` is erased only around a direct valid non-null Date literal. The exporter
folds the complete shape to existing Date literal/NULL nodes, so no Interval IR
or Python evaluator support is added. This moved q37, q40, and q82 through
formula construction and raised TPC-DS to 21/99. Before restricted Concat was
added, TPC-DS q5, q80, and q84 then stopped at `Concat`; q21 reaches `Double`; q72
remains outside the Date gate because its dynamic expression does not have the
exact Optional-Date cast shape; and q77 reaches the verifier's Decimal-SUM
headroom gate.

The constant DateTime2 calendar-shift normalization is a separate closed gate.
It accepts only the optimizer-generated optional-Date
`Map(Shift(Split(Date), Int32), MakeDate)` tree for `ShiftYears` and
`ShiftMonths`. The exact Date, Int32, `DateTime2.TM` resource, callable and
cached descriptors, UDF user types, Void fields, settings, AutoMap flags, and
unary lambda binding must all agree with the reviewed normalized shape. It uses
MiniKQL's Date split/make tables, including February-29 and month-end clamping,
and reproduces the runtime's signed month quotient/remainder sequence. A shift
that would wrap TM's unsigned 12-bit year field fails closed; a valid calendar
result outside the Date domain becomes typed NULL. No general `Map` or
DateTime2 execution is admitted.

Synthetic exact-result, leap-day, month-end, Date-boundary, structural-mutation,
and year-wrap cases plus a real-host pushed-filter proof cover the gate. The
full TPCH dashboard emits formulas for q5, q6, q10, and q14 at 153/109,903,
55/213, 109/8,144, and 59/247 ms of preparation/verifier work, respectively.
q12 passes this fold but remains unsupported on unordered scalar children at
both snapshot boundaries. This raises TPCH formula coverage to 6/22 and total
workload formula coverage to 27/121 (22.3%).

## Curated proof floor and focused results

- The complete current-code proof floor returns `VERIFIED_BOUNDED` for TPCH q3,
  q6, q14, and q19 plus TPC-DS q3, q42, q48, q52, q55, q90, q93, and q96,
  each at two rows per referenced table and two tasks. These are twelve bounded
  proofs (9.9% of the workload) for the modeled pre-physical semantics, not
  unbounded SQL-equivalence claims. The current TPCH run spent 128/13,291 ms of
  preparation/verification on q3, 72/749 ms on q6, 97/33,152 ms on q14, and
  110/902 ms on q19; q42 spent 95 ms preparing and 15,210 ms verifying in its
  recorded proof-floor run.

- TPCH q19 newly reaches formula construction through exact scoped unary
  `IfPresent` and restricted static-membership lowering. Its focused
  formula-only run spent 117 ms preparing and 290 ms in verification. A focused
  solver run spent 116 ms preparing and 851 ms in verification before returning
  `VERIFIED_BOUNDED`; the checked-in floor now retains that two-row/two-task
  obligation.

- Restricted `Substring` moves TPC-DS q15, q19, q62, q79, and q99 through
  formula construction. In the complete dashboard they spent respectively
  3,530, 266,730, 21,696, 24,552, and 23,703 ms in verifier/formula emission.
  Focused q15 and q62 solver experiments return `UNKNOWN` at the 60000 ms
  budget; q79 is classified separately below. None of these five is added to
  the proof floor.

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

- Direct String/Utf8-literal `SafeCast` is normalized only for a non-empty
  7-bit-ASCII, non-null literal and an exactly matching
  `Optional<Decimal(p,s)>` result, descriptor, outer annotation, nested item
  annotation, and `MayFail | MayLoseData` cast classification. The exporter
  calls `FromStringEx`: parser errors become typed NULL, finite results preserve
  round-half-to-even behavior, NaN and signed infinities remain tagged
  specials, overflow saturates to signed infinity, and underflow may round to
  zero; a successful nonnormal finite result fails closed. The result reuses
  existing Decimal literal/NULL snapshot nodes, with no Python IR change.
  Before constant Date/Interval folding, focused q21 and q40 reached initial
  `Interval` and final OLAP `just`. q65 passes both exports, then reports
  unmodeled aggregate `avg` after 231 ms of preparation and 255 ms of verifier
  work. This Decimal-cast normalization itself added no formula.

- Constant Date/Interval normalization admits only direct non-null String/Utf8
  literal `SafeCast` to exactly `Optional<Date>`, followed by `+` or `-` with
  the strict normalized eight-child `DateTime2.IntervalFromDays` UDF applied to
  a direct non-null `Int32` literal in `[-49672, 49672]`. MiniKQL
  `ValueFromString` parses the Date; parser failure or a result outside
  `[0, 49673)` becomes typed Date NULL. The corresponding OLAP `just` wrapper
  is erased only for a direct valid non-null Date literal. Runtime-oracle,
  annotation/descriptor/UDF-shape mutation, boundary, and real-host pushed
  filter tests cover the gate. q37, q40, and q82 newly emit formulas, raising
  TPC-DS to 21/99 and the workload to 23/121 (19.0%); formula emission is not a
  proof. The complete dashboard recorded 2,342, 55,463, and 1,820 ms in their
  respective verifier/formula-emission phases. Focused 60-second solver runs
  returned `UNKNOWN` for q37 after 162 ms of preparation and 63,782 ms of
  verifier work and for q82 after 130 ms and 63,078 ms; their retained SMT
  files were 4,201,832 and 2,841,844 bytes. A separate non-gating q40 scaling
  experiment used a 10-second solver budget, prepared in 178 ms, retained a
  97,319,076-byte formula, and spent 104,804 ms in verifier processing before
  reporting `SOLVER_ERROR` because the external solver exceeded its 15.0-second
  process deadline. That focused `ya` experiment failed as designed on the
  solver error; it is neither a proof nor a counterexample. At that milestone,
  the proof floor remained ten.

- Direct numeric Date/Interval normalization admits only an exact non-null
  `Date` left operand and `Interval` right operand under an `Optional<Date>`
  `+` or `-`. It
  reproduces MiniKQL's midnight-microsecond scaling, signed arithmetic,
  scaled-domain validation, and post-arithmetic truncation to days; malformed
  types or out-of-domain Interval literals fail closed, while an arithmetic
  result outside the Date domain becomes typed NULL. Synthetic boundary and
  fractional-day tests plus a real-host pushed-filter obligation cover the
  gate. TPCH q1 now passes both snapshot exporters and reaches verifier-side
  aggregate `avg` after 109 ms of preparation and 214 ms of verifier work. It
  emitted no formula, so the formula slice and proof floor were unchanged at
  that milestone.

- Constant DateTime2 calendar-shift normalization accepts only the exact
  optional-Date `Map` over `ShiftYears` or `ShiftMonths`, whose input is the
  reviewed `Split(Date)` UDF shape and whose unary lambda is exactly
  `MakeDate(bound_tm)`. All descriptors, annotations, user types, settings,
  flags, and binder identity are checked. MiniKQL calendar tables reproduce
  leap-day and month-end clamping; potential 12-bit TM-year wrap fails closed,
  while a shifted value outside the Date domain becomes typed NULL. Synthetic
  result/boundary/mutation tests and a real-host pushed-filter obligation cover
  the gate. TPCH q5, q6, q10, and q14 now emit formulas, raising the formula
  slice to 27/121 (22.3%). q12 remains unsupported on unordered children.
  At that milestone, focused solver experiments did not promote any proof: q5
  reported `SOLVER_ERROR` after 180/230,982 ms of preparation/verifier work and the
  65-second external-process watchdog, and q10 returned `UNKNOWN` after
  142/74,871 ms. q6 and q14 returned symbolic counterexamples after 54/788 and
  87/1,046 ms. Inspection indicates verifier false positives: q6's equivalent
  final Decimal predicates are opaque where the initial predicates are
  structured, while q14 fingerprints equivalent zero constants differently.
  Neither was replay-confirmed or evidence of an optimizer bug. The checked-in
  proof floor was unchanged.

- Exact wrapper normalization resolves those two verifier-modeling gaps. It
  lowers only a nullable direct comparison under exact `Coalesce(..., false)`
  to schema-preserving `if_present`, and only a direct Decimal literal or
  complete integer-literal Decimal cast under matching `Just` to
  `if(true, value, typed-null)`. Broader wrappers remain opaque, and structural,
  type, nullability, safety, source-depth, normalized-node, and live-binding
  gates fail closed. The former q6/q14 witnesses disappear; the policy-backed
  TPCH floor returns `VERIFIED_BOUNDED` after 72/749 and 97/33,152 ms of
  preparation/verification, respectively. Both obligations now belong to the
  proof floor, with no bounded witness requiring replay.

- Restricted stored-String `Concat` is admitted only as a Map-body root whose
  binary tree contains canonical String literals and one or two catalog-backed
  stored String occurrences. A nullable occurrence must be exactly
  `Coalesce(member, String(""))`. Structural provenance excludes system views,
  generated/external values, and computed strings; it follows only preserving
  operators, widens outer-join sides, drops semi/anti sides, and ORs UnionAll
  nullability. The final Member annotation must match the catalog-derived
  nullability. Datashard's 16 MiB value cap and Olap's `INT32_MAX` Arrow
  Binary-cell bound are charged per occurrence; MiniKQL's allocation-capacity
  bound must then prove the whole tree total before it is encoded as one
  syntax-preserving opaque function. One generic Olap occurrence can pass when
  its exact literals fit the remaining allocation headroom; any two generic
  Olap occurrences fail closed. Generic or nested-parent `Concat`, `Utf8`,
  nonempty fallbacks,
  and every unproved shape fail closed. Focused C++ tests cover the grammar,
  provenance failures, all ten join kinds, and the two-Olap-occurrence
  rejection; a one-Olap-occurrence real-host initial/final obligation is
  `VERIFIED_BOUNDED`.
  The complete dashboard records TPC-DS q5 as verifier-side `UNSUPPORTED` on Decimal
  SUM headroom after 1,800 ms of preparation and 822 ms of verifier work, and
  q80 on its 82,944-pair grouped aggregate after 1,946 ms and 11,975 ms. q84's
  two Olap String occurrences exceed the allocation-totality bound and remain
  unsupported. The policy pins TPC-DS q5/q80 at verifier entry; at that milestone the
  formula slice remained 23/121 and the proof floor remained ten.

- Decimal aggregate `max` is exact only when its input and output have the same
  canonical Decimal type and phase-aware nullability. It ignores NULL and uses
  MiniKQL `AggrMax`'s raw signed-code order, so NaN is greater than positive
  infinity; the same scalar state combines associatively across intermediate
  and final phases. Exhaustive special-value, grouped/global, all-NULL, split
  task, wrong-shuffle, and fail-closed type tests cover the contract. Focused
  q74 passes MAX and then rejects 65,536 join-matching pairs above the 16,384
  construction cap after 463 ms of preparation and 375 ms of verifier work.
  It emitted no formula, so the then-current formula slice and proof floor were
  unchanged.

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
  `not(exists(x))`; malformed or unknown tags and unavailable columns fail
  closed. The separate Date gate admits `just` only around a direct valid
  non-null Date literal. `Coalesce(predicate, false)` is erased only at the
  filter boundary and through AND/OR, never beneath NOT, comparison, or a
  unary presence operation. Exporter fail-closed tests and real-host `IS NULL`
  and `IS NOT NULL` obligations cover that contract and return
  `VERIFIED_BOUNDED`.
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
- TPC-DS q8 now passes exact ordinary `Uint64 > Int32` comparison at both
  real-host snapshot boundaries. Its focused run spent 480 ms preparing and 0
  ms in verifier construction before both exports failed closed on unsupported
  scalar callable `Unwrap`. A separate real-host `COUNT(*) > 1` fixture captures
  the same type pair at both snapshots and returns `VERIFIED_BOUNDED`. q8
  remains unsupported and changed neither the then-current formula slice nor
  the proof floor.
- TPC-DS q79 initially returned a symbolic counterexample with
  `d_year = 1998`. The initial nullable `Int64` membership test and the final
  lowering through `SafeCast(Int64 -> Int32)` disagreed only because the cast
  was an independent opaque function that could incorrectly return NULL for
  that in-range value. The closed-world `cast_integral` node admits only
  signed/unsigned 8/16/32/64-bit `SafeCast` pairs classified by YQL as
  `MayFail`, requires an exactly matching optional target descriptor and nested
  annotations, propagates source NULL, preserves in-range values, and returns
  NULL outside the target domain. Regeneration removes the witness. A focused
  direct-membership versus `Exists`/cast/`IfPresent` lowering is
  `VERIFIED_BOUNDED`; the full q79 query returns `UNKNOWN` at the 60000 ms
  solver budget. This was a verifier-modeling false positive, not a confirmed
  optimizer bug.
- TPC-DS q88 initially returned `COUNTEREXAMPLE`, but inspection showed a
  verifier false positive: source expressions `0 + 2`, `1 + 2`, and `3 + 2`
  were independent opaque functions while the optimized snapshot contained the
  folded literals 2, 3, and 5. The explicit scalar core now models same-type
  fixed-width integer `+`, `-`, and `*` with strict NULL propagation and exact
  modular/two's-complement overflow. The regenerated q88 obligation contains
  no opaque scalar functions and no longer produces that candidate; Z3 returns
  `UNKNOWN` at 60000 ms. q88 is therefore still an open solver-performance
  item, not a bounded proof and not a known optimizer bug.

No proof-floor or focused run has confirmed an optimizer correctness bug. q6,
q14, and q19 are bounded proofs, while q68 is a construction-bound coverage gap
rather than a candidate divergence. The former q6/q14 candidates and the q79
and q88 candidates above were verifier-modeling false positives; replay remains
the confirmation boundary for any future symbolic counterexample.

When this inventory changes, retain the old report as a test artifact, inspect
every newly supported, unsupported, failed, or solver-changed query, and update
this document only after distinguishing exporter/model changes from optimizer
changes.
