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

The checked-in proof floor runs the thirteen curated obligations with the pinned
Z3 4.16.0 target and a fixed 60-second per-query budget. It selects TPCH q3,
q6, q12, q14, and q19 plus TPC-DS q3, q42, q48, q52, q55, q90, q93, and q96
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
exporters and invoke the verifier. All four now satisfy the stronger formula
floor. Any later formula or proof still satisfies every weaker floor. The
formula-construction floor requires TPCH q1, q3, q5, q6, q10, q12, q14, and q19
plus TPC-DS q3, q5, q15, q19, q25, q29, q37, q40, q42, q43, q46, q48, q50,
q52, q55, q61, q62, q65, q68, q71, q76, q77, q79, q80, q82, q88, q90, q91,
q93, q96, and q99.
Both floors are enforced only for a complete formula-only suite. The proof floor
requires TPCH q3, q6, q12, q14, and q19 plus TPC-DS q3, q42, q48, q52, q55,
q90, q93, and q96;
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

## Latest measured formula coverage

Both complete formula-only dashboards were rerun on current code on 2026-07-23.
Together they emitted the 39-query floor below and recorded an outcome for
every workload entry. The TPC-DS rerun includes exact Decimal wrapper
hardening, nonrecursive structural IDs, grouped-key classes, the small-sequence
representation selector, exact direct Date-cast normalization, and exact
phase-aware Decimal AVG. The complete thirteen-query proof floor was not
expanded and remains green in the fresh run reported below.
`FORMULA_EMITTED` is not a solver proof. Both measured suites meet the updated
checked-in floors: TPCH q1 and TPC-DS q5, q65, and q80 reach verifier entry and
formula construction.

| Suite | Formula emitted | Unsupported | Optimizer failure | Total |
|---|---:|---:|---:|---:|
| TPCH_YQL | 8 (q1, q3, q5, q6, q10, q12, q14, q19) | 11 | 3 | 22 |
| TPCDS_YQL | 31 (q3, q5, q15, q19, q25, q29, q37, q40, q42, q43, q46, q48, q50, q52, q55, q61, q62, q65, q68, q71, q76, q77, q79, q80, q82, q88, q90, q91, q93, q96, q99) | 39 | 29 | 99 |

Complete preparation/verification totals were 6,944/11,582 ms for TPCH and
68,255/249,242 ms for TPC-DS.

The supported formula slice is 39/121 queries (32.2%), with 50 unsupported
queries and 32 optimizer-preparation failures. This is a useful end-to-end
pre-physical optimizer sample, but it remains a bounded and feature-limited
slice rather than a claim about the remaining 82 workload entries or larger
inputs. Formula construction is not a bounded proof.

### TPCH inventory

Optimizer preparation failed for q16, q18, and q20 on unsupported PG
semantics. Eleven queries fail closed at a snapshot boundary as follows; a
query can have both an initial and final reason.

| Unsupported reason | Initial snapshot | Final snapshot |
|---|---|---|
| Catalog required for subplans | q2, q4, q11, q15, q17, q21, q22 | q2, q4, q11, q15, q17, q21, q22 |
| `Apply` | q13 | - |
| `StringContains` | q9 | - |
| Callable `Map` | q7, q8 | q7, q8 |
| OLAP `string_contains` | - | q9 |
| `KqpOlapApply` | - | q13 |

Exact Date literals and ordering removed the previous Date blockers and exposed
the deeper scalar and OLAP reasons above. Restricted static `IN` similarly
removed the first blocker from q12 and q19. A later exact same-member String
membership/complement gate removes q12's composite-Boolean blocker.
Exact Decimal comparison, non-null integral `SafeCast`, scoped unary
`IfPresent`, and its restricted
static-membership lowering now let q19 construct a complete formula and prove
bounded equivalence.

Exact Decimal arithmetic, ordering, and SUM remove `DecimalMul`, the Decimal
sort key, and the widened partial/final aggregate as q3's first blockers.
Routing-aware row compaction and symbolic Sort ordinals then let both snapshots
construct a complete formula for q3. Exact direct numeric Date/Interval folding
moved q1 through both snapshot exporters to verifier-side aggregate `avg`
after 109 ms of preparation and 214 ms of verifier work at that historical
milestone. Exact constant DateTime2 calendar-shift folding then moved
q5, q6, q10, and q14 through formula construction, raising TPCH to 6/22 at that
milestone. The complete run recorded preparation/verifier times of
170/113,378, 55/222, 108/7,886, and 53/267 ms, respectively. q12 clears the
calendar shift and now
passes the narrow exact composite-Boolean gate described below. Its fresh
complete-dashboard row is `FORMULA_EMITTED` after 109/5,343 ms, raising TPCH to
7/22 at that milestone.

Exact phase-aware Decimal AVG now moves q1 through formula construction. Its
focused formula-only run spent 111/998 ms in preparation/verifier work, and
the complete current TPCH dashboard is 8/22 formulas after 6,944/11,582 ms.
A separate non-gating 60-second solver run returned `UNKNOWN` after
159/63,937 ms; this is neither a proof nor a counterexample. q7 and q8 now
reach their deeper generic `Map` exporter blocker and remain unsupported.

### TPC-DS inventory

The 29 optimizer-preparation failures were q9, q12, q14, q17, q20, q23, q27,
q33, q36, q39, q41, q44, q45, q47, q49, q51, q53, q56, q57, q58, q60, q63,
q67, q70, q83, q86, q89, q95, and q98.

The exporter matrix below covers the boundary failures among 34 of the 39
currently known unsupported queries. IDs can appear in both exporter columns or
under more than one reason because both snapshots are audited independently.
The five queries that pass export and fail closed inside the verifier are listed
after the matrix.

| Unsupported reason | Initial snapshot | Final snapshot |
|---|---|---|
| Catalog required for subplans | q1, q6, q10, q16, q24, q30, q32, q35, q54, q69, q81, q92, q94 | q1, q6, q10, q16, q24, q30, q32, q35, q54, q69, q81, q92, q94 |
| Unavailable physical column `__kqp_rbo_ignore_arg_149` | - | q2 |
| Unavailable physical column `__kqp_rbo_ignore_arg_152` | - | q59 |
| Unavailable physical column `__kqp_rbo_ignore_arg_100` | - | q97 |
| Unavailable physical column `year` | - | q66 |
| Restricted `Concat` has no storage-bounded String member | q66 | - |
| Nullable integral `SafeCast` to Decimal | q18 | q18 |
| Scalar expression is not Data or Optional&lt;Data&gt; | - | q28 |
| Callable `/` | q73, q78 | q73, q78 |
| Callable `Unwrap` | q8, q38, q87 | q8, q38, q87 |
| Restricted `Concat` exceeds its allocation-totality bound | q84 | q84 |
| Type `Double` | q7, q13, q21, q22, q26, q34, q75, q85 | q7, q13, q21, q22, q26, q34, q75, q85 |
| Dynamic Date fold requires `SafeCast` with `Optional<Date>` result | q72 | q72 |

After both snapshots export, q4 rejects a 20,736-pair join match above the
16,384-pair construction bound after 1,468/329 ms of
preparation/verification. q64 rejects an 8,192-row join output above the
4,096-row relation bound after 7,229/610 ms. q11, q31, and q74 now reach a
deeper 8,386,560-pair Sort construction preflight after 705/10,064,
556/31,238, and 440/10,112 ms, respectively.

The exact representation milestone moves every other former verifier-side
construction blocker through formula construction: q5, q25, q29, q46, q68,
q77, q80, and q91 emit formulas after 1,588/2,653, 249/11,564, 263/4,142,
284/2,574, 276/2,301, 2,122/3,323, 1,810/42,847, and 227/3,754 ms of
preparation/verifier work, respectively. Formula emission invokes no solver and
is neither a proof nor a counterexample.

The subsequent exact Decimal AVG milestone moves q65 through formula
construction after 687/30,318 ms in the focused run. The complete current
TPC-DS dashboard is 31/99 formulas, 39 unsupported queries, and 29 optimizer
failures after 68,255/249,242 ms of preparation/verifier work.

Restricted static `IN` with exact types or lossless common-integer equality has
now moved all ten affected TPC-DS queries to deeper reasons. Exact Decimal
comparison removed every old Decimal-comparison blocker: q48 now emits a
formula. At that milestone q13, q21, q28, q31, q37, q40, q43, q65, q74, q82,
q85, and q91 reached deeper cast, scalar, OLAP, construction, aggregate, type,
or ordering reasons.

Exact arithmetic, ordering, SUM, and Decimal-only MAX remove the old `+`, `-`,
`DecimalMul`, `DecimalDiv`, Decimal sort-key, and modeled Decimal aggregate
blockers.
Occurrence-aware non-Merge StageGraph gathers compact mutually exclusive
routing copies, and large Sort/Merge choices use bounded symbolic ordinals
instead of factorial outcome expansion. That moves q3, q52, q55, q61, q71,
and q93 through formula construction. Exact non-null integral `SafeCast` to
Decimal then moves q90 through formula construction and raises TPC-DS to 10/99.
Exact bounded String/Utf8 comparison and ordering then move q42 and q50 through
formula construction and raise the measured TPC-DS slice to 12/99. At that
milestone every other former String blocker exposed a deeper reason: q4, q11,
q25, q29, q46, q64, and q91 reached construction bounds. Exact direct
String/Utf8-literal `SafeCast` to optional Decimal then moves q21 and q40 to
initial `Interval` and final OLAP `just`, while q65 passes both exports and
reaches verifier aggregate `avg`. Those were intermediate blockers before the
constant Date/Interval fold described below. Exact `If`, `Exists`, and scoped
unary `IfPresent` move q34 to `Double` at both
boundaries, q73 to `/` at both boundaries, q79 to the formerly opaque
`Substring` at both boundaries, and q68 through both exports to its
then-current Merge construction cap. q31 likewise reached its then-current
construction cap, and exact Decimal MAX moved q74 to a 65,536-pair
join-matching cap. Before the Date fold, q5 and q80 likewise reached
`Interval`/OLAP `just`, while q75 reached `Double`. Exact OLAP
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
added, TPC-DS q5, q80, and q84 then stopped at `Concat`; q21 reached `Double`;
q72 remained outside the Date gate because its dynamic expression did not have
the exact Optional-Date cast shape; and q77 reached the verifier's Decimal-SUM
headroom gate. Those were historical intermediate outcomes.

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
At that milestone q12 passed this fold and exposed unordered scalar children at
both snapshot boundaries, leaving TPCH formula coverage at 6/22 and total
workload formula coverage at 27/121 (22.3%). A subsequent gate accepts only
exact `Coalesce(Or(member == literal, member == literal), false)` and
`Coalesce(And(member != literal, member != literal), false)` forms whose leaves
compare the same direct `Optional<String>` member with non-null `String`
literals.
It lowers the wrapper through schema-preserving `if_present` and leaves broader
Boolean trees opaque. q12 now emits a formula, raising TPCH formula coverage to
7/22 and total workload formula coverage to 28/121 (23.1%).

A subsequent exact Decimal-wrapper gate accepts only a direct
`Coalesce(Optional<Decimal(p,s)> member, zero)` whose member, result, fallback,
canonical type, closed-world safety metadata, and live binding all match. The
zero may be either a typed Decimal semantic zero or a complete
`SafeCast(Int32("0"), Decimal(p,s))`; narrower or incomplete casts remain
opaque. The corresponding reviewed `Just` shape is lowered recursively through
the existing schema-preserving `If` representation. This moves q43 through
formula construction after 145/4,760 ms and moves q77 past finite Decimal `SUM`
headroom to the 25,600-pair grouped-aggregate construction cap after
2,063/442 ms. The first complete dashboard exposed a classifier regression:
an incomplete Decimal cast was rejected instead of remaining opaque, losing
q40's formula and q80's verifier entry. A negative near-match test and a
fail-closed classifier fix restore both rows in the final complete rerun.
TPC-DS therefore reaches 22/99 formulas and the combined workload reaches
29/121 (24.0%). A focused q43 solver run returned `UNKNOWN` after
147/69,391 ms at the 60-second budget, so the proof floor remains 13/121 and no
candidate or optimizer bug arose.

At the preceding grouped-comparison milestone, exact sharing activated only
above the old directional cap and cached the symmetric null-safe group-key
upper triangle. q25/q29 then moved from 65,536 to 32,896 comparisons, q80 from
82,944 to 41,616, and q77 through both aggregates to a 51,360-pair Sort. That
historical dashboard remained at 22/99 TPC-DS and 29/121 workload formulas.

The current representation selector assigns exact structural IDs with an
iterative post-order walk, avoiding recursive term hashing. It partitions
grouped candidates whose complete ordered `(type, is-null term, value term)`
group keys are structurally identical. For `N` input candidates and `K`
classes, the repeated-class form is eligible only when `K < N`, its `K*N`
memberships fit the pair bound, and its `K*(K+1)/2` comparisons separately fit
the bound; it is selected when required by the directional cap or when their
combined cost is strictly less than `N^2`. Aggregate membership still covers
all original rows, while singleton provenance and common class partition facts
are preserved. Separately, Sort and latent sequence families enumerate
permutations only when every outcome contains at most three candidate rows and
the outcome cap fits; four or more rows use exact bounded symbolic ordinals.
These are exact representation changes, not approximations or raised audit
bounds.

The resulting representation-selector dashboard had 30/99 TPC-DS formulas,
40 unsupported queries, and 29 optimizer failures. q5, q25, q29, q46, q68,
q77, q80, and q91 were the eight newly formula-covered queries, with the
timings recorded above. Combined coverage was 37/121 (30.6%); the proof floor
remained 13/121 (10.7%). Subsequent regenerated full TPC-DS solver runs return
`UNKNOWN` for q5 and q77 after
1,552/64,916 and 2,035/66,344 ms of preparation/verification, respectively.

The completed Decimal AVG milestone exposes the physical accumulator in each
`avg` trait as
`{sum_type: "Decimal(35,s)", count_type: "Uint64", nullable:
<input-nullability>}` and requires identical canonical `Decimal(p,s)` input
and output types. Non-AVG traits omit this field. The decoder permits an
intermediate state only on one direct matching final-aggregate lineage with
the same ordered keys and identical state metadata; it cannot leak into an
ordinary scalar consumer or routing key. A HashShuffle transports the state as
payload.

Undefined and intermediate AVG build `(sum,count)` from non-NULL rows. Final
AVG combines both components, preserving the correct weights for unequal
partial counts, and then divides; it never averages partial averages. A group
with no non-NULL input returns NULL. Decimal special propagation matches the
existing exact sum/divide kernel: NaN is absorbing, opposite infinities produce NaN,
and a single infinity sign survives. Division by the positive `Uint64` count
uses signed round-to-nearest with ties to even, then the exact same-scale
narrow cast preserves specials and saturates finite overflow to signed
infinity. The verifier fails closed unless finite sum headroom remains inside
`Decimal(35,s)` and the accumulated count remains below `2^64`.

Independent exhaustive small-domain differential tests cover finite values,
NULL, NaN, signed infinities, positive and negative ties, grouped and scalar
aggregation, and unequal split-task counts. Decoder mutations cover missing
or malformed state, non-Decimal and mismatched types, wrong nullability,
leaked intermediate state, and broken direct lineage. Focused C++ exporter
tests pass 3/3 and the full exporter suite passes 147/147.

The current dashboards therefore emit TPCH q1, q3, q5, q6, q10, q12, q14,
and q19 (8/22) plus the preceding TPC-DS set with q65 added (31/99), for
39/121 formulas (32.2%). They record 50 unsupported and 32 optimizer-failure
queries. Focused q1 emits a formula after 111/998 ms and returns `UNKNOWN`, not
a proof or counterexample, in a non-gating 60-second solver run after
159/63,937 ms. Focused q65 emits a formula after 687/30,318 ms. The proof floor
remains the same thirteen obligations.

## Curated proof floor and focused results

- The latest complete proof-floor run returns `VERIFIED_BOUNDED` for TPCH q3,
  q6, q12, q14, and q19 plus TPC-DS q3, q42, q48, q52, q55, q90, q93, and q96,
  each at two rows per referenced table and two tasks. These are thirteen
  bounded proofs (10.7% of the workload) for the modeled pre-physical
  semantics, not unbounded SQL-equivalence claims. The latest TPCH run spent
  105/2,642 ms of preparation/verification on q3, 56/684 ms on q6,
  77/1,704 ms on q12, 80/30,141 ms on q14, and 102/817 ms on q19. The latest
  TPC-DS run prepared/verified q3, q42, q48, q52, q55, q90, q93, and q96 in
  103/4,833, 90/4,527, 174/3,846, 107/4,189, 96/3,747, 234/7,916,
  110/2,215, and 150/426 ms, respectively.

- Before exact direct literal-to-Date normalization, focused 60-second solver
  runs returned `COUNTEREXAMPLE` for q5 after 1,576/10,150 ms and q77 after
  2,062/36,163 ms of preparation/verification. Their exact historical query,
  snapshots, and raw verdicts are SHA-bound in retained artifacts. A separate
  fixed-witness inspector run reproduced q5's symbolic mismatch: the logical
  root sequence had six present rows while the staged root had one. Follow-up
  audit identifies the exact verifier false positive. Three initial
  String-literal-to-optional-Date lower bounds remained one shared
  zero-argument opaque function, allowing witness `date_dim` days 10,441 and
  10,457 outside the query's true 10,442..10,456 range, while the pushed final
  scans contained Date literal 10,442. Replacing those three opaque results
  with 10,442 made the pinned obligation `UNSAT` in about two seconds. q5 is
  not an optimizer bug.

  At that historical point q77's intact candidate remained diagnostically
  unresolved: a 180-second inspector run reached the 185-second process
  deadline, witness day 10,472 was in range, and narrowed obligations returned
  `UNKNOWN`.

- The generic and executed OLAP-filter exporter paths now exactly fold direct
  non-null String/Utf8-literal `SafeCast` to `Optional<Date>`. Focused exporter
  tests pass 4/4, the complete `cpp_ut` run passes 144/144, and a q5-shaped
  actual-host integration passes 1/1 with `VERIFIED_BOUNDED`. Regenerated full
  TPC-DS solver runs return `UNKNOWN` rather than rediscovering either
  candidate: q5 after 1,552/64,916 ms and q77 after 2,035/66,344 ms. q5's saved
  witness is refuted by the exact cast. q77's corrected fixed-witness
  diagnostic was also `UNKNOWN`, so its old witness remains historical and
  unconfirmed rather than proved false. Neither query enters the proof floor
  or provides evidence of an optimizer bug; replay remains mandatory if q77 is
  reproduced by the corrected model.

- Focused 60-second runs returned `UNKNOWN`, not proofs or candidates, for q25,
  q29, q46, q68, q80, and q91 after 302/86,108, 272/68,174, 313/64,717,
  293/64,427, 1,784/121,558, and 221/67,811 ms of
  preparation/verification, respectively. The proof floor therefore remains
  exactly the thirteen obligations above.

- TPCH q12's fresh complete-dashboard formula row spent 109/5,343 ms on
  preparation/verification; an earlier focused formula-only run spent
  108/5,816 ms. Focused and policy-backed solver runs returned
  `VERIFIED_BOUNDED` after 108/38,880 and 106/40,602 ms, respectively. Neither
  proof produced a candidate, so replay was not invoked and no optimizer
  correctness bug was found.

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
  `Interval` and final OLAP `just`. At that historical point q65 passed both
  exports, then reported unmodeled aggregate `avg` after 231 ms of preparation
  and 255 ms of verifier work. This Decimal-cast normalization itself added no
  formula.

- Direct String/Utf8-literal `SafeCast` to `Optional<Date>` is now normalized
  independently of surrounding arithmetic in both generic scalar expressions
  and the executed OLAP-filter dialect. The direct source must be non-null,
  and the result, target descriptor, outer and nested annotations, and reviewed
  `MayFail` classification must match exactly. MiniKQL `ValueFromString`
  supplies the result: valid text becomes an existing Date literal, while
  parser failure or an out-of-domain value becomes existing typed Date NULL.
  Dynamic, nullable, malformed, differently annotated, and non-`SafeCast`
  forms fail closed. The generic path retains its closed-world safety and
  totality validation; no snapshot IR or Python evaluator operation is added.
  Focused exporter tests pass 4/4, the complete `cpp_ut` run passes 144/144,
  and the q5-shaped actual-host pushed-filter obligation passes 1/1 with
  `VERIFIED_BOUNDED`.

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
  gate. At that milestone TPCH q1 passed both snapshot exporters and reached
  verifier-side aggregate `avg` after 109 ms of preparation and 214 ms of
  verifier work. It emitted no formula, so the formula slice and proof floor
  were unchanged at that milestone.

- Constant DateTime2 calendar-shift normalization accepts only the exact
  optional-Date `Map` over `ShiftYears` or `ShiftMonths`, whose input is the
  reviewed `Split(Date)` UDF shape and whose unary lambda is exactly
  `MakeDate(bound_tm)`. All descriptors, annotations, user types, settings,
  flags, and binder identity are checked. MiniKQL calendar tables reproduce
  leap-day and month-end clamping; potential 12-bit TM-year wrap fails closed,
  while a shifted value outside the Date domain becomes typed NULL. Synthetic
  result/boundary/mutation tests and a real-host pushed-filter obligation cover
  the gate. TPCH q5, q6, q10, and q14 now emit formulas, raising the formula
  slice to 27/121 (22.3%). At that milestone q12 exposed unordered children.
  Focused solver experiments at that milestone did not promote any proof: q5
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

- Exact q12 membership/complement normalization admits only binary
  `Or(==, ==)` or `And(!=, !=)` under `Coalesce(..., false)`, with the same
  direct `Optional<String>` member and a non-null `String` literal in each leaf.
  It reuses schema-preserving `if_present`; larger or differently
  shaped Boolean trees still fail closed. q12 now emits a formula and is
  `VERIFIED_BOUNDED`, raising formula coverage to 28/121 (23.1%) and the proof
  floor to 13/121 (10.7%). No candidate or optimizer bug arose.

- At that milestone, exact direct Decimal `Coalesce(member, zero)` and the
  corresponding reviewed Decimal `Just` form move TPC-DS q43 through formula
  construction after
  145/4,760 ms. Its focused 60-second solver run returned `UNKNOWN` after
  147/69,391 ms, so q43 is formula-covered but not proved. q77 clears both
  exporters and finite Decimal `SUM` headroom, then fails closed at the
  25,600-pair grouped-aggregate construction cap after 2,063/442 ms. It emits
  no formula. These results raise formula coverage to 29/121 (24.0%) without
  changing the thirteen-query proof floor or producing a candidate.

- At that milestone, scalable grouped-key sharing activated only when the old
  directional square exceeded the construction cap. It cached one unguarded
  composite null-safe group-key term per unordered row pair including the
  diagonal, then reused those terms beneath directional row-presence guards;
  small aggregates retained their established solver formula. Three-row
  call-identity tests proved that only six upper-triangle comparisons were
  encoded, explicit absent/NULL-key cases covered the directional guards, and
  the exhaustive aggregate differential remained green. The dashboard moved
  q25/q29 from 65,536 to 32,896 unique comparisons, q80 from 82,944 to 41,616,
  and q77 through both aggregates to a 51,360-pair Sort after 2,161/19,363 ms.
  That intermediate step changed no formula, proof, or candidate count; the
  later structural-class selector supersedes it.

- Finite Decimal headroom now starts at exact finite/special/NULL literals and
  complete non-null integral casts, propagates through exact `+`/`-` and
  `If`/`IfPresent`, and remains unknown when any required operand bound is
  unknown. The cast bound covers the complete signed/unsigned source-type
  domain rather than the current symbolic value. At that milestone a focused
  TPC-DS q5 run cleared its former Decimal-SUM rejection and stopped at the deeper
  32,896-pair Merge construction cap after 1,720/42,044 ms of
  preparation/verifier work. It was verifier-side `UNSUPPORTED`; the later
  exact representation selector moves q5 through formula construction.

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
  At that milestone the complete dashboard recorded TPC-DS q5 as verifier-side
  `UNSUPPORTED` on Decimal
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
  task, wrong-shuffle, and fail-closed type tests cover the contract. At that
  milestone focused q74 passed MAX and then rejected 65,536 join-matching pairs
  above the 16,384 construction cap after 463 ms of preparation and 375 ms of
  verifier work. It emitted no formula, so the then-current formula slice and
  proof floor were unchanged. Current q74 reaches the deeper Sort
  construction blocker listed above.

- TPC-DS q61 constructs a 1,572,871-byte SMT formula after exact
  `DecimalDiv` support. A focused solver run spent 955 ms preparing the query
  and 63,897 ms in verification before Z3 returned `UNKNOWN` at the 60000 ms
  budget. It is formula-covered, but is neither a bounded proof nor evidence of
  an optimizer bug.

- At that milestone TPC-DS q31 failed closed before a large join allocation:
  32,768 candidate-row pairs exceeded the 16,384 pair construction audit
  bound. It rejected that intermediate before exhausting memory. The current
  exact representation selector carries q31 farther to the deeper Sort
  construction blocker listed above.

- At that milestone TPC-DS q71 constructed a 118,276,852-byte SMT formula and
  recorded 83,339 ms in its verifier/formula-emission phase. A focused solver
  attempt reached the external solver-process deadline without producing a
  verdict. The current full formula-only dashboard records 318/1,387 ms of
  preparation/formula construction; no new solver result is claimed, so q71 is
  not a bounded proof.

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

No reported solver or proof-floor run has confirmed an optimizer correctness
bug. q3, q6, q12, q14, and q19 are TPCH bounded proofs. q5, q25, q29, q46,
q68, q77, q80, and q91 construct formulas but return `UNKNOWN` in their latest
solver runs. TPCH q1 also constructs a formula and is `UNKNOWN` in its focused
60-second run; TPC-DS q65 constructs a formula but has no claimed solver proof.
The former q6/q14, q5, q79, and q88 candidates were verifier-modeling false
positives. q77's historical candidate is not rediscovered by the exact
Date-cast model, but its regenerated and corrected fixed-witness results are
both `UNKNOWN`; it is neither a current candidate nor a confirmed false
positive. No corrected-model witness currently requires replay.

The next coverage milestone is exact support for captured uncorrelated
scalar/`IN`/`EXISTS` subplans, which currently block seven TPCH and thirteen
TPC-DS queries. After that, solver/formula-size work targets reproducible
promotion of supported `UNKNOWN` obligations into the proof floor.

When this inventory changes, retain the old report as a test artifact, inspect
every newly supported, unsupported, failed, or solver-changed query, and update
this document only after distinguishing exporter/model changes from optimizer
changes.
