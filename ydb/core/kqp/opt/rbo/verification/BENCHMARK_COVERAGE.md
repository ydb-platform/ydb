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
Both the dashboard host and benchmark-mode prefix-capture command link the
production PostgreSQL translator and runtime. The dummy PostgreSQL provider is
reserved for isolated tests and is not a faithful corpus-preparation host.

Every obligation has two symbolic row slots per referenced base table and a
fixed task bound of two. These bounds are part of every verdict. They do not
claim unbounded SQL equivalence or cover `ConvertToPhysical` and execution.
Query preparation and pre-physical equivalence are deliberately orthogonal.
The Final boundary is captured before `ConvertToPhysical`; a later lowering or
compilation failure can therefore coexist with a complete, auditable RBO pair.
Such a result says nothing about executability and does not extend the proof
beyond the captured pre-physical boundary.

## Running the dashboard

Formula-only runs prepare the query, capture and strictly decode both
snapshots, and construct the SMT obligation without invoking a solver:

```bash
set -o pipefail
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_ut \
  -F '*::TPCH' \
  --test-env=RBO_COVERAGE_USE_SOLVER=0 \
  --test-env=RBO_COVERAGE_TIMEOUT_MS=10000 \
  2>&1 | tail -n 100

./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_ut \
  -F '*::TPCDS' \
  --test-env=RBO_COVERAGE_USE_SOLVER=0 \
  --test-env=RBO_COVERAGE_TIMEOUT_MS=10000 \
  2>&1 | tail -n 100
```

The checked-in proof floor runs the twenty-seven curated obligations with the
pinned Z3 4.16.0 target and a fixed 60-second per-query budget. It selects TPCH
q3, q4, q6, q11, q12, q14, q15, q18, q19, q21, and q22 plus TPC-DS q3, q16,
q34, q38, q42, q48, q52, q55, q69, q73, q87, q90, q93, q94, q95, and q96 directly
from the policy, accepts only `VERIFIED_BOUNDED`, and ignores every ambient
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
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_ut \
  -F '*::TPCH' \
  --test-env=RBO_COVERAGE_USE_SOLVER=1 \
  --test-env=RBO_COVERAGE_TIMEOUT_MS=60000 \
  --test-env=RBO_COVERAGE_QUERIES=19 \
  2>&1 | tail -n 100
```

`RBO_COVERAGE_QUERIES` accepts comma-separated IDs and inclusive ranges, for
example `1,4-7,96`; omitted or empty selects the whole suite.
`RBO_COVERAGE_TIMEOUT_MS` is the positive per-query solver timeout and defaults
to 10000. Solver use is deliberately explicit: absent, empty, or zero
`RBO_COVERAGE_USE_SOLVER` selects formula-only mode. The value `1` selects the
hermetic `contrib/tools/z3/z3` build output; every other value fails closed.
Bare ambient shell variables are not inherited by the `ya` test sandbox; pass
each experimental setting with `--test-env=NAME=value` as above.

Each dashboard suite writes the stable report names `tpch_coverage.json` or
`tpcds_coverage.json`; proof-floor tests write `tpch_proof_floor.json` or
`tpcds_proof_floor.json` into their test output directories. The version-five
report has two independent row partitions. `summary` counts the terminal
semantic/capture `status`; `prepare_summary` counts `prepare_status`
(`SUCCEEDED`, `FAILED`, or `UNKNOWN`, with `NOT_RUN` reserved for suite-level
harness rows). Every row has a `prepare_reason`, empty on success. If
preparation fails after an exact Initial/Final pair was delivered, the harness
still audits both exports and invokes the verifier when both are supported. It
also preserves the assembled query and each supported snapshot or unsupported
boundary diagnostic with SHA-256 bindings.

`optimizer_failure_inventory` groups every failed preparation by
`prepare_reason`, including rows whose terminal semantic status is
`UNSUPPORTED`, `FORMULA_EMITTED`, or a solver result. It therefore overlaps
`unsupported_inventory`; neither inventory is a partition and their sizes must
not be summed. Terminal `OPTIMIZER_FAILURE` is narrower: preparation failed
before an exact normal pair was available.

Counterexample, unknown, schema-mismatch, and solver-error rows preserve the
two snapshots, the exact assembled query sent to KQP, the byte-for-byte
verifier verdict, SHA-256 digests for those four inputs, and, when one was
emitted, the SMT formula as test artifacts. The raw verdict artifact is
authoritative for counterexample witnesses. The report's parsed verdict copy
omits that witness so integers wider than `ui64`, including Decimal cells,
cannot be rounded during JSON re-encoding. The nested version-three
policy-evaluation object has its own format identifier; it cannot be mistaken
for the strict version-four checked-in input-policy document.

The checked-in policy has one orthogonal operational floor and three monotonic
semantic depths. `required_prepare_success_queries` preserves successful
preparation for the currently gated operational cases; this prevents version
five's independent semantic classification from hiding a preparation
regression. The verifier-entry floor requires TPCH q1 and TPC-DS q5, q9, q59,
q65, q78, and q80 to keep passing both snapshot exporters and invoke the
verifier. Every entry-floor query except q9 also satisfies the stronger formula
floor; q9, q59, and q78 retain weaker entry requirements as separate
diagnostic gates.
Any later formula or proof still satisfies every weaker semantic floor. A newly admitted
failed-preparation pair can enter a semantic floor without automatically
entering the preparation floor. The
formula-construction floor requires TPCH q1, q2, q3, q4, q5, q6, q7, q8, q9, q10,
q11, q12, q14, q15, q18, q19, q21, and q22 plus TPC-DS q2, q3, q5, q6, q7,
q10, q13, q15, q16, q18, q19, q21, q25, q26, q29, q33, q34,
q35, q37, q38, q40, q42, q43, q45, q46, q48, q50, q52, q54, q55, q56,
q58, q59, q60, q61,
q62, q65, q66, q68, q69, q71, q73, q75, q76, q77, q78, q79, q80, q82, q83, q87,
q88, q90, q91, q93, q94, q95, q96, q97, and q99.
The integral-AVG Slice A policy also pins TPC-DS q7, q13, and q26 in
`required_prepare_success_queries`; each must therefore preserve both
successful preparation and formula construction. The integral-extrema policy
does the same for q35.
The preparation-success, verifier-entry, and formula floors are enforced only
for a complete formula-only suite. The proof floor requires TPCH q3, q4, q6,
q11, q12, q14, q15, q18, q19, q21, and q22 plus TPC-DS q3, q16, q34, q38, q42,
q48, q52, q55, q69, q73, q87, q90, q93, q94, q95, and q96; dedicated hermetic
tests require each one both to complete preparation and to remain
`VERIFIED_BOUNDED`.
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
| `OPTIMIZER_FAILURE` | Preparation failed before an exact Initial/Final boundary-result pair was available, so no semantic pair was classified. |
| `SOLVER_ERROR` | The external solver failed or violated the expected protocol. |
| `HARNESS_ERROR` | Suite setup, snapshot capture, verifier invocation, or report protocol failed. |

Integral-AVG obligations have one additional interpretation rule. Their raw
formula is `semantic mismatch OR model-domain exclusion`, where the exclusion
means that a successful non-NULL completed average with more than two
non-NULL inputs is reachable. Solver mode checks that exclusion first:
`SAT`/`UNKNOWN` becomes `UNKNOWN`; only `UNSAT` permits the semantic check.
The shared `(count,min,max)` carrier is exact inside the count-at-most-two
domain, but it can still over-approximate equality between different binary64
results. A semantic `SAT` is therefore also `UNKNOWN` pending exact binary64
replay. Only semantic `UNSAT` is a proof, and standalone `SAT` on an emitted
raw formula is not a counterexample.

`prepare_status` is independent of `status`. In particular, `UNSUPPORTED`,
`FORMULA_EMITTED`, `VERIFIED_BOUNDED`, or `COUNTEREXAMPLE` may coexist with
`prepare_status: FAILED`. A bounded result in that case concerns only the
captured RBO transformation; it does not repair or contradict the later
preparation failure. `summary` and `prepare_summary` each partition the rows
along one axis, but values from the two summaries must not be added together.

The current test fails on `COUNTEREXAMPLE`, `SCHEMA_MISMATCH`, `SOLVER_ERROR`,
`HARNESS_ERROR`, or an enforced coverage-policy violation. `UNKNOWN`,
`UNSUPPORTED`, and `OPTIMIZER_FAILURE` remain visible coverage gaps in dashboard
or experimental runs, but each is a hard regression for a required proof-floor
query.

Every `COUNTEREXAMPLE` report is an input to the separate
`kqp_rbo_confirm` command documented in [README.md](README.md). That command
accepts version-four or version-five reports, validates the hashes for both
snapshots, the query, and the byte-exact raw verifier verdict; fixes the
inspector to the witness read directly from that authoritative raw artifact;
and drives every replayable single-result candidate through isolated real-YDB
replay. Multi-result TPC-DS q14, q23, and q39 currently fail closed as
`UNRESOLVED`. A symbolic candidate is not a confirmed real-execution
divergence before that result is `REAL_RESULT_DIVERGENCE`; attributing a
reproduced divergence to the captured StageGraph remains a separate diagnostic
step.

The current verifier can also make query errors part of an obligation: error
matches error, successful relations are compared only on success, and
error-versus-success is a mismatch. Inspector traces label those outcomes
explicitly. Version one does not distinguish error categories, codes, or text.
The external replay protocol is not yet error-aware and therefore fails closed
rather than treating an error trace as a replayed divergence.

For every failure, retain the exact query, both snapshots, byte-exact raw
verdict, SHA-256 bindings, and SMT formula when emitted. Inspection,
confirmation, and localization append the pinned witness, operator/stage
trace, child streams, retained YDB namespaces, and transformation-prefix
captures. Verifier/model changes remain separate from optimizer changes; an
optimizer fix lands atomically with its focused regression. Semantic and
finding notes may be updated with that fix, but numerical coverage reports
change only after a complete corpus rerun. The repository does not need an
intentionally red commit because the preserved repro and a pre-fix run against
the parent revision record the failure.

If real-host replay exposes a verifier-model error, retain the case as a model
regression and still audit the optimizer independently: a model bug and a real
execution divergence can coexist.

## Latest measured formula coverage

The complete exact integral-extrema checkpoint adds TPC-DS q35 formula
construction and raises the measured floor to 78 formulas. q35 is
policy-pinned and production-host captured. Commits `b6c8e8863bb`,
`cb50a1ee896`, `7785d8dd23c`, `90a7abd2334`, and `a39863e5b33` record the
semantics, stack-safe renderer, policy, odd-width regression, and
producer-local certificate cleanup.

The current proof-floor gate confirms all twenty-seven checked-in obligations
as `VERIFIED_BOUNDED`: 11/11 TPCH and 16/16 TPC-DS at two rows per table and
two tasks. Focused solver runs separately retain per-query evidence for q34,
q73, and the preceding three proof-floor additions, TPCH q21 and TPC-DS
q16/q94. The verifier-entry floor is TPCH q1 and TPC-DS q5, q9, q59, q65,
q78, and q80. Every one except q9 also belongs to the formula floor; q9 is
pinned at its deeper verifier construction bound.
`FORMULA_EMITTED` alone is not a solver proof.

Exact `DistinctAll` adds TPC-DS q6. The preceding correlated-COUNT correctness
repair intentionally moves TPCH q17 and TPC-DS q1, q30, q32, q81, and q92 from
formula construction to optimizer-side fail-closed results. Each requires
general computed post-aggregate empty-row reconstruction, which is not yet
implemented safely. None of those six formulas belonged to the solver proof
floor. With production PostgreSQL support restored, TPCH q20 now reaches the
same fail-closed reconstruction gate instead of stopping in host preparation.

The complete semantic-outcome partition is:

| Suite | Formula emitted | Unsupported | No-pair `OPTIMIZER_FAILURE` | Total |
|---|---:|---:|---:|---:|
| TPCH_YQL | 18 (q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11, q12, q14, q15, q18, q19, q21, q22) | 2 | 2 | 22 |
| TPCDS_YQL | 60 (q2, q3, q5, q6, q7, q10, q13, q15, q16, q18, q19, q21, q25, q26, q29, q33, q34, q35, q37, q38, q40, q42, q43, q45, q46, q48, q50, q52, q54, q55, q56, q58, q59, q60, q61, q62, q65, q66, q68, q69, q71, q73, q75, q76, q77, q78, q79, q80, q82, q83, q87, q88, q90, q91, q93, q94, q95, q96, q97, q99) | 21 | 18 | 99 |

Preparation is an independent partition:

| Suite | Preparation succeeded | Preparation failed | Total |
|---|---:|---:|---:|
| TPCH_YQL | 20 | 2 | 22 |
| TPCDS_YQL | 73 | 26 | 99 |

Eighteen TPCH and sixty-six TPC-DS queries pass both exporters and enter the
verifier. TPCH has twenty exact Initial/Final boundary-result pairs; TPC-DS has
eighty-one.

The preceding q66 complete TPCH dashboard spent 2,880/31,400 ms in
preparation/verifier work and produced report SHA-256
`59382f89eee68d48601d5bd102350b656e42cda17173970ce412dc83da40bdac`.
Its TPC-DS counterpart spent 64,372/683,109 ms and produced
`6aaaed8da14d46ecee9f6b3cfa31544077df5652c7c6168a01ee4ba93f0ac595`.
Those timings and hashes are historical q66 evidence, not digests of the
read-range checkpoint.

The preceding read-range complete dashboards spent 2,895/30,992 ms for TPCH and
64,296/706,547 ms for TPC-DS in preparation/verifier work. Their report
SHA-256 values are
`9a6c562fc3c8ef7d9d56dacf2411f1c87cc35d966e7ac538e4a947add9dded56` and
`1eff186049cceb773f6710ce29504bde5065b098d9d7aec1d692bf05f8f5fbec`.
Within TPC-DS, q9 spent 8,951/5,293 ms and q45 spent 511/14,367 ms.

Focused row-bound-two/task-bound-two formula-only runs retained from before
the complete dashboard record
`FORMULA_EMITTED` for q7, q13, and q26 after 194/1,181, 247/1,830, and
204/1,122 ms of preparation/verifier work. Their combined report SHA-256 is
`721507f60df911e5906865fb26710ed98772338b5aa74afc93532dad63881853`.
Separate 60-second solver runs all return `UNKNOWN`: q7 at branch 4/28, q13 at
branch 4/4, and q26 at branch 4/28. This evidence adds no bounded proof,
counterexample, or optimizer bug; the proof floor remains twenty-seven.

The integral-AVG Slice A suites passed 608/608 Python verifier, 237/237 C++
exporter, 47/47 inspector, and 14/14 coverage-policy tests. The complete TPCH
formula dashboard spends 3,273/37,511 ms in preparation/verifier work and has report
SHA-256
`f7430b2bc2e0dc3779b939831afa163d7fa7b45a7c12eeadae761117f3517b8f`.
TPC-DS spends 76,727/851,301 ms and has report SHA-256
`c37f457d0335a8b94ee10d48a5e15bffb86d6ec671050fba4538297e89688867`;
q7/q13/q26 spend 210/1,258, 279/2,049, and 224/1,361 ms.

Before exact integral extrema, q35 was `UNSUPPORTED` at
`n16.aggregates[2]` for `max(Int64)` after 598/265 ms; report SHA-256 is
`829ff76b7d3fb9849db3a13b86bac9a604bca84eaa7f64c939517560822d50b1`.
The first exact semantics run exposed a verifier-renderer `RecursionError`,
not an optimizer bug; its preserved report SHA-256 is
`d19f0e233fad50d4b6be279eaaa8fc9fdac2d48a01fb23f79ba7a33cc30cd7e1`.
After the stack-safe repair, focused q35 is `FORMULA_EMITTED` after
542/120,515 ms, report SHA-256
`b312b43d1ba4d20aeeb615c2fe75b54b8baeed87cfdb54bea85aa4a0e9ccc9b5`.
A separate 60-second solver run is `UNKNOWN` after 614/199,928 ms because it
cannot rule out integral-AVG count greater than two; report SHA-256 is
`164398b163725598b676c231349a19c30f161fdb012dc61f951934c89676f2e4`.

The final suites pass 615/615 Python verifier, 237/237 C++ exporter, 47/47
inspector, and 14/14 coverage-policy tests. The complete TPCH dashboard spends
3,207/36,148 ms and has report SHA-256
`499e0098afda7bed5198b2cb4cc2dfe35ca81e24252aa15c8e7b1803f26e2b3f`.
TPC-DS spends 70,746/858,347 ms and has report SHA-256
`8b194da2b89d4da4dbd9fd088bf8cc07e5224239e1b656322e3cfa43198d662a`;
q35 is `FORMULA_EMITTED` after 565/121,012 ms. Combined coverage is 78/121
(64.5%) of the corpus, 78/101 (77.2%) of exact pairs, 78/93 (83.9%) of the
preparation-successful subset, and 78/84 (92.9%) of verifier entrants. The
semantic partition is 78 formulas, 23 unsupported outcomes, and 20 no-pair
optimizer failures. The proof floor remains twenty-seven, and the slice found
no optimizer bug or counterexample.

A q83 row in the preceding complete TPC-DS dashboard covered two exact precursor
slices without changing that checkpoint's 50-formula/31-unsupported totals.
One-level closed `IN`-inside-`IN`
admission clears the initial boundary, and a narrow proven-present raw-tuple
Date-cast fold clears the final static `SqlIn` boundary. q83 prepares
successfully in 1,351 ms, but both snapshots first reject
`Unsupported scalar type Double`; verifier work is 0 ms. The preceding focused
run spent 1,310/0 ms and produced report SHA-256
`7f1bae257dfcede11aa2f6a37f8e1bc45e079be4f13f8b836887a1768b6d7113`.
This is neither formula/verifier-entry coverage nor a bounded proof or
optimizer-bug finding, so that checkpoint made no coverage-policy change.

The new restricted bridge admits only reviewed whole predicates over
`Optional<Int64>` with the exact floating constants and operators `>= 2/3`,
`<= 3/2`, `> 1.2`, and `< 0.9`, optionally under
`Coalesce(..., false)`. Exact IEEE-bit fingerprints bind the constants, and
all other floating values and arithmetic outside the separately audited q83
passive-carrier slice fail closed. Exact proven-present wrapper handling is
likewise limited to `Just(Date literal)` and
`Just(Convert(integer literal))` with a complete, target-typed conversion and
matching descriptor/type annotations. These gates move q21/q34/q75 through
formula construction.

Validation at that checkpoint passed 221/221 C++ exporter tests and 14/14
coverage-policy tests, and the Python verifier suite passed 577/577.

The completed passive-carrier slice admits exactly the four q83 result
expressions observed at both boundaries: three deviation expressions and one
average, all `Optional<Double>` over exactly three distinct direct
`Optional<Int64>` columns. Each complete expression is one `opaque_double`
identity with a `yql-passive-double-v1` structural fingerprint. The Python
model gives its NULL lane and payload deterministic uninterpreted functions
over the ordered arguments; it does not model IEEE arithmetic. `Double`
remains forbidden in source metadata, subplans, predicates and comparisons,
join keys, aggregate keys/inputs/results, sort and routing keys, and scalar
consumers. It may cross relational operators and StageGraph only as a direct,
uninspected payload column; q83's downstream path is Project, non-key Sort,
Limit, and Merge.

The complete dashboard records q83 as `FORMULA_EMITTED` after 1,338/6,175 ms.
A focused formula-only q83 run under the hardened gates independently returns
`FORMULA_EMITTED` after 1,301/6,081 ms of preparation/verifier work; its
report SHA-256 is
`04e5df3a8f55044002fdf9b231d75b707bf58fd51c8b60a4a8879d4d623b9a5b`.
The retained canonical formula is 10,953,698 bytes with SHA-256
`5228c142eef65eb7707ff039c58e6cfc85f599286a9ec2ccf480b5fd94903db6`.
A separate solver run returns `UNKNOWN` after 1,313/66,340 ms: the global
60-second solver deadline expires before branch 2/4
(`right_language_empty`). Its report SHA-256 is
`5571045865cbd30d7b2a35e61c379bdb7e3e24b63bfd1df2be8f514454487572`.
This is exact bounded formula construction, not a bounded proof or
counterexample. It adds no optimizer finding and leaves the proof floor at
twenty-seven. At that checkpoint the complete formula policy covered 72/121
queries. Validation passed 588/588 Python verifier tests, 225/225 C++ exporter
tests, 46/46 inspector tests, and 14/14 coverage-policy tests.

The subsequent q66 slice canonical-folds a Map-body-root binary `Concat` tree
containing only exact non-null String literals to one ordinary String literal.
Every node must retain the reviewed type and safety metadata, and the tree must
stay within the depth, node, and allocation budgets; nullable, nonliteral, or
otherwise near-match trees fail closed. Independently, a Decimal value with
certified finite coefficient bound `B` now propagates `B * max_abs(type)`
through multiplication by an integral value, capped at the Decimal type's
largest finite coefficient. Division by an integral value preserves `B`.
Same-Decimal operands and unknown input bounds deliberately remain unknown.
These two gates move TPC-DS q66 through formula construction after
2,138/35,635 ms in the complete dashboard.

A focused q66 solver run returns `UNKNOWN` after 2,134/98,339 ms because the
global deadline expires before branch 1/4 (`left_language_empty`). Its report
SHA-256 is
`60f9efb2609555474d6cd082c0a75e953f605c1a17ed821db71ca1da4c27c27e`.
The retained canonical formula is 97,279,426 bytes with SHA-256
`dcebfec17d3373e376f78ae0992aa45eb9a1e2006ea6598766ab3210379b83e5`.
This is formula coverage, not a bounded proof or counterexample. It adds no
optimizer finding and leaves the proof floor at twenty-seven. Validation
passes 593/593 Python verifier tests, 227/227 C++ exporter tests, and 14/14
coverage-policy tests.

The subsequent exact read-range slice treats `RangeInfo::ComputeNode` as the
runtime-semantic source and never derives meaning from `OriginalPredicate`.
It is restricted to a column-store read in a StageGraph, `SortDir::None`, one
catalog non-null `Int64` physical primary key, and exactly one emitted mapping
for that key. The descriptive `KeyColumns` entry must independently resolve to
the same output IU. q9's closed point grammar is
`RangeFinalize(RangeMultiply(10000, RangeUnion(RangeFor(...))))`; q45's closed
ten-point grammar begins with a typed static `Tuple` under
`Just`/`Map`/`AsList`/`Nth`, expands through
`IfPresent`/`FlatMap`/`RangeFor`, and checks the shared
`Collect(Take(...,10001))`, strict 10,000-cap overflow test, full-range
fallback, and final `RangeUnion`. Exact lambda binders, tuple indices, node
identities, descriptors, point counts, and `ExpectedMaxRanges` are part of the
contract. Generated prephysical nodes may be unannotated; present annotations
must agree. Duplicates and adjacent values are accepted as exact static
membership. A simultaneously pushed OLAP predicate is combined with the range
predicate by logical AND.

The matcher lives in `read_range_predicate_impl.h`, included exactly once
inside `semantic_snapshot.cpp`'s anonymous namespace. This keeps the
929-line closed grammar reviewable as one vertical slice while reusing the
existing scalar-safety, catalog, column-resolution, and JSON helpers. It adds
no new Python semantic node: accepted ranges lower to the existing exact
equality or static-`IN` IR.

Focused production-host captures confirm both concrete shapes. q45 reaches
`FORMULA_EMITTED`; a separate solver run is `UNKNOWN`, not a proof or
counterexample. q9 now passes both exporters and enters the verifier, where it
fails closed because an 8,192-row join output exceeds the 4,096-row
construction audit bound. No optimizer correctness bug was found in this
slice, and the proof floor remains twenty-seven. Validation passes 232/232 C++
exporter tests, 593/593 Python verifier tests, and 14/14 coverage-policy tests.

Focused solver evidence distinguishes the three promotions. q34 is
`VERIFIED_BOUNDED` after 263/2,471 ms, report SHA-256
`44bdcd9f105d4f334b628bb672fa8d6b4f6ffd43ceece0666cd03829dfa5b677`.
q21 is `UNKNOWN` after 170/60,950 ms, report SHA-256
`15ee95f2b59bc4ee41dddc19c8b98cdceb89bb3a2868678ab41539931c2c2a0e`;
the exact-wrapper repair eliminated its earlier spurious candidate. q75
remains `UNKNOWN` in the retained 1,134/128,182 ms evidence, report SHA-256
`1322068b8d57dfa984e91f6f775b063cc3c10e997e30a841e40c46f8d2058a9f`.
Only q34 joins the proof floor.

A focused version-five run selected TPC-DS q12, q20, q49, q51, q53, q63, q89,
and q98. All eight rows have `prepare_status: FAILED` and an exact
Initial/Final boundary-result pair, while their terminal semantic status is
`UNSUPPORTED`; none enters the verifier or constructs a formula. Seven initial
exports reject scalar callable `YqlAggWin`, while q49 first rejects a Decimal
`SafeCast` scale change. The final boundaries independently reject
`YqlAggWin` for q12/q20/q53/q63/q89/q98, `YqlWin` for q49, and Read
range/ordering semantics for q51. The run spent 3,186/0 ms in
preparation/verifier work and produced report SHA-256
`37b983f3247c653f5bf4a52c79375cdbc7df588ac79bd893d3a5a89ae25e16e0`.
It adds no formula or bounded proof; it exposes and preserves the semantic
blockers that the former single preparation status masked.

Exact integral `/` now accepts only operands and an Optional result with one
identical fixed-width signed or unsigned integer type. Operand NULL propagates;
a zero divisor and signed `MIN / -1` overflow return NULL; every other quotient
truncates toward zero through nonnegative magnitude division and sign
restoration. Mixed-type, mixed-width, non-Optional-result, and floating-point
forms fail closed. At the preceding integral-division milestone, TPC-DS q73
emitted a formula after 252/760 ms in the complete run. q78 passed both
exporters, then reported `UNSUPPORTED` after
1,075/27,987 ms at a 52,326-pair Sort construction above the 16,384-pair cap.
At that milestone, the policy pinned q73 at preparation, formula construction,
and bounded proof, and q78 at preparation plus verifier entry. That milestone passed 537/537
Python verifier tests, 207/207 C++ exporter tests, and 14/14 coverage-policy
tests. A focused solver differential passed 1/1: the unchanged division pair
was verified and reversed operands produced a bounded counterexample. The
focused q73 workload proof is `VERIFIED_BOUNDED` after 239/8,940 ms and
produced report SHA-256
`2c9dd4e765f4507bd952189055d67a0db5cf818ecb84abe188bfcdd8a15122e0`.

TPC-DS q54 now reaches `FORMULA_EMITTED` through an exact Map projection that
copies one visible input into multiple distinct output columns. Its dashboard
row spent 987/50,737 ms in preparation/verifier work at row bound two and task
bound two. The canonical formula is 57,271,400 bytes with SHA-256
`3494295db496d95d32019eb5aa0d0b14e099ef38cdb42d646e7c2f07f0035f4e`.
A separate 60-second solver run returned `UNKNOWN` because the global deadline
expired before branch 3/8 (`left_outcome_0_unmatched`). This is formula
coverage, not a bounded proof, and q54 is not in the proof floor.
The milestone passes 529/529 Python verifier tests, 205/205 C++ exporter
tests, 39/39 real-host integration tests, and 12/12 coverage-policy tests.

The current 27-obligation proof-floor gate is policy-valid and green: TPCH
passes 11/11 after 1,308/62,684 ms and produced report SHA-256
`0eed270ad0148908f05f59ad4e09f8710c280fca39871b5269b60ca1f707979e`;
TPC-DS passes 16/16 after 3,666/57,767 ms and produced
`e8b018abf0286bead86484b8a8739985554b70c823ef663abbeb938eb52a44b6`.
All 27 are `VERIFIED_BOUNDED`, or 27/121 (22.3%) of the workload. The
focused proof-floor policy target passes 5/5. The focused q34 evidence is
recorded above.

The immediately preceding 26-obligation gate passed 11/11 TPCH after
1,328/62,929 ms and produced report SHA-256
`a4604150df2e3875a18401c9e44fbbce62cf843843b601828258dea7cd42134b`;
TPC-DS passed 15/15 after 3,273/56,501 ms and produced
`4c297e703cf16dfb9b1eddefeea87ec90f5cdb509a68529d517324ead8e8afbd`.
The incremental focused q73 evidence for that gate is the 239/8,940 ms,
SHA-bound proof above. The preceding retained version-four 25-obligation gate passed
11/11 TPCH after 1,445/67,977 ms and produced report SHA-256
`c5668fdda1f40493fdcef4729118d634a63c25f44024106198cdd89775d9d4ed`.
TPC-DS passed 14/14 after 3,328/49,834 ms and produced
`489f58334593770ff80024bc50a055ed8d60712a34056839d837fbed29c7ff34`.
Together those historical reports contain 25/25 bounded proofs.

The three preceding focused solver rows are all `VERIFIED_BOUNDED`. TPCH q21
spent 168/1,901 ms in preparation/verification and produced report SHA-256
`851b3a040d3aa1126d5b0256da95c8851984b977f8d7671db47a9f2d1a9eccef`.
TPC-DS q16 and q94 spent 260/3,923 and 237/2,887 ms, respectively, and their
combined report SHA-256 is
`bc2b934bed75e48cb8ebeea112eb07ef282b6ecd1c331163ce99f927b3b0c848`.
These focused artifacts retain the individual new-row evidence behind the
complete gate.

The immediately preceding complete 59-formula dashboard was generated on
2026-07-24 from source `4c2c1359e28`, before the two-dependency `EXISTS`
extension. Its TPCH run spent 7,401/76,064 ms in preparation/verifier work and
produced report SHA-256
`92f8508dcc9eb47e49a4ecbd9ec3577f2ab84aaa254404f555f9f4b60207342a`.
Its TPC-DS counterpart spent 123,828/490,758 ms and produced
`e7eef8b14247a35a3c1eb822d15d87eb6c80151064aa89164d58bcaba568f405`.
TPC-DS q2, q59, and q97 spent 3,874/34,168, 993/1,218, and 595/895 ms,
respectively. Both complete policy runs were green under that checkpoint's
59-formula and twenty-two-proof contract.

The immediately preceding 57-formula checkpoint was generated from source
`5dafcc79a4e`. Its TPCH dashboard spent 2,872/28,853 ms and produced
`c0eadbb10b2b1f394d604bb5cc5097d9fac26646e6d3aa97b90e2ff47b0712d2`.
Its TPC-DS counterpart spent 63,947/243,682 ms and produced
`6e895ad5385f95b0528362e228d992065bb44487262cb22e5ddb5ba38ba9b844`.
TPC-DS q18 emitted after 1,002/51,090 ms in that historical report.

The immediately preceding 56-formula q33 checkpoint was generated from source
`dfd6546dfd5`. Its TPCH dashboard spent 2,814/29,457 ms and produced
`b3bc23c618c62f73cbc362ed568a33caf484c98804160b799bf42530f9dc66e4`;
its TPC-DS dashboard spent 66,416/198,352 ms and produced
`e0ab31819ceb0b1764d0e2be5b0af56c20c41e693b51b8b3f33408b389650d3b`.
TPC-DS q33 emitted after 1,551/1,158 ms in those historical reports.

The immediately preceding 55-formula Date-`Unwrap` checkpoint was generated
from source `93a01455afe`. Its TPCH dashboard spent 2,810/30,317 ms and
produced
`464b67c4ae5ec2661789e659c349e94f7c45ef958f38214bb733d99b2814ef02`;
its TPC-DS dashboard spent 63,563/189,499 ms and produced
`cdfd41c4ab74b42a884b332c05b006f7a543c23f1f4ab4c924dbd08f2adc16f8`.
TPC-DS q38 and q87 emitted after 335/471 and 306/474 ms, respectively, in
those historical reports.

The preceding 53-formula TPCH/TPC-DS dashboards spent 2,801/30,006 and
68,622/200,340 ms and produced
`deb388eec49e32242cd66bfbe943ef2f73a692d95d96150fbfb68f8281390753` and
`9d808615985c7c6fce4bc76cfb7b1c92e68e82ecb02294e23921f7dad809af2d`.
TPC-DS q6, q56, q60, and q95 emitted after 344/11,938, 1,653/1,082,
1,464/1,092, and 474/454 ms, respectively, in those historical reports.

The preceding 51-formula q95-expanded TPCH run spent 3,004/31,688 ms and
produced
`9ba059ae97bc66d4fdbafa200ba9ce74f25f831d3651bf7d7c0ccbd2206a5774`;
its TPC-DS counterpart spent 65,034/193,746 ms and produced
`a31813a6a2f24365680b6209e14a062e75e575d781b43611ca62e998fd4b1c8e`.
Those digests remain historical artifacts of the integral-only dynamic-`IN`
inventory.

The preceding 50-formula policy-checked TPCH run spent 2,897/29,563 ms in
preparation/verifier work and produced
report SHA-256
`6a8cbbeb316d128880ae97295efcc763cdc5ce14d648adec3411e6b0bb8fa214`.
Its TPC-DS counterpart spent 64,077/192,905 ms and produced
`279318f3d46f585bba33ede252bf723a5ece36c989215015c0046eb6677e8f29`.
Those are retained historical artifacts rather than digests of the current
String-expanded reports.
At that checkpoint TPC-DS q6 emitted its formula after 347/11,882 ms. Its
canonical direct render is 32,055,251 bytes after the exact
already-alternative Sort ordinal representation; a 60-second solver experiment
remains `UNKNOWN`.

The preceding exact read-range complete dashboard is 74/121 queries (61.2%).
Its semantic partition is 74 formula-emitted, 27 unsupported, and 20 without
an exact initial/final pair because whole-query preparation failed.
Preparation is a separate axis: 93 queries succeed and 28 fail, with eight
failed preparations that nevertheless preserve an exact pair and therefore
also reach a semantic unsupported result.

Integral-AVG Slice A moves three exact, preparation-successful TPC-DS pairs
from unsupported to formula construction; exact integral extrema then move
q35. The complete dashboard therefore raises coverage to 78/121 (64.5%),
78/101 (77.2%) of exact pairs, 78/93 (83.9%) of the preparation-successful
subset, and 78/84 (92.9%) of verifier entrants. TPC-DS moves from 56 to 60
formulas and from 25 to 21 unsupported outcomes; its 18 optimizer failures and
73/26 preparation partition are unchanged. Across both suites the semantic
partition is 78 formulas, 23 unsupported outcomes, and 20 no-pair optimizer
failures. This is a useful end-to-end pre-physical optimizer sample, not a
claim about larger inputs. Formula construction is not a bounded proof.

The 23 semantic unsupported rows split by primary reason into 17
initial-export, zero final-export, and six verifier results. The strict C++
boundary therefore accounts for 17 primary outcomes; the six remaining exact
pairs fail closed in the verifier itself.

The integral-AVG contract is exact and narrow:
`Optional<Int64> -> Optional<Double>` with the strict phase-linked state
descriptor. Intermediate aggregation carries original-input
`(count,min,max)` ghost state; one shared UF supplies the result carrier only
under non-NULL count at most two. A tagged node-local
`IntegralAverageCertificate` is observed at the producer and centrally removed
before the family is cached or returned to any parent, so completed
certificates never become transported result state. Intermediate state remains
transportable.
The solver must first prove the count-greater-than-two model-domain exclusion
`UNSAT`. Exclusion `SAT`/`UNKNOWN` and semantic-UF `SAT` all yield `UNKNOWN`;
the latter requires exact binary64 replay. Only semantic `UNSAT` can prove.

Exact integral extrema admit only same-output-type `Int8/16/32/64` and
`Uint8/16/32/64`, with phase-aware nullability and a balanced, sentinel-free
guarded reduction. Decimal extrema are unchanged. The q35 semantics run
exposed a real verifier renderer recursion bug; explicit occurrence, level,
and output stacks repaired it, with old/new bytes matching across 3,000
randomized shared and quantified DAGs.

These counts also expose the approximate work needed to make formulas for most
of the captured workload. The remaining primary blockers include
floating-point/`Double` semantics (Slice A removes q7/q13/q26 and exact
integral extrema remove q35, leaving q22/q85 as derived-`Double` boundaries);
factorized Sort/Merge/join
shapes (six after q9 moved past its range boundary);
broader read-range grammars; scalar `Apply`/`Map`; and smaller
Date/`Unwrap`, count-distinct, and allocation-bounded `Concat` slices.
Same-type integral division removed q73 and q78 from the numeric first-blocker
inventory; both now emit formulas. Floating-point division and floating `avg`
remain parts of the broader `Double` program. The whole-predicate bridge is
deliberately not general floating arithmetic.
Literal-only `Concat` and integral-factor Decimal bounds remove q66 from this
inventory; q84's two Olap String occurrences still exceed the proven
allocation-totality bound.
The focused eight-query failed-preparation audit adds exact window semantics
and lowering as the main captured-pair family, Decimal scale-changing casts
for q49, and a secondary range-read boundary for q51. Including that overlap,
the captured-pair gap is approximately 7--9 families or 9--17 milestones.

These are planning estimates, not coverage floors. The new checkpoint starts
the next planning pass from 78 formulas; later blockers can invalidate any
query-count projection. The remaining 20 workload entries have no exact
captured pair and require frontend/optimizer work before verifier semantics
can help; consequently the present captured-pair ceiling is 101/121.
Even reaching that ceiling would establish formula construction, not solver
proof.
The next slice is narrowly tagged derived-`Double` ordering for q22/q85,
initially targeting formula construction only.

The exact sorting-network slice adds TPCH q2 to the formula and preparation
floors without changing the proof floor. A focused row-bound-two/task-bound-two
run constructs its problem in 11.469 seconds and renders a 62,274,331-byte
formula in another 15.931 seconds. The final plan uses two 128-row,
21-column local networks at 37,632 comparator/column pairs each and one
200-row Merge network at 96,768, beneath the 131,072 row-transport cap.
Ranks form a finite permutation, SQL keys dominate ranks, ranks order only
ties, and present rows dominate absent rows. Compare-exchange moves the full
nullable row and hidden Decimal AVG state coherently. Concrete semantic
producer ordinals add exactly the per-producer rank chains required by Merge.

The subsequent packed-row carrier replaces per-column network transport with
one exact row datatype and one exact defined comparator per ordering. Row
presence, NULL/value lanes, and hidden Decimal AVG state move as one value;
finite ranks still choose only among exact SQL-key ties. The audited selector
uses separate caps of 32,768 comparators, 131,072 logical payload cells, and 64
key columns. Focused q59 and q78 runs emit 116,879,360- and 202,469,546-byte
formulas in 44.66 and 57.06 seconds, respectively, with SHA-256
`3a140fcb1b5d6a5145c4aa30cbcd817167a27f21bed94d85ef969223dce73c8e`
and `fb0eaebb95ea9bdfb3b0f815f5078a70d1c2e3765ed5d6675be1c4f06b8249c4`.
In the complete TPC-DS dashboard q59 and q78 spend 828/74,462 and
1,042/113,216 ms in preparation/verifier work. Neither result is a solver
proof.

The next exact slice admits only an expression-level uncorrelated scalar
binding consumed inside an `IN` subplan root. Every consumer operator must
belong to exactly one main or subplan root; structural root nesting, correlated
scalars, and every other nested owner/kind fail closed. The ordinary scalar
zero/one/many-row semantics, local cardinality-error demand, eager inherited
errors, shared cache, and cumulative membership-pair budget apply unchanged.
TPC-DS q58 contains three such scalar-inside-`IN` pairs.

Its later Merge needs a symbolic producer order. For fixed producer orders, the
network retains adjacent tie-rank chains. For symbolic ordinals, it adds one
both-present-guarded direction equivalence per unordered producer pair; equal
input ordinals carry no edge, and the existing present-ordinal uniqueness
invariant excludes that case for two present rows. The symbolic producer-pair
count is preflighted against the 16,384 relation-pair cap.

In the complete TPC-DS dashboard q58 spends 3,285/109,215 ms in
preparation/verifier work. Separately, the focused q58 dashboard returns
`FORMULA_EMITTED` after 3,291/103,862 ms and has report SHA-256
`87c0a2e7d51b077c19c7b261fd899f00dc590106385764574eaa7e46aac50b94`.
The retained direct emission takes 101.43 seconds, peaks at 2,294,048 KiB RSS,
and contains eight datatype/comparator-definition pairs in a 324,938,538-byte
formula with SHA-256
`22f51f5d1a82091a35d29b6ac120344725f1272b8093ae9a0f1c3fa6fc6eaa70`.
This adds q58 to the preparation and formula floors but is formula coverage
only: q58 adds no bounded proof or optimizer-bug finding. At that checkpoint
validation passed 568/568 verifier tests, 208/208 C++ exporter tests, and
14/14 coverage-policy tests.

The subsequent one-level nesting slice admits closed leaf dynamic-`IN`
bindings inside a dynamic-`IN` root. Every descriptor retains the ordinary
exact type, NULL, positive-Filter, ownership, cache, eager-error, and cumulative
16,384-pair contracts. Each nested `IN` consumes no subplan binding, so
self-reference, cycles, and deeper chains fail closed. Exhaustive finite
reference checks, nullable and eager-error cases, cache and pair-cap tests, and
a solver comparison with two sequential `left_semi` joins cover the path;
omitting the inner membership yields a counterexample.

The separate q83 static-`SqlIn` slice accepts an `Optional<Date>` annotation on
a raw-tuple item only when that item is a direct String/Utf8-literal
`SafeCast` which MiniKQL parses to a present Date. It serializes the existing
non-null Date literal. Invalid text, a dynamic source, `Nothing`, `StrictCast`,
other optional types, and nullable `AsList` items fail closed. The q83 outcome
and current full-suite counts are recorded above.

At the output-IU milestone, the resolver mapped a physical read name, full
output-IU name, or short output-IU name to the same logical scan output. If a
referenced identifier denoted distinct outputs it failed closed as ambiguous;
an unused ambiguous spelling was accepted because it could not affect the
predicate. TPC-DS q2 and q97 reported `FORMULA_EMITTED`. q59 passed both
exporters and reached the verifier, then reported `UNSUPPORTED` at a
32,640-pair Sort construction
above the 16,384-pair audit cap. At the preceding complete checkpoint this
produced a TPC-DS split of 43 formulas, 30 unsupported queries, and 26
optimizer failures; TPCH was 16, 4, and 2. That dashboard records 3,874/34,168
ms for q2,
993/1,218 ms for q59, and 595/895 ms for q97. This result adds no bounded proof
and is not an optimizer-bug finding.

The latest exact Decimal-cast gate accepts only weak `SafeCast` and serializes
an explicit `cast_decimal` node with a mandatory canonical `source_type`. The
Python decoder independently infers the argument type and requires an exact
match; it also checks the serialized canonical `Decimal(p,s)` result type,
positive integral-digit count, and source-matching nullability. Independently,
the C++ exporter requires the target descriptor and its outer and nested
annotations to agree exactly with that result. An exact integral source
preserves NULL, scales each present value by `10^s`, and saturates finite
overflow to signed infinity rather than NULL. A canonical Decimal source is
accepted only for same-scale, nondecreasing-precision widening; the raw finite,
signed-infinity, or NaN code is identity and NULL propagates.

`StrictCast`, `Convert` outside the existing complete-literal normalization,
an absent or mismatched `source_type`, other source families, Decimal scale
changes or precision narrowing, nullability changes, zero-integral-digit
targets, and malformed or mismatched descriptors and annotations fail closed.
This moves TPC-DS q18 through formula construction after 1,002/51,090 ms of
preparation/verifier work. A separate real-host
two-row/two-task fixture proves nullable integral-to-Decimal and nullable
same-scale Decimal-widening expressions. That synthetic proof does not prove
the full q18 query: q18 is formula-only and produced no optimizer-bug finding.

At the preceding focused post-dashboard checkpoint, a semantic slice
canonicalized generic `EndsWith`/`StringContains` with their executed OLAP
spellings and added exact
same-type Decimal `MIN`. At that point it did not change the 46/121 formula
floor: TPCH q2 passed both exporters and `MIN` but failed closed at a
32,640-pair Merge construction above the 16,384-pair cap, while q9 reached
unsupported scalar `Map` in both snapshots. A small real-host column-store
fixture containing both String predicates returned `VERIFIED_BOUNDED`.

The initial exact slice accepted one uncorrelated dynamic-`IN` binding with one
typed lookup column, one typed inner-result column, and exactly one Filter
consumer. The two columns must have the same non-null fixed-width integral
type, and the binding itself must be non-null `Bool`. `OuterBind`,
`AddDependencies`, an observable `EnsureAtMostOne`, nesting, staging, fanout,
tuple/coercion semantics, nullable values, and nonintegral types fail closed.
Evaluation is existential equality over present inner rows:
duplicates collapse, empty input is false, consumer `NOT` supplies the
anti-membership form, repeated binding references share one cached subplan
family, and inherited root errors remain eager. Construction admits at most
16,384 outer/inner membership pairs cumulatively across alternatives and
nested evaluation.

At that checkpoint this moved TPCH q18 through formula construction. A focused
two-row/two-task solver experiment returned `VERIFIED_BOUNDED` after about
155/3,035 ms of
preparation/verification, and q18 is now pinned and confirmed in the complete
proof floor. The real-host integration captures initial
`IN`, final `left_semi`, and the normal bounded proof. It links production
PostgreSQL support because the dummy provider failed query preparation. TPCH
q16 and TPC-DS q95 passed this binding gate but stopped at later blockers in
that run;
nullable, `String`, and `Date` workload bindings remained unsupported at that
checkpoint.

The subsequent exact nullable Date-year slice accepts only an
`Optional<Uint16>` `Map` over the complete cast of one direct visible
`Optional<Date>` member to `Optional<Timestamp>`, followed by a unary
`DateTime2.GetYear(DateTime2.Split(argument))` lambda. It checks exact UDF
names, callable and cached descriptors, user types, AutoMap flags, settings,
annotations, and binder identity; every near miss fails closed. The snapshot
uses `if_present` for source NULL, the stable non-null typed opaque function
`yql-datetime-year-v1` over the bound Date payload, and an explicit typed-NULL
lift to `Optional<Uint16>`.

This moves TPCH q7, q8, and q9 through formula construction. Their complete
dashboard rows spent 237/3,318, 278/2,954, and 187/1,628 ms respectively in
preparation/verifier work. Focused 60-second solver experiments all returned
`UNKNOWN`: q7 after 230/64,641 ms at branch 4/4
`right_outcome_0_unmatched`, q8 after 280/65,107 ms at branch 4/28
`left_outcome_1_unmatched`, and q9 after 181/62,461 ms at branch 4/4
`right_outcome_0_unmatched`. Thus the formula floor rises by three while the
then-current proof floor remains 19/121. The accepted shape and fail-closed C++
mutation matrix, Python NULL/fingerprint/argument tests, and a
`VERIFIED_BOUNDED` real-host initial/final projection cover the bridge. The
complete suites at that Date-year checkpoint passed 183/183 C++ tests, 493/493
Python verifier tests, 46/46 inspector tests, and 32/32 real-host integration
tests.

The current q95 slice serializes every join equi-key as an explicit
`{left,right}` descriptor and keeps only `JoinFilters` in the residual
predicate. A shared IU name is accepted only for left/right semi or anti joins,
with no join filters and a literal-true residual; joins that return both sides
remain fail closed. Matching reads each key from its declared input before row
maps can merge. Each key's equality and two leaves, the implicit
key/residual conjunction, and the deeper residual all remain inside the shared
1,024-node/128-depth exact-expression budget.

Three narrow q95 bridges complete the path without generalizing that boundary.
`Just(member)` is normalized only for one direct visible non-null `Uint64`
member and exact `Optional<Uint64>` result. Final scalar unwrap is exact only
for keyless non-distinct `sum(Optional<Uint64>)` with a raw
`Optional<Uint64>` output; its effective physical value is non-null and
coalesces empty/all-NULL input to zero. Direct per-trait distinct is exact only
for a keyless, phase-`undefined`, non-`DistinctAll`
`COUNT(DISTINCT non-null Int64)` with non-null `Uint64` output, no unwrap, and
at most one distinct trait. Its first-occurrence encoding charges the
`N*(N-1)/2` equality triangle before construction. All nearby type,
nullability, phase, key, function, trait-count, and residual mutations fail
closed.

TPC-DS q95 now emits a 288,499-byte, 1,269-line formula and the dedicated
two-row/two-task proof-floor row returns `VERIFIED_BOUNDED` after 512/3,013 ms
of preparation/verification. It is pinned in both policy floors. The other
former shared-IU queries then exposed their earlier initial binding boundaries:
q16/q94 required exactly one outer `EXISTS` dependency, while q33/q56/q60
required a non-null fixed-width integral dynamic-`IN` result.

The preceding extension admits only the same exact non-null `String` type on both
sides of dynamic `IN`; it retains the same existential-equality model and
independent C++/Python validation as the integral slice. `Utf8`, nullable or
coercing comparisons, Bool, Date, Decimal, and mismatched types remain
fail-closed. This moves q56/q60 through formula construction and q45 through
initial export to its independent final Read range/ordering blocker. At that
checkpoint q33, q58, and q83 remained at the initial dynamic-`IN` type
boundary.

Pre-fix focused solver runs returned `COUNTEREXAMPLE` candidates for q56 after
1,260/2,356 ms and q60 after 1,260/2,072 ms. Fixed-witness inspection
reproduced both symbolic mismatches. A paired embedded real-YDB diagnostic with
CBO explicitly disabled confirmed that they exposed one RBO defect: legacy
execution returned `("same", 10)`, while new RBO returned zero rows. Commit
`6a2c3acb29b` preserves the finding.

After the exclusive-IU-ownership repair in `98176b0b48c`, both old witnesses
return `WITNESS_NOT_REPRODUCED`, and focused q56/q60 solver runs return
`UNKNOWN` at the 60-second limit. On source `4f73b38aaaf`, q56 spent
1,286/61,302 ms and q60 spent 1,224/61,274 ms in preparation/verification; the
focused report SHA-256 is
`1da4256d6b306933aa54cabc99fce262f12bcac69b1dd64c9dfd599fad7b6caa`.
That source retains the nonmanual production runtime regression. At that source
checkpoint, formula coverage stayed at 53/121, 53/93 optimizer-successful
queries, and 53/59 verifier entrants. Neither query entered the unchanged
20/121 proof floor.

The earlier exact proven-total Date-`Unwrap` gate accepts only
`Unwrap(Coalesce(member, zero))` with a non-null Date result, an exact binary
`Optional<Date>` coalesce, and one direct visible `Optional<Date>` member. The
observed initial boundary spells zero as the exact reviewed
`SafeCast(Int32(0), Optional<Date>)`; the observed final boundary spells it as
`Just(Date(0))`. The gate accepts either exact spelling. The Int32-to-Date cast
accepts zero, so both fallbacks are present Date zero and the `Unwrap` error
path is unreachable. Both spellings normalize to the existing non-null Date
`if_present`: return the bound member value when present and Date zero when
absent. No new verifier expression or calendar semantics are introduced, and
nearby arity, type, nullability, literal, visibility, metadata, and wrapper
shapes fail closed.

This gate moves TPC-DS q38 and q87 through formula construction after 335/471
and 306/474 ms of preparation/verifier work. Their checked-in proof-floor rows
return `VERIFIED_BOUNDED` after 333/1,115 and 324/1,052 ms, respectively, at
two rows and two tasks. The String `Unwrap` in TPC-DS q8 does not match this
Date-only totality argument and remains unsupported.

The preceding dynamic-`IN` extension accepts independently nullable lookup and
output columns only when their underlying types are the same fixed-width
integer and every binding reference is a direct positive top-level Filter
conjunct. In that context an inner NULL cannot make membership true, while both
SQL FALSE and UNKNOWN reject the outer row, so existential equality over
present non-NULL values is exact. `NOT`, `OR`, embedded binding references,
nullable `String`, coercions, and every other nullable type fail closed. This
moves TPC-DS q33 through formula construction after 1,551/1,158 ms of
preparation/verifier work. It is formula coverage only: q33 is not a bounded
proof or an optimizer-bug finding. At that checkpoint TPC-DS q58 and q83
remained at their dynamic-`IN` boundaries.

The subsequent Date extension accepts only an uncorrelated same-type Date
lookup/output pair. Nullability is independent, but if either side is nullable
every binding use must be a direct positive top-level Filter conjunct.
Membership is true exactly for a present non-NULL equal pair; Date values stay
inside the existing bounded domain. Coercions, nullable `String`, and nullable
non-positive Boolean contexts remain closed. Focused C++ checks and a
real-host nullable-Date `IN`-to-`left_semi` proof are green.

The subsequent closed-nesting slice admits only an uncorrelated scalar binding
inside an `IN` root and independently validates its owning plan root. Together
with exact symbolic producer-order Merge networks, it moves q58 through formula
construction with the focused evidence above. The next one-level closed-`IN`
and proven-present Date-cast tuple slices move q83 through those two boundaries
to the `Double` rejection recorded above.

### TPCH inventory

Optimizer preparation fails closed for q17 and q20 in correlated scalar
inlining because their computed aggregate results require general empty-row
reconstruction. Two other queries fail closed at a snapshot boundary as
follows; a query can have both an initial and final reason. q2 now passes both
snapshot boundaries and exact formula construction.

| Unsupported reason | Initial snapshot | Final snapshot |
|---|---|---|
| `Apply` | q13, q16 | - |
| `KqpOlapApply` | - | q13 |
| OLAP-filter `Coalesce` does not have the required false fallback | - | q16 |

Exact relational `EXISTS` moves q4 and q22 through formula construction. Their
focused formula-only rows spent 103/410 and 180/1,830 ms in
preparation/verification. The focused report SHA-256 is
`95e09e9db85590bc94bbecc15a8644da7603ad2ec5f8165bb39a87121586e0ed`.
Neither row is a solver proof.

At the equality-correlated milestone, q17 moved through formula construction.
The later correlated-COUNT correctness repair intentionally rejects that
broader computed empty-row shape before verification. The subsequent canonical
String-predicate bridge and Decimal `MIN` first moved q2 past both exporters to
the 32,640-pair Merge construction cap. The later exact sorting-network slice
moves q2 through formula construction. It remains outside the solver proof
floor.

The exact ordered two-dependency `EXISTS` slice moves q21 past its former
initial-export boundary and through complete formula construction. Its focused
solver row returns `VERIFIED_BOUNDED` after 168/1,901 ms of
preparation/verification at two rows per table and two tasks. This is a bounded
proof, not an unbounded SQL-equivalence claim or an optimizer-bug finding.

Exact uncorrelated dynamic `IN` moves q18 through formula construction and a
focused solver run returns `VERIFIED_BOUNDED` at two rows and two tasks. q16
passes the binding gate, then exposes initial `Apply` and a final pushed
OLAP-filter coalesce whose fallback is not the one supported false literal.

The exact nullable Date-year bridge removes the scalar-`Map` boundary from q7,
q8, and q9. Their complete dashboard rows emit formulas after 237/3,318,
278/2,954, and 187/1,628 ms of preparation/verifier work. Focused 60-second
solver runs return `UNKNOWN` after 230/64,641, 280/65,107, and 181/62,461 ms;
their first unresolved exact branches are respectively
`right_outcome_0_unmatched` (4/4), `left_outcome_1_unmatched` (4/28), and
`right_outcome_0_unmatched` (4/4). They are formula-covered, not proved, and
provide no optimizer-bug finding.

Exact Date literals and ordering removed the previous Date blockers and exposed
the deeper OLAP reasons above. Restricted static `IN` similarly
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
the complete dashboard at that milestone was 8/22 formulas after
6,944/11,582 ms.
A separate non-gating 60-second solver run returned `UNKNOWN` after
159/63,937 ms; this is neither a proof nor a counterexample. q7 and q8 now
reached their deeper generic `Map` exporter blocker at that historical
milestone; the later exact nullable Date-year bridge now moves them through
formula construction.

At the recorded static-proof milestone, exact uncorrelated scalar subplans
known to be at most one row moved q11 and q15 through formula construction. In
that complete dashboard they
spent 176/558 and 152/462 ms in preparation/verification. Their post-hardening
proof-floor rows return `VERIFIED_BOUNDED` after 158/6,585 and 199/2,750 ms.
The recorded complete TPCH dashboard is 10/22 formulas after 2,754/6,017 ms,
and both queries remain in the proof floor.

For the recorded staged plans, the then-current exporter validated task counts
before admitting physical properties. An `EnsureAtMostOne` proof could cross
into a producer stage only when that stage had exactly one inferred task. The
current implementation instead serializes and evaluates every marker exactly,
as described above.

### TPC-DS inventory

The 26 optimizer-preparation failures are q1, q12, q14, q17, q20, q23, q27,
q30, q32, q36, q39, q41, q44, q47, q49, q51, q53, q57, q63, q67, q70, q81,
q86, q89, q92, and q98. q1, q30, q32, q81, and q92 are the computed correlated
aggregate shapes rejected by the general empty-row reconstruction gate.
Eight failures--q12, q20, q49, q51, q53, q63, q89, and q98--still preserve
exact initial/final boundary results and therefore overlap the semantic
unsupported inventory. The other 18 are terminal no-pair preparation failures.

The exporter matrix below covers the recorded boundary failures among 15 of
the 21 unsupported TPC-DS queries after exact integral extrema. IDs can
appear in both exporter columns or under more than one reason because both
snapshots are audited independently. The six queries that pass export and
fail closed inside the verifier are listed after the matrix.

| Unsupported reason | Initial snapshot | Final snapshot |
|---|---|---|
| Scalar callable `Map` | q24 | q24 |
| Scalar callable `YqlAggWin` | q12, q20, q51, q53, q63, q89, q98 | q12, q20, q53, q63, q89, q98 |
| Scalar callable `YqlWin` | - | q49 |
| Exact Decimal `SafeCast` supports only same-scale widening | q49 | - |
| Read has range or ordering semantics | - | q51 |
| Ordinary count-distinct input is not exact non-null `Int64` | q28 | - |
| Scalar expression is not Data or Optional&lt;Data&gt; | - | q28 |
| Callable `Unwrap` | q8 | q8 |
| Restricted `Concat` exceeds its allocation-totality bound | q84 | q84 |
| Type `Double` | q22, q85 | q22, q85 |
| Dynamic Date fold requires `SafeCast` with `Optional<Date>` result | q72 | q72 |

q66 no longer appears in the matrix: its initial literal-only `Concat` is
canonical-folded under the exact Map-root gate, and its later Decimal
aggregates now retain sufficient certified finite headroom through
integral-factor multiplication and division. q84 remains because its two Olap
String occurrences exceed the allocation-totality bound.

The passive-carrier milestone removes q83 from both `Double` cells.
Integral-AVG Slice A subsequently removes q7, q13, and q26 and moves q35
through both exporters; exact integral extrema then move q35 through formula
construction. q22 and q85 remain derived-`Double` boundaries.

The exact point-range milestone removes q9 and q45 from the range row. q45 now
constructs a formula. q9 reaches the verifier and joins the construction-bound
inventory below.

After both snapshots export, q9 rejects an 8,192-row join output above the
4,096-row relation bound. q4 rejects a 20,736-pair join match above the
16,384-pair construction bound after 1,669/395 ms of
preparation/verification. q64 rejects an 8,192-row join output above the
4,096-row relation bound after 7,711/753 ms. q11 and q74 reach an
8,126,496-pair Sort construction preflight after 767/13,350 and 556/12,965 ms;
q31 reaches an 8,386,560-pair Sort preflight after 663/38,340 ms.
At the output-IU milestone q59 moved through both exporters before rejecting a
32,640-pair Sort construction above the same 16,384-pair cap. Exact integral
division likewise moved q78 through both exporters before rejecting a
52,326-pair Sort construction after 1,075/27,987 ms. The next exact-network
checkpoint rejected q59 and q78 at 640,512 and 253,440 comparator/column
pairs, above its then-current row-transport cap. The packed-row carrier now
moves both through formula construction. At the integral-division checkpoint,
q73 constructed a formula after 252/760 ms.

The exact ordered two-dependency `EXISTS` slice moves q16 and q94 through
formula construction. Focused solver rows return `VERIFIED_BOUNDED` after
260/3,923 and 237/2,887 ms of preparation/verification, respectively, at two
rows per table and two tasks. Their combined focused report SHA-256 is
`bc2b934bed75e48cb8ebeea112eb07ef282b6ecd1c331163ce99f927b3b0c848`.

Exact `DistinctAll` moved q6 through formula construction after 347/11,882 ms
in the retained 50-formula run. Its 32,055,251-byte canonical formula remains
`UNKNOWN` at the 60-second solver budget, so q6 is not part of the proof floor.

The exact representation milestone moves every other former verifier-side
construction blocker through formula construction: q5, q25, q29, q46, q68,
q77, q80, and q91 emit formulas after 1,588/2,653, 249/11,564, 263/4,142,
284/2,574, 276/2,301, 2,122/3,323, 1,810/42,847, and 227/3,754 ms of
preparation/verifier work, respectively. Formula emission invokes no solver and
is neither a proof nor a counterexample.

The subsequent exact Decimal AVG milestone moves q65 through formula
construction after 687/30,318 ms in the focused run. That complete TPC-DS
dashboard was 31/99 formulas, 39 unsupported queries, and 29 optimizer failures
after 68,255/249,242 ms of preparation/verifier work. The historical post-audit
relational-`EXISTS` rerun emitted 33/99 formulas and recorded 38 unsupported
queries and 28 optimizer failures after 54,643/176,453 ms. q10 and q69 were
the new relational-`EXISTS` formulas at that checkpoint.

At the later equality-correlated scalar milestone, the run emitted 38/99
formulas, with 33 unsupported and 28 optimizer failures. It added q1, q30, q32,
q81, and q92; q30 and q81 spent about 174,386 ms and 218,726 ms in formula
construction. q6 then failed at the deeper `DistinctAll` gate. None of those
formula-only results was a bounded proof. The subsequent COUNT repair and
`DistinctAll` rerun produced the then-current inventory, which later exact
semantic slices extended.
q9 now enters the verifier and fails at the 8,192-row join-output preflight;
q24 reaches `Unsupported scalar callable Map` at both boundaries. At the preceding
checkpoint q54 failed initial export because two Map outputs copied
`_yql_source_5.segment`; the exporter incorrectly treated the second use as an
invalid rename source. The exact duplicate-source projection fix retains both
copies under their distinct output names, and q54 now constructs its complete
formula.

The integral dynamic-`IN` gate first moved q95 through initial export. The later
side-explicit join-key and exact aggregate slice now carries it through formula
construction and bounded proof. At that checkpoint the other former shared-IU
cases stopped at their independent initial binding restrictions: q16/q94 had an
`EXISTS` binding without exactly one outer dependency, while q33/q56/q60
required a non-null fixed-width integral dynamic-`IN` result. q45, q58, and q83
likewise failed closed on nullable, `String`, or `Date` lookup/result identities
rather than introducing coercion or SQL-NULL semantics into that first slice.
The later exact String extension moves q56/q60 to formulas and q45 to final
range semantics; at that checkpoint q33/q58/q83 remained fail-closed. The
later nullable-integral positive-Filter gate moves q33 through formula
construction; q58 and q83 remained dynamic-`IN` blockers at that checkpoint.
The subsequent Date gate moves q58 to a nested-subplan blocker in the focused
run. The closed scalar-inside-`IN` gate and symbolic producer-order Merge
network now move q58 through formula construction. One-level closed
`IN`-inside-`IN` admission and proven-present Date-cast tuple items then clear
q83's two former boundaries; it now reaches `Double` in both snapshots.

The focused q10/q35/q69 report spent 513/11,387, 542/0, and 324/1,004 ms in
preparation/verification. q10 and q69 emit formulas; q35 clears the subplan
shape and fails both exporters on `Unsupported scalar type Double`. The report
SHA-256 is
`75e87970a3c8781a0db81a5123783da3159b7e87dcffa64bd4c0877f8aec914f`.
None of these rows is a new proof or counterexample.

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
tests passed 3/3 and the full exporter suite passed 147/147 at that
Decimal-AVG milestone.

The immediately preceding complete post-fix dashboards emit 16/22 TPCH and
43/99 TPC-DS formulas, or 59/121 (48.8%), with 34 unsupported and 28
optimizer-failure queries. Exact
`DistinctAll` adds TPC-DS q6; the correlated-COUNT correctness repair moves
TPCH q17 and TPC-DS q1/q30/q32/q81/q92 to intentional optimizer-side
fail-closed results. The preceding relational `EXISTS` milestone added TPCH
q4/q22 and TPC-DS q10/q69; TPCH q4/q22 and TPC-DS q69 belong to the required
proof floor. Exact uncorrelated dynamic `IN` adds TPCH q18 to formula
construction and the proof policy. The exact nullable Date-year bridge adds
TPCH q7, q8, and q9 to formula construction without changing that proof policy.
The side-explicit shared-IU and exact q95 aggregate slice adds TPC-DS q95 to
both policies. Exact String dynamic `IN` adds q56/q60 to formula construction
without changing the proof floor. Exact proven-total Date `Unwrap` adds q38
and q87 to formula construction and the proof policy. Exact nullable-integral
dynamic `IN` in a positive Filter context adds q33 to formula construction
without changing the proof floor. Exact weak nullable integral-to-Decimal
`SafeCast` and same-scale Decimal widening add TPC-DS q18 to formula
construction without changing the proof floor. The output-IU resolver adds
TPC-DS q2/q97 to formula construction and pins q59 at verifier entry without
changing the proof floor.
The exact ordered two-dependency `EXISTS` slice then adds TPCH q21 and TPC-DS
q16/q94 to both the formula and proof floors. Exact duplicate-source Map
projection then adds TPC-DS q54 to the formula floor without changing the proof
floor. Exact same-type integral division then adds TPC-DS q73 to the formula
floor and q78 to the verifier-entry floor; both join the preparation floor.
The exact sorting network then adds TPCH q2 to preparation and formula
construction without changing the proof floor. The packed-row network carrier
then adds TPC-DS q59/q78 to formula construction, also without changing the
proof floor. The closed scalar-inside-`IN` and symbolic producer-order Merge
slices then add TPC-DS q58. One-level closed `IN` nesting and exact
proven-present Date wrappers expose q83's `Double` carrier without changing
the floor. The restricted whole floating-predicate bridge and exact
proven-present wrapper normalization then add TPC-DS q21/q34/q75, while q34
also joins the proof floor. The preceding complete-dashboard floor is therefore
18/22 TPCH and 53/99 TPC-DS formulas, or 71/121 (58.7%); its complete semantic
partition contains 30 unsupported and 20 no-pair optimizer-failure queries.
Independently, preparation succeeds for 93 and fails for 28. Eight failed
preparations retain exact pairs and overlap the unsupported inventory. There
are 101 exact pairs and 76 verifier entrants, so the corresponding formula
ratios are 71/101 (70.3%), 71/93 (76.3%) within the preparation-successful
subset, and 71/76 (93.4%) at verifier entry.

The following passive-carrier milestone adds q83 formula construction. That
checkpoint confirmed 54/99 TPC-DS and 72/121 total formulas, with 77 verifier
entrants, while leaving the twenty-seven-query proof floor unchanged.

The subsequent q66 milestone confirmed 55/99 TPC-DS and 73/121 total formulas,
with 78 verifier entrants, while leaving the twenty-seven-query proof floor
unchanged. Its historical report hashes and timings are recorded above.

The subsequent exact point-range milestone confirms 56/99 TPC-DS and 74/121
total formulas, with 80 verifier entrants. q45 adds formula construction; q9
adds verifier entry and then fails closed on its join-output construction
bound. Cardinality-certified integral-AVG Slice A then adds TPC-DS q7/q13/q26:
59/99 TPC-DS and 77/121 total formulas. q35 also reaches the verifier, for 84
entrants total. Exact integral extrema then add q35: 60/99 TPC-DS and 78/121
total formulas, with 84 entrants. The
twenty-seven-query proof floor remains unchanged.

Focused q1 emits a formula after 111/998 ms and returns `UNKNOWN`, not
a proof or counterexample, in a non-gating 60-second solver run after
159/63,937 ms. Focused q65 emits a formula after 687/30,318 ms. The proof floor
contains twenty-seven confirmed `VERIFIED_BOUNDED` obligations.

## Curated proof floor and focused results

- The checked-in proof floor requires `VERIFIED_BOUNDED` for TPCH q3,
  q4, q6, q11, q12, q14, q15, q18, q19, q21, and q22 plus TPC-DS q3, q16,
  q34, q38, q42, q48, q52, q55, q69, q73, q87, q90, q93, q94, q95, and q96,
  each at two rows per referenced table and two tasks. These are twenty-seven
  bounded proofs, 27/121 (22.3%) of the workload,
  for the modeled pre-physical semantics, not unbounded SQL-equivalence claims.
  The current complete gates pass 11/11 TPCH after 1,308/62,684 ms and 16/16
  TPC-DS after 3,666/57,767 ms, all `VERIFIED_BOUNDED`. Their report SHA-256
  values are
  `0eed270ad0148908f05f59ad4e09f8710c280fca39871b5269b60ca1f707979e` and
  `e8b018abf0286bead86484b8a8739985554b70c823ef663abbeb938eb52a44b6`.
  Focused q34 is `VERIFIED_BOUNDED` after 263/2,471 ms with report SHA-256
  `44bdcd9f105d4f334b628bb672fa8d6b4f6ffd43ceece0666cd03829dfa5b677`.
  Focused q21 is `UNKNOWN` after 170/60,950 ms with report SHA-256
  `15ee95f2b59bc4ee41dddc19c8b98cdceb89bb3a2868678ab41539931c2c2a0e`;
  its former candidate was eliminated by exact wrapper normalization.
  Focused q75 remains `UNKNOWN` in retained 1,134/128,182 ms evidence with
  report SHA-256
  `1322068b8d57dfa984e91f6f775b063cc3c10e997e30a841e40c46f8d2058a9f`.
  The focused q73 proof spent 239/8,940 ms and produced report SHA-256
  `2c9dd4e765f4507bd952189055d67a0db5cf818ecb84abe188bfcdd8a15122e0`.
  The immediately preceding 26-obligation reports passed 11/11 TPCH after
  1,328/62,929 ms and 15/15 TPC-DS after 3,273/56,501 ms. Their SHA-256 values
  were
  `a4604150df2e3875a18401c9e44fbbce62cf843843b601828258dea7cd42134b` and
  `4c297e703cf16dfb9b1eddefeea87ec90f5cdb509a68529d517324ead8e8afbd`.
  The preceding 25-obligation reports passed 11/11 TPCH after
  1,445/67,977 ms and 14/14 TPC-DS after 3,328/49,834 ms. Their SHA-256 values
  were
  `c5668fdda1f40493fdcef4729118d634a63c25f44024106198cdd89775d9d4ed` and
  `489f58334593770ff80024bc50a055ed8d60712a34056839d837fbed29c7ff34`.
  The three preceding focused rows are all `VERIFIED_BOUNDED`: TPCH q21 spent
  168/1,901 ms of preparation/verification and produced report SHA-256
  `851b3a040d3aa1126d5b0256da95c8851984b977f8d7671db47a9f2d1a9eccef`;
  TPC-DS q16/q94 spent 260/3,923 and 237/2,887 ms and produced combined report
  SHA-256
  `bc2b934bed75e48cb8ebeea112eb07ef282b6ecd1c331163ce99f927b3b0c848`.
  They retain focused evidence for the three additions independently of the
  complete gate.
  The immediately preceding complete post-fix policy gate on source
  `4c2c1359e28` passed 10/10 TPCH and 12/12 TPC-DS, all `VERIFIED_BOUNDED`.
  Its isolated TPCH report spent
  1,171/70,555 ms and
  has SHA-256
  `95b250728e656081f7a0469035bef4cd3df289a7ec1f0ce08c17d9cf76698554`;
  TPC-DS spent 7,482/113,885 ms and has
  `a4b72350384d051958576505f5daf8e09106c59ec87104aa9ebebe1485ca4384`.
  TPCH q14 spent 85/37,202 ms in the isolated green run.
  At the immediately preceding q18 checkpoint, TPCH passed 10/10 after
  1,212/75,124 ms and produced
  `f90794bec99f5d739648c6f7fca81574ed52b8070257204d14f373edc0d38361`;
  TPC-DS passed 12/12 after 2,937/50,488 ms and produced
  `96e07f8139df89f7b2a0f216dd82ee0044afb592c10a7d43f3183275a796caa9`.
  Its complete verification subtree passed 34/34 suites and 934/934 tests.
  At the preceding q33 checkpoint, TPCH passed 10/10 after
  1,185/62,768 ms and produced
  `1b68432f4e269bd19ca6064338fd008439391a1b1ffc9fa3f511d96418c6a8c6`;
  TPC-DS passed 12/12 after 2,800/43,618 ms and produced
  `2b32e78f680ca78e59ca158ceaf35e46cc61623f9f5bfe33c0aa938a525ac5e0`.
  Its complete verification subtree passed 34/34 suites and 925/925 tests.
  At the preceding Date-`Unwrap` checkpoint, the same
  twenty-two-query proof floor spent 1,234/58,883 ms for TPCH and
  2,522/41,013 ms for TPC-DS and produced
  `db65dfe267b0b343f3cded64a32a028fab5561f4ad7b48a5803e0d3629c77f37` and
  `ea0aaa45b9cc8e7de40ad97ce23420bec926838acc8e17c4925edfad9e481751`;
  its q38 and q87 rows spent 333/1,115 and 324/1,052 ms in
  preparation/verification, and its complete subtree passed 919/919 tests.
  The preceding twenty-query gate
  spent 1,164/61,112 ms for TPCH and 2,063/40,374 ms for TPC-DS and produced
  `1971377b7fa14ab2b6823cdacb99a4d79a76ac4ceea9de46117d30df94a154f9` and
  `9a0d87075982d9ef4138b1d55b2265bd9ef461c237fec239c817e601da02bf7f`.
  Its q95 row spent 589/3,429 ms in preparation/verification. A preceding
  dedicated q95 run measured 512/3,013 ms. The preceding nineteen-query report
  SHA-256 values were
  `20540ba5eb16c0d239cd6ed5c9369d4372b774820c4b9033550b0343d577a5d1`
  and
  `62d7539a519ae370278b313d50e83b30a7d50d279cd12f6347b3b1e011163a95`,
  respectively. The preceding retained eighteen-query canonical-first
  exact-branch run spent 1,145/56,389 ms for TPCH and 1,446/36,036 ms for
  TPC-DS. Its historical SHA-256 values are
  `6d7329166c0cff497adcd86fd2d061bb409ca170c473b51529ed76ca8d80280c`
  and
  `136deef295abfe9c1fa8b4c7d8b01fe8e5131a76886ec998c0a90cbd8b778846`,
  respectively.

- The exact solver portfolio retains the grouped mismatch as its canonical
  artifact and gives that check a three-quarter SMT timeout. After `UNKNOWN`,
  it checks the exact distributive cover: the two language-absence predicates,
  then one guarded unmatched-result predicate per normalized source outcome
  in either direction. Canonical `UNSAT`, or `UNSAT` for every branch, proves
  the same theorem under one monotonic deadline. Branch-only solving lost the
  existing TPCH q15 proof, so preservation of every checked-in corpus
  obligation is part of the accepted portfolio evidence.

- A fresh portfolio run keeps TPC-DS q19, q65, and q99 `UNKNOWN` after
  207/61,602, 259/73,190, and 206/62,883 ms of preparation/verification. The
  first unresolved exact predicates are q19's left outcome 0 unmatched (branch
  3/28), q65's right language absent (branch 2/4), and q99's left outcome 0
  unmatched (branch 3/4). The retained report SHA-256 is
  `58cc491e30e2b866f36916f2b01db36e385f005ffe3b38685f250d95ccd10164`.
  These are localized proof bottlenecks, not proofs or counterexamples.

- Independent focused and repeat runs returned `VERIFIED_BOUNDED` for TPCH q4
  after 85/924 and 98/949 ms, TPCH q22 after 200/5,645 and 158/5,636 ms, and
  TPC-DS q69 after 374/3,781 and 359/3,758 ms of preparation/verification.

- TPCH q7, q8, and q9 now construct complete formulas through the exact
  nullable Date-year bridge. Focused 60-second solver runs return `UNKNOWN` at
  q7 branch 4/4 `right_outcome_0_unmatched` after 230/64,641 ms, q8 branch
  4/28 `left_outcome_1_unmatched` after 280/65,107 ms, and q9 branch 4/4
  `right_outcome_0_unmatched` after 181/62,461 ms. These three queries extend
  formula coverage only and did not change that checkpoint's proof floor.

- At the recorded static-proof milestone, exact uncorrelated scalar subplans
  known to be at most one row moved
  TPCH q11 and q15 through both snapshot boundaries. Their final
  `EnsureAtMostOne` checks cross serial UnionAll boundaries whose aggregate
  producer stages have exactly one task, so the then-current task-aware
  structural proof established that the physical checks were inert. The
  current `*AtMostOneMarker*` matrix instead passes 3/3 by requiring marker
  serialization directly, across multi-task producers, and across single-task
  producers; the focused exporter suite passes 180/180. A mutation that changes
  the scalar aggregate input from `a.x` to `a.k` returns `COUNTEREXAMPLE` with
  a concrete bounded witness, exercising the subplan proof path rather than
  only decoder/exporter acceptance.

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
  tests passed 4/4, the complete `cpp_ut` run passed 144/144 at that Date-cast
  milestone, and a q5-shaped actual-host integration passed 1/1 with
  `VERIFIED_BOUNDED`. Regenerated full
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
  preparation/verification, respectively. None changed the then-nineteen-query
  proof floor; the later q95 promotion is independent of these results.

- A fresh TPC-DS sweep returned `UNKNOWN` for q10 after 524/81,517 ms, q19
  after 219/61,811 ms, q65 after 283/80,633 ms, and q99 after 218/63,299 ms of
  preparation/verification. These formulas remain outside the proof floor.

- TPCH q12's complete-dashboard row at that milestone spent 104/502 ms on
  preparation/verification; an earlier focused formula-only run spent
  108/5,816 ms. The focused and then-current policy-backed solver runs returned
  `VERIFIED_BOUNDED` after 108/38,880 and 103/1,739 ms, respectively. Neither
  proof produced a candidate, so replay was not invoked and no optimizer
  correctness bug was found.

- TPCH q19 newly reaches formula construction through exact scoped unary
  `IfPresent` and restricted static-membership lowering. Its focused
  formula-only run spent 117 ms preparing and 290 ms in verification. A focused
  solver run spent 116 ms preparing and 851 ms in verification before returning
  `VERIFIED_BOUNDED`; the checked-in floor now retains that two-row/two-task
  obligation.

- Restricted `Substring` moves TPC-DS q15, q19, q62, q79, and q99 through
  formula construction. In the complete dashboard at that milestone they spent
  respectively 136/745, 207/1,428, 234/2,097, 249/2,003, and 216/1,978 ms in
  preparation/verification. Focused q15 and q62 solver experiments return
  `UNKNOWN` at the 60-second budget; q79 is classified separately below. None
  of these five is added to the proof floor.

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
  spent 194 ms preparing the query and 4,051 ms in verification before returning
  `VERIFIED_BOUNDED`; the checked-in floor retains that proof obligation.

- TPC-DS q90 reaches the verifier after exact non-null integral `SafeCast` to
  Decimal support. Its two `Uint64` count expressions become explicit
  `cast_decimal` nodes targeting `Decimal(15,4)`, including the runtime's exact
  scale multiplication and signed-infinity saturation. The proof-floor run
  spent 280 ms preparing the query and 8,201 ms in verification before returning
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
  Focused exporter tests passed 4/4, the complete `cpp_ut` run passed 144/144
  at that Date-cast milestone, and the q5-shaped actual-host pushed-filter
  obligation passed 1/1 with
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

- Literal-only String `Concat` is handled by a separate canonical fold, not by
  the stored-value opaque bridge. It admits only a binary tree at a Map-body
  root whose nodes and leaves are exact non-null String expressions with
  reviewed safety metadata. The fold enforces explicit node, depth, and
  allocation budgets and exports one ordinary canonical String literal.
  Optional leaves, nonliteral leaves, invalid literal payloads, unsafe
  metadata, wrong arity, and over-budget trees fail closed.
  The same q66 checkpoint extends certified finite Decimal headroom only across
  Decimal-by-integral multiplication and Decimal-by-integral division. A bound
  `B` is multiplied by the complete integral type's maximum absolute value and
  capped at the result Decimal's largest finite coefficient for `*`; `/`
  preserves `B`. Decimal-by-Decimal and unknown-bound forms stay unknown.
  Together these gates move q66 through formula construction without admitting
  generic expression rewriting or relaxing Decimal aggregate overflow checks.

- Decimal aggregate `max` is exact only when its input and output have the same
  canonical Decimal type and phase-aware nullability. It ignores NULL and uses
  MiniKQL `AggrMax`'s raw signed-code order, so NaN is greater than positive
  infinity; the same scalar state combines associatively across intermediate
  and final phases. Exhaustive special-value, grouped/global, all-NULL, split
  task, wrong-shuffle, and fail-closed type tests cover the contract. At that
  milestone focused q74 passed MAX and then rejected 65,536 join-matching pairs
  above the 16,384 construction cap after 463 ms of preparation and 375 ms of
  verifier work. It emitted no formula, so the then-current formula slice and
  proof floor were unchanged. Current q74 reaches the deeper 8,126,496-pair
  Sort construction blocker listed above.

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
  verdict. The current full formula-only dashboard records 361/1,551 ms of
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
  The complete dashboard at that milestone recorded 437 ms of preparation and
  3,846 ms of verification/formula construction; a focused run recorded 391 ms
  and 14,169 ms. A focused 60-second solver experiment recorded 419 ms and
  88,305 ms before returning `UNKNOWN`. q76 is formula-covered, but is not a
  bounded proof or evidence of an optimizer bug.

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

The corrected-model q56/q60 candidates are the first solver findings in this
audit to lead to a confirmed optimizer correctness bug through paired real-YDB
execution. They share the single ninth root cause documented below. At the
preceding output-IU checkpoint, the proof policy contained TPCH q3, q4, q6,
q11, q12, q14, q15, q18, q19, and q22 plus
TPC-DS q3, q38, q42, q48, q52, q55, q69, q87, q90, q93, q95, and q96. That
preceding complete expanded run confirmed all twenty-two as
`VERIFIED_BOUNDED`. The current policy additionally contains TPCH q21 and
TPC-DS q16/q34/q73/q94; the current complete gate confirms all twenty-seven.
TPC-DS
q5, q10, q19, q21, q25, q29, q46, q54, q56, q60, q65, q68, q75, q77, q80,
q91, and q99
construct formulas but return `UNKNOWN` in their latest solver runs. TPCH q1,
q7, q8, and q9 also construct formulas and are `UNKNOWN` in their focused
60-second runs.
The former q6/q14, q5, q79, and q88 candidates were verifier-modeling false
positives. q77's historical candidate is not rediscovered by the exact
Date-cast model, but its regenerated and corrected fixed-witness results are
both `UNKNOWN`; it is neither a current candidate nor a confirmed false
positive. No corrected-model witness currently awaits replay: q56/q60 were
confirmed, localized, repaired, and invalidated against the corrected
snapshots.

The subplan coverage milestone began with a corrected source inventory and a
final-boundary visibility prerequisite. The then-catalog-blocked seven TPCH and
thirteen TPC-DS queries contain 32 subqueries: fifteen scalar, seventeen
`EXISTS`, twenty-five correlated, and seven uncorrelated. No dynamic `IN`
subplan occurs in this slice. Only TPCH q11/q15 and TPC-DS q24/q54 are fully
uncorrelated.

Catalog capture validates the ordered subplan registry and includes tables
reachable only from subplan roots. The exact initial-boundary slice now records
each scalar binding's root, selected output, type/nullability, empty dependency
list, and explicit Project/Filter consumers. At that milestone it admitted
only uncorrelated plans statically known to produce at most one row:
`EmptySource`, eligible ungrouped aggregation, a literal `Limit <= 1`, and
Project/Filter/Sort wrappers. Zero rows produced typed NULL, one row produced
the selected value, and every other shape or nondeterministic direct outcome
failed closed.

At that static-proof milestone, this moved q11/q15 from the TPCH catalog
blocker through formula construction and into the checked-in proof floor. It
did not move TPC-DS: q24 reached `Unsupported scalar callable Map` at both
boundaries, and q54 had a non-statically-single-row initial scalar binding plus
a final `Limit` with physical properties outside logical snapshot v1.

After that historical dashboard, commits `b2cd6e3c5bb` and `f930f1352e7`
introduced general uncorrelated scalar subplans with an explicit query-error
outcome for more than one row: zero rows yield typed NULL, one yields the value,
and more than one generates a cardinality-error term. Commit `1aaf281c07a`
gives enumerated latent-sequence alternatives a stable scoped decision, keeping
one scalar choice correlated across consumers. The fresh complete dashboards
above incorporate this implementation.

Only the current binding's newly generated more-than-one-row error is gated by
the presence of a row in its immediate Project/Filter consumer. Once that row
exists, even a dead scalar-expression branch demands the binding. An error
inherited from evaluating the subplan root remains observable; an empty
enclosing consumer does not gate it a second time. Intrinsic errors already
raised inside that root are eager too. Model commit `125962c87df` keeps
inherited `Outcome.error` separate from the binding-local
`cardinality_error`. Production commit `9e50d234264` correctly gates one
binding's direct cardinality check. CBO could then commute the order-sensitive
synthetic Cross, letting physical Cross drain an empty right outer input before
the inherited scalar error. Commit `cab0dd1e89c` marks both synthetic Crosses
`PreserveInputOrder`, makes BuildInitial/Expand CBO respect the barriers while
optimizing both sides, and prevents filter absorption through them.
Relational `EXISTS` now admits no dependency and two deliberately narrow
correlated forms. The original correlated form has exactly one outer
dependency and one strict, non-null-safe direct equality with one inner
column. The new form has exactly two ordered, distinct outer dependencies.
Each occurs in a separate predicate conjunct: exactly one strict direct
equality and exactly one strict direct inequality compare them with distinct
direct inner columns. Corresponding outer and inner base types match exactly,
although nullability may differ, and every residual conjunct is inner-only.

The correlated source is limited to plain column-projection Maps above one
Filter directly over `AddDependencies`. The snapshot retains the complete
predicate; source `!=` is encoded canonically in JSON as `not(eq)`, not as an
independent inequality node. Python independently validates that normalized
shape, preserves dependency order, and binds both dependencies from the same
outer row. The evaluator computes, per outer row, the OR of present inner rows
for which SQL filter truth holds. A strict comparison involving NULL does not
match, duplicate matches collapse, and `NOT EXISTS` remains consumer negation.

The C++ exporter validates the exact `AddDependencies` output schema, order,
and types before serialization. Python does not reconstruct that optimizer
operator; it independently validates the serialized dependency order,
predicate, types, and consumer contract. Both sides fail closed on observable
`EnsureAtMostOne`, nested or staged bindings, consumer or type mismatches, more
than two dependencies, malformed two-dependency correlations, and correlated
Limit, TopSort, or scan `pushed_limit` row selection. A same-name `Void`
carrier may be dropped from the unselected side of a one-sided witness join
only when the selected side retains the same `Void`; an unmatched dropped
`Void` and every join key that inspects `Void` fail closed. Evaluation caps the
outer/inner product at 16,384 pairs. The final side is still the ordinary
StageGraph; no special equivalence path is introduced.

At the original one-dependency milestone, focused gates passed 11/11 in Python,
4/4 in C++, and 4/4 through the real host; full validation passed 472/472
verifier, 177/177 C++, 45/45 inspector, 37/37 replay, and 29/29 integration
tests. At the two-dependency milestone, its focused gates passed 17/17 in
Python, 6/6 in C++, and 1/1 for its new real-host case; the complete verifier,
exporter, and inspector suites passed 527/527, 203/203, and 46/46. The
real-host targeted
two-dependency obligation returns `VERIFIED_BOUNDED`. Independent solver
differentials prove the exact form equivalent to `left_semi` and its negation
equivalent to `left_anti`; omitting the second correlation produces a
counterexample. The focused workload obligations prove TPCH q21 and TPC-DS
q16/q94 at the declared row/task bounds. None is an unbounded theorem or a new
optimizer-bug finding.

Dynamic `IN` now has its own typed relational descriptor rather than being
treated as generic `EXISTS`. It names exactly one lookup column from the one
Filter consumer and one result column from the inner root. Non-null columns
may have the same fixed-width integral, exact `String`, or Date type. Same-type
fixed-width integral or Date lookup and result columns may instead be
independently nullable only when every binding reference is a direct positive
top-level Filter conjunct. The binding remains non-null `Bool`, virtual, and
uncorrelated. `OuterBind`, `AddDependencies`, observable `EnsureAtMostOne`,
staging, fanout, tuple mappings, coercions, nullable `String`, `Utf8`, Bool,
Decimal, other nullable types, and mismatched types fail closed. Its root may
reference closed uncorrelated scalar bindings and closed leaf `IN` bindings.
Every consumer operator must belong to exactly one plan root. Each nested `IN`
consumes no subplan binding; structural root nesting, a correlated scalar,
cycles or depth greater than one, and every other nested owner/kind fail
closed.

The evaluator computes one Boolean membership value per present outer row as
the OR of present, non-NULL equal pairs. Duplicate inner rows collapse and
empty input is false. For non-null columns, `NOT` in the consumer expresses
anti-membership. For nullable integral or Date columns, the admitted positive
Filter is true exactly when that OR is true; SQL FALSE and UNKNOWN both reject
the outer row, while `NOT`, `OR`, and embedded uses fail closed. A repeated
binding reference reuses the same cached subplan family. Errors inherited while
evaluating the inner root are eager, including when no outer row is present.
The outer/inner product is charged to one cumulative shared 16,384-pair cap.
A nested scalar uses the ordinary cached zero/one/many-row semantics. Its new
cardinality error is demanded by its immediate consumer inside the `IN` root;
an inherited error remains eager. A nested `IN` recursively evaluates the same
membership construction, shares the cache, and charges both levels to the
cumulative pair cap.
Date uses the same equality construction under the exact bounded Date domain;
the nullable truth restriction is identical to the integral case.
Focused tests cover duplicates, empty input, `NOT`, independent integral
nullability, NULL truth, rejected Boolean contexts, cache reuse, left-semi and
left-anti reference equivalence, inherited errors, mapping mutations, every
descriptor gate, and the exact cap. Real-host integral, String, and positive
nullable-integral and nullable-Date fixtures prove initial dynamic `IN`
equivalent to the final ordinary `left_semi` StageGraph at two rows and two
tasks. Exhaustive
finite-domain String tests cover duplicates, empty inputs, `NOT`, row presence,
and reference equality. TPC-DS q58 exercises three admitted scalar-inside-`IN`
pairs and now constructs a formula. TPC-DS q83 exercises the admitted
one-level `IN` nesting and reaches its later `Double` boundary.

Equality-correlated scalar aggregation admits one dependency and one Project
or Filter consumer. The no-fanout root path is
`Project* -> Aggregate -> Project* -> Filter -> outer_bind`, with exactly one
ungrouped, phase-`undefined`, non-`DistinctAll` Aggregate. The explicit typed
`outer_bind` appends one outer value to the closed inner input for each
invocation. Exactly one Filter conjunct is a strict non-null-safe direct
outer-column/inner-column equality; residual conjuncts are inner-only.

Each present outer row evaluates the full scalar root once: zero rows yield
typed NULL, one yields the selected value, and more than one raises the scalar
cardinality error. Invocation errors are gated by the row's presence, and
repeated expression references share its binding value. Limit, Sort, scan
`pushed_limit`, ordered `UnionAll`, `EnsureAtMostOne`, nested or staged
bindings, and per-invocation choice families fail closed. All invocations reuse
one validated plan context and share one cumulative 16,384-pair
outer/closed-inner construction budget. The final side remains the ordinary
StageGraph. A real-host Decimal-AVG left-join case returns
`VERIFIED_BOUNDED`.

The auditability consolidation and exact solver portfolio are complete.
Equality-correlated scalar aggregation and exact `DistinctAll` are implemented.
Exact uncorrelated dynamic `IN` and the exact nullable Date-year bridge raise
the formula floor to 50 queries after the correlated-COUNT repair's intentional
fail-closed reclassification. Side-explicit one-sided join keys, exact direct
Uint64 `Just`, scalar final Uint64 SUM unwrap, and direct scalar
`COUNT(DISTINCT Int64)` then raise the floor to 51. Exact String dynamic `IN`
raises the next floor to 53. Exact proven-total Date `Unwrap` raises the next
floor to 55, positive nullable-integral dynamic `IN` raises the next floor to
56, and exact weak nullable integral-to-Decimal `SafeCast` plus same-scale
Decimal widening raises the next floor to 57. Exact pushed-OLAP
physical/full/short output-IU resolution raises the next floor to 59. Exact
ordered two-dependency equality/inequality `EXISTS` raises the next floor to
62, duplicate-source Map projection raises it to 63, and exact same-type
integral division raises the next floor to 64. The bounded exact sorting
network raises the next floor to 65 through TPCH q2, and the packed-row carrier
raises the next floor to 67 through TPC-DS q59/q78. Exact nullable-Date dynamic
`IN`, closed scalar-inside-`IN` consumption, and symbolic producer-order Merge
then move q58 through formula construction and raise that floor to 68.
One-level closed `IN` nesting and exact proven-present literal Date casts in
raw static-`SqlIn` tuples then move q83 to `Double` without changing that
floor. The restricted whole floating-predicate bridge and exact proven-present
literal-wrapper normalization then add TPC-DS q21/q34/q75, raising the
then-current complete-dashboard floor to 71; q34 also raises the proof floor to
twenty-seven. The following exact passive-carrier slice adds q83 to the focused
formula policy and complete dashboard, raising the floor to 72 without adding
a proof. The subsequent literal-only String-`Concat` fold and exact
integral-factor Decimal-bound propagation add q66, raising the complete formula
floor to 73 without adding a proof. The exact point and finite point-set
read-range matcher then adds q45 to the formula floor and q9 to verifier entry,
raising the formula floor to 74 and the entrant floor to 80 without adding a
proof. Cardinality-certified integral-AVG Slice A then adds q7/q13/q26,
raising the formula floor to 77; q35 raises the entrant floor to 84 without
adding a proof. Exact fixed-width integral `MIN`/`MAX` then adds q35 to the
formula floor, raising it to 78 without adding a proof. Narrowly tagged
derived-`Double` ordering for q22/q85 is next. More than two dependencies, other
correlation shapes, coercing dynamic `IN`, nullable String and non-positive
nullable contexts, broader range grammars, and other OLAP pushdowns remain
later work. Solver/formula-size
work promotes supported queries only after reproducible
`VERIFIED_BOUNDED` results. The required policy now contains twenty-seven
obligations, and the current complete gates confirm all 27/27.

### Confirmed subplan optimizer defects

The audit and solver/real-YDB confirmation workflow produced nine production
optimizer findings:

- With a nonempty inner table, `WHERE NOT flag AND EXISTS (subquery)` returned
  no rows under new RBO but returned the expected row under the legacy
  optimizer. The simple-subplan rule retained `negated=true` from the unrelated
  first conjunct and lowered the later positive `EXISTS` as `NOT EXISTS`.
  Commit `95a2afad1d3` resets the flag for each candidate conjunct and retains
  the focused real-YDB regression.
- A two-row uncorrelated scalar subquery formerly returned its first row under
  new RBO instead of the required `PRECONDITION_FAILED` error. Commit
  `e1e3419012c` consumes the scalar-cardinality contract and retains real-YDB
  regressions for the error and valid zero/one-row cases.
- `TOpMap::GetSubplanIUs()` passed the source and destination of `AddUnique` in
  reverse, losing scalar binding metadata. Commit `52a1d7c4084` corrects the
  binding discovery path.
- `ConvertTKqpOpMap` did not call `RemoveSubplans` on a projection lambda, so a
  direct scalar projection retained the inner query plan inside a scalar Map
  expression instead of replacing it with the registered binding. The same
  commit repairs the projection path.
- The repaired path exposed an invalid `Nothing<Int64>` empty branch for a
  nonaggregate YQL scalar projection. It failed type annotation instead of
  returning optional NULL. Commit `52a1d7c4084` aligns the virtual binding,
  empty branch, and present branch types and retains aggregate, singleton,
  computed, zero-row, and multirow real-YDB regressions.
- A multirow scalar raised `PRECONDITION_FAILED` under new RBO even when its
  outer consumer produced no rows; legacy execution correctly returned an
  empty result. The direct scalar-cardinality check ran eagerly in an
  independent producer. Commit `9e50d234264` bounds the scalar input, gates
  that check with one outer row, and safely renames colliding outer/scalar IUs.
  At that production-fix checkpoint, `ScalarSubplanEvaluationTest` passed
  14/14, and `KqpRboYql::ExpressionSubquery` passed 1/1 with the empty-consumer
  and same-IU regressions plus the existing scalar-cardinality cases. The
  prerequisite shared-input fix is separate in `a51c2459ad5`; its two direct
  Limit-pushdown tests pass 2/2 for shared Read and Sort inputs.

The seventh finding is both a verifier-model correction and a confirmed
optimizer defect. In a reliable warmed paired real-host probe,
`nested_empty_outer.sql` gives its inner scalar a nonempty immediate consumer
and more than one row while leaving the top-level consumer empty. Legacy
execution raises `PRECONDITION_FAILED` with “More than one row in a scalar
subquery”. Before the fix, two warmed default-CBO new-RBO runs instead
deterministically exited successfully with an empty result JSON beginning
`{"columns":[{"name":"value",...}]}`. Commits `125962c87df` and
`1aaf281c07a` correct the model and shared choices, exposing the mismatch.

CBO had commuted the order-sensitive synthetic Cross, and physical Cross drains
its right input first; the empty outer input could therefore suppress the
inherited scalar error. Optimizer commit `cab0dd1e89c` installs the
`PreserveInputOrder` barriers described above. At that checkpoint its direct
rules passed 2/2, the real-host regression passed 1/1, full `cpp_ut` passed
165/165, and the affected Python gates passed 507/507. Defect seven is fixed.

The eighth finding is a live equality-correlated scalar `COUNT(*)` defect.
The initial keyless Aggregate returns the required non-NULL zero on empty
input. Correlation pull-up adds a key, turning it into grouped aggregation;
for an unmatched outer key it emits no row, and scalar inlining's left join
then exposes NULL rather than restoring COUNT's zero identity. The real-host
finding is preserved separately in commit `605dca7e9f0`, including the exact
snapshots and a row-bound-two, task-bound-two `COUNTEREXAMPLE` with an
unmatched outer row. It deliberately did not pin solver-selected cell values.

The fix records whether correlation pull-up changed an originally keyless
Aggregate, traces a unique exact Member path to the selected direct COUNT
trait, and restores `Just(Coalesce(joined_count, Uint64(0)))` after the left
join. The exporter gives only that generated shape exact existing
`if`/`if_present` semantics. The preserved finding now returns
`VERIFIED_BOUNDED`; runtime cases cover projection, Filter consumption, and
the negative originally grouped COUNT case. Computed post-aggregate empty-row
expressions remain a general reconstruction extension and fail closed in new
RBO; legacy fallback is not treated as a semantic repair for that broader
class. The focused fix did not itself revise workload numbers. The later
complete dashboards show the intended consequence: TPCH q17 and TPC-DS q1,
q30, q32, q81, and q92 now stop at this optimizer gate, reducing the formula
floor while leaving the eighteen-query proof floor unchanged.

The ninth finding is the shared-IU String-`IN` result loss exposed by both q56
and q60. They had one production root cause in `TPushFilterIntoJoinRule`: IU
membership alone could classify an equality as a cross-input key even when one
endpoint name appeared on both `LeftSemi` inputs. The rule then consumed a
predicate belonging on the selected left input as an additional semi-join key.
With CBO explicitly disabled, the paired embedded real-YDB finding returned
`("same", 10)` under legacy optimization and zero rows under new RBO. Commit
`6a2c3acb29b` preserves that pre-fix diagnostic.

Commit `98176b0b48c` requires each extracted key endpoint to belong exclusively
to its declared side, leaving ambiguous shared-IU equalities to the existing
side-routing logic, and retains a direct rule regression. Commit
`4f73b38aaaf` adds the nonmanual production runtime regression. After the fix,
both old witnesses return `WITNESS_NOT_REPRODUCED`; focused q56/q60 solver runs
return `UNKNOWN` at 60 seconds, with their SHA-bound timings and report digest
recorded above. The fix changes neither the 53/121, 53/93, and 53/59 formula
counts nor the 20/121 proof floor.

An additional legacy probe with intrinsic
`Ensure(foo.id, false, "inner scalar error")` in the scalar producer also
raised `PRECONDITION_FAILED` despite the empty top-level consumer, confirming
the same eager contract beyond nested cardinality checks.

The focused inherited-error/empty-consumer model regression preserves the
corrected distinction.

When this inventory changes, retain the old report as a test artifact, inspect
every newly supported, unsupported, failed, or solver-changed query, and update
this document only after distinguishing exporter/model changes from optimizer
changes.
