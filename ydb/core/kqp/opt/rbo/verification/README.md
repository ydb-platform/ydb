# New RBO equivalence verifier

This directory contains the standalone bounded-equivalence checker described in
[PLAN.md](PLAN.md). It compares two versioned semantic snapshots and asks Z3 for
a bounded input database on which their result bags or ordered sequences differ.
The reproducible 121-query dashboard contract and current unsupported inventory
are recorded in [BENCHMARK_COVERAGE.md](BENCHMARK_COVERAGE.md). The current
proof-producing trust boundary, assumptions, and slice-by-slice review
procedure, together with the latest audited size baseline, are indexed in
[TRUSTED_CORE.md](TRUSTED_CORE.md).

The current implementation contains the M1 logical kernel, the M2 C++ boundary
hooks, the supported M3 StageGraph routing slice, and the aggregate, Limit,
ordered Sort/TopSort/Merge including bounded exact bitonic-network
representations with fixed or symbolic producer-order preservation, pushed
OLAP-filter including exact presence tests, exact audited point and finite
point-set pushed read ranges,
restricted static `IN`, exact `Exists`/`If`/unary `IfPresent`, exact all-pairs
ordinary integral comparison, exact String/Utf8 comparison and ordering, exact
same-type fixed-width integral division, exact partial integral `SafeCast`,
exact direct String/Utf8-literal `SafeCast` to
optional Decimal or Date, exact reviewed `Coalesce(..., false)` forms over
either a direct comparison, a binary same-member String
membership/complement predicate, or the reviewed canonical String predicates,
exact direct Decimal
`Coalesce(member, zero)`, exact reviewed Decimal `Just` forms, and exact Decimal
semantics for comparison, weak integral-to-Decimal casts, same-scale
nondecreasing-precision Decimal widening, arithmetic, ordering, `SUM`, and
Decimal `MIN`/`MAX` and phase-aware Decimal `AVG`, exact same-output-type
fixed-width signed/unsigned integral `MIN`/`MAX`, narrowly certified ordering of
completed integral-`AVG` `Optional<Double>` results, exact ordered logical `UnionAll`,
explicit query-error outcomes, exact physical `EnsureAtMostOne`, and general
uncorrelated scalar subplans with consumer-demanded local cardinality errors
and eager inherited errors, including the exact closed scalar-inside-`IN`
and one-level closed `IN`-inside-`IN` nesting slices, exact
one-equality-correlated scalar aggregate subplans, plus
uncorrelated, one-equality-correlated, and exact ordered
two-dependency equality/inequality relational `EXISTS`,
exact uncorrelated single-column dynamic `IN`, including independently nullable
same-type fixed-width integers or Date only as a direct positive top-level
Filter conjunct,
the exact nullable
`Date -> Timestamp -> DateTime2.Split -> DateTime2.GetYear` projection shape,
the exact proven-total Date `Unwrap(Coalesce(member, zero))` shape,
exact row-level `DistinctAll` aggregation, side-explicit join keys including
the narrow shared-IU one-sided join slice, exact direct non-null Uint64
`Just`, exact scalar-final Uint64 sum unwrap, and exact direct scalar
`COUNT(DISTINCT Int64)`, plus the benchmark-dashboard parts of M4. The reviewed
exact wrapper forms
retain their Optional schema through existing `IfPresent`/`If` IR instead of
being erased. This includes only `Just(Date literal)` and
`Just(Convert(integer literal))` when the conversion is complete and both the
descriptor and type annotations prove the exact target value; nearby wrapper
shapes remain opaque or unsupported. The exporter also admits a restricted
floating-point bridge for reviewed whole predicates over `Optional<Int64>`:
the exact operators and constants are `>= 2/3`, `<= 3/2`, `> 1.2`, and
`< 0.9`, optionally inside the reviewed `Coalesce(..., false)` envelope.
Constants are fingerprinted by exact IEEE bits and the complete predicate is
kept as a typed opaque function. A separate passive-carrier slice admits only
the four q83 result expressions: three reviewed deviation forms and one
reviewed average form, each producing `Optional<Double>` from exactly three
distinct direct `Optional<Int64>` columns. They are exported as
`opaque_double` with an exact `yql-passive-double-v1` fingerprint and modeled
as one nullable deterministic uninterpreted function over the ordered
arguments. No floating arithmetic or ordering semantics enter the Python
kernel for this passive carrier. `Double` remains forbidden in base tables,
subplans, predicates, comparisons, join keys, aggregate keys/inputs/results,
and every scalar consumer; except for the separately tagged integral-AVG rank
contract below, it also remains forbidden in sort or routing keys. The passive
derived value may only travel through
relational operators and StageGraph as a direct, uninspected payload column.
q83's concrete downstream path is Project, non-key Sort, Limit, and Merge.
The inspector renders `opaque_double` explicitly. Every other `Double` shape
and broader floating arithmetic fail closed. The exporter also exactly folds
the reviewed constant String/Utf8-to-Date plus-or-minus
`DateTime2.IntervalFromDays` shape, erases the corresponding direct Date-literal
OLAP `just` wrapper, exactly folds direct numeric Date/Interval literal
arithmetic, exactly folds the reviewed constant
`DateTime2.Split`/calendar-shift/`MakeDate` shape, and admits the reviewed
catalog-bounded stored-String `Concat` shape.
A disjoint literal-only `Concat` gate folds an audited non-null String tree at
a Map-body root to the ordinary canonical literal node. Exact integral-right
Decimal multiplication and division also propagate conservative finite
coefficient bounds into later Decimal aggregates without changing their value
semantics.
The read-range slice accepts only a column-store StageGraph source with no
read ordering, one catalog non-null `Int64` physical primary key, and that
physical key emitted exactly once. `RangeInfo::ComputeNode` is authoritative;
`OriginalPredicate` is intentionally ignored because runtime range extraction
consumes `ComputeNode`. `KeyColumns` is descriptive evidence and must resolve
to the same emitted physical-key IU. The closed grammar covers q9's one-point
`RangeFinalize(RangeMultiply(10000, RangeUnion(RangeFor(...))))` form and
q45's static finite-point `IfPresent`/`FlatMap`/`Collect`/`Take` form. It
checks exact operators, descriptors, binders, tuple indices, pointer sharing,
the 10,000/10,001 extractor caps, full-range overflow fallback, and
`ExpectedMaxRanges`. Generated range nodes may lack annotations before the
physical annotation pass; every annotation that is present must agree with
the audited syntax. The result is ordinary exact equality or static-`IN`
predicate IR and is conjoined with an independently pushed OLAP filter.
Duplicate and adjacent points retain exact membership semantics. Every other
range shape fails closed. The closed matcher is isolated in
`read_range_predicate_impl.h`, included exactly once inside
`semantic_snapshot.cpp`'s anonymous namespace so it can reuse the existing
scalar safety, catalog, column-resolution, and JSON helpers without creating a
second exporter API or duplicating trusted helpers.
The cardinality-certified integral-`AVG` slice accepts one strict aggregate
contract: `Optional<Int64> -> Optional<Double>`, with the exact
`integral_double_v1` state descriptor on undefined/intermediate and directly
linked final phases. Intermediate rows carry a ghost `(count,min,max)` summary
of the original non-NULL `Int64` inputs. One script-global uninterpreted
function maps that summary to the completed value; it is exact only when the
non-NULL count is at most two, because then `(count,min,max)` uniquely
identifies the unordered input multiset. Completed aggregates instead expose a
node-local `IntegralAverageCertificate`. The verifier observes that certificate
at the aggregate producer, builds a model-domain exclusion for a reachable
successful non-NULL result with count greater than two, and centrally removes
it before caching the family or returning it to any parent projection, sort,
limit, compaction, or StageGraph route. Intermediate
`IntegralAverageState` remains transportable; a completed certificate never
is.

The disjoint `integral_avg_rank_v1` ordering contract admits only a completed
integral-AVG `Optional<Double>` column whose provenance is independently
derived from the aggregate producer and preserved through exact direct
Map/Project aliases and pass-through operators. Join propagation is restricted
to retained payloads, and `UnionAll` requires every positional branch to carry
the same certification. Sort and StageGraph Merge must carry the explicit tag;
untagged or forged `Double`, intermediate AVG state, and every other binary64
use still fail closed. The existing shared `(count,min,max) -> Int` carrier is
used as an abstract rank. Under the mandatory count-at-most-two domain, the
runtime binary64 AVG equivalence classes have an integer ranking, while the
unconstrained carrier also admits extra collisions or orderings. This is a
sound over-approximation for `UNSAT` and can only make proof harder.

Integral extrema admit `Int8/16/32/64` and `Uint8/16/32/64` only with an exact
same-type result and phase-aware nullability. A guarded, balanced,
sentinel-free reduction preserves exact NULL, group, and split-state behavior;
the existing Decimal path is unchanged. SMT occurrence traversal, dependency
level assignment, and term output use explicit stacks, so deep exact
obligations do not depend on Python recursion depth. The new renderer preserved
the old canonical bytes on 3,000 randomized shared and quantified DAGs.

Solver execution must first prove the model-domain exclusion `UNSAT`. `SAT` or
`UNKNOWN` there makes the result `UNKNOWN`; the semantic mismatch is not
queried. Inside the certified count-at-most-two region, a semantic `SAT` is
also reported as `UNKNOWN`, because distinct summaries can round to the same
binary64 result and the uninterpreted carrier can therefore admit a spurious
inequality. Exact binary64 replay would be required to classify such a
candidate. Only semantic `UNSAT` produces `VERIFIED_BOUNDED`. The raw emitted
formula is the disjunction of semantic mismatch and model-domain exclusion,
so solving that standalone formula to `SAT` is not a counterexample.

Separate normalized-plan, concrete-counterexample inspection, and isolated
real-YDB replay tools are also implemented. A real-host transformation-prefix capture
command and sequential localizer are implemented outside the verifier kernel.
Version-five benchmark reports keep query preparation and semantic
classification as independent axes. A failed `SyncPrepareDataQuery` does not
discard an already captured exact Initial/Final boundary-result pair: export
and verification continue, while `prepare_status` and `prepare_reason` retain
the later failure. The reports SHA-bind the exact assembled query and each
preserved snapshot, unsupported boundary diagnostic, and byte-exact raw
verifier verdict. A separate all-candidates confirmation driver pins each saved
solver database from that raw verdict before inspection and replay.
Committed rule applications and mutating non-rule stages share one explicit
transformation-event stream. Solver-backed tests use the pinned, standalone Z3
target under `contrib/tools/z3`; it is not linked into `ydbd`.
The checked-in policy currently requires formula construction for
TPCH q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11, q12, q14,
q15, q18, q19, q21, and q22 plus TPC-DS q2, q3, q5, q6, q7, q10, q13, q15,
q16, q18, q19, q21, q22, q25, q26, q29, q33, q34,
q35, q37, q38, q40, q42, q43, q45, q46, q48, q50, q52, q54, q55, q56, q58, q59,
q60, q61, q62, q65, q66, q68, q69, q71, q73, q75, q76, q77, q78, q79, q80,
q82, q83, q85, q87, q88, q90, q91, q93, q94, q95, q96, q97, and q99: 80/121
workload queries (66.1%).
The complete derived-ordering dashboards leave TPCH at eighteen
formulas, two unsupported semantic outcomes, and two no-pair optimizer
failures; TPC-DS has sixty-two formulas, nineteen unsupported semantic
outcomes, and eighteen no-pair optimizer failures. Across both suites the
semantic partition is 80 formulas, 21 `UNSUPPORTED`, and 20
`OPTIMIZER_FAILURE`.
Preparation is a separate partition: twenty TPCH and seventy-three TPC-DS
queries succeed, while two TPCH and twenty-six TPC-DS queries fail. Eight
TPC-DS rows belong to both the preparation-failure and semantic-unsupported
inventories, so those inventories must not be added as disjoint workload
counts.

Integral-`AVG` Slice A moves TPC-DS q7, q13, and q26 from initial
export rejection to formula construction; exact integral extrema then move
q35 from verifier rejection to formula construction. Certified derived
integral-AVG ordering then moves TPC-DS q22 and q85 through both exporters and
formula construction. The resulting measured formula coverage is 80/121
(66.1%) over the corpus, 80/101 (79.2%) over exact Initial/Final
boundary-result pairs, 80/93 (86.0%) within the preparation-successful subset,
and 80/86 (93.0%) among verifier entrants. The
preparation-success ratio uses
the intersection of formula rows with preparation-success rows; version five
permits a formula to coexist with failed later preparation. Twenty TPCH and
eighty-one TPC-DS queries have exact boundary-result pairs. Eighteen TPCH and
sixty-eight TPC-DS pairs enter the verifier. The 21 unsupported outcomes
consequently split into 15 initial-export, zero final-export, and six verifier
results.

A focused version-five run selected TPC-DS q12, q20, q49, q51, q53, q63, q89,
and q98. Every query produced an exact Initial/Final boundary-result pair and
later failed preparation; every semantic outcome was `UNSUPPORTED`, with no
verifier entry or formula. Seven initial exports reject `YqlAggWin`; q49 first
rejects a Decimal `SafeCast` scale change. The independently audited final
boundaries reject `YqlAggWin` for q12/q20/q53/q63/q89/q98, `YqlWin` for q49,
and Read range/ordering semantics for q51. The focused run spent 3,186/0 ms in
preparation/verifier work and produced report SHA-256
`37b983f3247c653f5bf4a52c79375cdbc7df588ac79bd893d3a5a89ae25e16e0`.

The preceding q66 complete TPCH dashboard spent 2,927/30,624 ms in
preparation/verifier work and produced report SHA-256
`97c0048b4bc31c8c02785bc3dea18c676b9ba6e2452411912c8984f06b376205`.
TPC-DS spent 63,931/643,722 ms and produced
`60a7c324365ab1f038d636db53acc387b4d3ae5e35a157122309de966e8adf6f`.
Those timings and hashes are historical q66 evidence, not measurements of the
read-range checkpoint.

The preceding read-range complete dashboards spent 2,895/30,992 ms for TPCH and
64,296/706,547 ms for TPC-DS in preparation/verifier work. Their report
SHA-256 values are
`9a6c562fc3c8ef7d9d56dacf2411f1c87cc35d966e7ac538e4a947add9dded56` and
`1eff186049cceb773f6710ce29504bde5065b098d9d7aec1d692bf05f8f5fbec`.
Within TPC-DS, q9 spent 8,951/5,293 ms and q45 spent 511/14,367 ms.

Focused production-host captures validate both admitted range shapes. TPC-DS
q45 constructs a formula; its separate solver run is `UNKNOWN`, so this adds
no bounded proof or counterexample. TPC-DS q9 now passes both exporters and
enters the verifier, where it fails closed because its join output requires
8,192 candidate rows above the 4,096-row construction audit bound. The proof
floor remains twenty-seven. Validation passes 232/232 C++ exporter tests,
593/593 Python verifier tests, and 14/14 coverage-policy tests.

Focused row-bound-two/task-bound-two formula-only evidence for integral `AVG`
records `FORMULA_EMITTED` for TPC-DS q7, q13, and q26 after 194/1,181,
247/1,830, and 204/1,122 ms of preparation/verifier work. The combined report
SHA-256 is
`721507f60df911e5906865fb26710ed98772338b5aa74afc93532dad63881853`.
Separate 60-second solver runs are all `UNKNOWN`: q7 at branch 4/28, q13 at
branch 4/4, and q26 at branch 4/28. These are focused results, not full-suite
dashboard evidence, bounded proofs, counterexamples, or optimizer bugs. The
policy pins all three queries at preparation plus formula construction.

The completed implementation is recorded by commits `8d3e44f59a6` and
`abe190f6344` for the semantic slice and policy, respectively. The Slice A
suites passed 608/608 Python verifier, 237/237 C++ exporter, 47/47 inspector,
and 14/14 coverage-policy tests. The complete TPCH formula dashboard spends
3,273/37,511 ms in preparation/verifier work and has report SHA-256
`f7430b2bc2e0dc3779b939831afa163d7fa7b45a7c12eeadae761117f3517b8f`;
TPC-DS spends 76,727/851,301 ms and has report SHA-256
`c37f457d0335a8b94ee10d48a5e15bffb86d6ec671050fba4538297e89688867`.
Its q7/q13/q26 rows spend 210/1,258, 279/2,049, and 224/1,361 ms. The proof
floor remains twenty-seven, and Slice A found no optimizer bug.

The integral-extrema checkpoint is recorded by commits `b6c8e8863bb`,
`cb50a1ee896`, `7785d8dd23c`, `90a7abd2334`, and `a39863e5b33`. Before the
slice, q35 was `UNSUPPORTED` at `n16.aggregates[2]` for `max(Int64)` after
598/265 ms; report SHA-256 is
`829ff76b7d3fb9849db3a13b86bac9a604bca84eaa7f64c939517560822d50b1`.
The first exact semantics run exposed a verifier-renderer `RecursionError`, not
an optimizer bug; its preserved report SHA-256 is
`d19f0e233fad50d4b6be279eaaa8fc9fdac2d48a01fb23f79ba7a33cc30cd7e1`.
After the stack-safe repair, focused q35 is `FORMULA_EMITTED` after
542/120,515 ms, report SHA-256
`b312b43d1ba4d20aeeb615c2fe75b54b8baeed87cfdb54bea85aa4a0e9ccc9b5`.
A separate 60-second run is `UNKNOWN` after 614/199,928 ms because it cannot
exclude integral-AVG count greater than two; report SHA-256 is
`164398b163725598b676c231349a19c30f161fdb012dc61f951934c89676f2e4`.

The integral-extrema suites pass 615/615 Python verifier, 237/237 C++
exporter, 47/47 inspector, and 14/14 coverage-policy tests. The complete TPCH
dashboard spends 3,207/36,148 ms and has report SHA-256
`499e0098afda7bed5198b2cb4cc2dfe35ca81e24252aa15c8e7b1803f26e2b3f`;
TPC-DS spends 70,746/858,347 ms and has report SHA-256
`8b194da2b89d4da4dbd9fd088bf8cc07e5224239e1b656322e3cfa43198d662a`.
q35 is `FORMULA_EMITTED` there after 565/121,012 ms. The proof floor remains
twenty-seven, and this checkpoint found no optimizer bug or counterexample.

The derived integral-AVG ordering checkpoint is recorded by implementation
commit `e8abaff7ff4` and policy commit `3e91814d64e`. Validation passes 619/619
Python verifier, 242/242 C++ exporter, 50/50 inspector, and 14/14
coverage-policy tests. Focused formula-only TPC-DS q22 and q85 runs emit after
324/1,704 and 347/8,346 ms of preparation/verifier work; their report SHA-256
is `0a7612f430d9dbff68d60afdcd79cf3a7cf97170d54a5287e315be9270ba954e`.
A separate 60-second run leaves both `UNKNOWN`: q22 spends 348/61,611 ms before
the global deadline at branch 2/4 (`right_language_empty`), while q85 spends
315/71,360 ms and cannot rule out an integral-AVG count greater than two. Its
report SHA-256 is
`6fbe29825c3e2863ad8c3a7d92ea661bd655e7245d455fcbb1db207dcd1e258c`.

The current complete TPCH dashboard has 18 formulas / 2 unsupported / 2
no-pair outcomes, spends 2,947/33,310 ms, and has report SHA-256
`8a231a04398f6ca176286bd9d4d658e7d836c36c34ddcc4d43dfde54cc413a4b`.
TPC-DS has 62 formulas / 19 unsupported / 18 no-pair outcomes, spends
68,923/846,363 ms, and has report SHA-256
`64fbda391ca5b50698aceaa2a38ba2210617fd0c1c0071bcb7c5c7967b260ecd`.
This is formula coverage only: the proof floor remains twenty-seven, and the
checkpoint found no optimizer bug or counterexample.

The retained q83-milestone complete TPC-DS dashboard records q83 as `FORMULA_EMITTED` after
1,338/6,175 ms of preparation/verifier work. The separate hardened focused run
returns `FORMULA_EMITTED` after 1,301/6,081 ms at two rows per table and two
tasks; its report SHA-256 is
`04e5df3a8f55044002fdf9b231d75b707bf58fd51c8b60a4a8879d4d623b9a5b`.
The canonical formula is 10,953,698 bytes with SHA-256
`5228c142eef65eb7707ff039c58e6cfc85f599286a9ec2ccf480b5fd94903db6`.
A separate 60-second solver run returns `UNKNOWN` after 1,313/66,340 ms because
the global deadline expires before branch 2/4 (`right_language_empty`); its
report SHA-256 is
`5571045865cbd30d7b2a35e61c379bdb7e3e24b63bfd1df2be8f514454487572`.
q83 raises the measured formula policy from 71 to 72, but it is not a bounded
proof, adds nothing to the twenty-seven-query proof floor, and reveals no
optimizer correctness bug.
Validation passes 588/588 Python verifier tests, 225/225 C++ exporter tests,
46/46 inspector tests, and 14/14 coverage-policy tests.

Focused solver evidence separates formula construction from proof. TPC-DS q34
is `VERIFIED_BOUNDED` after 263/2,471 ms and has report SHA-256
`44bdcd9f105d4f334b628bb672fa8d6b4f6ffd43ceece0666cd03829dfa5b677`.
TPC-DS q21 is `UNKNOWN` after 170/60,950 ms, with report SHA-256
`15ee95f2b59bc4ee41dddc19c8b98cdceb89bb3a2868678ab41539931c2c2a0e`;
the exact-wrapper work eliminated its earlier spurious candidate. TPC-DS q75
remains `UNKNOWN` in the retained 1,134/128,182 ms focused evidence, report
SHA-256
`1322068b8d57dfa984e91f6f775b063cc3c10e997e30a841e40c46f8d2058a9f`.
These outcomes add all three queries to the formula floor but only q34 to the
proof floor.

At the preceding integral-division milestone, TPC-DS q73 emitted a formula
after 252/760 ms of preparation/verifier work in the complete run. q78 passed
both exporters but failed closed after
1,075/27,987 ms at a 52,326-pair Sort construction above the 16,384-pair
audit cap. At that milestone, the checked-in policy pinned q73 at preparation
plus formula construction and bounded proof, and q78 at preparation plus
verifier entry.
That milestone passed 537/537 Python verifier tests, 207/207 C++ exporter
tests, and 14/14 coverage-policy tests. A focused solver differential passed
1/1: the unchanged division pair was verified, while reversing the operands
produced a bounded counterexample. The focused real-host q73 proof is
`VERIFIED_BOUNDED` after 239/8,940 ms and has report SHA-256
`2c9dd4e765f4507bd952189055d67a0db5cf818ecb84abe188bfcdd8a15122e0`.

The exact sorting-network slice adds TPCH q2 to the preparation and formula
floors without adding a bounded proof. A focused two-row/two-task run constructs
the problem in 11.469 seconds, renders in 15.931 seconds, and emits a
62,274,331-byte formula. Its two local 128-row networks cost 37,632
comparator/column pairs each and its 200-row Merge costs 96,768, beneath the
131,072 row-transport cap. Finite distinct tie ranks order only exact SQL-key
ties; present rows dominate absent rows; compare-exchange moves each complete
row, including Decimal AVG state; and concrete producer-order rank chains
denote exactly the legal Merge interleavings. The resulting proven
present-prefix invariant lets ordered Limit slice compact output slots.

The subsequent exact packed-row carrier makes those large networks auditable
without weakening their semantics. A one-constructor datatype carries row
presence, NULL/value lanes, and hidden Decimal AVG state through each
compare-exchange. One quantifier-free `define-fun` contains the complete SQL
key ordering for a network, while each comparator selects two whole output
payloads and their two finite tie ranks. Free symbols, foreign declarations,
malformed AVG state, and unsupported lane sorts fail closed. The retained caps are 32,768
comparators, 131,072 logical payload cells, and 64 key columns.

Focused q59 and q78 runs now return `FORMULA_EMITTED`, not bounded proofs. q59
constructs and renders a 116,879,360-byte formula in 44.66 seconds with
1,707,844 KiB peak RSS; its SHA-256 is
`3a140fcb1b5d6a5145c4aa30cbcd817167a27f21bed94d85ef969223dce73c8e`.
q78 emits 202,469,546 bytes in 57.06 seconds with 2,006,884 KiB peak RSS; its
SHA-256 is
`fb0eaebb95ea9bdfb3b0f815f5078a70d1c2e3765ed5d6675be1c4f06b8249c4`.
The policy therefore moves both queries from entry-only coverage into the
formula floor while leaving the twenty-six-query proof floor unchanged.
In the complete TPC-DS dashboard q59 and q78 spend 828/74,462 and
1,042/113,216 ms in preparation/verifier work. At that packed-row milestone,
validation passed 564/564 verifier tests and 14/14 coverage-policy tests; the
unchanged C++ exporter had passed 207/207 tests at the preceding
sorting-network milestone.

The next exact slice permits an expression-level uncorrelated scalar binding
inside an `IN` subplan root. Structural subplan-root nesting, correlated
scalars, and every other nested owner/kind remain unsupported. Each consumer
operator must belong to exactly one main or subplan root. The existing scalar
zero/one/many-row semantics, local cardinality-error demand, eager inherited
errors, shared cache, and cumulative membership-pair budget apply unchanged.
TPC-DS q58 contains three such scalar-inside-`IN` pairs. Its later Merge also
needs symbolic producer order: the network now preserves each producer with
one presence-guarded direction constraint per unordered row pair, while fixed
producer orders retain their adjacent rank chains.

In the complete TPC-DS dashboard q58 spends 3,285/109,215 ms in
preparation/verifier work. Separately, the focused q58 dashboard returns
`FORMULA_EMITTED` after 3,291/103,862 ms and has report SHA-256
`87c0a2e7d51b077c19c7b261fd899f00dc590106385764574eaa7e46aac50b94`.
The retained direct emission takes 101.43 seconds, peaks at 2,294,048 KiB RSS,
and contains eight datatype/comparator-definition pairs in a 324,938,538-byte
formula with SHA-256
`22f51f5d1a82091a35d29b6ac120344725f1272b8093ae9a0f1c3fa6fc6eaa70`.
This adds q58 to the preparation and formula floors, raising the latter to 68
without adding a bounded proof or optimizer bug finding. At that checkpoint
validation passed 568/568 verifier tests, 208/208 C++ exporter tests, and
14/14 coverage-policy tests.

The detailed planning estimate in [BENCHMARK_COVERAGE.md](BENCHMARK_COVERAGE.md)
groups the remaining formula gap by shared semantic feature rather than query
count. Integral-AVG Slice A removes q7/q13/q26, exact integral extrema remove
q35, and certified derived-AVG ordering removes q22/q85 from the generic
`Double` boundary.
The restricted whole-predicate bridge moved q21, q34, and q75 through formula
construction, and the passive-carrier milestone moved q83. q73 emits, and q78
emits through the packed-row network carrier. The
factorized-construction cluster has five remaining primary blockers after q2,
q59, and q78 emit.
Including exact window semantics for the newly visible failed-preparation
pairs gives roughly
six to eight families and eight to sixteen milestones for the complete
captured-pair gap; the remaining twenty workload entries need frontend or
optimizer progress before verifier feature work can reach them.
Those milestone ranges are planning estimates, not coverage promises, and
assume deliberately workload-targeted gates.

The new correlated form has exactly two ordered, distinct outer dependencies.
Each dependency occurs in its own predicate conjunct: exactly one conjunct is a
strict direct equality and exactly one is a strict direct inequality against
distinct direct inner columns. Remaining conjuncts are inner-only. Each
outer/inner pair must have the same base type, but its nullability may differ;
ordinary strict comparison semantics make a NULL operand fail Filter truth.
The C++ exporter normalizes source `!=` to JSON `not(eq)`, and the Python
decoder admits that exact normalized shape rather than a separate inequality
node. The ordered dependency list is semantic and all dependencies are bound
from the same outer row for one invocation.

The C++ exporter validates the exact `AddDependencies` output schema, order,
and types; Python independently validates the serialized ordered dependencies
and predicate contract. Correlated Limit, TopSort, and scan `pushed_limit` fail
closed because their row selection would otherwise need a fresh decision per
outer invocation.
A same-name `Void` carrier may disappear from the unselected input of a
one-sided witness join only when an identically named `Void` remains on the
selected side; an unmatched dropped `Void` and every `Void` join key remain
unsupported. This narrow rule preserves the canonical `COUNT(*)` carrier used
by TPCH q21 without generally permitting `Void` dataflow to disappear.

At the two-dependency milestone, focused tests passed 17/17 for Python
`EXISTS`, 527/527 for the complete Python verifier, 6/6 for C++ `EXISTS`,
203/203 for the complete C++ exporter, and 46/46 for the inspector. The
real-host two-dependency fixture passed 1/1 and
returned `VERIFIED_BOUNDED`. Independent solver differentials prove the exact
form equivalent to `left_semi` and its negation equivalent to `left_anti`;
omitting the second correlation produces a counterexample. These are bounded
results at the declared row/task limits, not unbounded SQL-equivalence theorems.

At the output-IU milestone, that slice mirrored the pre-physical OLAP source
builder: each physical read name, full output-IU name, and short output-IU name
resolved to the read's logical output column. Multiple spellings for that same
output were aliases, while a referenced spelling that denoted distinct outputs
failed closed; an ambiguity that the predicate never referenced was irrelevant
and accepted. TPC-DS q2 and q97 emitted formulas. q59 passed both exporters and
entered the verifier, then failed closed at a 32,640-pair Sort construction
above the 16,384-pair audit cap. That checkpoint's complete-dashboard
preparation/verifier times were 3,874/34,168 ms, 993/1,218 ms, and 595/895 ms
for q2, q59, and q97,
respectively. These are formula/entry coverage results only: they add no
bounded proof and expose no optimizer correctness bug.

The preceding exact Decimal-cast gate emits a `cast_decimal` node only for weak
`SafeCast`, with a mandatory canonical `source_type` that the Python decoder
independently checks against the inferred type of the serialized argument.
The C++ exporter requires the result descriptor and its annotations to be one
canonical `Decimal(p,s)` with at least one integral digit; the Python decoder
independently validates the serialized result type and source-matching
nullability. For an exact integer source, a present value is scaled by `10^s`
and finite overflow saturates to the corresponding signed infinity; source
NULL alone produces result NULL. For a canonical Decimal source, only
same-scale, nondecreasing-precision widening is admitted: it preserves the raw
finite, signed-infinity, or NaN code exactly, while propagating NULL.
`StrictCast`, `Convert` outside the existing complete-literal normalization,
an absent or mismatched `source_type`, scale changes, precision narrowing,
other source types, malformed or mismatched targets, and nullability changes
all fail closed.

This moves TPC-DS q18 through formula construction after 1,002/51,090 ms of
preparation/verifier work. The separate real-host regression proves both
nullable families at two rows and two tasks, but that synthetic result is not
a proof of the full benchmark query. TPC-DS q18 is formula-only and produced no
optimizer-bug finding.

The dynamic-`IN` gate accepts independently nullable lookup and result columns
when their underlying types are the same fixed-width integer or Date and the
binding appears only as a direct positive top-level Filter conjunct. In that
context, existential equality over present non-NULL values is exact: SQL FALSE
and UNKNOWN both reject the outer row. `NOT`, `OR`, embedded nullable binding
references, nullable `String`, coercions, and other nullable types fail closed.
The earlier integral step moved TPC-DS q33 through formula construction after
1,551/1,158 ms of preparation/verifier work; it is formula coverage only.

The Date step keeps values in the existing bounded Date domain. Focused C++
checks and the real-host nullable-Date `IN`-to-`left_semi` obligation are
green. The subsequent closed-nesting slice accepts only an uncorrelated scalar
binding consumed from an `IN` root and independently validates its owning plan
root. Together with exact symbolic producer-order Merge networks, it moves
TPC-DS q58 through formula construction with the focused evidence reported
above. The next one-level extension admits closed leaf `IN` bindings inside an
`IN` root; each leaf consumes no binding, so cycles and deeper chains remain
unsupported. A separate raw static-tuple gate folds only proven-present
literal-to-Date `SafeCast` items. Together they move q83 past both former
boundaries to `Double` in both snapshots, without constructing a formula or
changing the proof policy.

The preceding exact Date-`Unwrap` gate admits only a non-null Date
`Unwrap` of a binary Optional-Date `Coalesce` whose first argument is one
direct visible nullable Date member and whose fallback is known-present Date
zero. The initial snapshot spells that fallback as
`SafeCast(Int32(0), Optional<Date>)`; the final snapshot spells it as
`Just(Date(0))`. Both normalize to the existing non-null Date `if_present`
form, preserving the member when present and Date zero when absent. This moves
TPC-DS q38 and q87 through formula construction and into the bounded proof
floor. Other `Unwrap` semantics remain closed; in particular, TPC-DS q8's
String `Unwrap` is still unsupported.

The earlier String extension admits dynamic `IN` only when the lookup and result are
the same non-null `String` type, reusing the exact existential-equality
semantics already audited for fixed-width integers. It adds TPC-DS q56 and q60
to formula construction. q45 also clears its initial String-`IN` boundary and
now stops only at final Read range/ordering semantics. `Utf8`, nullable
`String`, coercing comparisons, Bool, Date, Decimal, and mismatched types still
fail closed.

Pre-fix non-gating solver experiments returned bounded counterexample
candidates for q56 after 1,260/2,356 ms and q60 after 1,260/2,072 ms of
preparation/verification. Fixed-witness inspection reproduced both symbolic
mismatches. A paired embedded real-YDB diagnostic with CBO explicitly disabled
then confirmed that both queries exposed one RBO defect: legacy execution
returned `("same", 10)`, while new RBO returned zero rows. The finding is
preserved in commit `6a2c3acb29b`.

After the exclusive-IU-ownership repair in `98176b0b48c`, both old witnesses
return `WITNESS_NOT_REPRODUCED`, and focused q56/q60 solver runs return
`UNKNOWN` at the 60-second limit. On source `4f73b38aaaf`, q56 spent
1,286/61,302 ms and q60 spent 1,224/61,274 ms in preparation/verification; the
focused report SHA-256 is
`1da4256d6b306933aa54cabc99fce262f12bcac69b1dd64c9dfd599fad7b6caa`.
That source also retains the nonmanual production runtime regression. Both
queries remained formula-covered rather than proved at that checkpoint:
formula coverage was 53/121, 53/93 optimizer-successful queries, and 53/59
verifier entrants, while the proof floor was 20/121.

The q95 bridge is a composition of four narrow exact contracts rather than a
query-specific equivalence shortcut. Join keys are serialized as ordered
side-explicit `(left, right)` descriptors, so an IU name shared by both inputs
cannot collapse a left/right equality. Shared names are admitted only for
`left_semi`, `left_anti`, `right_semi`, or `right_anti`, with no JoinFilters
and a literal-true residual; the output contains only the selected side.
StageGraph child occurrences remain separate streams, and the existing
occurrence provenance and routing guards prevent either accidental stream
aliasing or unsound copy deduplication.

The exporter also normalizes only
`Just(direct non-null Uint64 member) -> Optional<Uint64>` to an explicit
always-present `if`, preserving its raw Optional schema. A keyless, final,
non-distinct `sum(Optional<Uint64>)` with `unwrap` and a raw
`Optional<Uint64>` output receives the physical coalesce contract: its
effective result schema is non-null and empty or all-NULL input returns zero.
Finally, one direct keyless phase-`undefined`
`count(distinct non-null Int64) -> non-null Uint64` trait is exact; duplicates
are suppressed by first present equal representative, with its
`N*(N-1)/2` equality checks charged to the normal relation-pair ceiling before
construction. Every nearby type, phase, grouping, second distinct trait,
unwrap, or residual shape still fails closed.

Together these contracts model q95 before and after its multi-distinct rewrite.
Its focused formula is 288,499 bytes and 1,269 lines. A preceding dedicated
two-row, two-task proof-floor run returned `VERIFIED_BOUNDED` after
512/3,013 ms of preparation/verification. At that q95 milestone, the enforced
floor was 10/10 TPCH and 10/10 TPC-DS, including q95: 20/121 workload queries
(16.5%). The complete verification-subtree gate at that milestone passed
34/34 suites and 914/914 tests.

`DistinctAll` moves TPC-DS q6 through formula construction. The earlier
correlated-COUNT correctness repair intentionally moves TPCH q17 and TPC-DS
q1, q30, q32, q81, and q92 from formula construction to an optimizer-side
fail-closed result: those computed correlated aggregate shapes require general
empty-row reconstruction, which is not yet implemented safely. This is a
reduction in the formula floor, but not a loss of an established proof; none of
those six formulas belonged to the solver proof floor.

At the preceding focused post-dashboard checkpoint, generic
`EndsWith`/`StringContains` and their pushed OLAP spellings received one
reviewed canonical opaque identity, and same-type Decimal `MIN` was modeled
exactly. TPCH q2 passed both exporters and
Decimal `MIN`, then failed closed at the verifier's 32,640-pair Merge
construction, above the 16,384-pair cap. TPCH q9 clears the String-predicate
bridge but at that checkpoint still reached unsupported scalar `Map` in both
snapshots. A small
real-host column-store fixture containing both String predicates is
`VERIFIED_BOUNDED`. Those focused results left formula coverage at 46/121,
before the subsequent dynamic-`IN` and Date-year slices.

The initial exact dynamic-`IN` slice raised that floor by one. Its descriptor
records one typed lookup column and one typed subplan-result column. Both must
have the same non-null fixed-width integral type; the binding itself must be
non-null `Bool` and
uncorrelated, virtual, and consumed by exactly one Filter, with no `OuterBind`,
`AddDependencies`, or observable `EnsureAtMostOne`. Evaluation is existential
membership: an outer row matches when any present inner row has an equal
value. Duplicates collapse, an empty inner input is false, and `NOT IN` remains
ordinary scalar negation. Repeated uses share one cached subplan family,
inherited root errors remain eager, and one cumulative construction budget
admits at most 16,384 outer/inner membership pairs across alternatives and
nested evaluation.

TPCH q18 now emits a complete formula and a focused two-row/two-task solver
experiment returned `VERIFIED_BOUNDED` after about 155/3,035 ms of
preparation/verification. At that dynamic-`IN` checkpoint, TPCH q16 and TPC-DS
q95 passed the new binding gate but stopped at later unrelated blockers. q16 is
now classified at an outer-dependency `EXISTS`; q95's later boundary is closed
by the exact bridge above. At that checkpoint, dynamic-`IN` bindings with
nullable, `String`, or `Date` lookup/result columns failed closed. A real-host
fixture captures uncorrelated `IN` in the initial plan and an ordinary
`left_semi` join in the final plan, then proves the normal bounded obligation.
That integration target links production PostgreSQL support because the dummy
provider failed preparation for this valid query shape.

The exact nullable Date-year slice admits only an
`Optional<Uint16>` `Map` over a complete `SafeCast` from one direct visible
`Optional<Date>` member to `Optional<Timestamp>`. Its unary non-null
`Timestamp` lambda must be exactly the reviewed
`DateTime2.GetYear(DateTime2.Split(argument))` chain, including UDF names,
callable and cached descriptors, argument flags, settings, annotations, and
lambda identity. Every nearby shape fails closed.

The exporter preserves source NULL with `if_present`, represents the reviewed
non-null year operation as the stable typed opaque function
`yql-datetime-year-v1` over the bound Date value, and lifts that value back to
`Optional<Uint16>` with an explicit typed-NULL branch. This is deliberately a
shared deterministic-total function, not a calendar reimplementation: it can
prove that the same reviewed operation and argument were preserved without
inventing relationships to other calendar expressions.

This moves TPCH q7, q8, and q9 through formula construction. Their complete
dashboard rows spent 237/3,318, 278/2,954, and 187/1,628 ms respectively in
preparation/verifier work. Separate focused 60-second solver experiments all
returned `UNKNOWN`: q7 after 230/64,641 ms at branch 4/4
`right_outcome_0_unmatched`, q8 after 280/65,107 ms at branch 4/28
`left_outcome_1_unmatched`, and q9 after 181/62,461 ms at branch 4/4
`right_outcome_0_unmatched`. At that Date-year checkpoint they extended the
formula floor, not its then unchanged nineteen-query proof floor. Focused C++
mutations exercise the material
Date-year-specific type, UDF, setting, flag, and binder gates, while shared
exporter tests retain the generic UDF-envelope and scalar-safety checks. Python
tests check exact NULL propagation and prove that the opaque fingerprint and
argument are semantic. A real-host nullable-Date projection captures both
optimizer snapshots and is `VERIFIED_BOUNDED`. The fresh complete suites pass
183/183 C++ tests, 493/493 Python verifier tests, 46/46 inspector tests, and
32/32 real-host integration tests.

The immediately preceding complete policy-checked TPCH formula dashboard, from
source `4c2c1359e28` before the two-dependency `EXISTS` extension, spent
7,401/76,064 ms in preparation/verifier work and produced report SHA-256
`92f8508dcc9eb47e49a4ecbd9ec3577f2ab84aaa254404f555f9f4b60207342a`;
TPC-DS spent 123,828/490,758 ms and produced
`e7eef8b14247a35a3c1eb822d15d87eb6c80151064aa89164d58bcaba568f405`.
Both complete policy runs were green at that checkpoint.
The immediately preceding 57-formula checkpoint was generated from source
`5dafcc79a4e`. Its TPCH dashboard spent 2,872/28,853 ms and produced
`c0eadbb10b2b1f394d604bb5cc5097d9fac26646e6d3aa97b90e2ff47b0712d2`;
TPC-DS spent 63,947/243,682 ms and produced
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
The preceding 53-formula TPCH/TPC-DS reports spent 2,801/30,006 and
68,622/200,340 ms and produced
`deb388eec49e32242cd66bfbe943ef2f73a692d95d96150fbfb68f8281390753` and
`9d808615985c7c6fce4bc76cfb7b1c92e68e82ecb02294e23921f7dad809af2d`.
In those reports, TPC-DS q6, q56, q60, and q95 emitted after 344/11,938,
1,653/1,082, 1,464/1,092, and 474/454 ms, respectively. Earlier milestone
reports and their timings/hashes remain historical records in
[BENCHMARK_COVERAGE.md](BENCHMARK_COVERAGE.md).
The canonical q6 formula is 32,055,251 bytes after the exact
already-alternative Sort ordinal representation, down from 627,951,195 bytes
with explicit permutation multiplication. A 60-second solver experiment
returned `UNKNOWN` after 327/72,599 ms, with the global deadline exhausted
before mismatch branch 3/6. q6 is therefore formula-covered, not proved.
Formula emission means that both snapshots were modeled and SMT was
constructed; it is not a solver proof. The checked-in solver policy now
requires `VERIFIED_BOUNDED` for TPCH q3, q4, q6, q11, q12, q14, q15, q18,
q19, q21, and q22 plus TPC-DS q3, q16, q34, q38, q42, q48, q52, q55, q69, q73,
q87, q90, q93, q94, q95, and q96: twenty-seven obligations (22.3% of the
workload).

The current complete proof-floor gate is green and policy-valid: 11/11 TPCH
and 16/16 TPC-DS obligations are `VERIFIED_BOUNDED`, for 27/27 or 27/121
(22.3%) of the workload at the declared bounds. TPCH spent 1,308/62,684 ms in
preparation/verification and produced report SHA-256
`0eed270ad0148908f05f59ad4e09f8710c280fca39871b5269b60ca1f707979e`;
TPC-DS spent 3,666/57,767 ms and produced
`e8b018abf0286bead86484b8a8739985554b70c823ef663abbeb938eb52a44b6`.
The focused proof-floor policy target passes 5/5.
The incremental q73 focused run spent 239/8,940 ms and produced report SHA-256
`2c9dd4e765f4507bd952189055d67a0db5cf818ecb84abe188bfcdd8a15122e0`.
The immediately preceding retained 25-obligation gate passed 11/11 TPCH after
1,445/67,977 ms and 14/14 TPC-DS after 3,328/49,834 ms; its report SHA-256
values were
`c5668fdda1f40493fdcef4729118d634a63c25f44024106198cdd89775d9d4ed` and
`489f58334593770ff80024bc50a055ed8d60712a34056839d837fbed29c7ff34`.

The three preceding focused real-host solver rows independently return
`VERIFIED_BOUNDED`: TPCH q21 spent 168/1,901 ms of
preparation/verification and produced report SHA-256
`851b3a040d3aa1126d5b0256da95c8851984b977f8d7671db47a9f2d1a9eccef`;
TPC-DS q16 and q94 spent 260/3,923 and 237/2,887 ms, respectively, and their
combined report SHA-256 is
`bc2b934bed75e48cb8ebeea112eb07ef282b6ecd1c331163ce99f927b3b0c848`.
These focused rows retain isolated evidence for the three obligations added by
the two-dependency slice.

The immediately preceding complete proof-floor gate on source `4c2c1359e28`
was green and policy-valid: TPCH passed 10/10 `VERIFIED_BOUNDED` after
1,171/70,555 ms and produced report SHA-256
`95b250728e656081f7a0469035bef4cd3df289a7ec1f0ce08c17d9cf76698554`;
TPC-DS passed 12/12 after 7,482/113,885 ms and produced
`a4b72350384d051958576505f5daf8e09106c59ec87104aa9ebebe1485ca4384`.
TPCH q14 spent 85/37,202 ms in the isolated green run.
At the immediately preceding q18 checkpoint, TPCH passed 10/10 after
1,212/75,124 ms and produced
`f90794bec99f5d739648c6f7fca81574ed52b8070257204d14f373edc0d38361`;
TPC-DS passed 12/12 after 2,937/50,488 ms and produced
`96e07f8139df89f7b2a0f216dd82ee0044afb592c10a7d43f3183275a796caa9`.
Its complete verification subtree passed 34/34 suites and 934/934 tests.
At the preceding q33 checkpoint, TPCH passed 10/10 after
1,185/62,768 ms with report
`1b68432f4e269bd19ca6064338fd008439391a1b1ffc9fa3f511d96418c6a8c6`,
and TPC-DS passed 12/12 after 2,800/43,618 ms with report
`2b32e78f680ca78e59ca158ceaf35e46cc61623f9f5bfe33c0aa938a525ac5e0`;
its complete verification subtree passed 34/34 suites and 925/925 tests.
At the preceding Date-`Unwrap` checkpoint, the same twenty-two
obligations were already enforced: TPCH passed 10/10 after 1,234/58,883 ms
with report
`db65dfe267b0b343f3cded64a32a028fab5561f4ad7b48a5803e0d3629c77f37`,
and TPC-DS passed 12/12 after 2,522/41,013 ms with report
`ea0aaa45b9cc8e7de40ad97ce23420bec926838acc8e17c4925edfad9e481751`.
Its q38 and q87 rows spent 333/1,115 and 324/1,052 ms, respectively, and the
complete subtree passed 919/919 tests. Thus the q33 extension raises the
formula floor from 55 to 56 without changing the 22-query proof floor.
The preceding twenty-query proof floor passed 10/10 TPCH after
1,164/61,112 ms and 10/10 TPC-DS after 2,063/40,374 ms, producing report
SHA-256 values
`1971377b7fa14ab2b6823cdacb99a4d79a76ac4ceea9de46117d30df94a154f9` and
`9a0d87075982d9ef4138b1d55b2265bd9ef461c237fec239c817e601da02bf7f`.
The preceding nineteen-query proof floor used the same ten TPCH obligations
and nine TPC-DS obligations without q95. It spent 1,226/59,673 ms for TPCH and
1,448/36,162 ms for TPC-DS, producing report SHA-256 values
`20540ba5eb16c0d239cd6ed5c9369d4372b774820c4b9033550b0343d577a5d1` and
`62d7539a519ae370278b313d50e83b30a7d50d279cd12f6347b3b1e011163a95`.
The preceding retained eighteen-query canonical-first exact-branch TPCH run
spent 1,145/56,389 ms and produced report SHA-256
`6d7329166c0cff497adcd86fd2d061bb409ca170c473b51529ed76ca8d80280c`;
the TPC-DS run spent 1,446/36,036 ms and produced
`136deef295abfe9c1fa8b4c7d8b01fe8e5131a76886ec998c0a90cbd8b778846`.
That earlier relational `EXISTS` milestone extended the proof floor through
TPCH q4/q22 and TPC-DS q69. Independent focused sweeps returned
`VERIFIED_BOUNDED` for TPCH q4 after 85/924 ms and again after 98/949 ms, TPCH
q22 after 200/5,645 ms and again after 158/5,636 ms, and TPC-DS q69 after
374/3,781 ms and again after 359/3,758 ms.
The proof-scaling milestone keeps the grouped mismatch as the canonical
formula, gives it three quarters of one global solver budget, and falls back on
an exact cover containing the two result-language-absence predicates and
one guarded unmatched predicate per source outcome in either direction.
Canonical `UNSAT`, or `UNSAT` for every branch, proves the same bounded theorem;
any unknown or untried branch prevents a proof. This restored all eighteen
then-policy proofs after branch-only solving exposed a TPCH q15 regression.
Focused q42 returned `VERIFIED_BOUNDED` after 106 ms of preparation and 15,904
ms of verification. q50 emits a formula but its solver experiment reached the
65.0-second external process deadline; q71 did likewise, and TPC-DS q15, q61,
q62, q76, q79, and q88 are `UNKNOWN` at the 60-second solver budget. The q37
and q82 obligations are likewise `UNKNOWN` at 60 seconds. q43 is formula-covered
but returned `UNKNOWN` after 147/69,391 ms at that same budget. Fresh TPC-DS
experiments also returned `UNKNOWN` for q10 after 524/81,517 ms, q19 after
219/61,811 ms, q65 after 283/80,633 ms, and q99 after 218/63,299 ms. A separate
non-gating q40 experiment with a 10-second solver budget reported
`SOLVER_ERROR` after the external solver exceeded its 15.0-second process
deadline; the focused `ya` experiment failed on that status as designed. These
obligations are formula-covered, not proved, and not part of the proof floor.
None is evidence of an optimizer correctness bug.
The post-decomposition portfolio repeat keeps q19, q65, and q99 `UNKNOWN` after
207/61,602, 259/73,190, and 206/62,883 ms of preparation/verification. Their
first unresolved exact branches are q19's left outcome 0 unmatched (branch
3/28), q65's right language absent (branch 2/4), and q99's left outcome 0
unmatched (branch 3/4). The retained three-query report SHA-256 is
`58cc491e30e2b866f36916f2b01db36e385f005ffe3b38685f250d95ccd10164`.
This localizes proof work but adds no proof or bug finding.
After exact direct literal-to-Date normalization, regenerated full TPC-DS
solver runs return `UNKNOWN` for q5 after 1,552/64,916 ms and q77 after
2,035/66,344 ms of preparation/verification. Those results likewise extend
neither the proof floor nor the formula count.
The complete formula dashboard also enforces a monotonic verifier-entry floor
for TPCH q1 and TPC-DS q5, q9, q59, q65, q78, and q80: both snapshots must
continue to export and reach the verifier. Every entry-floor query except q9
satisfies the stronger formula-construction floor; q9, q59, and q78 retain
weaker entry requirements as separate diagnostic regression gates.
Later formula or proof results satisfy every weaker floor automatically.
The complete nineteen-obligation proof floor was confirmed after the dynamic
`IN` slice: 19/121 workload queries (15.7%) were `VERIFIED_BOUNDED` at two rows
and two tasks. After the later nullable Date-year bridge, formula coverage was
50/121 (41.3%) and that proof floor remained unchanged. These are retained
historical checkpoints; the q95 bridge then raised the figures to 51 formulas
and twenty enforced proofs, and exact String dynamic `IN` raised the formula
count at the next checkpoint to 53 without changing that proof floor. The
exact proven-total Date `Unwrap` gate then raised the counts to 55 formulas
and twenty-two enforced proofs. The positive nullable-integral dynamic-`IN`
gate raised the formula count to 56; q33 was not added to the proof floor.
The weak nullable Decimal-`SafeCast` gate raised the formula count to 57;
the subsequent pushed-predicate output-IU resolver added TPC-DS q2/q97 and
raised the count to 59. TPC-DS q18, q2, and q97 were not added to that
twenty-two-query proof floor. The exact two-dependency `EXISTS` slice then added
TPCH q21 and TPC-DS q16/q94 to both tiers, yielding 62 formulas and twenty-five
bounded proofs. Exact repeated-source Map projection plus cached structural SMT
term hashes subsequently move TPC-DS q54 through formula construction, yielding
63 formulas while leaving that proof floor unchanged. Exact same-type integral
division then adds q73 to the formula and proof floors and moves q78 to the
verifier-entry floor, yielding 64 formulas and twenty-six proofs. The
preparation floor independently pins both q73 and q78. The bounded sorting
network then adds TPCH q2 to preparation and formula construction, yielding 65
formulas while leaving the twenty-six-proof floor unchanged. The packed-row
network carrier then adds TPC-DS q59/q78 to formula construction, yielding 67
formulas while again leaving the proof floor unchanged. Closed scalar
consumption inside `IN`, followed by exact symbolic producer-order Merge
networks, then adds TPC-DS q58 and raises the checked-in formula floor to 68;
the proof floor remains twenty-six.

One-level closed `IN` nesting and the proven-present raw-tuple Date-cast gate
then clear q83's two former boundaries. That checkpoint's complete-dashboard row
prepares successfully in 1,351 ms, but both snapshots first reject
`Unsupported scalar type Double`; verifier work is 0 ms. The preceding focused
row spent 1,310/0 ms and produced report SHA-256
`7f1bae257dfcede11aa2f6a37f8e1bc45e079be4f13f8b836887a1768b6d7113`.
Thus q83 adds no formula, verifier entry, proof, policy change, or optimizer
finding, and at that checkpoint the floors remained 68 formulas and twenty-six
proofs. Validation at that checkpoint passed 577/577 Python verifier tests,
214/214 C++ exporter tests, and 14/14 coverage-policy tests.

The subsequent restricted floating-predicate and exact-wrapper checkpoint adds
TPC-DS q21/q34/q75 to the formula floor and q34 to the proof floor, producing
the historical 71-formula and twenty-seven-proof gates. The following
passive-carrier milestone admits q83's four exact `Optional<Double>` output
expressions and raises the complete formula gate to 72 while leaving the proof
floor at twenty-seven. The complete-dashboard and focused formula/solver
evidence are recorded above. Validation at the preceding 71-formula checkpoint
passed 221/221 C++ exporter tests and 14/14 coverage-policy tests, and the
Python verifier suite passed 577/577.

Before that exact Date-cast gate was implemented, a focused 60-second solver
experiment returned `COUNTEREXAMPLE` for TPC-DS q5 after 1,576/10,150 ms of
preparation/verification and for q77 after 2,062/36,163 ms. The SHA-bound
historical artifacts are preserved. A fixed-witness q5 inspection independently
reproduced the symbolic mismatch, with six present logical root rows and one
staged root row. Follow-up audit identifies q5 as a verifier false positive:
the witness chose `date_dim` days 10,441 and 10,457 outside the query's true
10,442..10,456 range because three initial String-literal-to-optional-Date
casts remained shared zero-argument opaque functions while the pushed final
scans used Date literal 10,442. Replacing those three opaque lower-bound results
with 10,442 made the pinned obligation `UNSAT` in about two seconds.

At that historical point q77's saved candidate remained unresolved: a
180-second inspector run reached the 185-second process deadline, its witness
day 10,472 was in range, and narrowed diagnostics returned `UNKNOWN`. The
generic and OLAP exporter paths now fold the exact direct String/Utf8-literal
`SafeCast` to `Optional<Date>`, and the regenerated q5/q77 obligations return
the `UNKNOWN` results above instead of rediscovering either candidate. q5's
old witness is conclusively invalid under the exact cast. q77's old witness
was not refuted—the corrected fixed-witness diagnostic was also `UNKNOWN`—so
it remains a historical, unconfirmed diagnostic rather than a current
counterexample. Neither run is evidence of an optimizer bug; real-YDB replay
remains the confirmation boundary if q77 is reproduced by the corrected model.

Separately, the isolated manual
[Decimal `SUM` runtime diagnostic](runtime_ut/README.md) confirms that execution
depends on partitioning in both the new-RBO and legacy optimizer modes. It is a
shared aggregation/runtime defect, not a new-RBO-only counterexample. These
results retain the two-row-per-table, two-task, pre-physical-boundary
qualifications described below.
The same manual target also retains the paired shared-IU String-`IN`
diagnostic that confirmed the q56/q60 result loss with CBO disabled before the
fix; the normal real-host suite retains the passing production regression.

`CaptureSemanticSnapshotCatalogV1` records the initial query-level catalog once,
and `ExportSemanticSnapshotV1`
deterministically lowers supported RBO operators without doing file I/O. An
optional sink on `TKqlTransformContext` receives the initial snapshot before the
first RBO stage and the final snapshot immediately before physical generation.
`CreateKqpHost` accepts the same sink as immutable instrumentation configuration
and copies it into every per-query transform context created by the host.
Supported final plans include exact stage membership and Map, HashShuffle,
Broadcast, serial or parallel UnionAll, and ordered Merge connections.

Logical Join no longer synthesizes key equalities into an ambiguous scalar
column namespace. Each key is an ordered `{"left": ..., "right": ...}`
descriptor and evaluation reads the two values from their declared input rows
before applying ordinary SQL equality; a NULL key therefore does not match.
The residual `predicate` contains only JoinFilters. Exported snapshots always
include `keys`; legacy version-one snapshots may omit it only as the empty
list. A shared IU name is accepted solely for a one-sided semi/anti join with
an exact literal-true residual. Output-both joins and a shared-IU join with a
residual filter fail closed.

Stage execution uses at most two bounded source/shuffle tasks. Source-row task
placement and hash routing are shared symbolic functions, so a two-task source
covers both partitions instead of fixing rows to one convenient task. Map
preserves the producer count, serial UnionAll requires one consumer task,
parallel UnionAll uses the runtime's cross-input round-robin offset, and a
broadcast-only stage uses one task just as KQP task construction does. Invalid
connection combinations are rejected. Merge has one consumer task, validates
that every producer stream has an order compatible with the edge order, and
represents every sorted interleaving that preserves each producer sequence. It
uses explicit interleavings for small inputs and bounded symbolic ordinals once
enumeration would cross the ordinary outcome cap. The final stage is gathered
before root column projection.

Rows carry structural occurrence provenance and facts about symbolic source or
hash routing. Separate StageGraph child occurrences remain separate even when
their schemas contain the same IU name. When a non-Merge gather sees task
copies of the same occurrence with contradictory routing facts, those guards
are mutually exclusive and the copies can be coalesced exactly, including
conditional task-local values. Broadcast copies have no such proof and retain
their bag multiplicity; distinct or unknown occurrences also remain separate.
This keeps routed StageGraphs compact without silently aliasing input streams
or deduplicating SQL rows.

The exporter and decoder both validate the StageGraph independently: plan nodes
partition into stages, each stage has one logical sink, every cross-stage child
occurrence has one edge, and producer output indices are a bijection with
outgoing edge occurrences. Shuffle-elimination/co-partitioning assumptions are
rejected until the snapshot can substantiate them.

Production RBO diagnostics traverse operator and stage structures as DAGs.
`PlanToString` expands each operator body once and marks later occurrences
`[shared]`; the optimizer HTML trace represents later occurrences as
`Shared=true` leaf nodes. Explain and execution JSON emit one CTE-style
definition per shared operator or stage and connection-shaped `CTE Name`
references thereafter. This keeps diagnostic size linear without changing the
occurrence-sensitive semantic snapshot. Explain JSON remains outside the
verifier's trusted input.

Stage task counts are validated before staged physical properties are admitted.
Every Limit snapshot carries `ensure_at_most_one`; omission is accepted only as
the legacy default `false`. A set marker is no longer erased through a
structural cardinality proof. It raises an explicit outcome error exactly when
the post-Skip/post-Take relation contains more than one present row. The check
runs independently in each stage task, and any task error propagates through
the StageGraph.

The verifier represents each family member as an enabled relation plus an
explicit query-error Boolean. Operators preserve input errors, products combine
them, and row-removing operators do not hide an error that has already
occurred. Equality treats two error outcomes as the same observable status,
compares rows only when both outcomes succeed, and rejects error-versus-success
as a mismatch. Inspector traces label each enabled outcome `success` or
`error`. Because an error outcome's relation is unobservable, a
cardinality-checked Limit may quotient alternative payloads in its
greater-than-one error region while retaining the exact zero- and one-row
result language. Version one does not distinguish error categories, codes, or
text. Replay currently fails closed on an error outcome until its external
error-comparison protocol is implemented.

Version one preserves exact supported YQL scalar identities (`Bool`, signed and
unsigned integer widths, `String`, `Utf8`, `Date`, and canonical parameterized
`Decimal(p,s)`) even when several identities use the same SMT domain. Decimal
identity requires `1 <= p <= 35` and `0 <= s <= p`, with no alternate spelling.
Integer identities use SMT integer carriers with exact signed or unsigned
8/16/32/64-bit domains on literals, source cells, and non-null opaque results.
Date is likewise exact: numeric literals, source cells, and non-null opaque
results are constrained to unsigned day values in `[0, 49673)`.

`String` and `Utf8` use one exact bounded integer-rank quotient of YDB's
unsigned UTF-8/raw-byte lexicographic order; there is no collation or Unicode
normalization. Z3 sees integer ranks rather than its string theory. Ordinary
and null-safe equality and ordinary ordering accept either identity on either
side. HashShuffle likewise shares one symbolic hash family for `String` and
`Utf8`, matching the runtime's type-independent raw-byte hash, while snapshot
type identity remains distinct. The deliberately narrower static-`IN` gate
still requires identical string types.

The quotient fixes every strict-UTF-8 snapshot literal and registers every
distinct nonliteral string-generating root from both plans. Derived `if`,
`IfPresent`, and row-selector terms are pure selections of registered roots or
literals and therefore need no additional representative. A generating root
independent of bounded plan choices contributes one to `M`; a dependent root
contributes the product of the positive registered bounds of the choices it
uses. Those capacities are summed after structural deduplication. It keeps the
resulting `M` concrete representatives in every infinite open
literal interval and all available representatives up to `M` in the finite
prefix/NUL gaps. This is enough to preserve every equality and ordering among
the literals and at most `M` observable assigned values in both directions.
Representatives are constructed from complete UTF-8 literals by NUL extension,
so every decoded witness is valid UTF-8 and replayable even when it stands for
an arbitrary-byte `String` value. Construction preflights limits of 65,536
representatives, 64 MiB of total encoded representative bytes, and 1,000,000
bytes per value; exceeding any limit returns `UNSUPPORTED`. The per-value limit
is shared with witness inspection and replay.

Literal ranks and term bounds are deferred until SMT rendering, after both
plans and any fixed witness strings have registered their values. Rendering
seals the universe; later value registration fails closed, and the model
decoder accepts only ranks in that sealed universe. Bounds for a
choice-dependent string term are universally quantified over exactly its
dependent choices and guarded by their legal ranges, so every rebound
valuation is constrained without restricting irrelevant out-of-range values.

Opaque integer, Date, and Decimal results use the same universally
range-guarded domain-invariant mechanism. Their uninterpreted functions remain
shared globally, preserving deterministic congruence across choice valuations
and both plans. Raw top-level global assertions remain choice-independent.
Before family quantification, comparison verifies that every observable
registered-choice dependency is carried by the outcome, adds all carried
ranges to effective enablement, and rejects shared left/right choice symbols
that could otherwise be captured by a target quantifier.

Decimal uses the exact YDB scaled-integer representation. A legal non-null
`Decimal(p,s)` value is a finite code `c` with `-10^p < c < 10^p`, negative or
positive infinity, or NaN. The snapshot literal is explicitly tagged; finite
codes use a canonical signed-integer string, while specials have no `scaled`
field:

```json
{"kind":"literal","type":"Decimal(7,2)","value":{"kind":"finite","scaled":"1234"}}
{"kind":"literal","type":"Decimal(7,2)","value":{"kind":"neg_inf"}}
```

The other special tags are `pos_inf` and `nan`. Source cells and non-null opaque
Decimal results receive the same typed-domain constraint.

Same-type integer `+`, `-`, and `*` are structural scalar nodes. Both operands
and the result must have exactly the same signed or unsigned width, and result
nullability must be the OR of operand nullability; otherwise the expression
remains opaque. Evaluation is strict on NULL and wraps modulo the result width,
using the signed two's-complement representative for `Int8` through `Int64`.
This narrow rule was added when TPC-DS q88 exposed a spurious witness between
source arithmetic constants and optimizer-folded literals.
Source and opaque integer range constraints also prevent out-of-width witnesses
from observing a difference between a direct integer use and its wrapped
arithmetic identity.

Static `SqlIn` is exact for a deliberately narrow shape: a direct raw tuple or
`AsList` with 1..512 recursively supported items of one scalar type. Items are
non-null except for one exporter-only normalization: a raw tuple item
annotated `Optional<Date>` may be an exact direct String/Utf8-literal
`SafeCast` whose MiniKQL parse proves it present. That item folds to the
existing non-null Date literal. Nullable `AsList` items, invalid text, dynamic
sources, `Nothing`, `StrictCast`, and other optional types remain unsupported.
The item type is identical to the lookup type, or both are integers for which
one lossless common type represents both domains: equal signedness, or a
signed width greater than the unsigned width. This gate is deliberately
narrower than ordinary integral comparison. Membership evaluates as the SQL
three-valued OR of those equalities, so only a nullable lookup can make the
Boolean result nullable. `ansi`, `warnNoAnsi`, `isCompact`, and
`nullsProcessed` normalize to the same node under this gate. Dynamic, empty,
oversized, other nullable, heterogeneous-item, lossy or non-integer
mixed-type, `tableSource`, malformed-option, and unknown-option forms fail
closed.

`Exists` is the exact non-null presence test for one scalar value. `If` models
MiniKQL's lazy branch selection, including NULL propagation from an optional
Boolean condition. Unary `IfPresent` binds the non-null payload of exactly one
`Optional<Data>` input; the snapshot represents nested handlers with lexical
de Bruijn depths, so argument names and allocations cannot affect identity.
At most 64 handler bindings may be live at once.
The new RBO's lowered static-membership form is normalized to the same `in`
node only for `Contains(ToDict(List(...), identity, Void, (One, Auto)), bound)`
inside that handler. Every item must be non-null, have the bound value's exact
type, and pass the ordinary recursive scalar audit. Other lambdas, dictionary
settings, payloads, lookups, and generic `Contains` remain unsupported.

The exact Decimal zero wrapper admits only
`Coalesce(direct_optional_member, zero) -> Decimal(p,s)`. The member must be a
visible `Optional<Decimal(p,s)>` input and the fallback must be either a
canonical non-null Decimal zero of that exact type or a complete
`SafeCast(Int32("0"), Decimal(p,s))`. It lowers to
`if_present(member, bound(0), zero)`, preserving NULL-to-zero behavior and the
non-null result schema. A matching `Just` may wrap that exact result and reuses
the existing `if(true, value, typed-null)` Optional-preserving form. Nonzero or
special fallbacks, dynamic or incomplete casts, `Convert`, computed first
children, mismatched types, and broader `Coalesce` shapes remain opaque when
the closed-world safety audit admits them; malformed or unsafe trees fail
closed.

A separate exact Uint64 wrapper admits only
`Just(direct non-null Uint64 member) -> Optional<Uint64>` for the current row
and a visible column. The exporter validates the complete wrapper, member, row
argument, types, metadata, and scalar budgets, then lowers it to
`if(true, member, null Uint64)`. The explicit dead NULL branch preserves the
static Optional result while making runtime presence exact. Nullable,
computed, missing, foreign-row, wrong-result-type, or unsafe variants fail
closed.

Exact integral `/` requires both operands and the result to have the same
fixed-width signed or unsigned integer identity, and the result must be
Optional. An absent operand produces NULL. A zero divisor and signed
`MIN / -1` overflow also produce NULL; every other quotient truncates toward
zero by dividing nonnegative magnitudes and restoring the sign. Mixed-type,
mixed-width, non-Optional-result, and floating-point forms fail closed.

Scalar syntax outside the explicit Boolean, equality, ordering, integer
arithmetic, and restricted static-membership core is represented by a shared
typed uninterpreted function when
the C++ exporter can positively audit it as deterministic and total. The
current reviewed opaque families are scalar comparisons; `Just` and
`Coalesce`; `SafeCast`; non-failing `Convert`; `Substring`; the canonical
String-predicate bridge described below; and stored-String `Concat` in the exact
workload shapes described below. The same audit
treats the explicit `DecimalDiv` core node as total, so a supported opaque
parent may contain it; the same is true of the exact integral `/` core above.
YQL has no complete generic totality or determinism flag, so all other
callables fail closed rather than relying on a denylist. This includes UDF and
PG calls, floating-point or mixed-type division, strict casts, `Unwrap`,
runtime-dependent generators, free variables, and unsafe AST metadata.

The reviewed `Substring` shape is exactly
`Substring(Optional<String>, start, count) -> Optional<String>`. Both bounds
must be non-null `Uint32` literals, either directly or as an in-range integer
literal converted to `Uint32`; that conversion exception is confined to those
two direct bound positions. The canonical fingerprint retains both bounds and
the String column is the only external function argument. Other arities,
`Utf8`, nullable or dynamic bounds, and out-of-range conversions fail closed.

The canonical String-predicate bridge accepts only generic
`EndsWith`/`StringContains` or executed OLAP
`ends_with`/`string_contains` with a direct `Optional<String>` column on the
left, a non-null `String` literal on the right, and an `Optional<Bool>` result.
Each operation has its own stable `yql-string-predicate-v1` fingerprint and
retains the ordered column/literal arguments. The verifier treats it as one
deterministic total uninterpreted function shared across the two dialects; it
does not reimplement the byte predicate. Generic
`Coalesce(predicate, false)` retains exact `if_present` NULL handling, while
the pushed coalesce is erased only in a positive filter context. Operand, type,
nullability, result-descriptor, catalog-column, and operation near-misses fail
closed.

The two reviewed `Concat` shapes are confined to the body root of a Map
expression and return exactly non-null `String`. A literal-only binary tree
must contain only canonical String literals, pass the closed-world scalar
safety and shared node/depth limits, and remain within the exact allocation
bound below. The exporter concatenates its leaf bytes in evaluation order and
emits the existing canonical String literal node. This exact normalization can
therefore compare an unfolded initial constant with the optimizer's folded
literal without adding a new Python expression kind.

The separate stored-member shape is a binary tree whose leaves are canonical
String literals, non-null stored String members, or exactly
`Coalesce(nullable stored String member, String(""))`. At least one and at
most two stored-member occurrences are required; repeated occurrences count
separately. Generic or nested-parent `Concat`, `Utf8`, computed strings,
nonempty nullable fallbacks, and every other leaf fail closed. The entire
stored-member tree is encoded as one opaque function: its canonical fingerprint
retains tree shape, literal bytes, argument order, and repeated uses, while IU
names are alpha-normalized. Consequently this rule can prove only
syntax-preserving uses of the same total function; reassociation or another
semantic rewrite may cause a false counterexample, never a false proof.

Stored-member provenance begins only at catalog-confirmed Datashard or Olap
tables and excludes system views, generated values, and external sources. It
survives Map pass-through/rename, Filter, Limit, Sort, aggregate group keys,
and the value-preserving sides of joins and UnionAll. Outer and exclusion joins
widen the affected side to nullable; semi/anti joins drop the absent side; and
UnionAll ORs catalog-derived nullability from both inputs. The final Member
annotation must equal that carried nullability, so stale intermediate type
annotations cannot manufacture provenance.

Totality is justified by representation, storage, and allocation bounds, not
by assuming Concat cannot fail. Datashard caps a stored value at 16 MiB through
`NDataShard::NLimits::MaxWriteValueSize`. Column-store String values reach
MiniKQL as Arrow `BinaryType`; its validated signed 32-bit offsets bound one
logical cell by `INT32_MAX` bytes independently of compression. The auditor
charges the bound carried by each stored occurrence plus exact literal bytes.
It also requires the complete result to stay below the largest `ui32` size for
which MiniKQL's `newSize + newSize / 2` allocation capacity cannot wrap. Thus
one Olap occurrence plus the audited literals is safe, while two generic Olap
occurrences fail closed. The authoritative implementations are
`ydb/core/tx/datashard/const.h`,
`ydb/core/tx/datashard/datashard_write_operation.cpp`,
`ydb/core/tx/datashard/datashard_common_upload.cpp`,
`ydb/core/formats/arrow/switch/switch_type.h`,
`yql/essentials/public/udf/arrow/dispatch_traits.h`,
`contrib/libs/apache/arrow/cpp/src/arrow/type.h`,
`contrib/libs/apache/arrow/cpp/src/arrow/array/validate.cc`, and
`yql/essentials/minikql/mkql_string_util.cpp`.

This gate moves TPC-DS q5 and q80 past snapshot export to the deeper Decimal-SUM
headroom and 82,944-pair grouped-aggregate construction checks. q84 has two
Olap String occurrences, so it fails the allocation-totality gate. The formula
slice at that milestone remained 23/121 (19.0%) and the proof floor remained
ten.

The later literal-only gate moves q66 through both boundaries. Integral-right
Decimal bound propagation then proves sufficient accumulator headroom for its
projected products, quotients, inner sums, and outer sums, so the complete
two-row/two-task obligation reaches formula construction. This is formula
coverage, not a bounded proof.

An explicit `cast_integral` node models only partial integer `SafeCast`: the
source is any exact signed or unsigned 8/16/32/64-bit integer expression, and
YQL cast analysis must classify the conversion to the optional integer target
as `MayFail`. The target descriptor, its outer and item annotations, and the
optional result must agree exactly. Evaluation propagates a source NULL,
preserves an in-range integer value, and returns NULL for a value outside the
target's exact integer domain; the verifier gives every NULL result the
canonical zero payload. The entire callable still passes the closed-world
opaque-expression safety audit before entering this exact gate. Complete
integer conversions remain opaque, while `Convert`, `StrictCast`, non-integer
pairs, and non-optional partial results are outside this exact node.

TPC-DS q79 exposed the need for this node. Its first symbolic candidate used
`d_year = 1998`: the initial plan tested the nullable `Int64` directly against
the `Int32` membership constants, while the final lowering first performed an
opaque `SafeCast(Int64 -> Int32)`. Treating that cast as an independent function
allowed it to return NULL for 1998, which the runtime cannot do. Regenerating
the obligation with the exact partial-cast semantics removes that witness. A
focused direct-membership versus `Exists`/cast/`IfPresent` lowering returns
`VERIFIED_BOUNDED`; the full q79 solver run returns `UNKNOWN` at the 60-second
budget. The candidate was therefore a verifier-modeling false positive, not a
confirmed optimizer bug.

A complete cast of a non-null integer constant to a non-null Decimal is handled
before opaque fallback: the exporter evaluates the YDB cast and emits the
resulting tagged Decimal literal. A separate explicit `cast_decimal` node models
weak `SafeCast` from any exact signed or unsigned 8/16/32/64-bit integer
expression, or from a canonical Decimal expression under the widening rule
below, to a canonical `Decimal(p,s)`. The snapshot carries a mandatory
`source_type`; the decoder requires it to equal the independently inferred
argument type. The target descriptor and its outer and nested annotations must
agree exactly with the result, `p - s >= 1`, and result nullability must equal
source nullability.

For an integer source, evaluation propagates source NULL and otherwise
multiplies the integer by `10^s`; a coefficient whose absolute value reaches
`10^p` becomes the corresponding signed infinity, matching weak MiniKQL
`SafeCast`. A present overflow is therefore not NULL. Complete integer literals
retain the normalized-literal representation, while value-specific incomplete
literals use `cast_decimal`. For a Decimal source, only a same-scale cast to
equal or greater precision is accepted. That conversion is raw-code identity:
finite coefficients, signed infinities, and NaN are preserved exactly, and a
source NULL remains NULL.

`Convert`, `StrictCast`, sources outside exact integers and canonical Decimals,
missing or mismatched `source_type`, cross-scale Decimal casts, precision
narrowing, changed nullability, zero-integral-digit targets, and malformed or
mismatched descriptors or annotations remain outside this explicit gate. A
real-host two-row/two-task regression covers nullable integral-to-Decimal and
nullable same-scale Decimal-widening expressions and returns
`VERIFIED_BOUNDED`. The full TPC-DS q18 obligation only emits a formula; this
extension is neither a q18 proof nor an optimizer-bug finding.

A second constant normalization covers only a direct non-null `String` or
`Utf8` literal passed to `SafeCast` with an `Optional<Decimal(p,s)>` result and
target. The result, outer target annotation, nested non-null Decimal item
annotation, and descriptor must agree exactly, and YQL cast analysis must
classify the source-to-item conversion as `MayFail | MayLoseData`. The literal
must be non-empty 7-bit ASCII; dynamic, nullable, malformed, or differently
annotated sources fail closed.

The exporter evaluates that fixed conversion with the runtime's
`NDecimal::FromStringEx`. A parser error becomes an existing typed `null` node.
Successful finite values use the parser's round-half-to-even behavior and must
be normal for the requested precision; a successful nonnormal value is rejected
rather than normalized. NaN and signed infinities remain tagged Decimal
literals, numeric overflow saturates to signed infinity, and underflow can round
to zero. Successful casts become existing Decimal literal nodes, so this
milestone changes neither the snapshot IR nor the Python decoder/evaluator.

A separate normalization evaluates a direct non-null `String` or `Utf8`
literal under `SafeCast` to exactly `Optional<Date>`, independently of any
surrounding arithmetic. The result and target descriptor, outer and nested
annotations, and reviewed `MayFail` cast classification must agree exactly.
MiniKQL `ValueFromString` supplies the runtime value: a valid input becomes an
existing Date literal and an invalid or out-of-domain input becomes existing
typed Date NULL. The generic expression path retains the closed-world safety
and totality audit used for opaque expressions; the executed OLAP-filter path
uses the same exact fold. Dynamic, nullable, malformed, differently annotated,
or non-`SafeCast` forms fail closed. The fold introduces no new snapshot IR or
Python evaluator semantics.

Focused exporter tests passed 4/4, the complete `cpp_ut` run passed 144/144 at
that Date-cast milestone, and a q5-shaped actual-host pushed-filter integration
passed 1/1 with
`VERIFIED_BOUNDED`. Regenerated q5 and q77 obligations contain the exact Date
constants and return `UNKNOWN` rather than reproducing the old symbolic
candidates. This refutes q5's saved witness; it does not turn q77's historical
candidate into either a proof or a confirmed false positive.

Another exact bridge covers the optimizer's nullable year-extraction
normalization:

```text
Map(
  SafeCast(direct Optional<Date> member, Optional<Timestamp>),
  (timestamp: Timestamp) ->
    DateTime2.GetYear(DateTime2.Split(timestamp)))
```

The outer result must be exactly `Optional<Uint16>`. The source must be one
direct visible member, the Date-to-Timestamp cast must be classified complete,
and the lambda must be unary with the exact non-null Timestamp argument reused
by `Split`. `Split` and `GetYear` must match their reviewed normalized UDF
names, callable types, cached result descriptors, user types, AutoMap flags,
settings, Void metadata, and annotations. Different result/source types,
computed or invisible members, other casts, non-unary or misbound lambdas,
different UDFs, and malformed metadata fail closed.

The normalized snapshot expression is exact about Optional behavior:
`if_present(column(Date), if(true, year(bound(0)), NULL<Uint16>),
NULL<Uint16>)`, with an `Optional<Uint16>` result. `year` is the non-null typed
opaque function with fingerprint `yql-datetime-year-v1`; its argument is the
bound original Date payload because the intervening cast is complete. The
inner constant-true `if` performs only the schema lift required by the current
strict scalar IR. A common fingerprint and ordered argument make the reviewed
operation identical across snapshots, while any mutation remains observable.

The C++ exporter suite includes the accepted shape and focused fail-closed
mutations for the material Date-year-specific gates; shared tests retain the
generic UDF-envelope and scalar-safety contract. Python checks exact NULL
lifting and obtains a bounded proof for identical expressions plus
counterexamples when either the fingerprint or argument changes. A
nullable-Date real-host projection verifies the initial and final optimizer
snapshots at two rows and two tasks. In the workload this bridge moves TPCH q7,
q8, and q9 from the former scalar-`Map` boundary to complete formula
construction; their focused solver runs remain `UNKNOWN`, so no query is added
to the proof floor.

The Date/Interval normalization additionally covers Optional-Date arithmetic
of the form `SafeCast(text, Optional<Date>) +/- Apply(udf, days)`. The text must
be a direct non-null `String` or `Utf8` literal. The cast result and descriptor
must be exactly `Optional<Date>`, and YQL cast analysis must classify it as the
reviewed `MayFail` conversion. The right side must be the strict normalized
eight-child `DateTime2.IntervalFromDays` UDF applied to a direct non-null
`Int32` literal in `[-49672, 49672]`, with its callable annotations, cached
descriptor, Void fields, empty configuration/file alias, and `blocks, strict`
settings all matching exactly. Other Date arithmetic remains unsupported.

The exporter parses the Date with MiniKQL `ValueFromString`, applies the signed
day offset, and reuses an existing typed Date literal or NULL node. An invalid
Date or a result outside `[0, 49673)` becomes typed Date NULL, matching runtime
optional arithmetic. No Interval snapshot node or Python evaluator extension
is introduced. At the pushed-OLAP boundary, `just` is erased only around a
direct, valid, non-null Date literal; nullable, malformed, dynamic, or non-Date
arguments fail closed.

An independent constant normalization admits only a direct non-null numeric
`Date` left operand and `Interval` right operand under an exactly
`Optional<Date>` `+` or `-`.
The Interval atom must satisfy the runtime type's strict open range. The
exporter follows MiniKQL exactly: it scales Date midnight to microseconds,
performs signed arithmetic, rejects a scaled result outside
`[0, MAX_TIMESTAMP)` as typed NULL, and only then truncates back to whole days.
This matters for fractional-day intervals such as one microsecond. Valid input
ranges prove that the arithmetic cannot overflow `i64`; every other shape
fails closed. Synthetic open-boundary, fractional-day, malformed-shape, and
typed-NULL tests cover the gate, while a real-host pushed-filter fixture folds
the exact TPCH q1 constant `Date('1998-12-01') - Interval('P90D')` to day
10,471 and returns `VERIFIED_BOUNDED`.

This exact fold moved TPC-DS q37, q40, and q82 through formula construction.
At that milestone the dashboard covered 23/121 queries (19.0%). q37 and q82
are `UNKNOWN` at a 60-second solver budget. A separate non-gating q40 scaling
experiment retained a
97,319,076-byte formula but reported `SOLVER_ERROR` after the external solver
exceeded its 15.0-second process deadline; it is neither a proof nor a
counterexample. Before restricted stored-String `Concat` was added, TPC-DS q5, q80,
and q84 stopped at the generic callable. At that milestone q5 and q80 moved to
the deeper outcomes described above; subsequent finite Decimal-bound
propagation moved q5 again to the 32,896-pair Merge construction cap. The later
exact representation selector moves both through formula construction. q84
remains unsupported on its
two-cell allocation bound. Before constant DateTime2 calendar-shift folding,
the dashboard covered 23/121 queries and the proof floor remained ten. At that
earlier milestone TPCH q1 passed both snapshot exporters and reached unmodeled
aggregate `avg`; q21 exposed `Double`; q72 still had a dynamic Date-fold
`SafeCast(Optional<Date>)` mismatch; and, at that earlier milestone, q77 passed
export but failed the verifier's Decimal-SUM headroom gate.

A further constant normalization accepts only the exact optimizer-generated
`Map(Shift(Split(Date), Int32), MakeDate)` tree for `DateTime2.ShiftYears` or
`DateTime2.ShiftMonths`. The root and shift must be optional; every Date,
Int32, TM-resource, callable, cached descriptor, Void field, setting, AutoMap
flag, and UDF user type must match the reviewed normalized form; and the unary
lambda may pass its bound TM value only to `DateTime2.MakeDate`. All other
`Map`, DateTime2, dynamic, or differently annotated shapes fail closed.
The exact signatures for `IntervalFromDays`, `Split`, both shifts, and
`MakeDate` live in one five-row reviewed table shared by uniform envelope,
cached-type, and `Apply` validators.

The fold uses MiniKQL's Date split/make tables and the runtime calendar rules:
year shifts clamp February 29, while month shifts use the runtime signed
quotient/remainder sequence and clamp the day to the target month. It rejects
shifts that would wrap the unsigned 12-bit `DateTime2.TM` year field; a valid
shift outside the Date domain becomes typed NULL. Synthetic exact-shape,
leap-day, month-end, Date-boundary, descriptor, flag, setting, lambda-binding,
and year-wrap tests cover the gate. A real-host pushed-filter obligation folds
q5/q6-style `1994-01-01 + 1 year` and q10-style `1993-10-01 + 3 months`
constants to days 9,131 and 8,766 and is `VERIFIED_BOUNDED`.

The complete formula dashboard now emits TPCH q5, q6, q10, and q14 in addition
to q3 and q19. Their preparation/verifier times were respectively 153/109,903,
55/213, 109/8,144, and 59/247 ms. At that milestone q12 passed the DateTime2
fold and exposed unordered scalar children at both snapshot boundaries. This
raised the formula slice at that point to 27/121 (22.3%). In the initial focused
solver experiments q5 reported `SOLVER_ERROR` after
180/230,982 ms of preparation/verifier work and the 65-second external-process
watchdog, while q10 returned `UNKNOWN` after 142/74,871 ms. q6 and q14 produced
symbolic counterexamples after 54/788 and 87/1,046 ms. Inspection indicates
verifier false positives: equivalent final Decimal predicates or constants are
opaque or fingerprinted differently from their initial forms. Neither was
replay-confirmed, and neither established an optimizer bug.

The subsequent narrow scalar gates lower only a nullable direct comparison
under exact `Coalesce(..., false)` and only an exact constant Decimal `Just`.
They retain wrapper semantics and Optional schema with existing `IfPresent` and
`If` nodes; broader shapes still fail closed to opaque modeling. The old q6 and
q14 witnesses disappear, and the then-current policy-backed run returned
`VERIFIED_BOUNDED` after 72/749 and 97/33,152 ms, respectively. Both obligations
now enter the proof floor. A bounded UNSAT result has no witness to replay; q5
and q10 remain unproved, and no optimizer correctness bug is confirmed.

The subsequent q12 gate admits only exact
`Coalesce(Or(member == literal, member == literal), false)` and
`Coalesce(And(member != literal, member != literal), false)` forms. Both leaves
must compare the same direct `Optional<String>` member with a non-null `String`
literal; broader Boolean trees remain opaque. The wrapper again
lowers through schema-preserving `if_present`. At that milestone the complete
dashboard recorded q12 as `FORMULA_EMITTED` after 109/5,343 ms of
preparation/verification; an earlier focused formula run recorded 108/5,816 ms. Focused and
policy-backed proofs returned `VERIFIED_BOUNDED`: the focused run recorded
108/38,880 ms and the then-current policy-floor run 106/40,602 ms. This raises
TPCH formula coverage to 7/22, total formula coverage to 28/121 (23.1%), TPCH
proofs to five, and the total proof floor to 13/121 (10.7%). No proof produced
a candidate, so replay was not invoked and no optimizer correctness bug was
found.

The following historical exact gate normalized only a direct
`Optional<Decimal>` member under
`Coalesce(member, zero)`, including its matching `Just` wrapper, to the
schema-preserving forms described above. At that milestone the complete TPC-DS
dashboard moves q43 through formula construction after 145/4,760 ms and moves
q77 past Decimal `SUM` headroom to the 25,600-pair grouped-aggregate cap after
2,063/442 ms. At that milestone TPC-DS emitted 22/99 formulas and the combined
workload emitted 29/121 (24.0%); q77 remained unsupported. A focused q43 solver run returned
`UNKNOWN` after 147/69,391 ms at the 60-second budget, so the proof floor stays
at thirteen. The first complete run also caught incomplete Decimal-zero casts
in q40 and q80 being rejected while classifying a near-match. Classification
now leaves those forms opaque before invoking the strict exact-cast exporter;
targeted regressions and the repeated complete dashboard restore q40's formula
and q80's verifier entry. No candidate or optimizer bug arose.

The preceding exact-representation milestone assigned nonrecursive bottom-up
structural IDs to SMT terms, then partitions grouped-aggregate candidates by
the complete `(type, is-null term, value term)` structure of their ordered
group keys. Aggregate membership still ranges over every original row, so bag
multiplicity is unchanged. When there are fewer exact key classes than input
candidates, the class costs fit the ordinary construction bounds, and the form
is either required to avoid the directional cap or strictly cheaper than the
directional square, one result candidate is retained per class and null-safe
comparison is shared over the class upper triangle. Singleton provenance and
common partition facts are retained; no SQL rows are deduplicated. Sort and
latent sequence families enumerate permutations only when every outcome has at
most three candidate rows and the outcome cap also fits; four or more rows use
the same exact bounded symbolic-ordinal language. The structural-ID walk is
iterative, so deep exact terms do not depend on recursive Python hashing.

At the preceding representation-selector milestone, the complete TPC-DS
dashboard was policy-valid at 30/99 formulas, 40 unsupported queries, and 29
optimizer failures. The eight newly constructed formulas were q5, q25, q29,
q46, q68, q77, q80, and q91. Their measured preparation/verifier times were
1,588/2,653, 249/11,564, 263/4,142, 284/2,574, 276/2,301, 2,122/3,323,
1,810/42,847, and 227/3,754 ms, respectively. Together with TPCH's then-current
seven formulas this was 37/121 (30.6%); the thirteen-query proof floor was
unchanged. Formula construction is not a proof, and the regenerated q5/q77
results did not extend that proof floor. Full TPC-DS solver runs returned
`UNKNOWN` for q5 and q77 after
1,552/64,916 and 2,035/66,344 ms of preparation/verification. Focused 60-second
runs returned `UNKNOWN` for q25, q29, q46, q68, q80, and q91 after 302/86,108,
272/68,174, 313/64,717, 293/64,427, 1,784/121,558, and 221/67,811 ms,
respectively. All eight are formula-covered but neither proved nor current
candidate divergences.

The completed phase-aware Decimal `AVG` milestone makes its hidden runtime
state explicit and auditable. Every `avg` trait must have identical canonical
Decimal input and output type `Decimal(p,s)` and carries exactly
`state: {sum_type: "Decimal(35,s)", count_type: "Uint64", nullable:
<input-nullability>}`; non-`avg` traits omit `state`. An intermediate state has
one direct matching final-aggregate consumer with the same ordered keys and
metadata, and it may cross StageGraph routing only as payload. The final phase
adds partial sums and counts, so unequal partitions are weighted correctly
rather than averaging partial averages. NULL inputs are skipped, and a group
with no non-NULL input produces NULL. The Decimal kernel preserves NaN and signed
infinities, reproduces signed round-to-nearest/ties-to-even division by the
positive count, and applies the runtime's same-scale narrow cast to
`Decimal(p,s)`, including finite overflow saturation. Finite sum headroom must
remain below the `Decimal(35,s)` accumulator range and the count bound below
`2^64`; otherwise verification fails closed before modeling non-associative
overflow.

Concrete inspector traces render every state-bearing cell with an
`average_state` object containing its two types, optional `{sum, count}` value,
and separate conservative `proof_bounds`. Both state terms are registered as
probes; an incomplete trace fails closed instead of silently showing only the
derived partial scalar.

Independent exhaustive small-domain tests cover finite values, NULL, NaN,
signed infinities, positive and negative ties, grouped and scalar aggregation,
and unequal split-task counts. Strict decoder/dataflow mutations cover missing
or incorrect state, non-Decimal and mismatched types, bad nullability, leaked
state, and broken intermediate-to-final lineage. Focused C++ exporter tests
passed 3/3 and the complete C++ exporter suite passed 147/147 at that
Decimal-AVG milestone.

The preceding Decimal-AVG dashboards added TPCH q1 and TPC-DS q65 to formula
construction: TPCH was 8/22 formulas after 6,944/11,582 ms of
preparation/verifier work, and TPC-DS was 31/99 after 68,255/249,242 ms.
Focused q1 emits a formula after 111/998 ms; its non-gating 60-second solver run
is `UNKNOWN` after 159/63,937 ms, not a proof or counterexample. Focused q65
emits a formula after 687/30,318 ms. At that milestone the combined formula
slice was 39/121 (32.2%) and the proof floor had thirteen queries. q7 and q8
were visible at the deeper generic `Map` exporter blocker at that milestone;
the later exact nullable Date-year bridge now moves them through formula
construction.

The subplan audit of the then-catalog-blocked slice found 32 source subqueries
across seven TPCH and thirteen TPC-DS queries: fifteen scalar, seventeen `EXISTS`,
twenty-five correlated, and only seven uncorrelated. There are no dynamic `IN`
subplans in this workload slice. Only TPCH q11/q15 and TPC-DS q24/q54 are fully
uncorrelated.

Initial catalog capture validates the subplan registry and follows every subplan
root, so a failed initial semantic export no longer hides the final boundary.
The snapshot records each binding's kind, root, exact type/nullability,
dependencies, consumers, and scalar output or `EXISTS` predicate. The first
deliberately narrow milestone accepted only uncorrelated bindings with no
dependencies, explicit Project/Filter consumers, and roots statically known to
produce at most one row: `EmptySource`, an eligible ungrouped aggregate, a
literal `Limit <= 1`, or Project/Filter/Sort wrappers over one of those shapes.
Join, UnionAll,
intermediate or `DistinctAll` aggregation, nested subplans, and staged subplans
failed closed at that milestone.

The current exporter always emits the `plan.subplans` array. Its absence in a
legacy-v1 snapshot means an empty array: the earlier version-one exporter could
not encode a residual subplan and failed closed whenever one was present.

That exact static slice moved TPCH q11 and q15 through formula construction and
into the checked-in proof floor. In the complete formula dashboard they spent
176/558 and 152/462 ms in preparation/verification. The post-hardening proof
floor returned `VERIFIED_BOUNDED` after 158/6,585 and 199/2,750 ms,
respectively. At that historical checkpoint the dashboard had 41/121 formulas
(33.9%) and the proof floor had 15/121 obligations (12.4%). These preserved
measurements predate the general scalar-error implementation; the fresh current
measurements are reported at the top of this document.

Commit `b2cd6e3c5bb` adds the explicit outcome algebra, and `f930f1352e7`
introduces general uncorrelated scalar subplans within the modeled relational
surface. A binding reuses the source family's decisions and choices, returns
typed NULL for zero present rows, returns the selected value for one, and
generates a cardinality-error term for more than one. Commit `1aaf281c07a`
assigns enumerated latent-sequence alternatives a stable scoped decision, so
the cached family cannot select different permutations at different consumers.
The optimized form remains the ordinary main plan, so comparison with the final
StageGraph has no scalar-specific equivalence shortcut.

Only the current binding's newly generated more-than-one-row error is
consumer-demanded. It is observed when that binding's immediate Project/Filter
consumer has a present row, including when the binding appears under a dead
scalar-expression branch. An error inherited from evaluating the subplan root
remains observable and is not gated again by an empty enclosing consumer. Thus
a nested scalar demanded by its own nonempty consumer can error even when the
top-level outer consumer is empty. An intrinsic error already raised inside the
producer is eager in the same way.

Model-correction commit `125962c87df` preserves inherited `Outcome.error`
separately from the binding-local `cardinality_error` until the immediate
consumer applies that demand gate.

Production commit `9e50d234264` correctly gates the direct cardinality check
inserted for one binding. CBO could then commute this order-sensitive synthetic
Cross. Physical Cross drains the right side first, so a commuted empty outer
side could finish without evaluating the inherited scalar error. Commit
`cab0dd1e89c` marks both synthetic Crosses `PreserveInputOrder`, keeps them as
BuildInitial/Expand CBO barriers while optimizing their sides, and blocks filter
absorption through the barriers.

Physical `EnsureAtMostOne` Limits are now serialized and evaluated rather than
proved inert during export. The focused `*AtMostOneMarker*` C++ matrix passes
3/3 for direct, multi-task-producer, and single-task-producer serialization.
The check observes the exact post-Skip/post-Take relation independently in each
stage task.

TPC-DS q24 still has the independent `Unsupported scalar callable Map` blocker.
TPC-DS q54's Map copies one source IU to two distinct output IUs. The exporter
now preserves that exact projection instead of rejecting the repeated source,
and the complete dashboard emits q54's two-row/two-task bounded formula after
50,737 ms of verifier work. A separate 60-second solver attempt returned
`UNKNOWN` when the global deadline expired before mismatch branch 3/8
(`left_outcome_0_unmatched`). q54 is therefore formula-covered, not proved,
and did not change the then-current twenty-five-query bounded proof floor. The
milestone passes 529/529 Python verifier tests, 205/205 C++ exporter tests, 39/39
real-host integration tests, and 12/12 coverage-policy tests.

Equality-correlated scalar aggregation is exact for one deliberately narrow
shape. The descriptor has exactly one outer dependency and one Project or
Filter consumer. Its subplan root is a unary path matching
`Project* -> Aggregate -> Project* -> Filter -> outer_bind`: there is exactly
one ungrouped, phase-`undefined`, non-`DistinctAll` Aggregate, and `outer_bind`
is an explicit typed snapshot node over the closed inner input. The Filter has
exactly one dependency-bearing conjunct, a strict non-null-safe direct equality
between the outer dependency and one inner column; every residual conjunct is
inner-only.

The evaluator instantiates the complete scalar root once per present outer row.
Zero result rows yield typed NULL, one yields its value, and more than one
raises the scalar-cardinality error. Both inherited and cardinality errors are
gated by that outer row's presence. Repeated uses in the one consumer read the
same per-row binding value. The model rejects Limit, Sort, scan `pushed_limit`,
ordered `UnionAll`, `EnsureAtMostOne`, nested or staged bindings, and any
per-invocation choice family. All invocations reuse one validated plan context
and share one cumulative 16,384-pair outer/closed-inner construction budget.
The optimized side remains the ordinary final StageGraph, without a
scalar-specific equivalence shortcut. A real-host
Decimal-AVG left-join case returns `VERIFIED_BOUNDED`.

Relational `EXISTS` is exact for uncorrelated bindings and two deliberately
narrow correlated shapes. An uncorrelated descriptor has no dependency or
predicate and returns the non-null Boolean presence of its root. The original
correlated form has exactly one outer dependency and exactly one
dependency-bearing conjunct: a strict, non-null-safe equality between that
dependency and one direct inner column. The two-dependency form has exactly two
ordered, distinct outer dependencies. Each occurs in a separate conjunct:
exactly one strict direct equality and one strict direct inequality compare
them with distinct direct inner columns. The corresponding outer and inner
base types must match, although nullability may differ, and every residual
conjunct is inner-only.

The exporter retains the complete predicate and normalizes source `!=` to JSON
`not(eq)`. Python independently recognizes that exact normalized two-key shape,
preserves dependency order, and binds both values from the same outer row. SQL
filter truth is applied for each outer/inner row, then matching inner rows are
collapsed with Boolean OR, so a strict comparison involving NULL is not a
match and duplicates do not multiply the outer row. `NOT EXISTS` remains
ordinary negation in the consumer expression.

The exporter peels only plain column-projection Maps above one Filter directly
over `AddDependencies`, then records the underlying inner root rather than
inventing residual plan nodes. Every `EXISTS` binding has exactly one Filter
consumer and remains virtual. C++ export validates the exact
`AddDependencies` schema/order/type and the descriptor topology; Python
independently validates the serialized registry, ordered dependencies,
predicate, base types, consumers, nesting, staging, and binding leaks. An
`EXISTS` root with an observable `EnsureAtMostOne` error also fails closed.
Correlated roots additionally reject Limit, TopSort, and scan
`pushed_limit` because their row choice would have to be independent per outer
invocation; plain Sort and exact uncorrelated row selection remain admissible.
When a one-sided witness join receives a same-name `Void` carrier on both
inputs, the unselected copy may disappear only because the identical selected
copy remains. An unmatched dropped `Void` and every `Void` join key fail
closed. Evaluation preflights at most 16,384 outer/inner row pairs. The
optimized side remains the ordinary final StageGraph, with no subplan-specific
equivalence shortcut.

At the two-dependency milestone, its focused gates passed 17/17 in Python, 6/6
in the C++ exporter, and 1/1 for its new real-host case; the complete verifier,
exporter, and inspector suites passed 527/527, 203/203, and 46/46. Exact
semi/anti solver differentials and an intentionally omitted-second-correlation
counterexample cover the evaluator boundary. The focused workload obligations
return `VERIFIED_BOUNDED` for TPCH q21 and TPC-DS q16/q94 at two rows per table
and two tasks. Those proofs establish bounded equivalence only.

Dynamic `IN` is now exact for one narrow uncorrelated relational shape. The
typed descriptor names one lookup
column from the sole Filter consumer and one result column from the inner root.
Their underlying types must match exactly. Non-null columns may be fixed-width
integral, exact `String`, or Date; lookup and output nullability may vary
independently only for the same fixed-width integral or Date type. If either
column is nullable, every binding reference must be a direct positive top-level
Filter conjunct. The binding has no dependency, `OuterBind`,
`AddDependencies`, observable `EnsureAtMostOne`, staging, or additional
consumer. Its root may reference closed uncorrelated scalar bindings and
closed leaf `IN` bindings. C++ and Python independently require every consumer
operator to belong to exactly one plan root. Each nested `IN` consumes no
binding, so self-reference, cycles, and depth greater than one fail closed.
Structural root nesting, a correlated scalar, and every other nested
owner/kind also fail closed.

For every present consumer row, dynamic `IN` ORs equality with every present
inner row. This is exact existential membership: duplicates do not multiply
the outer row and empty inner input is false. For the original non-null slice,
consumer `NOT` may supply the anti-membership form. For the nullable-integral
slice, an inner NULL cannot make the positive Filter true and a NULL lookup
cannot match: SQL FALSE and UNKNOWN both reject the outer row, exactly matching
the encoded OR of present non-NULL equal pairs. Consequently `NOT`, `OR`, and
embedded nullable-binding uses fail closed instead of silently collapsing
UNKNOWN to FALSE. The cached subplan family is shared by repeated binding
references, while an error inherited from the inner root remains eager even
when the outer input is empty. The evaluator rejects more than 16,384
outer/inner membership pairs cumulatively before construction. A nested scalar
uses the ordinary cached zero/one/many-row semantics; its new cardinality error
is demanded by its immediate consumer inside the `IN` root, while an inherited
error remains eager. A nested `IN` recursively evaluates the same cached
membership family before the outer level and shares the cumulative pair
budget. Tuples,
coercions, `Utf8`, Bool, Decimal, nullable `String`, correlations,
multiple consumers, and malformed or mismatched lookup/output mappings fail
closed. Date membership uses the existing exact bounded
`[0, NUdf::MAX_DATE)` domain.

The String extension's focused dynamic-`IN` checks pass 16/16 in Python, 4/4
in the C++ exporter, and 1/1 through the real host. The integration case proves
initial String `IN` equivalent to final `left_semi` at two rows and two tasks;
exhaustive finite-domain tests cover String duplicates, empty inputs,
anti-membership, presence, and bounded reference equivalence. The exact
nullable Date-year projection bridge is also implemented and tested
independently as described above. The nullable-integral extension adds
independent NULL, duplicate, empty-input, shape-rejection, and `left_semi`
checks plus a real-host positive nullable `IN` fixture. The Date extension
reuses those exact truth semantics, adds independent Date nullability and
bounded-domain checks, and proves a real-host nullable-Date `IN` equivalent to
`left_semi`. TPC-DS q58 exercises the closed scalar-inside-`IN` contract and
now emits a formula. Independent nested-`IN` evidence includes an exhaustive
finite reference, nullable positive-Filter and eager-error cases, cache and
cumulative-pair checks, a bounded proof against two sequential `left_semi`
joins, and a counterexample when the inner membership is omitted. Other
subplan combinations, multiple dependencies, broader dynamic-`IN`
correlations, nullable `String`, nullable anti-membership or embedded Boolean
contexts, coercing dynamic `IN`, broader read-range grammars, and other OLAP
pushdowns remain separate extensions. Cardinality-certified integral `AVG`
Slice A is complete for q7/q13/q26, and exact fixed-width integral extrema are
complete for q35. Narrowly tagged derived-`Double` ordering is complete for
q22/q85. The next slice targets exact dynamic `Optional<Date>` plus/minus
literal `IntervalFromDays` normalization for TPC-DS q72.
The auditability consolidation is complete in commits `7a3639d1c16`,
`ebcfdbb1263`, and `4b7f27d492e`. The checked-in proof policy added TPC-DS q95
after the earlier TPCH q18 addition, then q38 and q87 through the exact
proven-total Date `Unwrap` gate. The two-dependency `EXISTS` slice now adds
TPCH q21 and TPC-DS q16/q94, producing an 11-query TPCH and 14-query TPC-DS
bounded proof floor.

The audit has found nine production optimizer defects. A stale negation flag could
turn a later positive `EXISTS` into `NOT EXISTS`; its focused regression and fix
are committed in `95a2afad1d3`. The missing scalar-cardinality enforcement made
a two-row scalar subquery select its first row instead of raising
`PRECONDITION_FAILED`; the enforcement fix and real-YDB regressions are
committed in `e1e3419012c`. While enabling direct scalar projection, the audit
also found that `TOpMap::GetSubplanIUs()` passed the source and destination of
`AddUnique` in reverse and that `ConvertTKqpOpMap` did not call
`RemoveSubplans` on the projection lambda. Together those plumbing defects lost
the binding metadata while leaving an inner query plan embedded in a scalar Map
expression. The same regression then exposed that an empty nonaggregate YQL
scalar projection constructed `Nothing<Int64>` instead of an optional NULL,
causing type annotation to fail. Commit `52a1d7c4084` fixes those three
projection-path defects and covers already-optional aggregation, a plain
singleton, a computed projection, zero-row NULL, and the multirow error.

The sixth defect is the converse demand case: a multirow scalar raised
`PRECONDITION_FAILED` under new RBO even when the outer consumer was empty,
where legacy execution correctly returned no rows. The generated cardinality
check ran eagerly in an independent scalar producer. Commit `9e50d234264`
bounds the scalar input, observes its direct cardinality only after crossing
one demanded outer row, and renames colliding outer/scalar IUs. At that
production-fix checkpoint, `ScalarSubplanEvaluationTest` passed 14/14, and the
real-host `KqpRboYql::ExpressionSubquery` test passed 1/1 with the new
empty-consumer and same-IU cases plus its existing cardinality cases. The
prerequisite shared-input rule correction is kept separate in `a51c2459ad5`;
its two direct Limit-pushdown regressions pass 2/2 for shared Read and Sort
inputs.

The seventh defect was confirmed with a reliable warmed paired real-host probe
of `nested_empty_outer.sql`. Its inner scalar has a nonempty immediate consumer
and more than one row, but the top-level consumer is empty. Legacy execution
raises `PRECONDITION_FAILED` with “More than one row in a scalar subquery”.
Before the fix, two warmed default-CBO new-RBO runs instead deterministically exited
successfully with an empty result JSON beginning
`{"columns":[{"name":"value",...}]}`. Model commits `125962c87df` and
`1aaf281c07a` correct inherited-error separation and shared enumerated choices,
respectively, exposing the mismatch.

CBO had commuted the order-sensitive synthetic scalar Cross. Physical Cross
drains its right input first, so the commuted empty outer input could prevent
evaluation of the inherited scalar error. Optimizer commit `cab0dd1e89c` marks
both synthetic Crosses `PreserveInputOrder`, makes BuildInitial/Expand CBO stop
at those barriers while still optimizing both sides, and prevents filter
absorption through them. At that checkpoint the two direct rule regressions
passed 2/2, the real-host `KqpRboYql::ExpressionSubquery` test passed 1/1 with
the CBO2 nested case, the full `cpp_ut` passed 165/165, and the affected Python
gates passed 507/507. Defect seven is fixed.

The eighth defect is a correlated scalar `COUNT(*)` empty-input mismatch.
Before correlation pull-up, its keyless Aggregate produces the required
non-NULL zero for an empty input. Pull-up adds the correlation key, so the
Aggregate becomes grouped and emits no row for an unmatched outer key.
Scalar inlining then exposes the absent right side of its left join as NULL
instead of restoring COUNT's zero identity. The real-host
finding is preserved separately in commit `605dca7e9f0`: its regression
required `COUNTEREXAMPLE` at row and task bound two and checked for an
unmatched outer row without pinning solver-chosen cell values.

The production repair records explicit originally-keyless provenance before
pull-up changes the Aggregate shape, traces only unique exact Member aliases
to the selected direct COUNT trait, and computes
`Just(Coalesce(joined_count, Uint64(0)))` after the left join. The `Just`
retains the scalar binding's `Optional<Uint64>` type. The semantic exporter
normalizes only this exact generated shape to existing `if`/`if_present`
semantics. The original finding now returns `VERIFIED_BOUNDED`; real execution
covers projection and Filter consumers, while an originally grouped COUNT
retains NULL for a missing group. Computed post-aggregate empty-row expressions
such as `COUNT(*) + 1` require general reconstruction and fail closed in the
new-RBO inliner. Legacy fallback shares this broader risk, so fallback is not
claimed as a semantic repair. This focused fix does not change the benchmark
dashboard counts.

The ninth defect is the shared-IU String-`IN` result loss first exposed by the
q56 and q60 solver candidates. Both candidates had one production root cause
in `TPushFilterIntoJoinRule`: IU membership alone was used to turn an equality
into an additional semi-join key, even when one endpoint name was present on
both inputs and therefore did not identify a side. The predicate that belonged
on the selected left input was instead consumed as a second `LeftSemi` key.
With CBO explicitly disabled, the paired real-YDB finding returned
`("same", 10)` under legacy optimization and zero rows under new RBO; commit
`6a2c3acb29b` preserves that pre-fix diagnostic.

Commit `98176b0b48c` requires each extracted join-key endpoint to belong
exclusively to its declared side, leaving ambiguous equalities to the existing
side-routing logic, and adds the direct rule regression. Commit `4f73b38aaaf`
retains the nonmanual production runtime regression. After the fix, the two old
witnesses are `WITNESS_NOT_REPRODUCED` and focused 60-second q56/q60 solver
runs are `UNKNOWN`; the SHA-bound timings and report digest are recorded above.
Neither query entered the proof floor at that source checkpoint. Formula
coverage there remained
53/121 overall, 53/93 among optimizer-successful queries, and 53/59 among
verifier entrants, with 20/121 bounded proofs.

An additional legacy probe placed
`Ensure(foo.id, false, "inner scalar error")` inside the scalar producer; it
also raised `PRECONDITION_FAILED` despite the empty top-level consumer. This
confirms that inherited intrinsic errors share the eager contract.

The focused
`test_inherited_scalar_error_is_observed_without_a_consumer_input_row`
regression preserves the corrected distinction.

The explicit ordinary comparison core accepts every pair drawn from signed and
unsigned 8-, 16-, 32-, and 64-bit integers for equality, null-safe equality,
and ordering. MiniKQL `DataCompare` uses sign-aware mathematical-integer
semantics for these pairs: a negative signed value remains below every unsigned
value, rather than being converted or wrapped to the unsigned width. The SMT
carrier is already a mathematical integer, and the exact declared-width domain
constraints on literals, source cells, and non-null opaque results make that
encoding exact: width `w` is constrained to `[-2^(w-1), 2^(w-1)-1]` for signed
types and `[0, 2^w-1]` for unsigned types. Ordinary equality and ordering return
SQL NULL if either operand is NULL. Null-safe equality is always non-null: two
NULLs are equal, one NULL is unequal, and two values use the same mathematical
comparison.
Static `IN` deliberately retains its separate lossless-common-type gate above.
`String` and `Utf8` are mutually compatible under the raw-byte comparison
above. Date comparison requires Date on both sides.

This expansion removes TPC-DS q8's former `Uint64 > Int32` snapshot blocker.
The focused real-host run prepared q8 in 480 ms, then failed closed before
verifier construction (`verify_ms = 0`) on unsupported scalar callable
`Unwrap` at both the initial and final boundaries. It therefore changes neither
the then-current formula-construction slice nor the proof floor.

Ordinary Decimal `=`, `<`, `<=`, `>`, and `>=` are strict on NULL; NaN makes
every ordinary comparison false, and infinities participate in the YDB order.
Decimal/Decimal and Decimal/integer operands use the same scale alignment as
YDB `DataCompare`: integers first receive their declared decimal width, scales
are raised, precision is capped at 35, and narrowing or scale-up saturates to an
infinity exactly where the YDB Decimal conversion does. Alignments that would
require an invalid zero-precision Decimal fail closed. Null-safe Decimal
equality is admitted only for exactly identical Decimal types and compares the
encoded non-null values, including NaN; its usual both-NULL/one-NULL behavior is
unchanged.

Decimal Sort, TopSort, and Merge deliberately use a different comparison.
MiniKQL orders the raw signed 128-bit codes, giving the total non-null order
`-Inf < finite values < +Inf < NaN`; descending reverses it. NaN therefore ties
with NaN instead of making the ordering predicate false. Every order item keeps
one exact canonical `Decimal(p,s)` identity. Separate tuple keys may have
different Decimal identities, but one key is never scale-aligned as if it were
a scalar `DataCompare` expression. Explicit `nulls_first` remains orthogonal to
that non-null order at the selected pre-physical boundary.

Decimal arithmetic is deliberately canonical and narrow. The exporter accepts
only binary `+` and `-` with both operands and the result having one exact
canonical `Decimal(p,s)` type, or binary `DecimalMul`/`DecimalDiv` whose left
operand and result have that exact Decimal type and whose right operand is
either the same Decimal type or a signed/unsigned 8/16/32/64-bit integer.
Result nullability must be exactly the OR of operand nullability, and the
subtree must still pass the reviewed closed-world scalar checks. These
callables normalize to explicit `add`, `sub`, `mul`, and `div` snapshot nodes.
An integer is accepted only on the right of `DecimalMul` or `DecimalDiv`; YQL
canonicalizes the supported reversed multiplication spelling before this
boundary.

Evaluation matches `NDecimal` scaled-integer behavior and is strict on NULL.
Addition and subtraction preserve the common scale. Same-type Decimal
multiplication rescales the coefficient product by `10^s` with round-to-nearest,
ties-to-even for both signs; an integer right operand multiplies the coefficient
without rescaling and therefore preserves the Decimal scale. NaN propagation,
indeterminate opposite-infinity addition/same-infinity subtraction,
infinity-times-zero, signed infinities, and finite precision overflow are
explicit. Finite overflow saturates to the appropriate infinity before an
in-band NaN code can be mistaken for a calculated NaN.

Same-type Decimal division multiplies the left coefficient by `10^s` before
division; an integer right operand divides the coefficient directly. The model
matches `NDecimal::Div`'s current signed-remainder behavior rather than assuming
algebraic sign symmetry: positive divisors round to nearest with ties to even,
negative-divisor non-ties truncate toward zero, and exact ties still round to
even. Zero divisors, NaN, signed infinities, global 35-digit normalization,
result-precision saturation, and finite collisions with the reserved NaN code
are explicit.

The arithmetic kernel is checked against independent rational and literal
NDecimal-control-flow references on every legal finite code and all specials
for precisions up to two. Adversarial cases cover both signs of ties-to-even,
negative-divisor non-ties, every integer width, infinity-times-zero, precision
overflow, and finite products or quotients numerically colliding with the NaN
code. C++ exporter tests audit the admitted and rejected signatures. Separate
solver tests send unchanged `add`/`sub`/`mul`/`div` through the normal
logical-to-StageGraph obligation and require `VERIFIED_BOUNDED`; mutations
between those operations must produce concrete counterexamples.

Opaque identity is an inspectable `yql-opaque-v1` canonical string, not a hash.
It preserves exact callable/atom bytes, normalized atom flags, formatted types,
child order, constants, settings, and repeated arguments. Input IU names are
replaced by first-use ordinals and emitted separately as the ordered `args`, so
the same expression remains one UF across optimizer renames while swapped or
repeated values retain their meaning. Positions, allocations, and DAG sharing
do not affect identity. The representation fails closed above 256 expanded
nodes, depth 64, or 64 KiB.

Every complete normalized scalar tree has a separate 1,024-expanded-node and
128-level structural-depth budget, with its root at level one. Repeated source
DAG uses count as separate emitted occurrences. A scan or filter predicate,
each projection, and each literal count or offset reset the budget; generated
side-explicit join-key equalities and residuals share one effective
conjunction budget, including its implicit conjunction,
while chained pushed OLAP filters share one assembled scan-predicate budget.
The C++ exporter charges before expansion, guards source recursion, and then
audits the completed normalized JSON iteratively. The Python parser
independently enforces the same contract and turns excessive JSON decoder
nesting into a normal fail-closed error; replay's strict loader does likewise.
The opaque fingerprint limits above, the 512-item static-`IN` limit, and the
64-live-`IfPresent` binding limit remain independent.

The chosen final boundary is immediately before `ConvertToPhysical`. Therefore
the verifier does not prove physical lowering, task construction, or execution.
In particular, the current lowering does not visibly preserve
`TSortElement::NullsFirst`; explicit NULL ordering remains a replay case until
that contract is clarified.

Sort order is an exact, non-empty sequence of `(column, ascending,
nulls_first)` entries. Small inputs are represented by explicit permutations.
For larger inputs, the bounded evaluator gives each syntactically live row slot
an integer ordinal. A slot is syntactically live unless its guard is the literal
`false`; fixed-false padding uses constant ordinal zero and consumes no choice
or pair budget. A symbolically guarded slot still counts as live and is forced
to zero only when its guard evaluates false. Present-row ordinals are in range
and pairwise distinct, and strict key comparisons imply the corresponding
ordinal order. Tied keys remain free in either order, so the symbolic encoding
denotes the same complete sequence set without factorial expansion. Small
explicit permutation and interleaving selection continues to use the full
shaped row vector. A non-null Sort `limit` is TopSort and applies an exact
prefix by compressed ordinal rank after sorting. Sort and Limit phases
(`undefined`, `intermediate`, and `final`) are preserved but are not otherwise
semantic.
Project nodes carry the exact `TOpMap::Ordered` Boolean. Both `ordered: true`
and `ordered: false` currently preserve an input sequence and compatible order
metadata. This matches RBO's streaming WideMap lowering; retaining the exact
flag makes a future semantic distinction an explicit verifier change.
Map rename sources are removed from pass-through as a set. The same existing
source may feed more than one distinct, nonempty target, in which case each
target receives an exact copy and is appended in operator order. Missing
sources and duplicate or empty target names still fail closed.

Limit on an ordered input takes the exact `offset:offset+count` slice of the
compressed present-row sequence. If the initial root is ordered, equivalence is
sequence equality; an unordered initial root retains bag equality. The Merge
encoding is exact within the declared row/task/family bounds: it rejects an
unordered or differently ordered producer and represents all sorted,
producer-order-preserving interleavings. Symbolic Merge ordinals preserve the
relative input ordinals within each producer as well as the output sort order.

Every plan choice carries an explicit finite bound. Symbolic ordinal bounds use
the syntactically live slot count, not the shaped row-vector length. When result
languages are compared, one side's bounded choices describe a candidate result
and the other side's choices are existentially quantified inside the membership
test; the reverse direction is checked as well. The SMT renderer shares
repeated DAG terms through hygienic, dependency-ordered `let` bindings
separately inside each quantifier scope, never hoisting an expression past a
binder. Immutable SMT terms cache their full structural hash at construction;
term equality remains exact structural equality, so a hash collision cannot
identify distinct terms. Equality traverses deep DAG pairs iteratively and
tracks already-compared identity pairs, preserving exact argument order and
runtime classes without relying on Python recursion depth. The cache changes
set/dictionary lookup cost only and does not add a semantic quotient or
approximation. These are exact finite encodings and rendering transformations,
not unbounded ordering proofs or semantic approximations.

`String` and `Utf8` comparison, Sort, TopSort, and Merge use the exact bounded
byte-order quotient above. Date is an exact bounded integer-day type with
literals, equality, ordinary ordering, Sort, and Merge. Decimal has exact
literals, its legal typed domain, and the comparison and arithmetic semantics
above, plus exact raw-code Sort/TopSort/Merge ordering. Same-type fixed-width
integral division has the exact nullable semantics above. Floating-point,
mixed-type, and other division forms, casts outside the exact
integral-`SafeCast` and constant normalization gates, and aggregate functions
outside the modeled subset below remain unsupported.

The aggregate subset covers grouped and scalar `count`, integer `sum`, Decimal
`sum`, Decimal-only `min`/`max`, phase-aware same-type Decimal `avg`, row-level
`DistinctAll`, one exact direct scalar Int64 count-distinct shape, and one
exact scalar-final Uint64 sum-unwrap shape, including NULL grouping and inputs
and optimizer-generated intermediate/final phases.
Signed inputs widen to `Int64`, unsigned inputs widen to `Uint64`, and both
integer sums use the runtime's exact 64-bit modular overflow.
`sum(Decimal(p,s))` widens every input, partial state, and result to
`Decimal(35,s)` and preserves YDB's NaN/infinity algebra.
Decimal `AggrMin` and `AggrMax` keep their input type and use the runtime's raw
signed 128-bit-code order, `-Inf < finite < +Inf < NaN`; this is intentionally
different from ordinary Decimal comparison. They respectively select the least
and greatest non-NULL input. A group with no non-NULL input returns NULL, while
a lone NaN remains NaN. The same scalar state combines exactly across partial
and final phases.
Independent small-domain references cover guarded raw codes, NULL, every
special, grouped/scalar and split-task execution, wrong shuffle keys, and
phase nullability. Mutating a staged final `min` to `max` produces a two-row
solver counterexample.

Decimal `AggrAdd` saturates each intermediate result and is not associative when
finite overflow is possible. The verifier therefore carries a conservative
absolute finite-code bound through partial states and admits a Decimal sum only
when the total bound is strictly below `10^35`. Within that headroom every row
order and distributed parenthesization has the same exact result; otherwise the
query fails closed. A known bound covers every non-NULL finite coefficient; it
does not approximate NaN or infinities, whose exact value terms remain separate.
Finite literals seed their absolute coefficient, while typed NULL and special
literals seed the vacuous zero bound. Exact same-type `+` and `-` use the capped
sum of operand bounds, and an exact integral-to-Decimal cast derives its bound
from the complete signed or unsigned source-type domain, target scale, and
finite saturation point. If the left Decimal coefficient has bound `B`,
`DecimalMul` by an integral right operand uses
`min(decimal_max, B * max_abs(right_type))`; `DecimalDiv` by an integral right
operand retains `B`, because every nonzero divisor has magnitude at least one
and zero produces only a special result. Same-Decimal multiplication/division
still has no propagated bound. `If`/`IfPresent` branches and stage alternatives
take conservative maxima, aliases preserve bounds, and Decimal `SUM` states
accumulate them. Any missing operand proof remains unknown and can only make a
later sum fail closed.

A column-storage source is split into symbolic source tasks before a pushed
intermediate aggregate executes. The separate manual runtime
diagnostic deliberately crosses this rejected boundary with the same three
valid rows in one- and two-partition column tables. Both optimizer modes return
`M` in the former case and `inf` in the latter, confirming why the verifier must
not assume associativity outside the proved headroom.
The optimizer's canonical `COUNT(*)` extractor is represented by the exact
zero-child, typed `Void` expression and evaluated as one non-null unit value;
it may pass through routing and relational operators, but every path must
terminate in a non-distinct, non-unwrapped `count`. Inspecting, dropping, or
exposing that unit fails closed. `Void` is not a catalog, literal, NULL, or
opaque-result type.
`DistinctAll` accepts a nonempty ordered key list with exactly one positional
plain `distinct` alias per key. Each alias must preserve the key's exact type
and nullability; within this shape, trait-level `distinct`/`unwrap` flags fail
closed. Evaluation emits one representative renamed key tuple per null-safe
composite group, so empty input remains empty and duplicate NULL-containing
tuples collapse.
Undefined, intermediate, and final phases use the same local deduplication
semantics. StageGraph routing remains observable: partial per-task
deduplication followed by HashShuffle on every intermediate key and final
deduplication is equivalent, while a non-shuffled split can expose duplicate
rows normally.

Outside `DistinctAll`, `distinct` is exact only for one keyless,
phase-`undefined`, non-unwrapped
`count(non-null Int64) -> non-null Uint64` trait per Aggregate. Other ordinary
traits may coexist. For each present input slot, evaluation counts the value
only when no earlier present slot is equal, so duplicates collapse and empty
input returns zero. The exact upper-triangular comparison count
`N*(N-1)/2` is charged before construction; crossing the relation-pair ceiling
returns `UNSUPPORTED`.

`unwrap` is exact only for a keyless, final, non-`DistinctAll`, non-distinct
`sum(Optional<Uint64>)` whose raw trait output is
`Optional<Uint64>`. The snapshot retains that raw nullability for auditing,
while validation exposes a non-null effective node column and evaluation
returns zero for empty or all-NULL input, matching the physical builder's
coalesce. The wrapping and non-wrapping sum cases are independently tested so
the contract cannot silently spread to ordinary nullable `SUM`.

All other ordinary aggregate `distinct` or `unwrap` shapes, non-Decimal
min/max, non-Decimal or distinct average, and variance remain `UNSUPPORTED`.
Intermediate aggregation models the pre-physical logical state per task and
key; memory-pressure batching performed later by a physical hash combiner is
outside the snapshot boundary.

Any literal zero-offset Limit whose count is at least the syntactically live
candidate-row bound is an exact identity, including statically dead shaped
rows. Otherwise, an unordered `Limit` is not modeled as an arbitrary fixed
vector prefix. For a
nontrivial `Take(1)`, each source outcome has at most one bounded selector and
one conditional output row; empty and single-candidate cases require no
selector. The row is present exactly when the input's present-row count exceeds
the offset; when present, the selector must name a syntactically live present
slot. The selector conditionally chooses the typed value, NULL term, and hidden
Decimal AVG state. Static proof bounds are conservatively joined, occurrence
becomes unknown, and only partition facts common to every syntactically live
candidate survive. Other unchecked nontrivial counts use every exact
guarded-row mask of size `min(count, max(input_size - offset, 0))`.

With `ensure_at_most_one`, the exact zero- and one-row output language is
retained. In the unordered mask representation, every greater-than-one mask for
one source outcome is quotiented into one error outcome with an unobservable
all-false payload. When offset is zero and count is greater than one, masks are
not constructed; checking the input family directly is exact. Plans are
equivalent only when their sets of enabled output bags mutually include one
another. Bounded choices are carried through the DAG, so two uses of one Limit
node share a choice, while stage-task instances choose independently. Count
and offset are restricted to non-null `Uint64` literals in v1; parameterized or
otherwise computed limits fail closed. Phase is preserved as `undefined`,
`intermediate`, or `final`, but does not itself change runtime semantics.
Distinct Limit observers downstream of one shared unordered plan stream fail
closed until their latent-order correlation is modeled. Ordered Limit is
deterministic. Aggregate and Join start new unordered streams. Unordered
UnionAll does too, while ordered UnionAll gives each input every legal local
sequence and concatenates the complete left sequence before the right.

Every logical `union_all` node has a required Boolean `ordered` field. The
strict decoder rejects absence or non-Boolean values, keeping logical stream
ordering separate from the StageGraph connection’s independent `parallel`
routing field.

```json
{
  "id": "n2",
  "op": "limit",
  "input": "n1",
  "count": {"kind": "literal", "type": "Uint64", "value": 10},
  "offset": null,
  "phase": "final"
}
```

The current exporter emits `pushed_limit` on every scan, either `null` or the
same exact literal shape. Its absence in a legacy-v1 snapshot means `null`,
because the earlier v1 exporter rejected every pushed read limit. A non-null
pushed limit is valid only on a column-storage source and executes independently
on each source task after symbolic row partitioning.

The exporter also emits `predicate` on every scan, with legacy absence meaning
`null`. A non-null predicate is decoded from the executed `OlapFilterLambda`
rather than `OriginalPredicate` statistics metadata. Version one accepts only a
one-argument chain of `KqpOlapFilter` operations ending at that exact argument;
its scalar subset is read-column references, supported literals, Boolean
AND/OR/NOT, the equality and ordering families described above (including
all-pairs ordinary integral `DataCompare`), and exact presence tests. A
`TKqpOlapFilterUnaryOp` is admitted only as an exact two-child tuple whose
operator is the Atom `exists` or `empty`; its recursively decoded argument is
lowered respectively to `exists(x)` or `not(exists(x))`. A non-Atom or unknown
operator (including `just`) and an unavailable read column fail closed. Each
physical read name, full output-IU name, and short output-IU name resolves to
the corresponding logical scan output. A referenced identifier that maps to
distinct outputs is ambiguous and fails closed; an unused ambiguous spelling
does not affect the predicate and is accepted.
`Coalesce(predicate, false)` is erased only in a positive filter context: at
the filter boundary or beneath AND/OR. The same form beneath NOT, comparison,
or a unary presence operation fails closed because the erasure is not
value-preserving there. Projection wrappers, range callables inside an OLAP
filter, malformed type descriptors, and unknown operations also fail closed.
The predicate filters raw
scan rows before symbolic source partitioning and any per-task pushed limit.

Every relation is capped at 4096 candidate rows. Join matrices and outputs,
UnionAll, and grouped aggregation are checked before construction; Sort, Merge,
and latent-sequence pair preflights may charge at most 16384 audited pairs of
syntactically live slots before permutations or ordinals are allocated. Sort
and concrete-order Merge can instead use an exact bitonic network with at most
32768 comparators and 131072 comparator/column pairs. Intermediate TopSort
uses it for compaction only when retaining both fixed verifier tasks' shaped
slots would make the downstream Merge exceed the pair cap.
Representation selection and small explicit permutations/interleavings remain
based on the full shaped row vector. A small Sort is enumerated only when its
input family has one outcome; an already-alternative family uses exact bounded
ordinals so Sort does not multiply those alternatives. Every explicit outcome
family is separately capped at 256 alternatives, including non-singleton
unordered-Limit masks, canonical checked-error outcomes, at-most-three-row
sequence choices, Cartesian products, and gathers. Nontrivial unordered
`Take(1)` instead uses at most one bounded choice per source outcome. Other
ordered families use bounded symbolic ordinals. Network output has a
present-prefix invariant, so ordered Limit slices physical slots directly;
Filter, repartition, and multi-input gather conservatively clear that metadata.
Cross-plan bag or sequence equality is capped at 4096 explicit outcome pairs.
Exceeding any audit bound returns `UNSUPPORTED` rather than allocating an
unbounded intermediate or approximating semantics.

Grouped aggregation first gives every complete ordered group-key value an exact
nonrecursive structural signature. For `N` input rows and `K` distinct
signatures, the repeated-class representation is eligible only when `K < N`
and both its `K*N` memberships and `K*(K+1)/2` symmetric comparisons fit the
pair ceiling. It is selected above the directional cap, or below it only when
their combined count is strictly less than `N^2`. Membership still ranges over
all original rows; presence and first-representative suppression remain
directional. If classes are ineligible, the established `N^2` formula is
retained while it fits, and the exact singleton-class upper-triangle fallback
is used above that point. No directional predicate is treated as symmetric and
no bag occurrence is discarded.

Aggregate nodes preserve ordered keys and traits, output type/nullability, and
phase explicitly:

```json
{
  "id": "n1",
  "op": "aggregate",
  "input": "n0",
  "keys": ["t.k"],
  "aggregates": [{
    "input": "t.v",
    "function": "count",
    "output": "cnt",
    "type": "Uint64",
    "nullable": false,
    "distinct": false,
    "unwrap": false
  }],
  "phase": "intermediate",
  "distinct_all": false
}
```

Decimal `avg` is the only aggregate trait with an additional field:

```json
"state": {
  "sum_type": "Decimal(35,2)",
  "count_type": "Uint64",
  "nullable": true
}
```

The input and output must both be the same canonical `Decimal(p,s)`;
`sum_type` must be `Decimal(35,s)`, `count_type` must be `Uint64`, and state
nullability must equal input nullability. Intermediate AVG state is accepted
only on one direct intermediate-to-final aggregate lineage with the same
ordered keys and identical metadata.

## StageGraph v1 shape

The strict decoder rejects unknown fields and missing required fields, except
for the documented legacy-v1 compatibility cases above. The abbreviated shape
is:

```json
{
  "root_stage": "s2",
  "stages": [{
    "id": "s0",
    "nodes": ["n0"],
    "inputs": [],
    "outputs": [{"index": 0, "node": "n0"}],
    "source_storage": "row"
  }],
  "edges": [{
    "id": "e0",
    "producer": "s0",
    "consumer": "s2",
    "occurrence": 0,
    "producer_output": 0,
    "consumer_input": 0,
    "kind": "hash_shuffle",
    "keys": ["a.k"],
    "hash_function": "HashV1",
    "use_spilling": true
  }],
  "assumptions": []
}
```

Map and Broadcast add no variant fields. UnionAll adds `parallel`. Merge adds
`order`, whose elements contain `column`, `ascending`, and `nulls_first`.
`source_storage` is `row`, `column`, or `null`. Version one requires an empty
`assumptions` array.

## Development setup

The Python kernel has no package dependencies. Its CLI accepts an explicit
Z3-compatible executable when solving a formula. Tests run through `ya` instead
resolve the pinned Z3 4.16.0 target under `contrib/tools/z3`; no ambient solver
installation or `RBO_Z3` setting can affect those hermetic test runs.

```bash
python3 -m unittest discover -s ydb/core/kqp/opt/rbo/verification/ut
```

Run its tests with:

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/ut 2>&1 | tail
```

The `ya` test target always builds and runs the solver integration tests. The
raw `unittest` command above still supports lightweight development outside the
`ya` environment: it skips solver cases unless `RBO_Z3` names an external
compatible executable.

The real-host integration tests construct a new-RBO `IKqpHost` and send each
captured initial/final pair through the normal CLI. This target links the
production PostgreSQL translator/runtime: the dummy provider failed to prepare
the valid dynamic-`IN` fixture and is not a faithful host for this boundary.
One test covers an explicit
`LIMIT 1` and its split intermediate/final form. The ordered test covers
`ORDER BY A DESC, B ASC LIMIT 1`: it checks the initial Sort+Limit, final
per-task intermediate TopSort, exact Merge metadata (including NULL placement),
and final Limit. Column-store tests compare initial logical Filters with final
pushed OLAP predicates, cover `IS NULL` combined with comparisons and
`IS NULL OR IS NOT NULL`, and require the normal bounded proof. A String
predicate fixture combines `EndsWith` and `StringContains`, requires their
canonical fingerprints at both generic and pushed OLAP boundaries, and is
`VERIFIED_BOUNDED` at two rows and two tasks. The benchmark test loads the
exact `TPCDS_YQL` schema and q96 source used by the new-RBO suite,
checks its split `COUNT(*)`, pushed predicates, four-table join, and StageGraph,
and proves the two-row/two-task obligation with a query-specific 60-second
solver budget. Nullable `String IN ('first', 'second')` and
`Int64 IN (Int32...)` expressions exercise both static-membership type gates
through the real host and prove the normal obligation. A separate uncorrelated
dynamic-`IN` fixture captures a typed non-null integral lookup/result
descriptor initially and an ordinary `left_semi` join finally, then proves the
two-row/two-task obligation. A second fixture captures independently nullable
`Int64` lookup/result columns used as a positive Filter conjunct, checks the
same `left_semi` lowering, and proves the normal obligation. A Date fixture
does the same for independently nullable Date lookup/result columns, retaining
the positive-Filter restriction and proving the bounded obligation. A
nullable-Date projection fixture checks the
exact `Date -> Timestamp -> DateTime2.Split -> DateTime2.GetYear` normalization,
including the `yql-datetime-year-v1` opaque node and explicit Optional lift in
both snapshots, then proves the two-row/two-task obligation. A separate
`COUNT(*) > 1` query captures ordinary `Uint64 > Int32` comparison at both
real-host boundaries and returns `VERIFIED_BOUNDED` at two rows and two tasks.
A native Decimal column filter likewise checks exact tagged literals and
comparison predicates at both real-host boundaries and proves its normal
two-row/two-task obligation.
A Decimal cast query checks `COUNT(*)` and `1 + COUNT(*)` as non-null `Uint64`
expressions cast to `Decimal(15,4)` at both boundaries, then proves the normal
two-row/two-task obligation.
A Decimal arithmetic query checks `+`, `-`, same-type multiplication and
division, integer-right multiplication and division, and YQL's normalization
of the reversed multiplication spelling at both boundaries, then proves the
two-row/two-task obligation. All equivalent real-host fixtures require
`VERIFIED_BOUNDED` from the hermetic solver. A
Decimal aggregate query checks the `Decimal(7,2)` to `Decimal(35,2)` widening,
undefined/intermediate/final `sum` phases, and serial partial-state UnionAll,
then proves its two-row/two-task obligation. A
separate Decimal ordered query covers logical Sort+Limit and the transformed
per-task TopSort+Merge+final-Limit path. That proof ends before physical
lowering, consistent with the boundary caveat above.

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/integration_ut 2>&1 | tail
```

Build the Ya-owned CLI with:

```bash
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/bin
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/confirm_bin
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/inspect_bin
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/replay_bin
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/prefix_capture/bin
./ya make --build relwithdebinfo contrib/tools/z3
```

## CLI

```bash
PYTHONPATH=ydb/core/kqp/opt/rbo/verification \
python3 -m rbo_verifier before.json after.json \
  --rows 2 --timeout-ms 10000 --solver /path/to/z3
```

The command prints a JSON verdict. A `COUNTEREXAMPLE` verdict contains only the
present base-table rows; opaque-function and symbolic-routing interpretations
and bounded plan-choice valuations, including an unmatched unordered-singleton
selector, are deliberately not treated as a stable witness format, so concrete
replay is the confirmation boundary. Every bounded verdict reports both
`row_bound` and the fixed `task_bound` of two. A
`SCHEMA_MISMATCH` verdict is a direct correctness failure and does not depend on
either bound. Use `--emit-smt formula.smt2` without `--solver` to inspect the
exact canonical proof obligation. The file is intentionally the stable grouped
formula, not a transcript of the equivalent internal solver portfolio, so its
standalone solve can have different performance. If satisfiability succeeds but
model extraction returns unknown, the result remains `COUNTEREXAMPLE` with a
reason and no witness.

Normal verification requires a logical initial snapshot and a final snapshot
with a complete StageGraph. The separate localization path may compare that
same logical initial snapshot with a captured logical or complete staged
transformation prefix:

```bash
PYTHONPATH=ydb/core/kqp/opt/rbo/verification \
python3 -m rbo_verifier initial.json prefix.json \
  --diagnostic-transformation-prefix --rows 2 --solver /path/to/z3
```

This mode uses the same formula kernel but an explicitly different boundary
contract. Every result, including errors, carries
`"comparison_scope":"OPTIMIZER_TRANSFORMATION_PREFIX"`; it is evidence about one
optimizer prefix, not a whole start-to-finish verdict. A standalone sequential
localizer drives this mode because equivalence is not monotonic across rule
applications or atomic stage commits and therefore cannot be bisected soundly
with binary search.
`kqp_rbo_prefix_capture` supplies the real-host capture side of that protocol;
see [tools/README.md](tools/README.md) for its strict artifact contract and the
complete localizer invocation.

## Plan and counterexample inspection

The separate `kqp_rbo_inspect` executable renders every modeled snapshot field
as deterministic line-oriented text:

```bash
ydb/core/kqp/opt/rbo/verification/inspect_bin/kqp_rbo_inspect \
  plan final.snapshot.json
```

Its `witness` command rebuilds and solves the normal start-to-finish obligation,
then prints the candidate database and concrete result family at every logical
operator, stage task, connection input, and compared root boundary:

```bash
ydb/core/kqp/opt/rbo/verification/inspect_bin/kqp_rbo_inspect \
  witness initial.snapshot.json final.snapshot.json \
  --query exact-query.yql \
  --solver /path/to/z3 --rows 2 --timeout-ms 10000
```

The inspector observes immutable SMT terms during normal evaluation. Only after
the obligation is complete does it add definitional aliases for model
extraction. Base rows, routing-dependent rows, opaque scalar results,
nondeterministic outcomes, and the exact root mismatch are requested together
from one SAT model. All enabled outcomes are printed; absent rows omit their
meaningless payloads. Every enabled outcome and unmatched root record includes
its bounded plan choices as concrete `{value,bound}` pairs. Those values are
diagnostic model data, not stable verifier-witness fields or observable query
results. Replay validates every integer bound/value pair and requires each
mismatch record to repeat its referenced outcome's choices exactly, but does
not use choices to classify observable nondeterminism. A direct
inspector-through-Z3-to-replay regression covers this protocol boundary with a
nonempty unordered-Limit choice. Trace extraction fails closed above 100,000
unique terms. Enabling the read-only observers without aliases is
regression-tested to leave the normal SMT-LIB obligation byte-for-byte
unchanged. Every trace carries SHA-256 digests of the complete normalized
before/after snapshots; supplying `--query` also binds the exact query bytes
and is mandatory for real replay. The semantic digest is defined by the
complete renderer in the producing revision. If renderer coverage changes,
older artifacts whose digest no longer matches fail closed during replay; the
trace protocol version alone does not claim cross-revision digest stability.
When tracing a saved verifier candidate, `--verifier-verdict verdict.json`
constrains the rebuilt obligation to that verdict's decoded base-table rows.
The inspector may resolve routing decisions, bounded plan choices, and
opaque-function values, including values under legal bounded choices, but
cannot silently select a different database. Raw global invariants remain
choice-independent; opaque-result domains use guarded quantified invariants.
If the saved database no longer makes the obligation satisfiable, the
diagnostic status is `WITNESS_NOT_REPRODUCED`, not a global equivalence proof.

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/inspect_ut 2>&1 | tail
```

## Real-YDB counterexample replay

`kqp_rbo_replay` consumes the inspector's version-one concrete trace and runs
the exact witness against two isolated YDB targets. The baseline target must
have the legacy optimizer enabled; the candidate must have the new RBO enabled
with fallback disabled. For parity with the benchmark host, use identical
settings except for `enable_new_rbo`:

```yaml
table_service_config:
  enable_new_rbo: true # false on the baseline target
  enable_fallback_to_yql_optimizer: false
  allow_olap_data_query: true
  default_lang_ver: 202602
  default_cost_based_optimization_level: 2
  backport_mode: All
query_service_config:
  script_result_rows_limit: 0
  script_result_size_limit: 0
```

The query file must contain the exact text used for snapshot capture. In
particular, TPC-DS replay includes the benchmark's `$to_decimal`,
`$to_decimal_max_precision`, and `$round` compatibility definitions.

```bash
ydb/core/kqp/opt/rbo/verification/replay_bin/kqp_rbo_replay \
  initial.snapshot.json final.snapshot.json concrete-trace.json query.yql \
  --ydb /path/to/ydb \
  --baseline-endpoint grpc://baseline-host:2136 \
  --baseline-database /Root/baseline \
  --candidate-endpoint grpc://candidate-host:2136 \
  --candidate-database /Root/candidate
```

Before connecting, the tool strictly validates both snapshots, the trace and
witness shape, primary keys, source integer/Date/Decimal ranges, table identity
encoding, storage inference, exact backtick-quoted source paths, result schema,
and observable determinism. Legal Decimal special codes are accepted and
rendered for BulkUpsert as `-inf`, `inf`, and `nan`; invalid or out-of-precision
finite codes still fail closed. The tool accepts exactly one top-level result
query; the current TPC-DS sources q14, q23, and q39 contain two result queries
and fail closed until replay gains an explicit multi-result contract. It returns
`INCONCLUSIVE_NONDETERMINISM` when the bounded model admits more than one
distinct result for either side.

Each target receives a fresh `_rbo_replay_<128-bit-id>` namespace containing
reduced two-partition tables. Rows are loaded with the CLI BulkUpsert import
path, not an SQL write that would exercise the optimizer under test. Explain is
then run through QueryService. The current new-RBO statistics shape is required
on the candidate, the legacy/absent shape on the baseline, and every candidate
CBO tree must be optimized; this also detects transparent fallback to the old
optimizer. Results use `json-base64-array`, preserve binary strings and
duplicates, and are compared as a sequence or bag according to the initial
snapshot contract.

`REAL_RESULT_DIVERGENCE` means the two real executions differed, while
`NOT_REPRODUCED` means this concrete realization did not differ; neither result
claims unbounded equivalence. The first is strong evidence of an optimizer
correctness problem, but it is not attributed to the supplied symbolic trace
without reproducing that exact final StageGraph. The reduced catalog does not
reproduce indexes, statistics, or every physical table setting, and the
external CLI cannot recapture the final semantic snapshot, so output explicitly
records `trace_plan_reproduced: false`. Column-store sources use two hash
partitions. Row-store synthesis currently fails closed unless the leading
primary-key column is `Uint32` or `Uint64`, the types for which YDB supports
auditable two-way `UNIFORM_PARTITIONS` creation.

Replay namespaces are deliberately retained for diagnosis and are printed in
the JSON result. The tool never deletes an existing or generated YDB object.
Its audit boundary is split by responsibility: `case.py` validates the
certificate and witness, `materialize.py` renders the isolated catalog and
read-only query, `observation.py` decodes real results, and `runner.py` contains
the external mutation boundary.

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/replay_ut 2>&1 | tail
```

## Automatic counterexample confirmation

A version-four or version-five benchmark coverage report preserves the exact assembled query,
both snapshots, and byte-exact raw verifier verdict, with SHA-256 bindings, for
every symbolic counterexample. The raw verdict artifact is the authoritative
witness source; the parsed verdict in the report contains metadata only and
deliberately omits the witness so wide Decimal integers cannot be rounded by a
JSON object round trip. `kqp_rbo_confirm` consumes one such report and processes
every counterexample in query-ID order. For each candidate it validates and
copies all four bound inputs, gives the raw verdict directly to the inspector
to fix that exact database, and then invokes `kqp_rbo_replay` against explicit
isolated targets:

```bash
ydb/core/kqp/opt/rbo/verification/confirm_bin/kqp_rbo_confirm \
  /path/to/tpcds_coverage.json \
  --inspector /path/to/kqp_rbo_inspect \
  --solver /path/to/z3 \
  --replay /path/to/kqp_rbo_replay \
  --ydb /path/to/ydb \
  --artifacts /new/path/tpcds_confirmation \
  --baseline-endpoint grpc://baseline-host:2136 \
  --baseline-database /Root/baseline \
  --candidate-endpoint grpc://candidate-host:2136 \
  --candidate-database /Root/candidate
```

The artifact directory must not already exist. It retains the source report,
byte-exact copies of the four SHA-bound inputs and their digests, exact child
commands/stdout/stderr, per-query results, and one versioned summary. The driver
never stops after the first candidate. `NO_COUNTEREXAMPLES` and
`ALL_NOT_REPRODUCED` exit zero; a fully processed
`REAL_RESULT_DIVERGENCE` exits one; any missing witness, artifact violation,
inspector inconsistency, nondeterminism, setup failure, or child protocol error
makes the whole run `UNRESOLVED` and exits two. A confirmed divergence is the
input to the separate transformation-prefix localizer; confirmation itself
does not add rule-bisection machinery. Automatic replay currently accepts one
top-level result query; a counterexample for multi-result TPC-DS q14, q23, or
q39 fails closed as `UNRESOLVED` until replay gains an explicit multi-result
contract.

This command is the mandatory follow-up for every coverage
`COUNTEREXAMPLE`. It is separate from recursive tests because it writes retained
namespaces to user-supplied external YDB targets.

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/confirmation_ut 2>&1 | tail
```

## Failure records and commits

Keep every solver candidate as a SHA-bound case containing the exact query,
both snapshots, raw verifier verdict, and emitted SMT formula. Add the pinned
inspector trace, confirmation streams, real-YDB namespaces, and transformation
prefix captures as those stages run. A hand-minimized working query is useful
during diagnosis, but the durable repro is a focused test checked in with the
production correction.

Replay can reclassify a symbolic discrepancy as a verifier-model error; retain
that case as a model regression, then audit the optimizer independently because
the model bug and a real execution divergence may coexist.

Verifier/model changes are reviewed in commits separate from optimizer
changes. An optimizer fix and its focused regression normally land atomically.
Semantic and finding notes may be updated with that fix, but numerical coverage
reports change only after a complete corpus rerun. There is no intentionally
failing commit solely to record the defect—the preserved repro and a pre-fix
run against the parent revision provide that evidence without leaving the main
history red.
