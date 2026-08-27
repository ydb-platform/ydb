# Trusted core and audit map

This document defines the current proof-producing trust boundary. It is an
audit index, not a feature history or a coverage report; see
`BENCHMARK_COVERAGE.md` for workload coverage and `PLAN.md` for the full
semantic contract.

## Bounded theorem

For a strictly accepted initial snapshot `I`, final snapshot `F`, and row bound
`N`, the verifier constructs:

```text
catalog constraints
and one shared symbolic database with at most N present rows per table
and not Equal(Eval(I), Eval(F))
```

`Equal` compares the complete modeled result languages. It distinguishes
success from query error, compares successful unordered results as bags,
preserves observable order, correlates choices created by shared DAG nodes, and
allows independent choices where executions are independent. Root names,
column order, types, and nullability are checked before the formula is built.

If the pinned solver returns `UNSAT`, `VERIFIED_BOUNDED` means:

> No database represented by the declared row bound and catalog constraints,
> and no modeled execution choice within the fixed task semantics, makes the
> initial and final modeled observable outcomes differ.

The canonical formula is retained as one grouped mismatch assertion. The
ordinary solver schedule first gives that check at most three quarters of the
one global deadline. If it returns `UNKNOWN`, the verifier replaces only that
assertion with an exact distributive cover: no enabled left language, no
enabled right language, then one guarded unmatched-result predicate for each
normalized left and right outcome.

There is one exact branch-first exception. When both result families are
ordered singletons with no decisions or choices, have position-compatible
schemas, carry the same nonempty positional null-safe unique key, and publish
the same positional order covering that key, `relation.py` may attach a
preferred keyed cover. It contains the possible language-absence and
asymmetric-error cases, bidirectional per-live-row key absence, and non-key
payload mismatch from the side with fewer live slots. Under the trusted
uniqueness certificate, bidirectional key inclusion gives the same set with
one row per key; one complete payload direction and the shared total order therefore
give exact sequence equality. The solver skips the canonical probe only for
this cover. Ineligible or over-budget shapes retain the ordinary schedule.

In either schedule, `VERIFIED_BOUNDED` requires the canonical assertion to be
`UNSAT` or every selected exact branch to be `UNSAT` before the same deadline.
Any branch `SAT` wins immediately; an unresolved or untried branch prevents a
proof. Model extraction reruns the exact winning assertion without resetting
the deadline. Mandatory proof-domain exclusions run before both schedules.
The first branch `UNKNOWN` is retained if the remaining deadline later
expires; a deadline message is synthesized only when no earlier unknown exists.

The critical construction invariant is:

```text
canonical mismatch = OR(general exact solver branches)
preferred admission premises =>
    canonical mismatch = OR(preferred keyed solver branches)
```

`relation.py` assembles the canonical and general forms by local Boolean
distribution. The preferred equivalence additionally relies on the admitted
null-safe uniqueness and key-covering total order. Solver-backed representative
regressions prove both equalities, including sparse nullable composite keys,
and pin branch order. A future edit to either representation requires reviewing
the construction itself, not merely rerunning one workload. `--emit-smt`
deliberately writes the canonical monolithic formula for stable inspection. It
is the exact theorem but not a transcript of the internal portfolio, so solving
that one file can have different performance or return `UNKNOWN` where the
portfolio succeeds.

Private `DecimalSumState` composition changes the construction cost of one
directly linked split Decimal SUM, not the theorem. An admitted intermediate
state records original-input presence, the three Decimal-special predicates,
the exact finite total, and its proven finite bound beside the authoritative
materialized scalar. Combining guarded states is exactly equivalent to
flattening their guarded original input bags: presence and special predicates
compose by disjunction and finite totals by addition. Finishing then applies
the existing NaN/opposing-infinity/single-infinity/finite precedence once.
The combined finite bound must stay strictly below the maximum-precision
accumulator limit. Any missing certificate, malformed reconstruction, lineage
near miss, or insufficient headroom retains the established scalar path or
fails closed. No ghost state enters result equality or the snapshot wire.

Cardinality-certified integral `AVG` adds a solver-first proof-domain
obligation without weakening that mismatch invariant. The raw asserted
obligation becomes:

```text
semantic mismatch
or a reachable successful non-NULL completed integral AVG with count > 2
```

The normal solver protocol checks the model-domain disjunct alone first. It
must be `UNSAT`; `SAT` or `UNKNOWN` returns `UNKNOWN` without classifying the
semantic mismatch. The semantic check then uses one shared
`(count,min,max) -> result` uninterpreted function. It is exact for an
unordered non-NULL `Int64` multiset of size at most two, because the summary
uniquely identifies that multiset, but it over-approximates binary64 equality
between different summaries. Consequently semantic `SAT` is also
`UNKNOWN` pending exact binary64 replay. Only model-domain `UNSAT` followed by
semantic `UNSAT` can produce `VERIFIED_BOUNDED`. `--emit-smt` retains the raw
disjunction for one-file auditability; standalone `SAT` is neither a
counterexample nor sufficient to identify which disjunct fired.

The command-line default is two row slots per table. Stage execution has a
fixed bound of two tasks; a modeled stage may use one or two. Explicit type,
value, expression, relation, choice, and construction ceilings are also part of
the accepted subset. Crossing a ceiling returns `UNSUPPORTED`; it does not
silently weaken the formula.

This is not unbounded SQL equivalence. It does not cover unsupported semantics,
inputs with more than `N` rows per table, execution with more than two tasks,
`ConvertToPhysical`, the execution engine, optimizer optimality, or error
codes/text beyond the modeled success/error distinction. `FORMULA_EMITTED` and
`UNKNOWN` are not proofs. A satisfiable semantic-mismatch assertion is normally
a symbolic candidate and may need real-YDB replay because admitted opaque
values can over-approximate runtime behavior. The integral-AVG raw disjunction
described above is stricter: standalone `SAT` is not classified as a
counterexample.

## Proof-producing trusted code

A defect in these files can turn inequivalent supported plans into
`VERIFIED_BOUNDED`.

| File | Trusted responsibility |
|---|---|
| `semantic_snapshot.h` | Version-one catalog, snapshot, boundary, and fail-closed exporter contract. |
| `semantic_snapshot.cpp` | Mechanical catalog and plan export; scalar normalization and safety gates, including reviewed generic/pushed compiled LIKE, exact pushed Boolean coalesce, exact literal-only String `Concat` folding, restricted stored-String `Concat` provenance and total/checked-result selection, the restricted nullable String-to-Utf8 `Unicode.ToUpper` bridge, exact checked nullable-String `Unwrap` projection metadata, private window-audit orchestration, exact nullable Decimal Abs, the q49 `Decimal(35,2)`-to-`Decimal(15,4)` rescale, the passive-Double constructor, strict direct and staged Decimal-AVG admission with certified physical-pad normalization, nullable Decimal and grouped fixed-width integer count-distinct admission, and independently tracked completed integral-AVG ordering provenance; operator, exact scalar- and one-level `IN`-inside-`IN` nesting, subplan, both checked-projection demand topologies, correlated outer-binding, StageGraph, topology, task, and resource validation; exact read-range integration; deterministic JSON serialization. |
| `window_expression_export_impl.h` | Closed whole-partition Decimal SUM/AVG, fixed q49 global-Rank, and fixed q51 ordered `ROWS` SUM/MAX source-expression grammars; annotations, binders, rename lineage, frame/order/name/type checks, and deterministic scalar JSON. Included exactly once inside `semantic_snapshot.cpp`'s anonymous namespace. |
| `window_projection_audit_impl.h` | Private Aggregate/Project topology and dataflow certificates for the admitted whole-partition windows, exact three-branch q49 global-Rank corridor, and exact four-leaf/three-window-Project q51 layout. Included exactly once after the private plan exporter is complete. |
| `read_range_predicate_impl.h` | Closed q9/q45 point and finite point-set `RangeInfo::ComputeNode` grammar, physical-key/catalog binding, extractor-cap and node-identity validation, and lowering to existing equality/static-`IN` predicate IR. Included exactly once inside `semantic_snapshot.cpp`'s anonymous namespace. |
| `rbo_verifier/ir.py` | Strict JSON decoding, version/schema validation, normalized IR, expression typing, independently checked nullable-String projection-error and checked-Concat source/result/demand topology, exact `window_sum`/`window_avg` partition types and private grouped Aggregate/Project dataflow, fixed q49 `window_rank` leaves/topology/types and rescale use confinement, fixed q51 `window_rows_sum`/`window_rows_max` names, order indices, normalized input/partition/order fields, frame, exact source/result and compatible partition/order types, SUM Aggregate provenance, distinct typed MAX inputs, and four-leaf/three-Project confinement, exact nullable Decimal Abs typing, tagged aggregate-state contracts including direct/staged Decimal AVG, its carrier topology/dataflow/routing checks, phase-linked integral AVG, and fixed-width/nullable-Decimal count-distinct, independently derived integral-AVG rank provenance, passive-Double use confinement, all-plan-root virtual-binding confinement, exact scalar- and one-level `IN`-inside-`IN` plus correlated-subplan shape checks, and operator/StageGraph invariants. |
| `rbo_verifier/types.py` | Supported scalar identities, exact domains, opaque-carrier family, and compatibility predicates. |
| `rbo_verifier/smt.py` | Typed immutable SMT terms, identity-scoped concrete String-atom equality, script-owned one-constructor product datatypes, closed quantifier-free exact function definitions, bounded exact structural common-subexpression sharing with byte-exact identity fallback, quantifier/definition-scope and owner-token isolation, stack-safe occurrence/level/output rendering, deterministic canonical bytes, exact marked-obligation substitution, and solver-output parsing primitives. |
| `rbo_verifier/string_order.py` | Finite exact bounded quotient for String/Utf8 equality and unsigned byte ordering. |
| `rbo_verifier/decimal.py` | Decimal representation, domains, comparison, arithmetic including exact raw-code Abs, the fixed q49 scale-changing rescale and saturation, extrema, specials, rounding, proof bounds, and exact summarize/combine/finish algebra for private headroom-certified Decimal-SUM state. |
| `rbo_verifier/scalar.py` | Nullable values, SQL three-valued predicates including exact early concrete String/Utf8 equality, exact nullable Decimal Abs and q49 rescale, exact scalar evaluation and relation-supplied `window_sum`/`window_avg`/`window_rank`/ordered-ROWS lookup, raw aggregate-code equality for Decimal count-distinct, conservative Decimal finite-coefficient propagation, tagged `AverageMetadata`, private `DecimalSumState` carriage and dependency terms, the shared cardinality-certified integral-AVG carrier, typed opaque functions, the checked-Concat failure function, and the domain-free passive carrier encoding. |
| `rbo_verifier/sort_network.py` | Audited power-of-two bitonic compare-exchange topology and exact construction cost. |
| `rbo_verifier/relation.py` | Symbolic database, unique-key constraints, logical operators including exact task-local whole-partition Decimal SUM/AVG, exact ordinary q49 Decimal global-rank peer/gap semantics with independent unstable-sort ordinals, exact q51 task-local ordered ROWS-prefix SUM/MAX with one independent peer-order family per leaf, null-safe item partitions, NULLs-first Date order, NULL-ignoring aggregation, Decimal headroom/raw-order checks, exact private intermediate/final Decimal-SUM lineage admission, state reconstruction/fallback/composition, row association, and no published sequence, and checked nullable-String/checked-Concat projection-error outcomes, fail-closed direct unique-RHS join compaction, literal-false join-slot erasure, narrowly gated delayed unique-RHS Filter/Cross scheduling with exact factor-local static rejection, certified innermost unique-seed rebasing, and original-column restoration, exact fixed-sequence singleton-`Limit` compaction, exact fixed-width integral extrema and fixed-width/nullable-Decimal count-distinct, aggregate ghost state and producer-local integral-AVG certificates, certified integral-AVG abstract rank ordering, private schema-validated null-safe unique-key and task-partition certificates, grouped-Aggregate/`DistinctAll` derivation, exact alias propagation, total-order predecessor counts and deterministic-tie networks, per-row scalar subplans, bags/sequences, packed exact Sort/Merge transport with concrete or symbolic producer order, exact present-prefix equality, errors, choices, result-family equality, and the exact mismatch cover. |
| `rbo_verifier/stages.py` | Two-task StageGraph execution, routing, global-rank and ordered-window locality validation, connection semantics including q51 item-only HashShuffle with Date as a liveness/order input, tagged integral-AVG Merge ordering, occurrence/fact-gated task-copy compaction including validated lane-wise selection of mutually exclusive Decimal-SUM states, Broadcast pre-fan-out selection, HashShuffle cell-gated selection, exact routing-key certificates and `P ⊆ K` global-key promotion, per-task evaluation, and root gathering. |
| `rbo_verifier/verify.py` | Boundary/catalog/schema checks, shared model construction, checked-Concat producer-cardinality demand proof, producer-local integral-AVG observation, mandatory model-domain precheck, canonical/branch solver portfolio, one-deadline status interpretation, and witness decoding. |

`Term` caches its structural hash when the immutable SMT DAG node is
constructed. A different hash proves inequality; equal hashes still require
the complete `(sort, operation, arguments, atom)` structural comparison, and
Python dictionary and set lookup therefore resolves collisions with exact
equality.
Equality uses an iterative identity-pair worklist, checks exact runtime classes
and ordered arguments, and therefore does not turn Python recursion depth into
a verifier limit. The cache changes the cost of repeated routing-fact and set
lookups, not the formula or proof obligation. Deep independently constructed
shared-DAG regressions check both equal-key coalescing and separation of unequal
terms with deliberately colliding hashes.

Canonical rendering likewise does not use Python recursion for ordinary term
DAG depth. Occurrence discovery is an explicit preorder worklist, dependency
levels use iterative postorder, and term output uses an explicit task stack.
Within one lexical scope containing at most 16,384 distinct object identities,
another iterative bottom-up pass interns the exact tuple of runtime class,
sort, operation, atom, and ordered child IDs. Independently constructed equal
compound terms therefore share one hygienic, dependency-ordered `let`.
Script-owned declaration tokens remain identity atoms; no commutative,
associative, constant-folding, or other algebraic rewrite occurs. Quantifiers
are opaque to the containing scope, each body starts a new scope, and exact
function bodies render in separate contexts. Above the identity ceiling the
renderer uses its preceding identity-sharing path byte-for-byte, while nested
scopes independently re-enter the bounded structural path. The earlier
stack-safe rewrite matched its predecessor on 3,000 randomized DAGs; M87 tests
instead establish exact structural coalescing and separation, the real
16,384/16,385 boundary, old-renderer fallback equality, owner/binder hygiene,
deep and colliding-hash behavior, determinism, and Z3 equivalence.

The exact read-range audit seam is intentionally closed and C++-only.
`RangeInfo::ComputeNode` is authoritative because it is the program consumed
by runtime range extraction; `OriginalPredicate` is not trusted as a semantic
proxy. An accepted read must be an unordered column-store StageGraph source
whose catalog has one non-null `Int64` physical primary key, and the read must
emit that physical key exactly once. The descriptive `KeyColumns` entry must
resolve independently to the same output IU. The matcher accepts only q9's
single-point `RangeFinalize`/`RangeMultiply`/`RangeUnion`/`RangeFor` tree or
q45's typed static tuple and exact
`IfPresent`/`FlatMap`/`Collect`/`Take`/overflow-fallback program. It checks
binders, tuple ordinals, shared-node identities, descriptors, the
10,000/10,001 caps, and `ExpectedMaxRanges`. Missing annotations on generated
prephysical nodes are permitted, while every present annotation is additional
evidence that must agree. The only output is existing exact equality or
static-`IN` IR, optionally conjoined with an independently decoded OLAP
predicate; there is no new Python semantic axiom.

The 929-line matcher lives in `read_range_predicate_impl.h` and is included
exactly once after the shared scalar-safety and read-column helpers inside
`semantic_snapshot.cpp`'s anonymous namespace. This layout makes the complete
grammar one review unit without duplicating general helpers or exposing a
second exporter API. Mutation tests cover each finite enumerated operator,
cap, binder, tuple-index, pointer-sharing, descriptor, primary-key, and
annotation condition, plus duplicate/adjacent values,
`OriginalPredicate` irrelevance, `ComputeNode` sensitivity, and conjunction
with pushed OLAP filtering. Focused production-host q45 reaches formula
construction; q9 reaches the verifier and then fails closed on the independent
4,096-row relation construction bound. This slice adds no proof or optimizer
finding. The checkpoint partitions TPCH as 18 formula / 2 unsupported / 2
no-pair and TPC-DS as 56 / 25 / 18. This is 74/121 corpus formulas, 74/101
exact-pair formulas, 74/93 preparation-success formulas, and 74/80 verifier
entrants; unsupported outcomes split 21 initial / 0 final / 6 verifier. The
proof floor remains twenty-seven, with 232/232 C++ exporter, 593/593 Python
verifier, and 14/14 policy tests green.

The cardinality-certified integral-`AVG` seam is exact only under a separately
proved model-domain condition. The exporter admits exactly
`Optional<Int64> -> Optional<Double>` with the strict
`integral_double_v1` state object and direct undefined/intermediate-to-final
lineage, matching keys, types, and nullability. `ir.py` rechecks that complete
contract independently. Intermediate evaluation carries the exact original
input summary `(count,min,max)`; final evaluation combines summaries. A single
script-global uninterpreted function is shared by the initial plan, final plan,
and every stage. At non-NULL count one or two, the summary determines the
unordered input multiset and therefore denotes the same runtime average
whenever it is equal on both sides.

The completed result carries a tagged node-local
`IntegralAverageCertificate(count)`, distinct from transportable
`IntegralAverageState`. `Evaluator.node()` invokes the observer at the
producer. The observer builds an exact bounded reachability predicate over
successful present non-NULL rows; one central evaluator boundary then removes
the completed certificate before caching the family or returning it to any
parent projection, sorting, limiting, compaction, or StageGraph route.
Intermediate state is deliberately not removed. Direct Project, tiny
enumerated Sort, ordinal/network Sort, ordered Limit, split finalization, and
StageGraph regressions pin that lifecycle. This design keeps one
`AverageMetadata` union instead of parallel hidden fields and makes the
proof-domain dependency explicit at its origin.

The solver protocol first proves that no completed integral average with count
greater than two is reachable. `SAT` and `UNKNOWN` both become verifier
`UNKNOWN`. After exclusion `UNSAT`, an uninterpreted-carrier semantic `SAT`
also becomes `UNKNOWN`, because distinct summaries can yield the same rounded
binary64 value; exact binary64 replay is required before treating it as a
runtime candidate. Only semantic `UNSAT` proves equivalence. The raw formula's
top-level OR is intentional and auditable, but its standalone `SAT` result is
not a counterexample.

The derived integral-AVG ordering seam is disjoint from general binary64
semantics. C++ tracks completed `Optional<Int64> -> Optional<Double>` AVG
provenance from its producer; Python derives the same fact independently.
Only exact direct aliases and pass-through operators retain it, Join retains
only output payloads, and positional `UnionAll` requires every branch to be
certified. A Sort or StageGraph Merge consumes it only with the exact
`integral_avg_rank_v1` tag. Intermediate state, passive or computed `Double`,
base columns, untagged or forged order, and all `Double` hash/group/join/
predicate uses fail closed.

The value term remains the existing shared `(count,min,max) -> Int` carrier, so
ordering and equality cannot disagree about carrier identity. Under the
mandatory count-at-most-two exclusion, the concrete binary64 AVG equivalence
classes admit an integer rank. The uninterpreted function quantifies over that
runtime ranking and additional collisions, separations, or reversals.
Consequently the abstraction may create an unresolved candidate, but its
larger model set cannot create a false `UNSAT` proof.

Exact integral extrema are a separate, fully exact aggregate seam. `ir.py`
admits `Int8/16/32/64` and `Uint8/16/32/64` only when input and result have the
same fixed-width type and phase-aware nullability. `relation.py` uses a
guarded, balanced, sentinel-free reducer, so every selected value is an actual
input and no type boundary doubles as an empty marker. Scalar-empty,
all-NULL, grouped, split intermediate/final, signed/unsigned boundary, and
three-row odd-width staged regressions cover both `MIN` and `MAX`. Decimal
extrema retain their existing independent implementation.

Focused two-row/two-task TPC-DS q7/q13/q26 runs all emit formulas after
194/1,181, 247/1,830, and 204/1,122 ms, with combined report SHA-256
`721507f60df911e5906865fb26710ed98772338b5aa74afc93532dad63881853`.
Their separate 60-second solver results are `UNKNOWN` at branch 4/28, 4/4,
and 4/28. This raises focused measured coverage to 59 TPC-DS and 77/121
combined formulas, but adds no proof or optimizer finding; the proof floor
remains twenty-seven. The policy pins all three at preparation plus formula
construction.

Commits `8d3e44f59a6` and `abe190f6344` record the completed implementation and
coverage policy. The Slice A suites passed 608/608 Python verifier, 237/237 C++
exporter, 47/47 inspector, and 14/14 policy tests. The complete semantic
partition is TPCH 18 formula / 2 unsupported / 2 no-pair and TPC-DS 59 / 22 /
18, with preparation 20/2 and 73/26. The 20 TPCH plus 81 TPC-DS exact pairs
produce 18 plus 66 verifier entrants. q35 is the fourth new TPC-DS entrant and
at that checkpoint rejects unsupported integral `MAX` in Python; q7/q13/q26
are the three new formulas. Coverage is therefore 77/121 corpus, 77/101
exact-pair, 77/93
preparation-success, and 77/84 verifier-entry formulas.

The complete TPCH dashboard spends 3,273/37,511 ms in preparation/verifier
work and has report SHA-256
`f7430b2bc2e0dc3779b939831afa163d7fa7b45a7c12eeadae761117f3517b8f`;
TPC-DS spends 76,727/851,301 ms and has report SHA-256
`c37f457d0335a8b94ee10d48a5e15bffb86d6ec671050fba4538297e89688867`.
Its q7/q13/q26 rows spend 210/1,258, 279/2,049, and 224/1,361 ms. Slice A adds
no proof or optimizer finding; the proof floor remains twenty-seven.

Commits `b6c8e8863bb`, `cb50a1ee896`, `7785d8dd23c`, `90a7abd2334`, and
`a39863e5b33` record exact integral extrema, the stack-safe renderer, q35's
formula policy, the odd-width exhaustive regression, and the central
producer-local certificate lifecycle. Before this slice q35 rejected
`max(Int64)` at `n16.aggregates[2]` after 598/265 ms; report SHA-256 is
`829ff76b7d3fb9849db3a13b86bac9a604bca84eaa7f64c939517560822d50b1`.
The first exact semantics run exposed the renderer `RecursionError`, a verifier
bug rather than an optimizer finding; preserved report SHA-256
`d19f0e233fad50d4b6be279eaaa8fc9fdac2d48a01fb23f79ba7a33cc30cd7e1`.
After repair, focused q35 is `FORMULA_EMITTED` after 542/120,515 ms, report
SHA-256
`b312b43d1ba4d20aeeb615c2fe75b54b8baeed87cfdb54bea85aa4a0e9ccc9b5`.
Its separate 60-second solver run is `UNKNOWN` after 614/199,928 ms because it
cannot exclude an integral-AVG count greater than two; report SHA-256
`164398b163725598b676c231349a19c30f161fdb012dc61f951934c89676f2e4`.

The integral-extrema suites pass 615/615 Python verifier, 237/237 C++
exporter, 47/47 inspector, and 14/14 policy tests. The semantic partition is
TPCH 18 formula / 2 unsupported / 2 no-pair and TPC-DS 60 / 21 / 18, with
preparation 20/2 and 73/26. Exact pairs remain 20 plus 81, and verifier entrants
remain 18 plus 66.
Coverage is 78/121 corpus, 78/101 exact-pair, 78/93
preparation-success, and 78/84 verifier-entry formulas. The complete TPCH
dashboard spends 3,207/36,148 ms and has report SHA-256
`499e0098afda7bed5198b2cb4cc2dfe35ca81e24252aa15c8e7b1803f26e2b3f`;
TPC-DS spends 70,746/858,347 ms and has report SHA-256
`8b194da2b89d4da4dbd9fd088bf8cc07e5224239e1b656322e3cfa43198d662a`.
Its q35 row emits after 565/121,012 ms. This is formula coverage only: the
proof floor remains twenty-seven, with no optimizer bug or counterexample.

Implementation commit `e8abaff7ff4` and policy commit `3e91814d64e` record the
completed derived integral-AVG ordering slice. That checkpoint's suites passed 619/619
Python verifier, 242/242 C++ exporter, 50/50 inspector, and 14/14 policy tests.
Focused formula-only TPC-DS q22/q85 emit after 324/1,704 and 347/8,346 ms, with
report SHA-256
`0a7612f430d9dbff68d60afdcd79cf3a7cf97170d54a5287e315be9270ba954e`.
Separate 60-second solver runs are both `UNKNOWN`: q22 spends 348/61,611 ms
before the global deadline at branch 2/4 (`right_language_empty`); q85 spends
315/71,360 ms and cannot exclude integral-AVG count greater than two. Their
report SHA-256 is
`6fbe29825c3e2863ad8c3a7d92ea661bd655e7245d455fcbb1db207dcd1e258c`.

The q72 verifier-entry semantic partition is TPCH 18 formula / 2 unsupported /
2 no-pair
and TPC-DS 62 / 19 / 18, with preparation 20/2 and 73/26. Exact pairs remain
20 plus 81; verifier entrants are 18 plus 69. Coverage is 80/121 corpus,
80/101 exact-pair, 80/93 preparation-success, and 80/87 (92.0%) verifier-entry
formulas. TPCH spends 2,947/33,310 ms and has report SHA-256
`8a231a04398f6ca176286bd9d4d658e7d836c36c34ddcc4d43dfde54cc413a4b`;
TPC-DS spends 68,923/846,363 ms and has report SHA-256
`64fbda391ca5b50698aceaa2a38ba2210617fd0c1c0071bcb7c5c7967b260ecd`.
These hashes and timings are the preceding derived-ordering full dashboards;
the post-q72 complete TPC-DS dashboard spends 67,551/841,054 ms and has report
SHA-256
`8fa6661f88bbbc3f45b8bbee7fec73c4262608f5b2755736e5b1425bce15ec15`.
q72 changes only verifier entry. The proof floor remains twenty-seven, and no
optimizer bug or counterexample was found.

Implementation commit `97f103ce060` adds one C++-only dynamic Date-shift gate;
policy commit `aa01e609499` pins q72 at preparation plus verifier entry. The
gate requires exact binary `+` or `-`, an `Optional<Date>` result, and one
direct visible `Optional<Date>` member on the left. The right side is either
an `Apply` of the reviewed eight-child `DateTime2.IntervalFromDays` UDF
envelope to an `Int32` literal or exact
`Just(Interval literal)`. The latter must be a whole-day multiple, and both
spellings must decode within `[-49672, 49672]`. Every other operand order,
wrapper, Date/Interval variant, dynamic day count, fractional day, annotation,
or UDF-envelope mutation fails closed.

The exported expression is exact about source NULL:
`if_present(column(Date), opaque(bound Date), NULL<Date>)`. The full
present-input result remains opaque and nullable, including the possibility of
Date overflow. Its versioned
identity includes operator and day count, and its one bound Date argument makes
the same deterministic operation shared across plans. Existing opaque
evaluation constrains every non-NULL result to `[0, NUdf::MAX_DATE)`. The
uninterpreted result can add impossible outcomes and therefore prevent a
proof, but it cannot make an inequivalent concrete operation prove `UNSAT`.

At the normal construction limits q72 rejects a 4,608-row join output above
4,096. A disposable 8,192-row experiment was fully reverted after it exposed
a 10,619,136-pair grouped aggregate above 16,384 in 12.151 seconds. This
supported exact unique-key-aware at-most-one right-side join compaction as the
next slice at that checkpoint; it did not justify enlarging a global audit cap.
The focused normal report has SHA-256
`b1a529f927b37f262ed71f0e7e3fec92eabe6d39a79801845c91d6b40a58935f`;
the reverted-cap diagnostic report has SHA-256
`3e34de0eaf4b5e0e5215f48706263de2c8bc4a8e7ccdebb4dc11389bd9dd0470`.

Milestone 65 is recorded by implementation commit `0b0025f2a11`, naming
polish `a55f3ecba73`, and policy commit `aa004084427`. It changes no exporter,
snapshot schema, or IR contract. The new trusted seam is localized to
`relation.py`: an `inner` or `left` join may use the compact representation
only when its RHS plan child is an unfiltered, unlimited direct scan, its
residual is exact literal true, and side-explicit equalities cover one complete
declared non-null RHS unique key.

The gate does not trust plan shape alone. It rechecks the evaluated RHS schema,
distinct base-table occurrences and in-range slots, syntactic
`row.present => source.present`, and exact source payloads. Each covering
left/right comparison must have the identical scalar type. This last condition
closes a concrete soundness hazard: Decimal alignment can saturate two distinct
catalog integers to the same infinity, destroying injectivity after coercion.
Every rejected or corrupted shape retains the generic join semantics.

For an accepted join, catalog uniqueness proves at most one RHS selector.
The formula still constructs and audits the complete `|L|*|R|` match matrix,
then emits one row per left candidate and ITE-selects the RHS payload over a
typed NULL fallback. Selector guards exclude task-local left presence so
unobservable cells of routed absent copies remain identical and StageGraph
gather can coalesce them. Output presence restores the left guard; only
left-local partition facts and occurrence provenance are retained.

Independent tests include exhaustive inner/left bag references, composite and
extra keys, nullable and partial keys, cross-type Decimal coercion, predicate,
Project, scan-limit, schema, occurrence, payload, hidden-metadata, row/pair-cap,
and two-task Broadcast/gather mutations. The complete Python verifier target
passes 629/629 tests and policy passes 14/14. Focused q72 emits after
376/1,359 ms (report SHA-256
`3e6875016128af45b91b41a2fdc427fcfc7a3ef9817fa51441dd28c3636bdc8f`);
its 60-second solver attempt is `UNKNOWN` after 366/61,896 ms at branch 4/4
(`right_outcome_0_unmatched`, report SHA-256
`5bdd19a912dbdbfb5c02d865547a692690f93f8c9cd2bac83db180dd18aeea1e`).
This adds formula coverage, not a proof or counterexample.

The complete post-M65 dashboards are TPCH 18 formula / 2 unsupported /
2 no-pair at 2,938/90,617 ms (SHA-256
`67aff9f9ce4404ca52b720d5155a05a6fc7943061ab20d6ad3cc8773d2e4017e`)
and TPC-DS 63 / 18 / 18 at 64,770/786,154 ms (SHA-256
`5ca1acabd6e83cf99476cdce6547be427368c74edf0d2ea8b829cc8826d9dd62`).
Combined formula coverage is 81/121; the proof floor remains twenty-seven.
No optimizer bug was found. The next reviewed slice is q9's ordered
singleton-`Limit` representation, not a global cap increase.

Milestone 66 is recorded by implementation commit `e8b81982299`, q9 formula
policy commit `6804f459df7`, q9 proof policy commit `0cba3c9262e`, direct
encoding commit `66db625c092`, and proof-floor fixture commit `0d2fc858b70`.
It changes no exporter, snapshot schema, or IR contract. The only new
proof-producing seam is in `relation.py`.

The compactor accepts only ordered `Limit(1)` with zero offset over a fixed
sequence whose `ordinals` are absent. The shaped relation must contain more
than one slot, exactly one to three syntactically live candidates, and no live
value with hidden average metadata. Dead padding does not consume the
candidate bound. Any offset, wider candidate set, symbolic ordinal vector,
unordered input, or hidden Decimal/integral AVG state retains the unchanged
generic semantics. In particular, symbolic ordinals may tie; treating them as
a total row order would be unsound, so that representation is never compacted.

For an accepted relation, the result has one fixed present-prefix slot. Its
presence is the disjunction of candidate presences, and each payload lane is a
right-to-left ITE fold over the candidates' raw guards, selecting the first
present row in fixed sequence order. The absent payload is canonical and
typed: nullable columns use NULL, non-null columns remain non-NULL, Bool uses
false, every other carrier uses zero, and Decimal has finite bound zero.
Decimal bounds are joined conservatively across all alternatives. The compact
slot retains only partition facts common to every candidate and drops
occurrence provenance; family enablement, errors, decisions, and choices are
unchanged.

Independent evidence exhausts all presence masks and every concrete
three-candidate permutation, checks nullable payloads, dead padding, Decimal
bounds, metadata rejection, the three-candidate cap, offsets, and tied
symbolic ordinals. A two-task StageGraph Merge regression observes two routed
input slots and one exact compact result slot. The first implementation used
explicit selected guards; it was semantically exact but made the existing q15
proof deterministically time out. The direct raw-guard ITE encoding is
equivalent, reduces that formula, and restores q15 while adding q9.

The complete Python verifier target passes 634/634 tests, the focused Limit
target passes 52/52, policy passes 14/14, and the proof floor passes 5/5 with
all twenty-eight obligations. Focused q9 emits after 8,926/5,796 ms (report
SHA-256
`1e188e3624d93d67439459eaaea112a263558b464f05a1d584ed20a7d174ab76`)
and proves `VERIFIED_BOUNDED` after 8,710/22,517 ms (report SHA-256
`c997e678ed070e12278b6425f2b8b90bfee2fa3d961a9abae4e8f4f7de3f46e0`).
The complete proof corpus is 11/11 TPC-H and 17/17 TPC-DS obligations.

The complete post-M66 dashboards are TPC-H 18 formula / 2 unsupported /
2 no-pair at 2,773/92,364 ms (SHA-256
`f56b6f3e402c83489331479b3a8c9a2337eb0c15b7ec5ec9904ec05f61476c83`)
and TPC-DS 64 / 17 / 18 at 64,839/660,548 ms (SHA-256
`8ba769a6bccb8e1a6b1a75821ee041810311ceb5ac2e48ec04ae5cf41e42efa8`).
Combined formula coverage is 82/121, 82/101 exact pairs, 82/93 successful
preparations, and 82/87 verifier entrants. Unsupported primary outcomes split
14 initial-export / 0 final-export / 5 verifier. The bounded proof floor is
twenty-eight. No optimizer bug or counterexample was found. At that checkpoint
q24's `Optional<Utf8>` `Unicode::ToUpper` export gap remained next; q64's
8,192-row join output remained a separate construction problem.

Milestone 67 is recorded by the semantics-neutral reviewed-UDF refactor
`bed31799d83`, exporter commit `0ca4097d444`, q24 formula-policy commit
`a26d42bdba8`, and cast-gate audit comment `f929a36b59c`. The proof-producing
change is confined to the C++ exporter. There is no new snapshot or IR kind,
Python semantic rule, Unicode implementation, or String quotient.

The exporter accepts only an `Optional<Utf8>` Map whose input is
`SafeCast` from one direct visible `Optional<String>` member to the exact
`Optional<Utf8>` descriptor. Its unary Utf8 lambda must apply the same binder
by pointer identity to the normalized built-in `Unicode.ToUpper` UDF. The gate
checks the complete eight-child UDF envelope, callable and cached descriptors,
one `AutoMap` Utf8 argument, zero optional arguments, Void configuration,
empty type configuration and file alias, `(blocks, strict)` settings,
scalar-safety metadata, the underlying String-to-Utf8 `MayFail`
classification, and the global 64-binding limit. Every near miss fails closed.
The generic reviewed-UDF refactor alone admits no callable: each bridge still
requires an explicit immutable reviewed specification.

The emitted expression reuses the existing
`if_present(source, present, missing)` contract. The missing branch is exact
typed Utf8 NULL. The present branch is a nullable opaque function with
fingerprint `yql-string-to-utf8-unicode-upper-v1` and the non-NULL source
payload as bound argument zero. It jointly covers invalid-UTF8 cast failure
and every valid deterministic uppercase result. This is a conservative
over-approximation: it can introduce `SAT` or `UNKNOWN`, but cannot remove a
runtime behavior and therefore cannot manufacture a false `UNSAT`.
Opaque-function congruence preserves the equal-input/equal-result property of
the deterministic combined operation.

Independent C++ evidence asserts the exact JSON and rejects thirty-two
separately isolated mutations across the Map, source, cast, descriptor,
lambda, Apply, UDF envelope, cached signature, and settings. Synthetic nesting
accepts 63 existing bindings and rejects 64 before emitting another bound
reference. The real q24 Initial and Final snapshots each contain exactly one
such nullable opaque application. Validation passes 247/247 exporter tests,
634/634 Python verifier tests, 14/14 policy tests, and the 5/5 focused
proof-floor policy target.

Focused q24 formula construction is `FORMULA_EMITTED` after 1,380/5,179 ms
(report SHA-256
`f9266984538cbb4ddfb7e0cbb8fa196d8137be1bb136841c185e23ef7f47c445`).
The 14,998,792-byte, 1,319-line SMT artifact has SHA-256
`cf0057462b5dce47c7ccc345e59286fe76cd93a59d4b174a338a13f4715f9e9d`.
A separate 60-second attempt is `UNKNOWN` after 1,366/65,594 ms because its
global deadline expires at branch 3/4 (`left_outcome_0_unmatched`); report
SHA-256 is
`83a4dd5c32b0c009d18245c7368a3cee23aba9130fc0575f25ec122566c52ce2`.
This is formula coverage, not a proof, counterexample, or optimizer finding.

The complete post-M67 dashboards are TPC-H 18 formula / 2 unsupported /
2 no-pair at 2,850/95,985 ms (SHA-256
`584cf92e304f0ef8ebc67937b4ed9ea80d0bd5d1bcbe7ae944a9da7a27978917`)
and TPC-DS 65 / 16 / 18 at 64,487/673,727 ms (SHA-256
`ab8422fcc7f830a6dd314cd4edd7f0203072846c9fcc73df0f5a36b922ce7d44`).
Combined formula coverage is 83/121, 83/101 exact pairs, 83/93 successful
preparations, and 83/88 verifier entrants. Unsupported primary outcomes split
13 initial-export / 0 final-export / 5 verifier. The bounded proof floor
remains twenty-eight. The complete proof-floor gate confirms 11/11 TPC-H
obligations after 1,351/56,500 ms (report SHA-256
`9fcc4b6967736dd408760b16469380aeb03871518e2dea9ea850ec08c3f1e563`)
and 17/17 TPC-DS obligations after 12,900/88,785 ms (report SHA-256
`c59c5e08f836e0a619969fa9afeac984f0c1715bd61a42e0c0f151001a86105c`).
No optimizer bug or counterexample was found.

Milestone 68 adds three narrow trusted seams. The compiled-LIKE recognizer in
`semantic_snapshot.cpp` audits both generic and pushed `KqpOlapApply`
spellings, their complete RE2 descriptor/options program, the direct nullable
String input, bounded ASCII pattern, outer NOT, and scalar resource limits.
Both spellings lower to existing `if_present`, bound, false-literal, and
deterministic-total opaque IR with the same pattern fingerprint. Exact pushed
Boolean `?? true/false` also lowers to `if_present`; `scalar.py` uses the
algebraically identical `is-null OR value` / `not-null AND value` term only
when the present branch is bound argument zero. The ordinary count-distinct
seam admits at most one phase-undefined, non-unwrapped trait per scalar or
grouped Aggregate over a non-null fixed-width signed/unsigned integer.
`relation.py` guards directional duplicate comparisons by exact group
membership and preflights `candidate_groups * N*(N-1)/2` in every relation
representation.

The complete post-M68 dashboards are TPC-H 20 formula / 0 unsupported /
2 no-pair at 2,859/93,926 ms (SHA-256
`9bf3c81dbeef81094ed2df0350acad101ebc6613c10cebbe6c44deb8484391a2`)
and TPC-DS 65 / 16 / 18 at 64,187/669,357 ms (SHA-256
`d6fd489bc6c24af3d5670eb4054bc5e7feadc5c1e25047d18b8b399773e0b5bb`).
Combined formula coverage is 85/121, 85/101 exact pairs, 85/93 successful
preparations, and 85/90 verifier entrants. Unsupported primary terminal
outcomes split 11 initial-export / 0 final-export / 5 verifier.

The hermetic gate confirms 13/13 TPC-H after 1,633/56,327 ms (SHA-256
`d641e3445696fce0f0a367a4f586aa20543d73cb6408a7cf0fefe49f42d64b47`)
and 17/17 TPC-DS after 12,286/97,337 ms (SHA-256
`b6005eb3ed976d111756976959571916ccafa29072f329f33c2eb6f166f27278`).
q13/q16 are `VERIFIED_BOUNDED` there after 106/2,570 and 105/3,626 ms.
Exact coalesce preservation initially made q15 return solver `UNKNOWN`; the
compact exact identity term restores its proof without discarding
value-sensitive semantics. Validation passes 638/638 Python verifier tests,
259/259 C++ exporter tests, 14/14 policy tests, and the 5/5 proof-floor target.
No new optimizer bug or counterexample was found.

Milestone 69 is recorded by delayed-compaction commit `743643cc20f`, scheduler
commit `ad3613f1816`, and policy commit `7f75c4f2bb0`. It changes no exporter,
snapshot schema, or IR contract. The trusted seam is confined to
`relation.py`: a Filter may replace the construction of its input by a
semantically equivalent scheduled construction only under the gates below.
The complete original Filter, including every residual conjunct, is still
evaluated after that construction.

The recognizer requires no StageGraph, no subplan consumed by the Filter, and
no Filter-input edge override. Its predicate must be either one strict equality
or a top-level `and`; only direct, depth-free, non-null-safe column-to-column
equalities are candidates. The input must contain a nonempty left-deep spine of
keyless `cross` joins whose residuals are exact non-null Boolean true. Every
spine node must have only its expected spine/Filter consumer. Cached or
edge-overridden Cross nodes, any node override in the transformed region, and
any transformed node used as a subplan root reject the whole optimization.

A factor can be promoted only through the existing direct unique-RHS seam:
its RHS must be an unfiltered and unlimited direct Scan, promoted key types
must be identical, and the comparisons must cover a complete declared
non-null catalog unique key. Evaluation then repeats the exact schema,
base-table occurrence, slot, presence-implication, and source-payload checks
from Milestone 65. Any plan-level or evaluated near miss uses the generic
Cross operation.

Scheduling is deterministic and progressive. At each step it selects the first
pending factor whose unique-RHS equality is provable from the columns currently
available on the left; if there is none, it consumes the first pending factor
as an ordinary Cross. This lets a later q64 provider become available before
the factor that needs it without introducing a general join reorderer.
Every step derives its left schema and output columns from the current
relation, not the original spine position. After the last step, the relation
column tuple and each row's value map are restored to the exact original
Filter-input column order.

The semantic argument is deliberately bag-local: Cross is associative and
commutative for these pure, true-residual inputs, while each promoted inner
join removes only rows that the retained strict equality must reject. Result
families still pass through `combine_families`, preserving errors, decisions,
and bounded choices. The StageGraph gate prevents reordered routing and
derived-occurrence structure from becoming task-gather evidence; the final
column restoration prevents the construction order from leaking into
downstream row layout.

Independent tests compare the compact path with exhaustive bag references for
single, reversed, composite, and extra equalities. A three-factor deferred
case checks the scheduled `A x B x C` path against explicit `A join C join B`,
and a payload mutation exposes a mismatch. Near-miss tests cover row and pair
caps, node overrides, a shared Cross producer, a Filter-consumed subplan,
StageGraph, retained residuals, bounded choices, and exact post-schedule column
order.

Focused TPC-DS q64 is now `FORMULA_EMITTED` after 7,356/121,306 ms (report
SHA-256
`858677db83fd7af634fc96982214c3a4d4d2db3eba2aa6f968a7ac007a22e2ec`).
Its retained SMT is 279,504,238 bytes and 3,589 lines (SHA-256
`478b4d0b72cef35684ef2c418afc11fa3d55ae9fbd21b8a0af866ca0f676c124`);
direct retained emission takes 2:01 wall time and peaks at 2,347,148 KiB,
approximately 2.24 GiB maximum RSS. This is formula evidence only: no solver
was run, so it is not a bounded proof, counterexample, or optimizer bug. The
M69 checked-in formula floor was therefore 86 queries and its proof floor
remained thirty.
The complete post-M69 dashboards are TPC-H 20 formula / 0 unsupported /
2 no-pair after 2,928/93,582 ms (SHA-256
`3e36c25a277c81ef0b817452c3b1fddad1ff96bfbb32d0096023af580682fb32`)
and TPC-DS 66 / 15 / 18 after 64,878/750,968 ms (SHA-256
`28ac807523973e4b963c2f9eef2a5271437d81a7d573377e5a9f35d887d4bb7b`).
Both preparation and formula floors are enforced and policy-valid.
The fresh hermetic proof gate confirms 13/13 TPC-H after 1,523/60,015 ms
(SHA-256
`4788bd065a9e0cb7e58b0c2d2be851e50d5eb411abe8216ea0df5f2fcc890ed5`)
and 17/17 TPC-DS after 12,465/100,402 ms (SHA-256
`325c5970a4decedf961bb8958a6871cf965f1f7237d4406e500b449981327194`),
all `VERIFIED_BOUNDED`.
Validation passes 647/647 Python verifier tests, 14/14 policy tests, and the
5/5 proof-floor target. M69 changes no exporter code; the most recent complete
C++ exporter gate remains M68's 259/259.

Milestone 70 is implemented by `3d74b1eadcf`. It adds one optional Project
field, `error_on_null`, whose absent value is false. C++ sets it only for a
non-rename physical Map expression whose complete lambda body is
`Unwrap(Member(row, column))`, where the member is one direct physical
`Optional<String>` input and the exact Map result is non-null `String`.
The ordinary expression remains a direct column reference; annotations,
physical provenance, safety metadata, and independently materialized Map
output type must all agree.

Python independently checks the marker, direct nullable-String source, and
non-null result. For each marked projection it retains the source payload,
forces only its result carrier to non-null, and adds
`row.present AND source.is_null` to the outcome error. The payload substituted
on that error path is therefore unobservable. Multiple markers are ORed, and
the result-family machinery composes their error with inherited, subplan, and
cardinality errors. There is no generic scalar `Unwrap` rule.

Admission is restricted to two demand-safe topologies. A marked Project may be
the main result root only when every marked output is returned. Otherwise it
must be outside every subplan, have exactly one direct consumer, be that
consumer's RHS of a `left_semi` Join, and place every marked output on an exact
RHS join key. Subplan descendants, fanout, the left side, another join kind,
unkeyed marked output, Filter, Limit, and every other consumer fail closed.
A `Limit` consumer need not demand every projected row, so it is rejected
rather than assuming Project errors are globally eager. The retained
empty-left keyed-semi runtime regression demonstrates the eager/build-side
premise used for TPC-DS q8.

Production q8 prepares in 695 ms, retains two snapshots, constructs the
formula, and is `VERIFIED_BOUNDED` at row/task bounds 2/2 after 2,041 ms with
a 60,000 ms solver timeout. The post-M70 dashboards partition TPC-H as
20 formula / 0 unsupported / 2 no-pair and TPC-DS as 67 / 14 / 18.
Formula construction reaches 87/121 workload queries (71.9%), 87/101 exact
pairs (86.1%), 87/93 preparation successes (93.5%), and 87/91 verifier
entrants (95.6%); primary unsupported outcomes split 10 initial / 0 final /
4 verifier. The proof floor is 31/121 (25.6%), 31/87 formulas, and 31/31
curated obligations.

The complete post-M70 formula reports are TPCH 20 / 0 / 2 after
3,068/91,127 ms (SHA-256
`9c83253534089d26e0c17a3a049e3411e5e4720707cdadf57fb9bc3db09a2d01`)
and TPC-DS 67 / 14 / 18 after 64,964/698,120 ms (SHA-256
`350b349fa618e016f3a485a7c614566694b6d947c202a0bc762d0dbfcaddf47b`);
both policy evaluations are valid. At that checkpoint, the proof reports were 13/13 TPCH
after 1,540/61,083 ms (SHA-256
`dfefd2bfd26a5013bc42d7d22a3f60620ece8aec8cbeaa271686a249aa0afca7`)
and 18/18 TPC-DS after 13,313/103,833 ms (SHA-256
`ea06c1e3e9072c5a9f9241233647e9c05696ec730cb1879e2ae291e891c4d214`),
all `VERIFIED_BOUNDED`; the proof-floor target passes 5/5.
Validation also passes 673/673 Python, 264/264 C++ exporter, 51/51 inspector,
14/14 policy, and 1/1 `RealRuntimeStringUnwrapEagerBoundaries` tests.
No new optimizer bug was found; the cumulative historical total remains nine.

Milestone 71 is implemented by `d2979a0e459`; policy commit `a754b499484`
pins q31 at successful preparation and formula construction. The trusted
change introduces no new runtime axiom and changes no global construction cap.
It selects between two exact representations already admitted by
`stages.py`.

Conditional-value compaction is eligible only for rows with the same non-NULL
`Occurrence` when every pair carries opposite values for at least one shared
`PartitionFact`. The row-presence invariant implies that at most one eligible
alternative can be present. `_merge_exclusive_rows` ORs the guards,
ITE-selects every value and NULL lane plus Decimal and integral AVG hidden
state, joins Decimal bounds conservatively, and intersects the routing facts.
Rows with unknown or distinct occurrences and copies without contradictory
facts remain separate. Overlapping Broadcast replicas therefore retain SQL bag
multiplicity.

A pre-Broadcast gather requests this representation for every eligible
producer-task group before replicating the gathered bag. HashShuffle requests
it when the existing more-than-eight-row trigger fires or when explicit task
copies transport more than eight candidate cells; the exact eight-cell
boundary remains explicit. The conditional key is then passed to the ordinary
shared extensional hash function and receives opposite new routing facts.
Root and serial/parallel Union gathers keep only the existing row trigger.
These choices affect SMT size, not denotation.

The q31 audit measured 2,048 candidates, 2,096,128 pairs, and 67,584 potential
network comparators at each former final local Sort; bypassing it exposed a
4,096-candidate Merge. The compact path has 64 candidates and 2,016 pairs per
local Sort, then 128 candidates and 16,192 global-plus-producer ordinal
constraints at Merge. All remain inside the unchanged audits. Focused q31 is
`FORMULA_EMITTED` after 626/74,700 ms (SHA-256
`3ee9274960c93c0bb42c014bd722e7ac9e194aefdf3150567a09ebd7b3b51a4e`).
The pinned 60-second experiment emits a 246,865,820-byte formula and returns
`UNKNOWN` after 771/140,345 ms before branch 1/4 `left_language_empty`
(SHA-256
`77142a4284c5ddc51ce5a7fbe6180ace15b511da608aeeb0588255436c2a1059`).
It is neither a proof nor a counterexample, needs no replay, and found no
optimizer bug. The proof floor remains 31/121, 31/88 formula-covered queries,
and 31/31 curated obligations.

The complete post-M71 formula dashboards are TPCH 20 / 0 / 2 after
3,020/93,251 ms (report SHA-256
`ac7f146bdfc39359254ad062cb310bb2d142cc8b1cd5ad4b0cca055870f4360c`)
and TPC-DS 68 / 13 / 18 after 65,438/699,067 ms (report SHA-256
`66b0de30b4b4a4681b0411d1019d58f96865cc16e5b0e37eeeb23dded429cc73`);
both formula policies are valid. At that checkpoint, proof-floor reports
verified 13/13 TPCH after 1,564/59,731 ms (SHA-256
`06f8d4a617c9550582902e59d793f2925158bf843fda277f47b1748ff374b78f`)
and 18/18 TPC-DS after 13,041/102,884 ms (SHA-256
`ccf48e76c6dd648db23ebf31572654dc4056d5be793ef3c935dc7706db4d3c22`),
all `VERIFIED_BOUNDED`; the proof-floor target is policy-valid and passes 5/5.

Milestone 72 is implemented by concrete-atom commit `9b9133fda8c`; policy
commit `a4a82dd6f7e` pins TPC-DS q11/q74 at successful preparation and formula
construction. The trusted change adds one partial theorem to `smt.py`.
`Script.string_atom()` retains each concrete strict-UTF-8 literal term and
records its value in an identity-keyed reverse map.
`known_string_atom_equality()` returns canonical TRUE or FALSE only when both
input terms are those retained atoms from that exact Script. A value lane that
is not one of those exact retained objects—including an unregistered, source,
opaque, or foreign-Script term—gets no concrete-atom fact and follows the
existing generic SMT equality path. Cross-Script term mixing is outside the
Script-owned verifier invariant; the identity gate ensures this helper does
not itself classify a foreign structurally identical `v_0` from an unrelated
literal value.

`scalar.py` consults that theorem only for equality when both operand types
belong to the String/Utf8 family. Those types intentionally share the same
raw-byte atom domain. Ordinary equality retains its SQL NULL result lane;
null-safe equality retains its existing two-valued NULL envelope. Ordering,
`IN`, relational `not_distinct`, subplan membership, and generic integer SMT
equality are unchanged. Later atom registration can move deferred numeric
ranks but cannot change equality of their underlying byte strings. The theorem
therefore shrinks a formula without adding a semantic axiom or changing a
construction cap.

The Initial plans for q11 and q74 each alias an eight-row `UnionAll` four
times. The resulting Cross has 4,096 candidates, of which 4,032 survived while
concrete sale-type equality remained symbolic, leading to an
8,126,496-pair Sort preflight. Exact atom equality removes the impossible
sale-type combinations immediately, leaving 256 live rows and carrying both
queries through their complete final StageGraphs. Focused formula-only q11 is
`FORMULA_EMITTED` after 853/19,725 ms, and q74 after 570/31,719 ms (combined
report SHA-256
`cf2106b2ae0e2d08dad1d59640fdde7fe0b51a5bde74e517fa116ddf0eab0018`).
Pinned 60-second runs both return `UNKNOWN` before branch 2/4
`right_language_empty`: q11 after 757/79,751 ms and q74 after 529/91,782 ms
(combined report SHA-256
`76fffb3c86848126cd8d88473ae069476e2e63940c1b9c1eec0dcfe9fd51f467`).
The full verifier suite passes 680/680 tests and policy passes 14/14. These
results add no proof, optimizer bug, or counterexample; the historical
optimizer-bug total remains nine.

The post-M72 semantic partition is TPCH 20 formula / 0 unsupported / 2
no-pair and TPC-DS 70 / 11 / 18. Formula construction reaches 90/121 workload
queries (74.4%), 90/101 exact pairs (89.1%), 90/93 preparation successes
(96.8%), and 90/91 verifier entrants (98.9%). Primary unsupported outcomes
split ten initial / zero final / one verifier. The proof floor remains 31/121
(25.6%), 31/90 formula-covered queries (34.4%), and 31/31 curated obligations.

The complete post-M72 formula dashboards are TPCH 20 / 0 / 2 after
2,856/91,743 ms (report SHA-256
`355d7d23510c7a239ac8cd25200e3af1572375ec2e3e9e109bb50bbb61c36528`)
and TPC-DS 70 / 11 / 18 after 65,567/707,008 ms (report SHA-256
`e56362daf059e0869496d27f06eedfae94b1703bc5ea7c57a480c1436e187c1b`);
both formula policies are valid. Fresh proof-floor reports verify 13/13 TPCH
after 1,511/60,760 ms (SHA-256
`42f292e6e8968457064acf858ccb2bfaf1a2ab5f76c5577620f32f494ed47d67`)
and 18/18 TPC-DS after 13,518/105,096 ms (SHA-256
`68fbd23bac77cf75a00bf83a2bcddf0e6e9877ebdea116e6c7bdf9c83a8917c4`),
all `VERIFIED_BOUNDED`; the proof-floor target is policy-valid and passes 5/5.

Milestone 73 is implemented by factor-rejection commit `7cf4a049f61`,
dead-slot commit `1e53b1fb9a0`, and seed-rebase commit `8c5eca71246`. It adds
three exact reductions to `relation.py` and exposes the existing typed
expression-column walk from `ir.py`; there is no snapshot-schema, exporter,
solver-protocol, or top-level-obligation change.

The first reduction remains inside the existing private, left-deep,
literal-true Cross spine beneath a retained Filter. A nonconstant top-level
conjunct is factor-local only when its complete referenced-column set is a
subset of exactly one factor schema. For each factor-family outcome, a slot is
discarded only if its guard conjoined with SQL truth of at least one such
conjunct simplifies to the canonical `FALSE` term. The full original Filter
still runs after the rebuilt Cross, so no unresolved predicate is assumed.
Surviving Row objects retain values, occurrences, and partition facts;
sequence, order, present-prefix, and ordinal metadata are preserved by taking
the same indices. Family conditions, errors, decisions, and choices remain
outside the row map. The sum of candidate-slot/conjunct evaluations across
every factor outcome is capped at the existing 16,384 construction audit. If
there is neither an existing certified unique-RHS schedule nor an actual
static rejection, the evaluator uses the generic path.

The second reduction runs at the common Join entry and removes only slots whose
presence guard is syntactically the canonical `FALSE` term. Such a slot
denotes no runtime row regardless of malformed or unconstrained payload,
occurrence, or fact lanes, so it contributes neither a match nor an unmatched
outer result. Symbolic guards remain. Input sequence metadata is cleared only
when slots are rebuilt; every admitted Join is unordered and consumes none of
that metadata. The identity is shared by all ten Join kinds, direct
unique-RHS compaction, both empty-side cases, and ordinary matching, before
the unchanged pair and output audits. Outcome guards, errors, and choices are
still composed by the surrounding family combinator.

The third reduction is deliberately narrower than general join reordering. If
the original orientation of the innermost Cross has no delayed unique-RHS
certificate, the scheduler may commute that one bag Cross only when its
original seed satisfies the complete existing right-side certificate: a
direct, unfiltered, unlimited Scan with a same-typed, catalog-declared,
non-null complete unique key covered by ordinary equality conjuncts. The
former first right factor becomes the new seed, the reversed pair uses the
existing exact unique-RHS join, and the ordinary scheduler may then continue
over later factors. Runtime validation is repeated at construction; failure
falls back to the commuted ordinary Cross, which is still bag-equivalent. The
retained Filter and final projection restore the original plan's predicate and
column order. StageGraph, edge override, node override, shared spine, subplan,
partial/nullable/coerced key, and null-safe equality gates are unchanged.

Independent references cover factor-local SQL NULL truth, family metadata,
sequence/ordinal preservation, the cumulative work cap, poisoned literal-dead
payloads, all Join kinds and side-emptiness combinations, symbolic-presence
retention, exact pair-cap accounting, a three-factor rebase-and-continue
schedule, a restricted-domain row-bound-two differential against an explicit
reversed inner join, semantic mutation detection, and every near-miss gate.
The full verifier passes 688/688 tests; the policy target passes 14/14.

TPC-DS q4 required an independent fixture repair before it could supply valid
workload evidence. Its web-sales branch had been mislabeled `sale_type = 's'`
during Decimal migration, while the canonical query and outer predicates
require `'w'`; fixture commit `e55c37f967f` restores that discriminator. This is
not an optimizer correctness defect. The historical optimizer-bug count
remains nine.

With a fresh corrected capture, q4 reaches `FORMULA_EMITTED`. The retained
canonical SMT is 269,969,712 bytes and 2,032 lines, with SHA-256
`d4740aeb93d18e9b2e1338bcb9db58d98eedd1e93d3d5f91cf02a38bc7f0a92d`;
standalone construction takes approximately 70.16 seconds and peaks at
1,582,448 KiB (about 1.51 GiB) RSS. A separate two-row/two-task run with a
60-second global Z3 deadline is `UNKNOWN` after 1,689/200,603 ms because the
deadline expires before branch 1/4 (`left_language_empty`). Its report has
SHA-256
`c2280dd7284f7ae7593c4a0fb8121d7830646c8d0db83c3f681973faf461dc38`.
Normalizing its harness cluster name and timeout makes its formula byte-for-byte
identical to the retained canonical obligation. It adds no model, witness,
candidate, bounded proof, counterexample, replay, or optimizer finding.

The post-M73 semantic partition is TPCH 20 formula / 0 unsupported / 2
no-pair and TPC-DS 71 / 10 / 18. Formula construction reaches 91/121 workload
queries (75.2%), 91/101 exact pairs (90.1%), 91/93 preparation successes
(97.8%), and 91/91 verifier entrants (100%). Primary unsupported outcomes
split ten initial / zero final / zero verifier. The fresh complete TPCH formula
dashboard spends 3,001/95,255 ms and has SHA-256
`19d2d5b34053889df31905fe0811a212defdfc0b8abb9bd846f088fdd452d22f`.
The fresh policy-bound complete TPC-DS dashboard spends 67,223/814,378 ms and
has SHA-256
`dfb7976c5207a9fdf2fd31d79fc950fedb0d2473b899526c28ab2ff8c2305e41`;
its q4-inclusive preparation and formula floors are both satisfied with zero
violations.
The bounded proof floor remains 31/121 (25.6%), 31/91 formula-covered queries
(34.1%), and 31/31 curated obligations. Fresh complete TPCH and TPC-DS proof
reports spend 1,569/62,282 ms and 13,463/103,079 ms and have SHA-256
`28079d5aaf1af70d6badd77f14652d28893f1b149acdcc0d6fda96f1fc590246`
and
`1b34081c5e98dcf9b7f6bfb82d59491d7862b40b4c14a1a3a45711bcfadbefa6`.
All 13 TPCH and 18 TPC-DS floor obligations remain `VERIFIED_BOUNDED`; M73
itself adds only formula coverage.

Milestone 74 adds two disjoint, closed aggregate certificates. Commit
`99557229439` admits canonical nullable Decimal count-distinct with MiniKQL raw
aggregate value-code equality: NULL is ignored and Decimal NaN deduplicates
with itself. Commits `36b7dd75d96` and `8c9c29ceaf7` implement independent
Python and C++ validation of the staged Decimal AVG carrier. The only accepted
staged shape is a keyless plain Final aggregate over a binary unordered
identity `UnionAll` tree whose leaves are unordered Projects directly over
keyless one-trait Intermediate aggregates. Exactly one leaf is the direct
same-name AVG producer. C++ alone certifies that every other physical leaf
contains the exact `Nothing(Optional<Tuple<Decimal(35,s),Uint64>>)` pad and
normalizes only such a pad to logical nullable-Decimal NULL. Python
independently validates the normalized logical NULL topology.

Both boundaries trace the carrier through every Aggregate, Project, Union,
and StageGraph payload hop and independently reject aliases, project chains,
direct Aggregate leaves, ordering, fanout, extra producers, malformed
descriptors, scalar/final/root/subplan exposure, HashShuffle keys, and Merge
ordering. The certificate therefore does not interpret arbitrary physical
tuples or extend general staged aggregation.

Policy commits `26c6d0387b3` and `1545921b5d1` pin q28 at formula and proof
depth. Validation passes 693/693 Python checks (676 functional, 16 lint, and
one import), 267/267 C++ exporter tests, 14/14 policy tests, and the 5/5
proof-floor target. The preserved pre-policy focused q28 formula row is
`FORMULA_EMITTED` after 696/1,196 ms (report SHA-256
`69fa31b540190c36e08d6b92a10df9a44e66004fd206c9889704d3ade2855c49`)
but is formula-construction evidence only: its embedded policy does not
require q28. The later focused solver row is `VERIFIED_BOUNDED` after
686/11,496 ms (report SHA-256
`7b52eaf52dbd17ccded3eb9cb7565a78a9ec1216ffef63b9097be7cfafe5e7a4`).
A deliberately corrupted snapshot is a `COUNTEREXAMPLE`; the production pair
proves within the two-row/two-task bound, so this is harness evidence rather
than an optimizer defect or replay. The historical production-defect count
remains nine.

The post-M74 partition is TPCH 20 / 0 / 2 and TPC-DS 72 / 9 / 18:
92/121 workload queries, 92/101 exact pairs, 92/93 preparation successes, and
92/92 verifier entrants construct formulas. The proof floor is 32/121,
32/92 formula-covered queries, and 32/32 curated obligations. The fresh TPCH
formula dashboard spends 2,996/107,275 ms and has SHA-256
`35187d30af02a953f75e94d80922589987432ac970e4930b2f717296141c679a`.
The fresh complete TPC-DS formula dashboard reports 72 `FORMULA_EMITTED`,
9 `UNSUPPORTED`, and 18 `OPTIMIZER_FAILURE`; preparation is 73 succeeded /
26 failed. It spends 71,782/895,806 ms in preparation/verifier work, with q28
at 769/1,245 ms. Its embedded policy is valid with zero violations (report
SHA-256
`65dfe8400b01a9b4fa66b1907ac9e8569ba47d8d7dcfe459750251450d7d88b4`).
Fresh proof reports verify 13/13 TPCH after 1,591/67,583 ms (SHA-256
`d8555efcaa715565a44a89f3fa94a1c3d904c170153f4e0182e571b36f093dc5`)
and 19/19 TPC-DS after 14,667/126,545 ms (SHA-256
`475ebb9751f19d6a3d71fb8dee93a6c2dda082e6c7c72f993a14eac1190454cb`);
q28's full-floor row spends 746/13,129 ms.

Milestone 75 adds one partial scalar outcome without interpreting String
lengths in SMT. C++ first applies the existing restricted stored-String
`Concat` audit: a non-null binary root tree containing canonical String
literals and one or two catalog-bounded stored-member occurrences, with a
nullable occurrence only under exact empty-String `Coalesce`. Provenance,
annotations, scalar safety, tree budgets, literal bytes, order, repetition, and
the worst-case `ui64` result length remain part of that audit. A maximum no
larger than `UINT32_MAX` retains the existing total `opaque` representation. A
larger audited maximum is emitted as non-null String `checked_concat` with the
same canonical root-`Concat` fingerprint and its distinct external stored
columns in first-use order. q84's two Olap occurrences and two-byte `", "`
literal have the exact audited maximum
`2 * INT32_MAX + 2 = UINT32_MAX + 1`.

The shared scalar encoder gives `checked_concat` the same successful value
function as the corresponding opaque fingerprint and a separate Boolean
failure function. Both functions receive the ordered canonical NULL/value
envelopes and are shared across the Initial and Final plans. The Project ORs
`row.present AND failure(arguments)` over its input rows into the ordinary
observable error outcome; the value carried by an error outcome is
unobservable. These functions deliberately over-approximate byte concatenation
and its capacity error. The deterministic runtime value/error behavior is one
interpretation, so extra interpretations can create `SAT` or `UNKNOWN` but
cannot create a false `UNSAT` result.

Admission has a separate demand certificate. Exactly one complete top-level
checked expression may occur, in one private main-plan Project, and its output
must reach the result. An unstaged rootward path has one consumer at every step
and permits only direct Project transport, a Sort which does not use the value
as a key, and an offset-free non-cardinality-error Limit or TopSort. If that
path contains a selector, Python independently bounds the Project input through
only Scan, Filter, Cross, and Inner Join and requires every selector count to
be at least that bound. Thus q84's six scans have at most `2^6 = 64` producer
rows at row bound two beneath
`LIMIT 100`; row bound three yields `3^6 = 729` and fails closed. In a staged
snapshot the checked Project must itself be the main result root after every
materializing edge. Subplans, nesting, multiple checked expressions, fanout,
computed consumers, sort-key use, offsets, error-bearing Limits, other producer
operators on a selector-bounded spine, and unreturned outputs are rejected.
C++ validates the source grammar and structural corridor; `ir.py`
independently validates the serialized
kind, root fingerprint, direct physical Project arguments, types, topology, and
result demand; `verify.py` owns the requested-bound cardinality check.

Focused production q84 is `FORMULA_EMITTED` after 186/4,461 ms of
preparation/verifier work (report SHA-256
`e9e59667815b676420d05b7e70decc3d106b22dbc28e7202c88d3979915341ce`).
The Initial and Final snapshots have SHA-256 values
`9f5d05ad7d373a9160df5d4d37220795dd615e42321d9c6dec56f79c33b5740a`
and
`39371b1e7a6b6c2cc97fa215721ae8cc3cb137437b5713fcae95dcf8076186e5`.
Their complete checked fingerprints are byte-identical and carry prefix
`format:13:yql-opaque-v1;node:8:callable;content:6:Concat;`. The ordered
external arguments are `/Root/test/ds/customer.c_last_name`, then
`/Root/test/ds/customer.c_first_name`. The canonical 9,339,706-byte, 977-line
SMT formula has SHA-256
`4ba91650e4486b5e7578a47708c9aeea8750edd44cb5cb4d596ef79bc0a86d97`.

The separate normal 60-second solver row returns `UNKNOWN` after 64,577 ms:
`counterexample decomposition remains unresolved; first: global solver
deadline expired before branch 2/4 (right_language_empty)` (report SHA-256
`5713bd9065c40c07e31d4f9a1a20cc0fa77e1eaaf62a2b0ef78441f79ab1e9f8`).
It establishes neither proof nor counterexample. Implementation commit
`cda99a952cb` records this checked outcome; policy commit `c6fbadcc9a8` pins
q84 at successful preparation and formula construction only. The complete
post-M75 formula-only TPCH dashboard is 20 / 0 / 2 after 3,198/112,378 ms of
preparation/verifier work (report SHA-256
`dc0ec2ac610b767e33fbb6e30ab9f1d60ec09beca8ac0a610a68ed89ecc88b2d`).
TPC-DS is 73 / 8 / 18 after 113,191/1,239,636 ms, with q84 at 177/4,490 ms
(report SHA-256
`bc7f0576091888f493971a21228902fd57c75d06c6bc3772d5ad636c531663ce`).
Both embedded policies are valid with no violations. The authoritative
partition is therefore 93 / 8 / 20 overall: 93/121 workload queries, 93/101
exact pairs, and all 93/93 preparation successes and verifier entrants
construct formulas. The checked-in proof floor remains 32/121, 32/93
formula-covered queries, and 32 obligations: 13 TPCH plus 19 TPC-DS. Fresh
post-M75 proof-floor reports are policy-valid with no violations and verify
every obligation as `VERIFIED_BOUNDED`: TPCH passes 13/13 after 1,744/81,538 ms
of preparation/verifier work (report SHA-256
`8fe212d2536b7561e630dbd3e1b3bac9b8dfa7510c55b1f2a91114e4b791c5f8`),
and TPC-DS passes 19/19 after 15,499/124,840 ms (report SHA-256
`6fe57d9e56cd4ed23755494831a2cd1450ad5a103652583fa234c5292cc4b023`).
This is 32/32 curated obligations; q84 is not separately pinned at verifier
entry and remains outside the proof floor with the `UNKNOWN` result above.

Validation gates pass: focused checked-Concat Python 19/19 with Z3, neighboring
IR/project-error Python 93/93, full Python 713/713, focused C++ checked Concat
6/6, full C++ exporter 275/275, policy fixed-contract 1/1, full formula
dashboards TPCH 1/1 and TPC-DS 1/1, and direct proof floors TPCH 1/1 and TPC-DS
1/1. These are individual gate results, not a fabricated combined test count.
The post-M75 physical-line audit is recorded below.

Milestone 76 semantic commit `dbdae0a107f` adds one narrow trusted seam for an
unordered whole-partition Decimal SUM. C++ accepts exactly one window-bearing
expression in one private main-plan Project. The raw scalar root must be
`DecimalDiv(DecimalMul(member(input), Int32("100")),
YqlAggWin(sum, ..., member(input)))`; input, window result, multiplication, and
division are exact `Optional<Decimal(35,2)>`, and both direct members name the
same input. `YqlAggWin` has exactly five children, a matching nonempty window
name, exact unit-typed `YqlWinFactory("sum")`, no options, and an exact nullable
Decimal result descriptor.

Its matching source metadata is exactly one five-child `YqlWindow`: no
inherited window; one `YqlGroup` containing a one-field `Optional<String>` row
descriptor, one unary lambda, and one direct four-child `YqlGroupRef` at
canonical index 3; no order-list entries; and the three ordered frame pairs
`("type","rows"), ("from","up"), ("to","uf")`. The source tree is bounded
at 64 nodes and depth 16, every visited node passes scalar-safety metadata, and
the partition name is resolved through every recorded rename batch. This is a
closed spelling of ROWS between unbounded preceding and unbounded following,
not general framing, ordering, factories, types, or expressions.

The Project directly consumes one nonempty grouped phase-`Undefined`
Aggregate, or one phase-`Final` Aggregate directly over one matching
phase-`Intermediate` Aggregate. The partition is exactly one direct key and
the window input is exactly one plain SUM output. For a split Aggregate,
intermediate and final keys match, one nullable-Decimal SUM state has one
producer and one final use, and types agree. C++ and `ir.py` independently
reject additional windows, subplans, aliases, fanout, extra Aggregate
consumers, nonmatching traits, and every other topology.

`relation.py` computes the window once for each present Project row over that
Project's current relation. It selects the partition with SQL `IS NOT DISTINCT
FROM`, so equal nullable Strings, including NULL with NULL, are together;
ignores NULL Decimal inputs; returns NULL when no selected input is non-NULL;
and uses the shared exact Decimal SUM encoding with bag multiplicity and
conservative construction/overflow bounds. StageGraph evaluation supplies the
current consumer task's relation. Consequently an accepted parallel plan is
sound only when every logical partition reaches one task.

The initial q12 formula made that condition observable. Before the production
routing repair, the final Aggregate used HashV2 on all five group keys and the
window Project was fused into that stage, allowing one `i_class` partition to
split across tasks. The bounded result is `COUNTEREXAMPLE` after 27,588 ms.
Its coverage report, raw verdict, and SMT SHA-256 values are
`cc6576c46b6603a355b81651fde20056aa0fb3842a74b8133351e3aa307adf28`,
`923e3e56d4a09fd04dc4fb552a764676b55a4e98247720aeeb05c42c9dfccaff`,
and
`c849d007958deac4851d8b91bfbf1616cbc78a4ac9f38091c1af78bf51f95709`.
Initial and pre-fix Final snapshot SHA-256 values are
`751ac06fb3658a4e6c45cde2e9f3e3b50402353a411b1ffbad9e12e68b01249c`
and
`ce0261fad4adafb790145b022c086e20bacf0b37c7465e69c5b7abed66a69462`.

Optimizer commit `70ab3d3631c` always starts a new stage for a window-bearing
Map. A fully tracked set of windows hashes on the ordered nonempty intersection
of its resolved partition-key sets. This is sufficient: equality on every
complete partition key implies equality on each key in any nonempty subset.
The implementation first proves every selected key is available at the
current input; a global, untracked, malformed, inherited, stale, unavailable,
or disjoint case uses a nonparallel `UnionAll` gather instead. Runtime HashV2
must route equal nullable keys, including NULL, identically. The new output
index is obtained once, and window/rename metadata plus fail-closed rule guards
prevent later Filter, correlated-Filter, Limit/TopSort, stage-limit,
predicate-factoring, or scalar-pruning work from moving the expression across
the boundary. q12/q20/q98 retain their first full-group-key shuffle and gain a
second `i_class` HashV2 edge before the Project.

That first routing implementation also revealed a metadata-transport
regression. q51's broader source definition was embedded in the final
`KqpOpMapElementLambda` while its partition and ordering lambdas still named
source-row members `x.item_sk` and `x.d_date`; final-plan annotation failed
before the Final capture. Optimizer commit `a7095c6a797` now attaches window
metadata only for the exact self-contained grammar trusted above. A raw
untracked `YqlAggWin` still makes the expression window-bearing, so stage
assignment serially gathers it and the exporter rejects it, but the
context-dependent source definition is no longer a child of the final KQP
AST. The focused real-host q51 regression passes 1/1 in 3.99 seconds and
captures exactly Initial then Final, both unsupported, with neither
missing-member diagnostic. The focused benchmark records `capture_count=2`,
a later unrelated 414 ms range-seam preparation failure, and report SHA-256
`b328b80f54a90a0ae3e01e38dc559944fe807e2a7c13e9643efed4d634af5c23`.
Test-lifetime commit `78e255b5e1f` gives the resolver an owned context before
the runner is created. The post-cleanup full real-host integration gate is
GOOD: one suite / 44 tests in 19.713023 seconds, including q51 in 3.916390
seconds, with approximately 32.190 seconds of `ya` wall time.

Post-fix q12 is `UNKNOWN` after 61,153 ms because the deadline expires before
branch 3/5 (`left_outcome_0_unmatched`). Coverage report, raw verdict, and SMT
SHA-256 values are
`26d7b4b1fe539508720a93b636f409c93c922830e0a200d0c8df4ebaa4067263`,
`846a4688309d7595a0bc648627e7c54b9d87e770c3f515a8aabaa7a43958b7d8`,
and
`2226888e0242bd0bd2f5163bc3d6e7c2d88668f4e3f96f5141dcacbbc3eab743`;
the post-fix Final snapshot is
`1cba9b0f71c48c11e084aec24056781d91b942d3df64de55647bf5045ee49271`.
Neither `UNKNOWN` nor the earlier symbolic candidate proves a compiled-runtime
result. Physical preparation still fails with `Missed callable: YqlAggWin`, so
replay is unavailable; the defect attribution relies on the exact source
grammar, the modeled and reviewed task-local semantics, and focused production
stage-routing tests.

Focused q12/q20/q98 evidence constructs three formulas after 209/934, 137/926,
and 167/933 ms of preparation/verifier work; its report SHA-256 is
`9c79494b48140df09addf88af4e76150ffd87807e80f83ed2a35c75a5f3cb3d9`.
Policy commit `1da14eb637b` advances the input schema to version five and the
evaluation schema to version four. It adds q12/q20/q98 to the formula floor
and the supplemental exact-pair-only list q49/q51/q53/q63/q89. Unioning those
with formula and explicit verifier-entry requirements yields pair floors of 20
TPCH and 81 TPC-DS, 101 total; the formula floor is 96. The exact pair check
requires exactly Initial then Final, so it catches q51's zero-capture
regression without pretending its normal unsupported pair entered the
verifier.

The complete post-policy TPCH formula dashboard is 20 / 0 / 2 after
3,403/108,336 ms of row-summed preparation/verifier work, satisfies all 20
pair and formula requirements with no violation, and has report SHA-256
`c5ad2d811bfdb4933d3b538a932f01d621726afc468aa2aa38c57ea303eae9a7`.
The complete TPC-DS dashboard is 76 / 5 / 18 after 76,740/896,728 ms,
observes 81/81 effective pairs, 76/76 formulas, 73 preparation successes, and
76 verifier entrants with no policy violation, and has report SHA-256
`5015b3fe8e47e1aad88295cc0f4b59088ef5f9087b99c4792ab7b0c999f796c1`.
The authoritative totals are 96/121 workload formulas, 96/101 exact-pair
formulas, 96/96 verifier entrants, and 93/93 preparation successes. The proof
floor is unchanged at 32 obligations. Fresh reports verify TPCH 13/13 after
1,843/81,211 ms (SHA-256
`33534f30a78bd392b7abd6206cfdd0ba94e8ef7bfb393966d4e88555fdd040b4`)
and TPC-DS 19/19 after 15,865/125,293 ms (SHA-256
`9b7ee8d66acec051d8f20b53e8e100c9d672d718fc07e830b3894477e0839dac`).
Both proof policies are valid with no violation; proof mode does not enforce
the dashboard-only exact-pair floor.

The completed code gates are distinct results: focused Python window semantics
8/8, full Python verifier 722/722, inspector 51/51, focused C++ window exporter
3/3, focused window-projection rule guards 3/3, focused stage assignment 8/8,
and full verifier C++ 291/291. The q12/q20/q98 formula cluster is 3/3. These do
not substitute for the complete dashboard and proof gates recorded above.
A separate synthetic full-group-key case is `COUNTEREXAMPLE` and its synthetic
partition-key-only repair is `VERIFIED_BOUNDED`; production q12 remains
`UNKNOWN` after the routing fix.

Milestone 77 semantic commit `3f9b9c8b2c6` broadens the trusted window seam
only to the exact q53/q63/q89 whole-partition AVG shapes. C++ requires a direct
five-child `YqlAggWin` with exact option-free Unit-typed
`YqlWinFactory("avg")`, nonempty
name, and `Optional<Decimal(35,2)>` input, descriptor, and result. Its matching
source is an exact non-inherited five-child `YqlWindow`, with no ordering and
the ordered whole-partition ROWS frame
`("type","rows"), ("from","up"), ("to","uf")`. The partition list has one
through four ordered `YqlGroup` children. Each has a one-field Struct
descriptor, unary lambda, and direct named four-child `YqlGroupRef`; the
canonical unsigned index, name, descriptor, reference, and annotations agree.
Indices and names are independently unique. Partition fields are restricted
to exact `Optional<String>` or `Optional<Int64>`, and ordered rename history
must leave nonempty unique names. Source-definition and expression safety
trees are each capped at 128 nodes and depth 16.

The raw partition ordinal is semantically checked against the ordered grouped
Aggregate key vector, not merely used as descriptive metadata. q53 uses
nullable Int64 `i_manufact_id` and q63 uses nullable Int64 `i_manager_id`, each
at ordinal 0. q89 uses nullable String `category`, `brand`, `store_name`, and
`company_name` at ordinals 0, 2, 3, and 4 of a six-key Aggregate. Each resolved key occurs
exactly once and has the same nullable type. The window input selects exactly
one Aggregate trait by result name; that trait is one plain
non-distinct/non-unwrap SUM of exact `Optional<Decimal(35,2)>`. Additional SUM
traits are permitted only under other output names, so the named carrier is
unambiguous. A phase-`Final` Aggregate directly consumes one matching
phase-`Intermediate` Aggregate with identical ordered keys, exactly one named
SUM-state producer, and exactly one final use. The M76 private
Aggregate/Project single-consumer, no-fanout, one-window, and no-subplan gates
remain intact.

The serialized `window_avg` uses an ordered unique one-through-four-element
partition array. The raw GroupRef index is deliberately not transported: C++
alone checks it against the Aggregate ordinal before export. `ir.py`
independently checks the ordered names, types, direct-key membership, phase
link, named SUM carrier, and topology.
`relation.py` evaluates the leaf once per present Project row against the
relation visible in that StageGraph consumer task. Every partition component
uses SQL `IS NOT DISTINCT FROM`; NULL inputs do not contribute. It carries an
exact special-aware Decimal `AggrAdd` sum and `Uint64` count, returns NULL for
count zero, and otherwise divides by the positive count with exact Decimal
nearest/ties-to-even rounding and same-scale narrowing. The finite-sum bound
must be below `10^35`, count below `2^64`, and relation-local `rows^2` within
the unchanged pair cap. Exceeding any bound is `UNSUPPORTED`, never an
approximate formula.

Exact Decimal Abs is a second small trusted branch. Both export and the
closed-world callable validator require one exact
`Optional<Decimal(35,2)>` child and the same result type. Python rechecks that
schema and emits `value < 0 ? -value : value` on the raw Decimal code while
propagating NULL. Thus finite negatives and `-Inf` become positive; `+Inf` and
the positive NaN code remain unchanged. The shared C++ checker is also used
when Abs is nested inside an otherwise admitted opaque expression, fixing the
initial narrow seam in `DecimalDiv(Abs(Sub(...)), ...)` without admitting
other Abs types or arities.

Production metadata transport mirrors the closed one-through-four-key source
syntax as a prefilter. It checks descriptor/name/type spelling, canonical
unique indices, frame, and order, but not annotations or index-to-Aggregate
ordinal agreement; the exporter checks those later. q51's
ordered/context-dependent definition still fails the transport gate, so the
earlier missing-member regression remains closed. The M76
stage-assignment boundary is unchanged: a fully tracked single window hashes
on the complete resolved partition tuple; untracked, malformed, unavailable,
or unsafe metadata gathers serially. The movement-rule barriers remain part of
the external production contract rather than the Python proof core.

The semantic correspondence was audited against runtime sources.
`yql/essentials/mount/lib/yql/window.yqls` wraps Optional inputs, uses Decimal
`WidenIntegral` plus `Uint64` count state, updates with `AggrAdd` and `Inc`,
uses NULL as the empty default, and computes
`Cast(Div(sum,count), item type)`. The positive count makes
`yql/essentials/public/decimal/yql_decimal.cpp`'s division branch round to
nearest with even ties; `.h` supplies the matching divisor/narrowing contract.
`yql/essentials/minikql/invoke_builtins/mkql_builtins_abs.cpp` negates only a
negative raw Decimal `TInt128` with `SafeNeg`. Runtime HashV2 equality for the
complete ordered nullable partition tuple, including NULL components, and
preservation of the explicit stage boundary remain external assumptions.

Focused q53/q63/q89 formula construction is 3/3. Preparation/verifier times
are 275/2,383, 238/2,322, and 273/3,233 ms. Each exact pair is captured before
later physical preparation fails with `Missed callable: YqlAggWin`; replay is
therefore unavailable. The formulas are 3,489,514,
3,436,841, and 5,757,911 bytes with SHA-256 values
`44b183e952114d322ba0f6656364da7e35027049eafbca22e59fc015773291c0`,
`a78ca722fd57d0a3e9061e6a414f6152a3d391f7d47f3e75a4f18b4cf19b8c4c`,
and
`eb385ea6a522499fb76d7c1efc97281107bdf85a38e8a00e94ad949ce3851c12`;
the combined report SHA-256 is
`35bc96ab52ca2dc02ce4a8fea6a762bb3b7d434a27f4a5935a403081a38550d9`.
Separate 60-second solver rows are all `UNKNOWN` after 62,781, 62,842, and
63,997 ms. Each first exhausts the global deadline before branch 3/5
(`left_outcome_0_unmatched`); combined report SHA-256 is
`7b30313b096b9a314b52c56e0048601a267f391d4854e52fd80e7837a4914bfa`.
They add no proof or finding.

Focused Python AVG/Abs/routing validation passes 11/11 in two suites. Correct
plus wrong routing takes 4.60 seconds, q89's four-key mutation 1.26 seconds,
the `Abs(Sub(...))` self-proof 64 ms, and concrete NULL/tie/multi-key evaluation
4 ms. Focused C++ exporter/production-shape validation passes 9/9, including
Abs nested under `DecimalDiv` in the shared closed callable validator. Policy
commit `adfe48088f5` promotes
q53/q63/q89 to the formula floor and leaves only q49/q51 supplemental, for
checked-in floors of 99 formulas, 101 effective pairs, and 32 proofs. The
complete post-M77 TPCH dashboard is authoritative: 20 formulas / 0 unsupported
/ 2 no-pair failures after 3,295/108,747 ms, preparation 20/2, and all required
pair, formula, and entry floors with no policy violation. Its report SHA-256 is
`23dcef98dc8eb5f7be6ca248b89f5d18c6469ad598d4102d764ba33790fd2398`;
the test takes 114.13 seconds (130.97 seconds wall). The complete TPC-DS
dashboard is also authoritative: 79 formulas / 2 unsupported / 18 no-pair
failures after 76,911/907,768 ms, preparation 73/26, 81/81 effective pairs,
79/79 formulas, 79 verifier entrants, and a valid policy with no violation.
Its report SHA-256 is
`42849489e72f7408aa03c441453c7c48c82e5c24eb0b73f2d74d78fa8d605887`;
the test takes 990.31 seconds (1,009.59 seconds wall). Thus 99/121 workload
queries, 99/101 exact pairs, all 93 preparation successes, and all 99 verifier
entrants construct formulas; all 101 effective pairs are present.

The fresh TPCH proof gate verifies 13/13 after 1,802/82,453 ms, with 13
successful preparations, 13 exact pairs, a valid policy, and no violation.
Its report SHA-256 is
`6cd133426b494541647cbd7d618785a7bddab72cd56b01c33f874796171d6219`;
the test takes 86.39 seconds (103.10 seconds wall). The fresh TPC-DS proof gate
verifies 19/19 after 16,306/114,124 ms, with 19 successful preparations, 19
exact pairs, a valid policy, and no violation. Its report SHA-256 is
`3a03efa79824900b7a5e4985294de39c290f0696eca253e27bd0d45c08c1e181`;
the test takes 134.25 seconds (151.42 seconds wall). Together the proof gates
verify 32/32 obligations, 32/121 workload queries (26.4%), and 32/99
formula-covered queries (32.3%) after 18,108/196,577 ms of summed
preparation/verifier work. M77 adds no qualified defect, leaving eleven
runtime-confirmed findings plus the one bounded pre-physical q12 routing
finding.

M78 adds one deliberately closed global-rank corridor for TPC-DS q49. The
exporter accepts exactly its three private
Aggregate-to-ratio-Project-to-Rank-Project corridors. Each Rank Project has two
direct `YqlWin(rank)` leaves, for six leaves total, with canonical metadata
names `_yql_anonymous_window0` through `_yql_anonymous_window5` and local
execution orders zero and one. Every leaf has an empty partition, one direct
non-null `Decimal(15,4)` ascending/null-first order key, a `ROWS` frame from
unbounded preceding through current row, and a non-null `Uint64` result. The
source Aggregate must be logical phase `Undefined` or `Final` over
`Intermediate`. Each snapshot contains exactly six integral-to-Decimal casts
and six `Decimal(35,2)`-to-`Decimal(15,4)` casts. The latter multiply a finite
raw coefficient by 100, saturate an absolute rescaled coefficient of at least
`10^15` to signed infinity, preserve Decimal specials, and propagate NULL.
Every other scale change remains unsupported.

The relation model gives every rank leaf its own unstable-sort ordinal family.
Ordinary finite and infinity peer ties share ranks and leave the corresponding
gaps. Decimal raw order is `-Inf < finite < +Inf < NaN`; ordinary Decimal
equality makes separate NaN rows non-peers, so duplicate NaN codes may receive
distinct ranks. `CalcOverWindow` itself publishes no sequence order; q49's
final TopSort remains observable. Subplans, mixed windows, fanout, nested or
malformed metadata, and any broader Rank grammar fail closed. Commit
`0f12406f6c4` contains this proof-producing slice.

The focused formula-only q49 row is `FORMULA_EMITTED` after 2,303/3,302 ms.
Its report SHA-256 is
`e20010534c98ae2589e39274cf59fd396bd87961ae7bf81e2ce4572f04f26838`,
with Initial/Final snapshot SHA-256 values
`6d05c8c3503d7457e73617a251d89d42e699ad49372d08c608deca09ab52a7ef`
and
`e0441964964197a66185053732180f03f2f5a080c8d66ec7515e249640012d1d`.
The 3,172,413-byte formula has 10-second formula SHA-256
`797f6ad7e264ce01d063a55a60a2b307f055bedde766619ab863ee899f19707d`.
A 60-second solver run is `UNKNOWN` after 2,267/63,562 ms, with the deadline
exhausted in branch 2/4 (`right_language_empty`); its report SHA-256 is
`a45d0f637cc9399f0567e41b3a4ae5d916a9092524e2419a38ab0b67d315dadf`.
Formula emission and `UNKNOWN` add no proof.

M78 also records a second qualified bounded pre-physical routing finding. In
the fixed symbolic database, an exact extracted q49 ratio-plus-two-rank slice
is `VERIFIED_BOUNDED` under serial gather in 0.32 seconds and becomes a
`COUNTEREXAMPLE` under `HashV2(item)` in 0.47 seconds. A concrete trace in
0.67 seconds assigns logical ranks `(2,2)` and `(1,1)` to items 1 and 2, while
task-local hash execution assigns `(1,1)` to both. Its semantic before/after
SHA-256 values are
`c1d4d545ea43b7c9489fbc367ee72d7c77a80adf0297f5a91b6b19b867bae93f`
and
`704125e48987615de9c4005021e8ec324094503878941edd896a27e387e0b657`;
the complete artifact manifest SHA-256 is
`04f58ec27c49638b3fe8c5c9065aa0737b8d7109a4fd1906280814a46cc28b79`.
A separately captured fixed web witness gives a hash counterexample in 45.90
seconds and serial `UNKNOWN` in 60.55 seconds. Both the complete serial q49 pair
and its hash mutation are `UNKNOWN` at 60 seconds. Commit `27e3f260017`
preserves order liveness and renames, treats unavailable window metadata as a
movement barrier, and serializes global rank across `UnionAll`. Physical replay
is blocked by `Missed callable: YqlWin`, so this is bounded StageGraph/task
evidence, not a runtime-confirmed result divergence.

Two production robustness regressions were found and fixed while making that
path preparable. Commit `97a03c64ab9` keeps metadata-bound member names and
their `StructType` aligned during normalization, avoiding a type-annotation
failure. Commit `68eb64102c7` makes an untracked window a preferred-alias
rewrite barrier, closing a repeated rewrite loop. These are production
preparation/termination failures, not observed wrong-result findings.

Policy commit `e926958d96c` promotes q49 and leaves q51 as the sole
supplemental exact-pair-only row. At that exact HEAD the authoritative TPCH
dashboard has 20 formulas / 0 unsupported / 2 optimizer failures, preparation
20/2, pair and formula floors 20/20, and zero violations after
3,275/110,159 ms; wall time is 132.84 seconds and report SHA-256 is
`796b138b95716c7d7c14c3498701686f69dade11dd2fdcc23bd369e2bed97c23`.
TPC-DS has 80 formulas / one unsupported q51 / 18 optimizer failures,
preparation 73/26, one raw pair-only row, 81/81 effective and observed pairs,
80 entrants, all 80/80 formulas, and zero violations after 78,270/919,472 ms;
wall time is 1,021.91 seconds and report SHA-256 is
`bd475fedf9e5e8a7c11cdbda7adbb5dc2208a34f99bff6cddfee29fe98a4355f`.
Its q49 row spends 2,298/3,316 ms and retains the focused Initial/Final hashes
above. Thus 100/121 workload queries (82.6%), 100/101 formula-eligible exact
pairs (99.0%), all 101/101 effective pairs, all 93/93 successful
preparations, and all 100/100 entrants emit formulas.

The proof floor is intentionally unchanged. TPCH verifies 13/13 after
1,745/82,550 ms, wall 103.57 seconds, report SHA-256
`8a3ca5e010d927d5f90d06c59a0dec6aeba73338adcfdba4b5d5234267428ffd`;
TPC-DS verifies 19/19 after 16,174/116,051 ms, wall 153.30 seconds, report
SHA-256
`94c68481abf54be64ef912412aa633519f96563dac302e472f955742a59c85ad`.
That is 32/32 obligations, 32/121 workload queries (26.4%), and 32/100
formula-covered queries (32.0%) after 17,919/198,601 ms. Component gates pass
738/738 Python, 310/310 C++, 51/51 inspector, 46/46 integration, 16/16 policy,
and 1/1 focused q49 integration tests.

M79 admits one further closed window shape: q51's four ordered Decimal
ROWS-prefix leaves in exactly three private Projects. Web/store SUM leaves use
names zero/one and local order zero; their dependent MAX leaves use names
two/three and independent local orders zero/one. SUM partitions are required
Int64 and outer MAX partitions are Optional<Int64>; every leaf has one nullable
Date ascending/NULLs-first key and the exact unbounded-preceding-to-current-row
frame. Each leaf takes and returns exact `Optional<Decimal(35,2)>`. Partition
comparison is `IS NOT DISTINCT FROM`; each leaf receives its own ordinal choice
family for Date peers.
SUM/MAX ignore NULLs, SUM is exact only within the checked Decimal headroom,
and MAX follows raw Decimal order including specials. Evaluation is task-local,
retains row association, and publishes no sequence. Each of the three accepted
q51 window-Project input boundaries in the Final graph is HashV2 on item; Date
remains a live ordering input and is not shuffled.

The trusted implementation is in isolated window audit headers plus `ir.py`,
`scalar.py`, `relation.py`, and `stages.py`. C++ validates source binders;
Python independently validates normalized names, exact source/result and
compatible partition/order types, local execution orders, frame, three-Project
topology, SUM Aggregate provenance, and distinct typed MAX inputs. C++ also
confines function-specific partition nullability. In captured q51 those inputs
are the two running-SUM results. Commits `beb329debc8` and `d2965d0a765`
implement the semantic model and exporter.
Production commits `43af260c8a6`, `a749e7800be`, and
`82fc8b25bd2` preserve and transport the required metadata and routing, while
`26f2210d0d7` isolates the private audit seams. Range commits `9faa9c19a82` and
`877d65c8f12` are production robustness changes: they give required-key
presence its exact tautological/empty behavior and prevent a tautological
required-Data `Exists` from creating an incomplete composite-key prefix. They
do not widen `read_range_predicate_impl.h` and are not semantic result-defect
findings.

Focused formula-only q51 is `FORMULA_EMITTED` after 521/8,669 ms; the normal
60-second run is `UNKNOWN` before branch 2/4 (`right_language_empty`) after
549/68,659 ms. Physical preparation still ends at `Missed callable:
YqlAggWin`, so neither result establishes runtime behavior or a bounded proof.
Policy commit `4609f334b0c` empties both supplemental lists and establishes
formulas for all 101/101 exact pairs, while the proof floor remains 32/121
workload rows and 32/101 formulas. Complete component evidence passes 747/747
packaged Python, 318/318 C++, 46/46 integration, and 16/16 policy checks. The
canonical artifact digests, dashboard/proof timings, and focused gate details
are recorded in [BENCHMARK_COVERAGE.md](BENCHMARK_COVERAGE.md).

M80 changes no proof-producing file or accepted semantic contract. The
already-supported TPC-DS q97 returns `VERIFIED_BOUNDED` at row/task bound 2/2
in focused evidence after 326/2,255 ms, and policy commit `ebb5c8806fc` adds it
as the twentieth TPC-DS proof obligation. The first expanded floor run is
retained as non-authoritative operational evidence: during observed suite-wide
host contention, q9 reached its global deadline. The timing correlation does
not establish cause. No code changed before the clean rerun, which prepares
and proves all 20/20 TPC-DS rows after summed
16,156/120,815 ms (report SHA-256
`50f4a7e37793c804265ef94a8ac29ec6a2a86211eb3a88795a8b665a3497cc00`).
Together with the unchanged M79 TPCH 13/13 report, the M80 checked floor was 33/33:
33/121 workload rows (27.3%) and 33/101 formula-covered exact pairs (32.7%).
This is stronger evidence under the existing bounded theorem, not an increase
in the TCB or a new optimizer defect.

M81 changes neither the TCB nor the checked policy. At unchanged HEAD
`3d1d99a953c`, the already-supported q33 pair reaches the existing 2x2 solver
contract but returns `UNKNOWN` after 1,543/61,936 ms when the global deadline
expires before branch 4/28 (`left_outcome_1_unmatched`). Its non-gating
`solver_experiment` policy is valid with zero violations; report SHA-256 is
`2dd30504eaa2e51f61fdd5894cb310360af4716471d40cbfa4aba54d5bdaffb4`.
This establishes no new theorem result, defect, or external assumption. q33
remains outside the then-33-query proof floor; any future promotion requires
exact semantic reduction and a reproducible proof, not timeout tuning.

M82 also changes no proof-producing file, accepted semantic contract, bound,
or external assumption. Two focused q88 runs on unchanged proof-producing code
reproducibly return `VERIFIED_BOUNDED` at row/task bound 2/2 after 2,141/41,735 and
2,139/41,728 ms. Their 5,770-byte reports have SHA-256 values
`303f4af0cc03844a75eab41c8e1eb0b13e4b50dee71d4f60c9cf4f03327bde60`
and
`3d6c2b61326f57b3eb33fb108761658b124ef7f5db09eac20709a2be0c2206f1`.
Both are valid zero-violation non-gating experiments; successful proof rows
retain no standalone formula or verdict artifact. Policy commit
`42a879e19f5` changes only the q88 obligation and its regression fixtures, and
policy validation passes 16/16. Fresh policy-valid gates verify 13/13 TPCH
after 1,779/83,055 ms (report SHA-256
`d558bc7f56e4d013b539a2023ccd02ad3c696ba9b5b29e54fc80fb92b7d556b0`)
and 21/21 TPC-DS after 18,227/162,856 ms (report SHA-256
`1f45ea729e1bb7f393023545123942c1fa6726c7ac7701b70fa4fb7923c676fd`).
The M82 checked floor was 34/34: 34/121 workload rows (28.1%) and 34/101
formula-covered exact pairs (33.7%). This strengthens evidence under the
existing bounded theorem; it does not enlarge the TCB or defect inventory.

M83 likewise changes no proof-producing file, accepted semantic contract,
bound, or external assumption. Two focused q99 runs on unchanged
proof-producing code reproducibly return `VERIFIED_BOUNDED` at row/task bound
2/2 after 238/28,834 and 245/29,337 ms. Their 5,783-byte reports have SHA-256
values
`905deaa1c381a24d853b95887c842480666ae5543cf7fbd7295fa2b5c29d7fbf`
and
`a44d73544e498f6180aaf9538a7073ca989d52ae95e2591943e181c1c878bdc1`.
Both are valid zero-violation non-gating experiments; successful proof rows
retain no standalone formula or verdict artifact. Policy commit
`cc85514862d` changes only the q99 obligation and its regression fixtures, and
policy validation passes 16/16. Fresh policy-valid gates verify 13/13 TPCH
after 1,841/83,004 ms (report SHA-256
`7b7904f9d460362253d0b87dc0f3439a67294f8e4a5f9dc35305cdd053c0e726`)
and 22/22 TPC-DS after 18,379/190,504 ms (report SHA-256
`883a491cc28c06731b47e25d6b7008f17514be0905da9d3eef850235acbd08bd`).
The M83 checked floor became 35/35: 35/121 workload rows (28.9%) and 35/101
formula-covered exact pairs (34.7%). This strengthens evidence under the
existing bounded theorem; it does not enlarge the TCB or defect inventory.

The fresh q21/q56/q60 batch is a next-work diagnostic, not a TCB change. All
three exact pairs return `UNKNOWN`, and each final ordering contains its
complete grouped key. A future optimization may derive null-safe uniqueness
from grouped Aggregate and cross-task disjointness from audited HashShuffle
keys, then use that certificate to eliminate only impossible tie choices.
Nothing in M83 assumes an implicit runtime tie-break. q33 remains outside that
slice because its ordering omits its grouped key.

M84 changes the trusted Python semantic core, but not the C++ exporter, JSON
wire contract, strict IR, solver theorem, external runtime boundary, or proof
policy. Commit `476f2ea38f4` adds two private `Relation` certificates. `K`, the
null-safe unique-key set, asserts that no two present rows are SQL
`IS NOT DISTINCT FROM`-equal on every member. `P`, the task-partition set,
asserts that equal `P` values cannot occur in different task partitions. Both
must be nonempty `frozenset[str]` values contained in the visible schema; a
malformed or stale certificate is rejected at construction.

The derivation is closed. A nonempty grouped Aggregate emits one row per
null-safe grouping tuple and therefore mints `K` from its complete grouping
output. `DistinctAll` mints the corresponding aggregate-output aliases.
Incoming `P` survives Aggregate only when `P` is a subset of the input group
keys and is remapped through `DistinctAll`. Filter and static row pruning only
remove rows. Root/output selection retains a certificate only if the complete
set remains visible. Project remaps it only through a direct column expression
with no `error_on_null`; a computed, missing, colliding, or marked key column
drops it. Sort, TopSort, Merge, ordered/unordered Limit, and the audited
row-preserving window/certificate-strip paths retain it. Join/Cross, logical
`UnionAll`, outer binding, scan construction, and delayed-join schema
restoration publish no certificate. HashShuffle sets `P` to its exact edge
keys. A one-input gather may retain `K`; a multi-task gather requires the same
local `K` and `P` on every input and promotes `K` only when `P ⊆ K`.
Broadcast replication loses the partition premise before any later gather, so
replicated local keys cannot become globally unique. Map passes certificates
unchanged. Serial and parallel StageGraph `UnionAll` use the same gather rule
and infer no disjointness from union alone. HashShuffle supplies `P` but never
invents a missing `K`; Merge becomes choice-free only after a valid global
`K` reaches it.

The total-order step is an implication, not a runtime tie-break assumption.
If the comparator columns contain `K`, equality under every ordered SQL
comparator implies null-safe equality on every member of `K`; the certificate
therefore excludes a tie between two present rows. Ordinary Sort and eligible
small Merge assign each present row its exact number of strict predecessors,
preserving the source outcomes, decisions, and bounded choices. Eligible
compact-prefix and large-Merge sorting networks use fixed input indices as tie
ranks; because present ties are impossible, those ranks affect only absent
slots. They add no symbolic choice. If the comparator omits any part of `K`,
the prior enumerated, ordinal, or symbolic-tie semantics and all existing
construction caps remain in force.

Independent proof-soundness review checked the `P ⊆ K` direction, nullable
composite grouping, `DistinctAll`, Project failure cases, all propagation/drop
constructors, Broadcast/HashShuffle/UnionAll/gather/Merge behavior, and both
predecessor and network encodings. A separate packaging/diff audit found no
blocker, duplicate public surface, or unregistered test module. The registered
Python package passes 760/760. The focused M84 q21/q56/q60 run is valid with
zero policy violations but all three rows remain `UNKNOWN`: the deadline is
reported before branch 4/4 `right_outcome_0_unmatched` after earlier solver
work, so that branch itself was not attempted. It shrinks normalized outcomes 55 to 6,
bounded order-choice variables 16 to zero, and SMT bytes 2,344,721 to
1,458,873; it establishes no new bounded theorem result. The M84 floor
therefore remains the M83 35/35, and q21's deterministic singleton-family/keyed
comparison is the next exact target.

At exact HEAD `476f2ea38f4`, q21/q56/q60 spend 377/61,341,
3,379/63,374, and 3,952/64,088 ms in preparation/verification. Their SMT
artifacts are respectively 194,997/634,486/629,390 bytes with SHA-256 values
`e8b528cecfaaeadb7fec86b5519e7e438092b2b1e3d27f75a0479a6c82d6d8f3`,
`2c52982ec75b3bbd886535332410f362921d2fbd54acea8029aa6726276a9883`,
and
`7164f0f1fc7cffb8404782469a3ab437c82322e6ab76edc3150aa905c8924e70`.
The 9,832-byte report SHA-256 is
`6d69883451d8b71ccb2cc78a8e7e42d59fdfdf563d574f7725cb8e9c157b8714`;
the 18,375-byte merged-trace SHA-256 is
`3e3492f504c32486f9a6b3a05b9e2de655a7a7f047e7379a488adce832238991`.
Preparation/verifier sums are 7,708/188,803 ms, and
subtest/suite/graph wall times are 204.231814/206.927671/262.480557 seconds.
The report's selected, prepared, paired, and verifier-entry sets are exactly
q21/q56/q60; its verified set is empty, as required by the three `UNKNOWN`
rows. Complete per-artifact digests are recorded in
`BENCHMARK_COVERAGE.md`.

M85 changes the trusted Python proof path in `relation.py` and `verify.py`, but
not the C++ exporter, snapshot/IR contract, scalar or relational semantics,
row/task bound, pinned solver, external runtime boundary, or canonical theorem.
Semantic commit `67655eaa786` adds optional preferred-branch metadata beside
the existing canonical mismatch and general exact decomposition. That metadata
does not enter `Problem.formula()` or the emitted SMT artifact.

Admission is deliberately positional and closed. Both families must be
ordered, have exactly one outcome, and carry no decisions or bounded choices.
Their visible columns must agree positionally in type, nullability, and
integral-AVG rank. Each relation must carry a nonempty null-safe unique key at
the same column positions. Their complete order signatures must match by
position, ascending direction, NULL placement, and comparison tag, and the
ordered positions must contain every key position. Duplicate names, a missing
certificate, an incomplete or mismatched order, a schema difference, a second
outcome, a decision, or a choice declines the optimization. The prospective
portfolio is also capped independently at 64 branches and 256 audited row-pair
cell comparisons. Either exceeded cap falls back to the general exact path;
it does not make the query unsupported or weaken the formula.

The preferred cover retains every non-false language-empty branch and both
asymmetric-error branches. When both singleton outcomes are enabled and
successful, one branch per live row in each direction asks whether that row's
key is missing on the other side. For each live row on the side with fewer
slots and each non-key payload column, another branch asks whether the unique
key-matching row carries a different null-safe payload value. Bidirectional
key inclusion plus the `K` certificate establishes the same present key set
and one row per key. Thus checking all payload cells from either complete side
establishes row equality, and the identical key-covering total order establishes
sequence equality. Syntactically false status branches are omitted; a builder
result with no branch becomes no preferred portfolio, and a manually supplied
empty portfolio is rejected by the protocol.

`verify.py` propagates the optional portfolio into `Problem`. Model-domain
soundness exclusions retain priority. If a portfolio exists, the solver does
not spend three quarters of the deadline on the canonical assertion first; it
checks the exact preferred branches under the same monotonic global budget.
Every branch must be `UNSAT` for proof. A `SAT` branch wins immediately and is
reused unchanged for model extraction. An `UNKNOWN` or an untried branch
prevents proof. Problems without preferred metadata retain the established
canonical-first protocol. The scheduler now records the first branch
`UNKNOWN` and does not overwrite it when a later budget check reaches zero.
This corrects future localization while leaving M84's SHA-bound verdicts as
historical artifacts.

Independent audit checked the key theorem, positional gates, nullable
composite keys, one-sided payload direction, error/language cases, cap
arithmetic, fallback, soundness priority, model replay, and deadline behavior.
Before the final scheduler-diagnostic regression, the Python runner reported
`754 passed in 273.95s` and the packaged checkpoint passed 776/776: 754
Python, 21 flake8, and one import check. The earlier `Preferred` cover filter
passed 7/7. On the frozen tree, the lowercase `preferred` scheduler filter
collected 755 Python tests, selected 11, deselected 744, and passed all 11,
covering the last-added regression. No post-regression full-package run is claimed.
Evidence includes an independent Z3 equivalence proof under explicit
uniqueness, exhaustive sparse nullable domains, a satisfiable preferred branch
for every mismatch class, canonical-formula invariance, and rejection of every
reviewed near miss.

At exact semantic HEAD `67655eaa786`, q21 is `VERIFIED_BOUNDED` after
216/47,563 ms and again after 199/47,320 ms. The batch and repeat reports have
SHA-256 values
`9f0814d5dbdc732d3dbd7e4eee7d8be398de170dd36492d76bc047901bb611ee`
and
`db25d8caa1a975d6d4264a5521879a7d9a451ccf93e0cd5b76dca2e6086d7a46`.
q56/q60 remain `UNKNOWN` after 1,561/61,589 and 1,609/61,586 ms, both at the
first preserved unresolved branch 7/8,
`preferred_left_row_0_column_1_payload_mismatch`. Their canonical formulas are
byte-identical to M84; successful q21 intentionally retains no standalone
formula or verdict artifact.

Policy commit `95182b541fb` adds only q21 as TPC-DS proof obligation
twenty-three; validation passes 16/16. Fresh policy-valid, zero-violation gates
prove 23/23 TPC-DS after summed 18,808/264,269 ms and 13/13 TPCH after
1,805/68,461 ms. Their report SHA-256 values are
`f5b103cc73d4d339973610812dfe766560c677a0107f2761fb7ba7cc99ef4888`
and
`5d07e36d8df12909ef1e408d72e2f1bb7a72d033374f8e6e794bd28d67bfdc72`.
The M85 checked floor is 36/36: 36/121 workload rows (29.8%), 36/101
formula-covered exact pairs (35.6%), and within TPC-DS 23/99 workload rows
(23.2%) and 23/81 formula-covered rows (28.4%). This strengthens evidence under
the same bounded theorem; it introduces no external assumption. The shared
q56/q60 payload branch supplies M86's measured target.

M86 semantic commit `374f8fb65df` changes `decimal.py`, `scalar.py`,
`relation.py`, and `stages.py`; the final one-line change in `verify.py` removes
an unused import and has no semantic effect. The exporter, strict IR, snapshot
wire, policy, solver schedule, and result-family comparison are unchanged. The
new metadata is private proof state, while the visible partial Decimal scalar,
NULL bit, and finite bound remain the independently checked authority.

`decimal.py` separates existing SUM construction into summarize, combine, and
finish. `DecimalSumState` validates a maximum-precision Decimal accumulator,
four Boolean lanes (`any_non_null` and the three special flags), one integer
finite-total lane, and an integer finite bound in `[0, 10^precision)`. For each
guarded original input, summarize records presence, special membership, and a
finite-only contribution. Finish returns NaN for an explicit NaN or opposing
infinities, then positive infinity, negative infinity, or the finite total in
that order. The old `sum_with_headroom` is now exactly finish-after-summarize.

Combine accepts guarded states of exactly one requested type. It ORs the guard
with each special flag, adds the guarded finite totals, and sums the static
bounds. Its `any_non_null` lane records selected partial scalars, not the OR of
their states' original-input bits. This distinction is required for a
non-nullable empty intermediate aggregate: its runtime scalar is present zero
and therefore participates in final SUM even though its original bag was
empty. If the bound sum reaches the precision limit, validation rejects the
state; `relation.py` preserves the established `RelationError` for possible
non-associative overflow.

`relation.py` derives producer and consumer sets once per validated snapshot.
The only producer is an unexposed, non-`DistinctAll` phase-`intermediate`
Aggregate with one direct parent. The parent must be its non-`DistinctAll`
phase-`final` Aggregate consumer over identical keys. A producer trait must be
a plain, non-distinct, non-unwrap Decimal `sum`, must not be a key, and must
have exactly one plain final `sum` use with the same output type. Root/subplan
exposure, fanout, duplicate use, keys, wrong phases/functions/types, distinct,
or unwrap yield empty certificate sets.

An accepted producer stores the state beside its ordinarily finished scalar.
Before combination, the final consumer checks exact state class, type and
bound identity, exact nullable/non-null NULL condition, and exact scalar
equality with `finish_sum_state`. Every selected row must pass. A single
missing, forged, stale, or mismatched state returns `None` from the private
combiner, causing the whole final aggregate to execute the preceding scalar
semantics rather than mixing representations. A successful final combine
finishes once and drops the state.

`stages.py` includes the hidden state in exact row equality. For mutually
exclusive route alternatives, it retains a state only if every alternative
validates and all types match, ITE-selects each Boolean/integer lane using the
same row-presence guards, and takes the maximum finite bound because at most
one alternative is present. Invalid alternatives retain the established
visible-scalar compaction but publish no state. `relation.py` also exposes all
five symbolic lanes to bounded-choice dependency discovery so no hidden
quantified choice escapes an outcome's domain.

The new semantic tests comprise two Decimal-kernel cases, one StageGraph
compaction case, and seven relation/differential cases. They check composed
versus flattened finite, NULL, inactive, NaN, positive/negative/opposing
infinity cases; malformed lane sorts/types/bounds and the exact limit;
exclusive state selection and invalid-alternative fallback; grouped and
ungrouped direct/staged two-row bags over NULL/special values and every task
placement; the targeted lineage near-miss matrix; scalar fallback for missing,
wrong-type, inconsistent-scalar, and inconsistent-bound state; registered
choice dependencies; nullable/non-nullable empty partials; exact combined
headroom rejection; and accepted SMT terms that consume finite lanes without
re-decoding finalized partial scalars. Independent theorem, gate, fallback,
transport, headroom, packaging, and commit-scope audits found no blocker.

The first full verification-subtree run completed 1,349/1,350 checks. Its sole
failure was an unused `Sort` import reported as flake8 F401. In that same run
the main Python target passed 765/765, all Python targets aggregated to
878/878, all ten imports passed, and both full dashboards and proof floors
were green. The one-line import removal is included in `374f8fb65df`; the
post-fix all-flake8 corrective run passed 68/68. No post-fix full-subtree rerun
was made or is claimed.

At committed HEAD the focused 2x2/60,000-ms q56/q60 run is policy-valid with
zero violations. Both exact pairs prepare and enter the verifier, then remain
`UNKNOWN` at branch 7/8,
`preferred_left_row_0_column_1_payload_mismatch`. q56 spends
1,554/61,600 ms and q60 1,533/61,579 ms. The 8,339-byte report and
18,419-byte trace have SHA-256 values
`4303275234ed765184f460d50ba17996f09e031a0fe5f0f89dc70918ffda6ef7`
and
`bd4b047a5ee0027c1a5287c257757de5ce697903efe5ec143aaed3d4385a58b1`.
The trace records 130.106070 seconds of subtest time, 131.122184 seconds of
chunk wall time, and 1,089,244 KiB peak process-tree RSS.

q56's canonical formula falls 634,486 -> 615,352 bytes (-19,134,
3.0157%) and has SHA-256
`e8b86462179dbb64863b939a6c4f0765cb9eede153594228414f4e6ca62e1d56`;
q60 falls 629,390 -> 610,286 (-19,104, 3.0353%) with SHA-256
`cb799a6af02dfd548ea9f4bd195d52c4aebf871d054a4a636946bb9deac1d060`.
Their branch-7 formulas fall 623,078 -> 603,906 (-19,172, 3.0770%) and
618,881 -> 599,709 (-19,172, 3.0978%), respectively, while remaining
98.1399%/98.2669% of the canonical formulas. Smaller exact syntax is not an
`UNSAT` result.

The complete policy-valid formula dashboards retain every existing floor.
TPCH has preparation 20 successful/2 failed, 20 exact pairs, verifier entries,
and formulas, plus two `OPTIMIZER_FAILURE` rows, after summed
3,446/107,695 ms. Its 17,334-byte report SHA-256 is
`3ff1b10464b6047e87fed8c75eb933aa27928fba0981452e570e9721e96b5d98`.
TPC-DS has preparation 73/26, 81 exact pairs, entries, and formulas, plus 18
`OPTIMIZER_FAILURE` rows, after 78,725/835,364 ms. Its 342,052-byte report
SHA-256 is
`607bc657da5ecfb1b50c274e5bd18609bd5684cdbc3fce741caa9ddf7c55f980`.
Combined preparation/verifier work is 82,171/943,059 ms for all 101 formulas.

The proof floors are policy-valid with zero violations. TPCH has 13 successful
preparations, pairs, entries, and `VERIFIED_BOUNDED` rows after
1,788/67,191 ms; its 10,169-byte report SHA-256 is
`19ada38b50826598462c4e7fe3ee4a89dbc62c1c8d24d5d3a2914f60efb89254`.
TPC-DS has 23/23 corresponding rows after 18,785/266,278 ms; its 18,758-byte
report SHA-256 is
`efae364213953fd25ee34d1e0777fd9b452ffa84713d77a882762c0a998715a3`.
Combined proof-floor work is 20,573/333,469 ms. M86 promotes nothing: the
trusted theorem remains bounded by the unchanged 13/13 TPCH plus 23/23
TPC-DS, 36/36 floor.

M87 structural-CSE commit `0077c196ea8` changes only `smt.py` and
`test_smt.py`. For a bounded render scope, it first discovers distinct object
identities without descending through quantifier nodes. A bottom-up scalar key
then consists exactly of the term's runtime class, sort, operation, atom, and
ordered child structural IDs. Equal keys share one representative and one
`let` alias; unequal class, sort, operation, atom, owner token, arity, child,
or child order cannot share. References, dependency levels, and first
discovery are subsequently computed over those exact IDs. Alias definitions
remain nested by dependency level with parallel bindings at one level, and the
reserved-name allocator remains global to the rendered script. The
transformation changes serialization only and preserves the direct SMT term.

Quantifier nodes have no structural children in their containing scope. When
one is emitted, its body enters a fresh `_render_scope` under the same
hygienic context; sibling and nested bodies cannot export aliases, and a
shadowed binder cannot be captured by an outer alias. Closed `define-fun`
bodies already render under separate contexts and retain their existing
parameter/owner checks. `_OwnerToken` uses object identity, so equal spelling
from two Scripts does not make declarations interchangeable. There is no
algebraic normalization beyond complete exact syntax equality.

The first prototype keyed an interning dictionary by `Term` directly. It was
rejected before commit when a deliberately colliding-hash deep-DAG probe took
about 26.5 seconds through repeated exact equality. The committed bottom-up
scalar key avoids that path: recorded depth-2,500 independently rebuilt equal
DAG probes take 0.047--0.058 seconds, and collision-heavy unequal probes take
0.071--0.082 seconds. Regressions cover independently rebuilt equality,
ordered near misses, owner separation, global/shadowed/nested/sibling
quantifier scopes, definition parameters, deterministic alias order, reserved
names, deep DAGs, adversarial hashes, and independent solver equivalence.

The unbounded structural implementation passed its semantic tests and the
ordinary TPCH policy, but the full run supplied a superseded performance
diagnostic: 20 formula rows and two optimizer failures after 5,918/223,834 ms,
with q2 at 764/178,922 ms rather than M86's 288/86,319 ms. The 17,342-byte
report and 18,332-byte trace have SHA-256 values
`d47711a72f437631466651ad87a4fcdf6d1e072fb45f01b56f8d9c687c706dec`
and
`511fe85603fc45a6a9e3c718a8d00bab95b977b5cee68c32bf3406fbca417d88`.
Because the classifications are unchanged and timing-stripped report content
matches M86, this run is evidence for the performance correction, not the
authoritative M87 workload checkpoint.

Cap commit `d9be39ad01b` makes `_bounded_identity_discovery` stop when a scope
exceeds 16,384 identities and invoke the complete preceding identity renderer.
That fallback retains every repeated-object alias and exact deterministic byte
order. Quantifier bodies still re-enter capped discovery. Real-boundary tests,
forced small-cap tests, and 1,000 generated-DAG differentials cover structural
bytes at or below the ceiling and legacy bytes above it. The retained q2 scope
census finds five oversized scopes of 1,236,701, 981,382, 929,609, 345,387,
and 51,824 identities; q56/q60 stay structural with maximum scopes
10,013/9,793. The solver-backed SMT suite passes 63/63, the full direct Python
suite passes 779/779, and the registered package passes 801/801: 21 flake8,
one import, and 779 Python checks.

The exact q56 canonical/branch-7 serializations become 480,892/470,708 bytes,
21.8509%/22.0561% below M86, with SHA-256 values
`1fa866f6c87699acfc6ea05a9cf9a912d9d4d0f319229ef16f8863ee9be63b5b`
and
`87650e134222b1642a882894e5344c9b70b65749734f2b3ab1518fcbf894709b`.
q60 becomes 478,281/468,829 bytes, 21.6300%/21.8239% below M86, with
SHA-256 values
`5a541e83dd92179bf057143593420953e9ef0c75bead71f393d60f4ba0f308c8`
and
`178b7c26361735196cf772b92589de23993ab351a1109fa023c21d69b96868a3`.
Direct branch-7 Z3 checks remain `UNKNOWN` after 60.085/60.083 seconds. These
are exact syntax reductions, not new bounded theorems.

The capped authoritative dashboards are policy-valid with no violations.
TPCH retains 20 formula rows and two optimizer failures after 3,325/107,979
ms, with q2 at 285/86,749 ms. Its 17,333-byte report and 18,353-byte trace have
SHA-256 values
`98be7aca3d174cf03d6f09224d72f17364158a4503984e86a1c15a0a6870e0ac`
and
`fd50903ec7b21f6fb43a5a2dff8ef0d1e633757584eab47f4ae63c9b9a7d3d14`.
TPC-DS retains 81 formula rows and 18 optimizer failures, with preparation
73/26, after 78,615/842,344 ms. Its 342,055-byte report and 18,409-byte trace
have SHA-256 values
`c594ac967e6a98c3190c4eeb4c962816f2eb3d34e9cc51072bc195b38e1c9eb5`
and
`f26102544159a3b183afec264371238874250013cdbdfcb9fe10174dc0de7929`.
All 101 captured pairs still enter the verifier and emit formulas.

Fresh capped proof gates are policy-valid with zero violations. TPCH proves
13/13 after 1,808/62,582 ms; its 10,171-byte report and 18,439-byte trace have
SHA-256 values
`9c878b942e75fc6d55983b876fdaff9c0a35600577782255cc056707894000fb`
and
`14f3d0d098b8b88df34e6e1e3599a05ddcb054a6bf15fd1572f9695d036ab4ad`.
TPC-DS proves 23/23 after 19,215/265,971 ms; its 18,759-byte report and
18,439-byte trace have SHA-256 values
`b1f42045517c78146e15049acf95e00d35e357e586bed73880ef47fb1076eb05`
and
`00a90afc96ed67a99c3ad0604c8dc1f7d3ff9869043be007c8e01ad0fa326800`.
Combined proof work is 21,023/328,553 ms. The trusted bounded theorem therefore
retains the unchanged 13/13 plus 23/23, 36/36 floor.

An attempted TPC-DS q18/q59/q78 discovery exhausted disk while Ya created its output
root, before any test executed. It emitted no report, trace, status, or timing;
the only accurate classification is `NO RESULT`. The failed materialization
was removed and supported Ya cache garbage collection restored headroom. This
operational event changes neither the TCB nor any query inventory.

M88 changes no trusted or proof-producing code. It re-examines historical
TPC-DS q15/q19 solver evidence that predates both M84's derived total
grouped-key ordering commit `476f2ea38f4` and M85's exact keyed-cover/
branch-first commit `67655eaa786`. q15 had been `UNKNOWN` at a 60-second
proof-scaling checkpoint. q19 had been `UNKNOWN` after 219/61,811 ms, then
again after 207/61,602 ms at branch 3/28, `left outcome 0 unmatched`; that
decomposed three-query report has SHA-256
`58cc491e30e2b866f36916f2b01db36e385f005ffe3b38685f250d95ccd10164`.
These are retained as stale historical observations. The fresh results below
justify policy promotion, but without an intermediate attribution run they do
not establish which intervening change, if any, altered solver behavior.

Two independent focused `solver_experiment` runs at unchanged M87 checkpoint
`bde0d7acdf53217fd25c14768ba7fbd6869eef7f` select exactly q15/q19 with the
same two-row/two-task bound and 60,000-ms solver deadline. Both are
policy-valid with zero violations and return 2/2 `VERIFIED_BOUNDED`. The first
spends 394/8,540 ms in summed preparation/verification: q15 166/2,443 and q19
228/6,097 ms. Its 6,385-byte report and 18,393-byte trace have SHA-256 values
`1e6be8cbdea843212680c194b22009c148812077946042052ef87bee3747477d`
and
`f55d5eb690ea61131682df7a4f2221bea566f9894d2b5866b1c0d642ace43a55`.
Its subtest/metadata/chunk-wall times are
12.479636/13.570516/13.5643751621 seconds, with 1,039,356 KiB peak process-tree
RSS. The repeat spends 406/8,513 ms: q15 174/2,447 and q19 232/6,066 ms. Its
6,385-byte report and 18,400-byte trace have SHA-256 values
`a6ef794e047894c30be7da91c9f4b09c3a851f29f740f2b29fc1500c2f303839`
and
`b579f2b9afcb024f97565c5adcf29f9fc5d905190997eca27d0395c6fe2e49ce`.
Its subtest/metadata/chunk-wall times are
12.636943/13.518327/13.5252709389 seconds, with 1,039,780 KiB peak process-tree
RSS. No standalone formula is retained for either successful proof.

Policy commit `b3ce77ab1d2868ef6c55e62670022a6055018558`, based on completed M87
documentation commit `bde0d7acdf5`, adds q15/q19 to the TPC-DS required-proof
set and updates only the corresponding C++ policy fixtures. It changes no
optimizer, exporter, snapshot/IR, semantic evaluator, SMT renderer, model
bound, solver protocol, or proof theorem. It also changes no formula,
exact-pair, verifier-entry, preparation, or defect floor. The focused policy
gate passes 16/16 in 0.956049 seconds; its 33,305-byte trace has SHA-256
`0366331f0ed73f91af18e54fad7fcbb4195e9e2163005a9ccb863e06e03402c4`.

Fresh committed-HEAD proof reports are policy-valid with zero violations.
TPC-DS has 25 successful preparations, pairs, entries, and
`VERIFIED_BOUNDED` rows after 19,216/274,946 ms. Its 19,964-byte report and
18,381-byte trace have SHA-256 values
`0d56ddbbe6c3667885e1b283bffda98ff7d73d618a827172413b5c677699b3e3`
and
`f78c07ad7e69dc15ce1ea91a045136642f18d6da0786cd9315eac41047f85af5`.
Subtest/metadata/chunk-wall times are 298.359329/299.251476/299.2260770798 seconds,
with 1,156,460 KiB peak process-tree RSS. TPCH retains the corresponding
13/13 rows after 1,752/61,729 ms. Its 10,170-byte report and 18,445-byte trace
have SHA-256 values
`f81e4130fa445801c5be5c0106c810b8aa72fd72610009a73636b9a576fdc775`
and
`3c0152c38f4223c2809348b4c2a260e400fb91547957f960b92e018b7e1c2104`.
Subtest/metadata/chunk-wall times are 65.455408/66.587568/66.5466856956 seconds,
with 861,528 KiB peak process-tree RSS. Combined proof work is
20,968/336,675 ms.

The trusted bounded theorem is unchanged; its current checked sample grows to
38/38: 38/121 workload rows (31.4%), 38/101 formula-covered exact pairs
(37.6%), and within TPC-DS 25/99 workload rows (25.3%) and 25/81
formula-covered rows (30.9%). M88 changes only policy, fixtures, and
documentation, so no new ordinary dashboard is claimed. M87's authoritative
capped dashboards continue to establish the unchanged formula, exact-pair,
entry, preparation, and defect inventories: TPCH 20 formulas/two optimizer
failures with preparation 20/2, TPC-DS 81 formulas/18 optimizer failures with
preparation 73/26, and 101 combined formulas.

The packed-row declaration substrate remains deliberately narrower than a
general SMT datatype or macro facility. A product has exactly one constructor,
contains only the verifier's existing `Bool` and `Int` lane sorts, and can be
packed or selected only through its script-owned handles. An exact defined
function may use those owned products and built-in sorts, but its body must be
quantifier-free and may contain no free constant, nullary declaration capture,
or declaration from another script. Deterministic rendering emits the
constructor/selectors and definition directly as SMT-LIB; there is no
production-side beta reducer whose behavior must be trusted. Solver-backed
constructor/selector and comparator tests plus malformed ownership/capture
tests independently exercise this boundary.

The equality-correlated scalar slice adds one explicit typed `outer_bind`
relational node. Its independently checked accepted path is
`Project* -> Aggregate -> Project* -> Filter -> outer_bind`, with exactly one
ungrouped phase-`undefined` non-`DistinctAll` Aggregate, one strict direct
outer/inner equality, inner-only residuals, one dependency, and one
Project/Filter consumer. Evaluation reruns the complete scalar root per present
outer row, scalarizes zero/one/many rows to NULL/value/error, gates invocation
errors by row presence, and shares repeated binding references. Limit, Sort,
scan `pushed_limit`, ordered `UnionAll`, `EnsureAtMostOne`, and
per-invocation choice families fail closed. Every invocation shares one
validated immutable plan context and one cumulative 16,384-pair construction
budget.

The relational `EXISTS` slice has no equivalence axiom or query-specific
shortcut. An uncorrelated descriptor denotes Boolean root presence. The
original correlated form has one outer dependency and one strict direct
outer/inner equality conjunct. The exact two-dependency extension instead has
two ordered, distinct outer dependencies, each in its own conjunct: exactly one
strict direct equality and one strict direct inequality target distinct direct
inner columns. Each pair has the same base type while nullability may differ;
all residual conjuncts are inner-only. The C++ exporter normalizes source `!=`
to JSON `not(eq)` and validates the exact `AddDependencies` output schema,
order, and types. `ir.py` independently validates the serialized dependency
order, types, and normalized predicate contract.

`relation.py` binds every dependency from the same outer row and evaluates the
complete predicate against each present inner row. It ORs only rows for which
SQL Filter truth is true, so strict comparisons involving NULL do not match
and duplicates collapse. Consumer negation supplies `NOT EXISTS`. Correlated
Limit, TopSort, scan `pushed_limit`, observable `EnsureAtMostOne`, nesting,
staging, fanout, and per-invocation choice families fail closed. A same-name
`Void` may disappear from the unselected input of a one-sided witness join only
when the selected input retains that `Void`; an unmatched dropped `Void` and
every `Void` join key remain unsupported. One cumulative preflight rejects more
than 16,384 outer/inner pairs.

Independent evidence includes the C++ descriptor/shape mutation matrix, Python
NULL/duplicate/ordering/cache/pair-cap and semi/anti differentials,
order-sensitive inspector digests, an omitted-second-correlation
`COUNTEREXAMPLE`, and a real-host two-dependency capture. Focused TPCH q21 and
TPC-DS q16/q94 all return `VERIFIED_BOUNDED` at two rows per table and two
tasks. At that checkpoint the complete policy gate independently returned
11/11 TPCH and
16/16 TPC-DS `VERIFIED_BOUNDED`, confirming all 27/27 curated obligations.
TPCH spent 1,308/62,684 ms and produced report SHA-256
`0eed270ad0148908f05f59ad4e09f8710c280fca39871b5269b60ca1f707979e`;
TPC-DS spent 3,666/57,767 ms and produced
`e8b018abf0286bead86484b8a8739985554b70c823ef663abbeb938eb52a44b6`.
TPC-DS q34 is the new proof: its focused run returns `VERIFIED_BOUNDED` after
263/2,471 ms with report SHA-256
`44bdcd9f105d4f334b628bb672fa8d6b4f6ffd43ceece0666cd03829dfa5b677`.
These results extend the bounded proof floor; they do not establish unbounded
SQL equivalence.

The dynamic-`IN` slice adds one explicit typed `in` subplan descriptor. C++ and
Python independently require exactly one lookup column from the sole Filter
consumer and one result column from the inner root, with exactly the same
underlying type. Non-null columns may use a fixed-width integral or exact
`String` or Date type. Lookup and result nullability may vary independently
only for a fixed-width integral or Date type, and if either is nullable every
binding reference must be a direct positive top-level Filter conjunct. The
binding is non-null `Bool`, uncorrelated, and virtual; `OuterBind`,
`AddDependencies`, observable `EnsureAtMostOne`, fanout, structural root
nesting, staging, tuples, coercions, nullable `String`, `Utf8`, Bool, Decimal,
mismatched identities, and nullable `NOT`/`OR`/embedded uses fail closed. A
dynamic-`IN` root may consume closed uncorrelated scalar bindings and closed
leaf `IN` bindings; each leaf consumes no binding, and every other nesting
owner/kind and deeper nesting fail closed.

`relation.py` evaluates membership per present outer row as the OR of present
non-NULL inner values equal to a non-NULL lookup value. Thus duplicates
collapse and empty input is false. In the non-null slice, ordinary consumer
negation implements `NOT`. In the nullable slice, the accepted positive Filter
is true exactly when that OR is true: SQL FALSE and UNKNOWN both reject the
outer row. The model therefore does not claim scalar three-valued `IN`
equivalence under negation or any other Boolean embedding. Repeated references
reuse the cached subplan family, while root errors remain eager even with no
present outer row. A shared preflight rejects more than 16,384 outer/inner
membership pairs cumulatively across alternatives and nested evaluation. The
optimized side is still evaluated as
the ordinary final StageGraph; there is no dynamic-`IN` equivalence shortcut.
Independent duplicate/empty/negation, cache, left-semi/left-anti, inherited
error, mapping-mutation, descriptor-boundary, pair-cap, exporter, inspector,
and real-host `IN`-to-`left_semi` tests cover the vertical path. String-specific
tests also exhaust the finite reference domain across duplicate values, empty
inputs, row presence, negation, and both semi/anti lowerings. Nullable-integral
tests independently cover lookup/result nullability combinations, NULL,
duplicates, empty input, exact positive-Filter truth, rejected Boolean
embeddings, bounded `left_semi` equivalence, and a real-host lowering.
Date-specific validation repeats the independent-nullability and positive
Filter gates, uses the existing bounded Date domain, and includes a real-host
nullable-Date `IN`-to-`left_semi` bounded proof. A focused TPC-DS q58 run
reached the later nested-subplan rejection at that Date-only checkpoint, so
that slice changed neither the formula count nor the proof policy.

The exact nested extensions admit only closed uncorrelated scalar bindings and
closed leaf `IN` bindings consumed from inside an uncorrelated dynamic-`IN`
root. The physical roots remain disjoint: nesting is an expression-reference
edge, not one relational root below another. Each binding-consuming operator
must belong to exactly one main or subplan-root context. A scalar root may not
itself consume another binding, and each nested `IN` consumes no subplan
binding. Correlated nested scalars, `EXISTS` nesting, ambiguous root ownership,
staged residual subplans, cycles, and depth greater than one remain
fail-closed.

After typing every reachable node, `ir.py` independently audits every main and
subplan-root descendant schema for declared binding names. A virtual binding
therefore cannot escape as a relational output from the nested `IN` root (or
from any other plan context), even if its name and declared `IN` output are
mutated together. A dedicated nested-`IN` regression covers that boundary.

Evaluation recursively constructs and caches the nested family at its
immediate Project/Filter consumer before evaluating the enclosing membership
test. Zero/one/many scalar rows retain the ordinary NULL/value/error semantics.
The scalar's new cardinality error is demanded by rows at that immediate nested
consumer; an error inherited from its root remains eager through the enclosing
`IN` and is not gated again by an empty top-level consumer. A nested `IN`
reuses the same existential-membership semantics, including the nullable
positive-Filter restriction. Existing choice correlation, cache identity, and
the cumulative 16,384 membership-pair budget apply across both levels.

Independent C++ and Python positive/near-miss tests cover the exact descriptor,
consumer ownership, nesting-kind, correlation, NULL, demand, cache, choice,
cycle/depth, and pair-cap boundaries. The nested-`IN` evaluator also has an
exhaustive finite reference, a solver proof against two sequential
`left_semi` joins, and a counterexample when the inner membership is omitted.
At the q58 checkpoint the complete hermetic Python verifier target passed
568/568 tests. A focused production-host TPC-DS q58 run emits the complete
324,938,538-byte formula after 3,291/103,862 ms of preparation/construction;
its SHA-256 is
`22f51f5d1a82091a35d29b6ac120344725f1272b8093ae9a0f1c3fa6fc6eaa70`.
The checked-in formula policy now pins q58. It was not solved, adds no bounded
proof, and revealed no optimizer correctness bug.

The q83 static-membership precursor changes only `semantic_snapshot.cpp`.
Within a direct raw tuple, it admits an item annotated `Optional<Date>` only
when the expression is an exact direct String/Utf8-literal `SafeCast` and
MiniKQL parsing proves a present Date. Export then emits the existing non-null
Date literal, so Python IR and evaluation are unchanged. Invalid text, dynamic
input, `Nothing`, `StrictCast`, other optional types, and nullable `AsList`
items remain fail-closed. This is a proof of presence at export time, not a
general nullable-item rule.

Together, the one-level nested-`IN` and Date-tuple slices move q83 past its
former initial and final boundaries. At that checkpoint its complete-dashboard row
prepares in 1,351 ms, then both snapshots reject
`Unsupported scalar type Double` before verifier entry; verifier work is 0 ms.
The complete TPC-DS report SHA-256 is
`595ac871a19699ebc6731bf0eb0f610bd6ba100c65251cca3545493d10f4ab90`;
the preceding focused 1,310/0-ms report SHA-256 is
`7f1bae257dfcede11aa2f6a37f8e1bc45e079be4f13f8b836887a1768b6d7113`.
There is no formula, bounded proof, policy change, or optimizer finding.
Validation at that checkpoint passed 577/577 Python verifier tests, 214/214 C++
exporter tests, and 14/14 coverage-policy tests.

The next C++-only audit admits four restricted floating predicates as complete
typed opaque expressions rather than attempting general floating arithmetic:
`Optional<Int64> >= 2/3`, `Optional<Int64> <= 3/2`,
`Optional<Int64> > 1.2`, and `Optional<Int64> < 0.9`, optionally inside the
exact `Coalesce(Optional<Bool>, false)` envelope. Exact IEEE-754 binary64
fingerprints identify the four constants. A pointer-scoped exception admits
only the reviewed root comparison and its constant; the whole input subtree
still passes the ordinary opaque-expression visibility, metadata, type, and
node-budget audit. Swapped operands, a mismatched operator/constant, a
different constant spelling or payload, nested floating arithmetic, a
different envelope, and `Double` dataflow outside the separately audited
passive-carrier slice fail closed.
Because the exported node remains an opaque expression, no new Python
floating-point semantics enter the TCB.

The following C++-only audit folds a bare complete conversion from one direct
non-null integer literal to a non-null integer target as an exact target-typed
literal. The target descriptor and type annotation must agree and
`CastResult == Complete`. Separately, only `Just(Date literal)` and
`Just(complete integer-literal Convert)` are normalized as always-present
wrappers through the existing typed `If(true, value, NULL)` IR, preserving
their Optional result. All nearby wrapper and conversion shapes remain opaque
or unsupported. These two audits add TPC-DS q21/q34/q75 to formula
construction. q34 is
`VERIFIED_BOUNDED` after 263/2,471 ms
(`44bdcd9f105d4f334b628bb672fa8d6b4f6ffd43ceece0666cd03829dfa5b677`);
q21 is `UNKNOWN` after 170/60,950 ms
(`15ee95f2b59bc4ee41dddc19c8b98cdceb89bb3a2868678ab41539931c2c2a0e`);
and q75 remains `UNKNOWN` in retained 1,134/128,182 ms evidence
(`1322068b8d57dfa984e91f6f775b063cc3c10e997e30a841e40c46f8d2058a9f`).
The q21 wrapper normalization eliminates the preceding spurious candidate.
At that checkpoint q83 remained unsupported because its passive `Double`
carrier was outside the reviewed whole-predicate shape.
Validation at that checkpoint passed 577/577 Python verifier tests, 221/221 C++
exporter tests, and 14/14 coverage-policy tests.

The following q83 slice introduces a distinct `opaque_double` IR constructor;
it does not reinterpret ordinary `opaque` or add floating arithmetic. The C++
auditor accepts exactly two roots over three distinct, direct,
`Optional<Int64>` members:

- the left-associated three-member total divided by exact non-null
  `Double("3.0")`, producing the average; or
- one member divided by that same three-member total, then by exact
  `Double("3.0")`, then multiplied by exact non-null `Int32("100")`, producing
  one deviation.

Every root and floating division must be `Optional<Double>`. Pointer-scoped
exceptions admit only those audited nodes and the exact binary64 `3.0`
constant; the remaining integer subtree still passes the ordinary opaque
visibility, type, metadata, depth, and node-budget audit. The structural
fingerprint starts with `yql-passive-double-v1`, retains ordered/repeated
member positions and all types, and exposes exactly three direct arguments.
The four q83 expressions—three deviations and one average—have byte-identical
fingerprints across Initial and Final for each corresponding result.

Python independently requires `opaque_double`, `Optional<Double>`, the audited
fingerprint prefix, three distinct direct `Optional<Int64>` columns, and a
nonempty identity suffix. `Double` is a carrier family represented by one SMT
integer token plus its NULL Boolean. Both are deterministic uninterpreted
functions shared by fingerprint and ordered argument types across the two
plans; there is deliberately no IEEE value domain, arithmetic, comparison, or
ordering rule. SMT congruence forces the same fingerprint and equal ordered
arguments to produce the same NULL and payload values. Distinct identities
remain unconstrained and may coincide in a model. This relaxation can produce
a spurious `SAT`/`UNKNOWN`, but never a false `UNSAT`.

The carrier is derived-only and passive-only. Base-table metadata, literals and
typed NULLs, `outer_bind`, subplan inputs/outputs, scalar consumers, comparison
and static `IN`, join keys, aggregate keys/inputs/results, sort keys, and
HashShuffle keys reject `Double`. Direct column pass-through may carry it
through relational operators and StageGraph only as an uninspected payload;
q83's observed downstream path is Project, non-key Sort, Limit, and Merge.
Inspector rendering preserves the explicit `opaque_double` kind and
fingerprint; the inspector remains outside the proof TCB.

Focused q83 formula construction under the hardened gates returns
`FORMULA_EMITTED` after 1,301/6,081 ms; its report SHA-256 is
`04e5df3a8f55044002fdf9b231d75b707bf58fd51c8b60a4a8879d4d623b9a5b`.
Its 10,953,698-byte canonical formula has SHA-256
`5228c142eef65eb7707ff039c58e6cfc85f599286a9ec2ccf480b5fd94903db6`.
A separate 60-second solver run returns `UNKNOWN` after 1,313/66,340 ms when
the global deadline expires before branch 2/4 (`right_language_empty`); its
report SHA-256 is
`5571045865cbd30d7b2a35e61c379bdb7e3e24b63bfd1df2be8f514454487572`.
The complete TPC-DS dashboard independently records q83 as
`FORMULA_EMITTED` after 1,338/6,175 ms and confirms a 72/121 formula policy.
q83 is not a bounded proof, reveals no optimizer correctness bug, and leaves
the proof floor at 27/121. Validation passes 588/588 Python verifier tests,
225/225 C++ exporter tests, 46/46 inspector tests, and 14/14 coverage-policy
tests.

The exact duplicate-source Map projection changes only
`semantic_snapshot.cpp`; the existing Project IR and evaluator already copy a
column expression independently into every declared output position. The
exporter requires every rename source to be a visible input, but permits that
same source in multiple Map elements. It suppresses the source once from the
untouched pass-through set, appends every renamed output in Map-element order,
and continues to require each output name to be nonempty and unique. Missing
sources, duplicate outputs, empty outputs, and computed expressions outside the
existing scalar grammar fail closed. C++ boundary mutations and a real-host
pair that selects one column under two aliases cover the path; there is no
Map-specific equivalence axiom.

This exact projection lets TPC-DS q54 construct its complete 57,271,400-byte
formula. Cached immutable `Term` hashes make that construction tractable
without changing its semantics; the canonical formula SHA-256 is
`3494295db496d95d32019eb5aa0d0b14e099ef38cdb42d646e7c2f07f0035f4e`.
The formula-only result is `FORMULA_EMITTED`; a separate 60-second solver run
is `UNKNOWN` after the global branch deadline, so q54 adds no bounded proof and
does not change the 25/121 proof floor.

The side-explicit Join-key slice crosses `semantic_snapshot.cpp`, `ir.py`,
`scalar.py`, `relation.py`, and the ordinary `stages.py` execution path.
The exporter records each JoinKey as an ordered left/right IU pair and keeps
JoinFilters in a separate residual expression. The decoder independently
checks side membership, equality compatibility, and the exact combined
node/depth budget. Evaluation reads each key value from its declared input row
and applies ordinary SQL equality, so a NULL key does not match and a repeated
IU spelling cannot overwrite one side before comparison.

Input schemas may overlap only for left/right semi or anti joins. Such a join
must have a literal-true residual; the exporter additionally requires no
JoinFilters. Output-both joins fail closed because their schema would be
ambiguous. StageGraph child occurrences remain structurally distinct when
their IU names match, while source-task placement and HashShuffle guards retain
the existing runtime correlations. Exhaustive one-sided join references,
shared-name mutations, key node/depth boundaries, and two-task
occurrence/routing checks cover this vertical path.

The correlated-COUNT repair has no new Python semantics. The C++ exporter
recognizes only the optimizer-generated
`Just(Coalesce(Optional<Uint64> direct-member, Uint64(0)))` shape and lowers it
to existing exact `if`/`if_present` IR. Type, nullability, direct-member,
literal, visibility, metadata, depth, and construction-budget checks all fail
closed; near-miss shapes remain opaque.

The `DistinctAll` slice crosses only `semantic_snapshot.cpp`, `ir.py`, and
`relation.py`. Both validation boundaries require nonempty ordered keys and
one positional, unflagged `distinct` alias per key with identical type and
nullability. Evaluation reuses the existing exact null-safe grouped-row
construction and returns only each representative key tuple under those
aliases; empty input therefore stays empty. Intermediate and final evaluation
remain ordinary task-local aggregation, so HashShuffle correctness is checked
by the normal StageGraph model rather than a special equivalence rule.
Independent nullable composite-key enumeration, malformed exporter/IR shapes,
staged routing, a non-shuffled duplicate witness, solver checks, and a
real-host transformation cover the vertical path.

The q95 aggregate bridge deliberately reuses those general relations. First,
`semantic_snapshot.cpp` normalizes only an exact
`Just(non-null Uint64 direct member) -> Optional<Uint64>` from the current row
to existing `if(true, member, typed-null)` IR. This preserves the raw Optional
schema while proving runtime presence; wrong types, nullable or computed
members, a foreign row, unsafe metadata, and budget crossings fail closed.

Second, `semantic_snapshot.cpp`, `ir.py`, and `relation.py` admit one keyless,
final, non-distinct `sum(Optional<Uint64>)` trait with `unwrap` and a raw
Optional Uint64 output. The raw trait remains inspectable in the snapshot, but
IR schema validation exposes its physical effective column as non-null and
evaluation returns non-null zero for empty or all-NULL input. Every other
unwrap shape remains closed.

Third, those same files plus `scalar.py` admit at most one direct ordinary
distinct trait per Aggregate, and only when it is a keyless
phase-`undefined`, non-unwrapped
`count(non-null Int64) -> non-null Uint64`. Evaluation keeps a present value
only if no earlier present value is equal. This directional representative
test counts every distinct value once without imposing an order on SQL
results. Its exact `N*(N-1)/2` equality count is charged before construction.
Small exhaustive duplicate/absence references, pair-cap boundaries, exporter
and IR near-miss matrices, wrapping-versus-unwrapping sum references, and the
focused q95 bounded proof cover the composition. There is no q95-specific
equivalence axiom.

Milestone 68 extends only that ordinary direct-distinct seam: scalar or
grouped phase-`undefined` Aggregates may contain at most one such trait over a
non-null fixed-width signed/unsigned integer, with a non-null Uint64 result and
no unwrap. Group membership guards every directional duplicate comparison,
nullable grouping keys use existing null-safe equality, and all three relation
representations preflight
`candidate_groups * N*(N-1)/2` before constructing value equalities. An
independent exhaustive bag reference covers duplicate values under nullable
keys and the equivalent `DistinctAll(group, value) -> count` decomposition.

The canonical String-predicate bridge adds no evaluator-specific truth table.
`semantic_snapshot.cpp` alone must establish the narrow generic and OLAP
grammars, catalog type/nullability, exact typed coalesce handling, and the
one-to-one mapping from `EndsWith`/`StringContains` to their two stable
fingerprints. Existing `ir.py`/`scalar.py` opaque-function validation then
shares each deterministic total function and its ordered column/literal
arguments across both plans. Cross-dialect exporter mutations and a
solver-backed real-host fixture are the independent evidence.

The compiled-LIKE bridge likewise adds no regex truth table to the Python
kernel. One closed C++ recognizer audits the complete generic and pushed
`KqpOlapApply` spellings, cached callable/run-config/type descriptors for
`Re2.Match`, `Re2.PatternFromLike`, and `Re2.Options`, canonical
case-sensitive options, a bounded ASCII pattern, the direct nullable String
input, and every safety/depth/node condition. Both spellings lower to existing
`if_present`, bound, false literal, and deterministic-total opaque IR with one
shared pattern fingerprint; pushed outer NOT remains explicit.

The pushed-predicate output-IU resolver also changes only
`semantic_snapshot.cpp`. For every OLAP read output, it registers the physical
read name, full output-IU name, and short output-IU name as references to the
same logical scan output. Repeated spellings for that same typed, nullable
output are equivalent aliases. If a predicate references a spelling registered
for distinct outputs, export fails closed as ambiguous; an ambiguity that is
never referenced is accepted because it cannot change the decoded predicate.
Focused exporter tests cover all three spellings, same-output aliases,
referenced collisions, and unused collisions. The complete benchmark
dashboards independently move TPC-DS q2/q97 to formulas and q59 to the
verifier's exact construction cap at that checkpoint. The later packed
sorting-network carrier moves q59 through formula construction. None of q2,
q97, or q59 was a bounded proof at that checkpoint.

The exact sorting-network slice crosses `sort_network.py`, `smt.py`,
`relation.py`, and the row-preserving paths in `stages.py`. The topology module
contains only the padded-size calculation and deterministic bitonic comparator
schedule. For each syntactically live candidate, `relation.py` allocates one
bounded finite rank; global distinctness makes the ranks a permutation. SQL
key comparison dominates rank comparison, so ranks choose exactly the
otherwise-unobservable order of equal keys. Present rows compare before absent
rows and power-of-two padding is always absent.

For each nontrivial network outcome, `relation.py` validates one common row
layout and uses `smt.py` to declare one one-constructor product datatype. The
payload contains presence, every NULL/value lane, and every hidden Decimal AVG
sum/count lane. One closed quantifier-free `define-fun` contains the exact SQL
key comparison over two payloads and their tie ranks. Every comparator then
selects each of its two output payloads under one `ite` and each output rank
under one `ite`; selectors recover the row only after the network. Nullable
values, Decimal finite bounds, and AVG state therefore cannot split across
candidate identities. Malformed lane sorts, mixed AVG layouts, invalid state
bounds, or foreign declarations fail closed. Zero- and one-live-row outcomes
require no datatype or definition.

The fixed output slots have a present-prefix invariant. Only root wrapping,
OuterBind, Project, and one-input Stage gather preserve it; Filter, source
partitioning, hash routing, and multi-input gather clear it. Ordered Limit
statically slices slots only while the invariant is present. Merge orders the
network ranks along each producer's semantic input order. A fixed producer
order, including concrete input ordinals, uses unconditional adjacent rank
edges. If a producer has symbolic input ordinals, every unordered pair of
syntactically live rows instead gets one exact guard: an absent row or equal
input ordinals impose no order; otherwise input-ordinal less-than is equivalent
to network-rank less-than. Every legal order of present rows extends to a full
rank permutation including absent holes, so these constraints denote precisely
the sorted producer-order-preserving interleavings. When both compared
sequences have the fixed present-prefix invariant, equality checks aligned
presence/value slots and requires every unmatched suffix slot to be absent.
That is exactly compressed-sequence equality and avoids the ordinary quadratic
compressed-rank matrix.
When exactly one side has the invariant, equality instead computes ranks only
for syntactically live slots on the sparse side. Cardinality equality fixes the
prefix length; a present sparse slot at rank `r` must equal prefix slot `r`,
and every prefix slot beyond the number of live sparse candidates is required
absent. Both operand orientations retain the original positional column
mapping. This is the same compressed-sequence relation as the general
rank-by-rank matrix, while a 64-candidate sequence compared with a
4,096-slot physical prefix no longer constructs ranks or value comparisons for
the unreachable prefix tail.

Selection is fail-closed: ordinary pair construction remains capped at 16,384;
the network is separately capped at 32,768 comparators, 131,072 logical packed
payload cells, and 64 ordering columns. The payload charge is live input rows
times scalar lanes. It is an auditable logical-width gate, not an estimate of
Python memory, rendered formula bytes, constructors, selectors, or downstream
equality terms. A Merge network with symbolic input ordinals also charges every
unordered syntactically live pair in each such producer, summed across all
outcomes, against the same 16,384 pair cap. Intermediate TopSort compacts only
when the uncompacted shaped slots from both fixed verifier tasks would make the
downstream Merge exceed the pair cap. Exhaustive topology, NULL/direction,
tie/duplicate, present-prefix, offset, concrete and symbolic producer order,
producer holes/equal or reversed ordinals, mixed-key Merge, Decimal-AVG state,
layout, declaration ownership, formula structure, and cap/fallback tests are
the independent evidence.

The complete policy dashboards move TPCH q2 and TPC-DS q59/q78 to formulas.
Focused production-host obligations for q59 and q78 contain four and three
product/comparator definition pairs respectively. Those focused formulas are
116,879,360 and 202,469,546 bytes and have SHA-256
`3a140fcb1b5d6a5145c4aa30cbcd817167a27f21bed94d85ef969223dce73c8e`
and
`fb0eaebb95ea9bdfb3b0f815f5078a70d1c2e3765ed5d6675be1c4f06b8249c4`.
The policy-valid complete TPC-DS report independently confirms both formula
rows and has SHA-256
`44254733785284105840e269653f3cae79384db985cf13260906df57cf1deaa6`.
Neither obligation was solved, so this slice adds no bounded proof and reveals
no optimizer correctness bug. At that packed-carrier checkpoint, the complete
hermetic verifier target passed 564/564 tests and the updated coverage-policy
target passed 14/14.

The nullable Date-year bridge likewise changes only `semantic_snapshot.cpp`.
It admits one direct visible `Optional<Date>` member, a complete cast to
`Optional<Timestamp>`, and the exact reviewed unary
`DateTime2.GetYear(DateTime2.Split(argument))` UDF envelope. The exporter
preserves NULL with existing `if_present`/`if` IR and assigns the non-null
operation the stable `yql-datetime-year-v1` opaque identity over the bound Date
payload. Treating that deterministic total function as otherwise arbitrary is
an over-approximation: it may make a proof harder, but cannot make an invalid
plan pair prove equivalent. Exporter near-miss mutations, NULL/fingerprint/
argument solver tests, and a real-host bounded proof cover the vertical path.

The proven-total Date-`Unwrap` bridge also changes only
`semantic_snapshot.cpp`. It admits exactly
`Unwrap(Coalesce(Optional<Date> direct-member, fallback)) -> Date`, where the
fallback is either the initial plan's
`SafeCast(Int32(0), Optional<Date>)` or the final plan's
`Just(Date(0))`. The gate checks the complete root, `Coalesce`, member, and
fallback annotations; child order and arity; direct-row visibility; literal
value; target descriptor; reviewed cast category; safety metadata; binding
depth; and expression budget. It then lowers both spellings to the existing
exact `if_present(member, bound-value, Date(0))` IR. The audited MiniKQL
premise is that Int32 zero is in the Date conversion range and is preserved,
so both fallbacks are present Date zero and `Unwrap` cannot raise an error.
At that Date-only checkpoint every other `Unwrap`, including the String shape
in TPC-DS q8, failed closed. C++ near-miss mutations, independent Python
NULL/present references and semantic mutations, real-host initial/final
normalization, and the q38/q87 bounded proofs cover this vertical path.

Exact Decimal weak `SafeCast` crosses `semantic_snapshot.cpp`, `ir.py`,
`decimal.py`, and `scalar.py`. The exporter admits nullable or non-null
fixed-width integral sources and canonical Decimal sources whose scale is
unchanged and whose precision does not decrease. It requires source/result
nullability parity, an exactly matching canonical target descriptor and
annotations, at least one target integral digit, and reviewed weak
`CastResult<false>` semantics. Integral source NULL propagates; a present
integer is scaled to the target coefficient and saturates out-of-range values
to signed infinity rather than NULL. Same-scale Decimal widening is encoded
identity: every finite coefficient, negative infinity, positive infinity, and
NaN is preserved, while source NULL propagates.

The exporter records the actual `source_type` on every explicit
`cast_decimal`. The Python decoder independently requires that field to equal
the inferred argument type before dispatching to integral conversion or
Decimal identity widening. This required cross-language seam prevents a
target-only encoding from silently applying the wrong source semantics.
Missing or mismatched source type, `StrictCast`, `Convert` outside existing
constant normalization, source/result nullability mismatch, Decimal scale
change or narrowing, noncanonical Decimal, other source families, and targets
without an integral digit fail closed. Exporter mutation tests, independent
decoder and evaluator rejections, finite/special/NULL references, and
solver-backed staged equivalence cover the path. A synthetic production-host
snapshot pair containing both nullable source families is
`VERIFIED_BOUNDED`; TPC-DS q18 only constructs a formula at this milestone, is
not in the proof floor, and revealed no optimizer correctness bug. The
complete verification subtree passed 34/34 suites and 934/934 tests at that
milestone.

Decimal `MIN` crosses `ir.py`, `decimal.py`, and `relation.py`. The decoder
admits only exact same-type Decimal input/output with phase-aware nullability;
the kernel reduces non-NULL values in raw signed-code order and preserves a
lone NaN; the relational layer supplies NULL for an emitted group with no
non-NULL value, including scalar empty input, and carries the same scalar state
through undefined, intermediate, and final phases.
Independent exhaustive guarded-code and concrete aggregate references, staged
routing, wrong-shuffle checks, and a final-min-to-max solver mutation cover the
path.

The preceding post-q83-precursor 2026-07-24 physical-line audit recorded implementation,
test, and diagnostic rows at source `7e7429bdcae`; documentation includes this
evidence update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 12,247 |
| C++ exporter (`semantic_snapshot.cpp` and `.h`) | 9,413 |
| **Proof-producing code total** | **21,660** |
| Tests, outside the TCB | 54,394 |
| Diagnostic/orchestration tools, outside the TCB | 5,230 |
| Documentation, outside the TCB | 7,716 |

These are raw physical `wc -l` counts over tracked files. The Python and C++
rows enumerate the trusted files in the table above. The test row counts every
tracked file under `ut/`, `*_ut/`, and `prefix_capture/ut/`, including
test-local fixtures, `ya.make`, policy JSON, and README files. Documentation is
every tracked Markdown file under this verification directory. Consequently a
test-local README appears in both audit-surface rows; the rows are not intended
as a disjoint partition. The diagnostic row is the remaining non-test,
non-document, non-TCB source. Build/configuration metadata outside test
directories (`ya.make` and `.gitignore`) is excluded. These figures are a
review baseline, not a generated invariant. The trusted core is a medium-sized
verification subsystem, so it should be audited by vertical semantic slice
rather than treated as one small script.

Relative to the preceding post-q58 audit, these two q83 precursor slices add 19
physical trusted Python lines, 40 C++ exporter lines, and 1,107 test lines;
diagnostic tooling is unchanged. The new trusted review seams are one-level
nested-binding ownership/topology validation in
`semantic_snapshot.cpp`/`ir.py` and the proven-present Date-cast gate in
`semantic_snapshot.cpp`. Existing membership evaluation, Date literals, and
static-`IN` semantics are reused. There is no q83-specific equivalence axiom or
new diagnostic path.

The preceding post-wrapper 2026-07-25 physical-line audit uses source
`a0ec2bc866b` plus this documentation update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 12,247 |
| C++ exporter (`semantic_snapshot.cpp` and `.h`) | 9,900 |
| **Proof-producing code total** | **22,147** |
| Tests, outside the TCB | 55,449 |
| Diagnostic/orchestration tools, outside the TCB | 5,230 |
| Documentation, outside the TCB | 7,885 |

Relative to the preceding q83 audit, the restricted floating-predicate and
exact-wrapper slices add 487 trusted C++ lines and 1,055 test lines. Trusted
Python and diagnostic-tool size are unchanged. The new TCB surface is confined
to the two fail-closed C++ exporter audits described above; existing Python
opaque, literal, and `If` semantics are reused.

The latest post-passive-Double 2026-07-25 physical-line audit uses source
`15fd238fd31`:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 12,391 |
| C++ exporter (`semantic_snapshot.cpp` and `.h`) | 10,245 |
| **Proof-producing code total** | **22,636** |
| Tests, outside the TCB | 56,719 |
| Diagnostic/orchestration tools, outside the TCB | 5,230 |

Relative to the preceding post-wrapper audit, the passive-Double slice adds
144 trusted Python lines, 345 C++ exporter lines, 489 proof-producing lines,
and 1,270 test lines; diagnostic tooling is unchanged. The new trusted seams
are confined to `semantic_snapshot.cpp`, `ir.py`, `types.py`, and `scalar.py`.

The latest post-q66 2026-07-25 physical-line audit uses source
`be52c6395de` plus this documentation update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 12,424 |
| C++ exporter (`semantic_snapshot.cpp` and `.h`) | 10,355 |
| **Proof-producing code total** | **22,779** |
| Tests, outside the TCB | 57,069 |
| Diagnostic/orchestration tools, outside the TCB | 5,230 |

Relative to the post-passive-Double audit, the q66 slice adds 33 trusted
Python lines, 110 C++ exporter lines, 143 proof-producing lines, and 350 test
lines; diagnostic tooling is unchanged. The exporter seam is one positive,
allocation-bounded fold from an audited literal-only String `Concat` tree to
the existing literal IR. The Python seam changes only conservative metadata:
for a Decimal value with finite-coefficient bound `B`, multiplication by an
integral right operand uses the full type-domain magnitude and division by an
integral right operand retains `B`. Exact Decimal value terms, same-Decimal
arithmetic, aggregate semantics, and the top-level obligation are unchanged.
Unknown bounds remain unknown and therefore fail closed at any aggregate that
cannot prove headroom. Independent scalar boundary/special tests, a two-row
aggregate regression, q66's real snapshots, the complete formula dashboards,
and a separate timed solver experiment cover the slice.

The preceding post-read-range physical-line audit on 2026-07-25 uses source
`6e5b1ab2d12` plus this documentation update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 12,424 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 11,332 |
| **Proof-producing code total** | **23,756** |
| Tests, outside the TCB | 57,915 |
| Diagnostic/orchestration tools, outside the TCB | 5,230 |
| Documentation, outside the TCB | 8,421 |

Relative to the post-q66 audit, exact range handling adds 977 trusted C++ lines
and 846 test lines; trusted Python and diagnostic-tool size are unchanged.
The added C++ total consists of the closed 929-line matcher plus its small
integration seam. It lowers only to existing equality/static-`IN` JSON, so no
Python evaluator or SMT theorem rule was added. The independent audit surface
is the complete `ComputeNode` grammar and its catalog/output-key binding,
isolated in `read_range_predicate_impl.h`; source safety, column resolution,
predicate conjunction, JSON validation, and Python equality/membership
semantics are reused.

The completed post-integral-AVG physical-line audit uses implementation commit
`8d3e44f59a6`, policy commit `abe190f6344`, and tracked raw `wc -l`:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 13,048 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 11,505 |
| **Proof-producing code total** | **24,553** |
| Tests, outside the TCB | 59,669 |
| Diagnostic/orchestration tools, outside the TCB | 5,230 |
| Documentation, outside the TCB | 8,724 |

Relative to the post-read-range audit, Slice A adds 624 trusted Python lines,
173 C++ exporter lines, 797 proof-producing lines, and 1,754 test lines;
diagnostic tooling is unchanged, while documentation adds 303 lines. The new
review seams are the strict C++
aggregate contract, `ir.py`'s tagged state validation, `scalar.py`'s
`AverageMetadata` and shared carrier, `relation.py`'s exact
summary/certificate lifecycle, and `verify.py`'s model-domain protocol. At that
checkpoint, fixed-width integral `MIN`/`MAX` for q35 was the next slice.

The completed post-integral-extrema physical-line audit uses committed tree
`a39863e5b33` for code, tests, and diagnostic tooling, plus this documentation
update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 13,108 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 11,505 |
| **Proof-producing code total** | **24,613** |
| Tests, outside the TCB | 59,982 |
| Diagnostic/orchestration tools, outside the TCB | 5,230 |
| Documentation, outside the TCB | 8,915 |

Relative to the post-integral-AVG audit, exact integral extrema, stack-safe
rendering, and central certificate stripping add a net 60 trusted Python lines,
no C++ exporter lines, 60 proof-producing lines, and 313 test lines;
diagnostic tooling is unchanged, while documentation adds 191 lines. The small
net size reflects deletion of
path-local certificate handling as the central evaluator boundary was added.
The new audit seams are the fixed-width extrema decoder/reducer, iterative SMT
renderer, and observer-before-strip lifecycle. The next slice is narrowly
tagged derived-`Double` ordering for q22/q85, initially targeting formula
construction rather than a proof-floor promotion.

The completed post-derived-ordering physical-line audit uses implementation
commit `e8abaff7ff4` and policy commit `3e91814d64e` for code, tests, and
diagnostic tooling, plus this documentation update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 13,257 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 11,791 |
| **Proof-producing code total** | **25,048** |
| Tests, outside the TCB | 61,053 |
| Diagnostic/orchestration tools, outside the TCB | 5,245 |
| Documentation, outside the TCB | 9,117 |

Relative to the post-integral-extrema audit, the slice adds 149 trusted Python
lines, 286 trusted C++ lines, 435 proof-producing lines, and 1,071 test lines.
Diagnostic tooling adds 15 lines, while documentation adds 202. Its review
seams are producer-local provenance tracking and tagged Sort/Merge
serialization in `semantic_snapshot.cpp`, independent schema propagation and
tag validation in `ir.py`, abstract rank comparison in `relation.py`, and
matching StageGraph Merge enforcement in `stages.py`. The tests independently
cover aliases, pass-throughs, retained and discarded Join payloads, all-branch
`UnionAll`, ordinary-order non-regression, forged/missing tags, intermediate
AVG state, and inspector visibility. At that checkpoint the next slice was
exact dynamic `Optional<Date>` plus/minus literal `IntervalFromDays`
normalization for TPC-DS q72.

The completed post-q72 physical-line audit uses implementation commit
`97f103ce060` and policy commit `aa01e609499` for code, tests, and diagnostic
tooling, plus this documentation update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 13,257 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 11,930 |
| **Proof-producing code total** | **25,187** |
| Tests, outside the TCB | 61,380 |
| Diagnostic/orchestration tools, outside the TCB | 5,245 |
| Documentation, outside the TCB | 9,298 |

Relative to the post-derived-ordering audit, the q72 slice adds no trusted
Python, 139 trusted C++ lines, 139 proof-producing lines, and 327 test lines.
Diagnostic tooling is unchanged, while documentation adds 181 lines. Its
review seam is one closed C++ exporter grammar: direct nullable Date member,
two exact literal-interval spellings, explicit source-NULL lifting, and a
stable operator/day opaque identity. Existing Python `if_present`,
opaque-function congruence, nullable result, and Date-domain semantics are
reused unchanged. At that checkpoint the next slice was exact
unique-key-aware at-most-one right-side join compaction for q72, not a global
construction-cap increase.

The completed post-M65 physical-line audit uses implementation commit
`0b0025f2a11`, naming polish `a55f3ecba73`, and policy commit `aa004084427`
for code and tests, plus this documentation update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 13,424 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 11,930 |
| **Proof-producing code total** | **25,354** |
| Tests, outside the TCB | 62,010 |
| Diagnostic/orchestration tools, outside the TCB | 5,245 |
| Documentation, outside the TCB | 9,540 |

Relative to the post-q72 audit, Milestone 65 adds 167 trusted Python lines, no
C++, 167 proof-producing lines, and 630 test lines. Diagnostic tooling is
unchanged, while documentation adds 242 lines. The proof-producing review seam
is one linear fail-closed gate, one shared match-matrix builder, and one
one-row-per-left compactor in `relation.py`; the existing generic join remains
the fallback. The larger test delta is outside the TCB and supplies the
independent exhaustive, coercion, provenance, cap, and StageGraph evidence.
At that checkpoint Milestone 66 next targeted exact ordered
singleton-`Limit` compaction for q9.

The completed post-M66 physical-line audit uses implementation commit
`e8b81982299`, q9 formula policy commit `6804f459df7`, q9 proof policy commit
`0cba3c9262e`, direct encoding commit `66db625c092`, and proof-floor fixture
commit `0d2fc858b70` for code and tests, plus this documentation update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 13,562 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 11,930 |
| **Proof-producing code total** | **25,492** |
| Tests, outside the TCB | 62,524 |
| Diagnostic/orchestration tools, outside the TCB | 5,245 |
| Documentation, outside the TCB | 9,816 |

Relative to the post-M65 audit, Milestone 66 adds 138 trusted Python lines, no
C++, 138 proof-producing lines, and 514 test lines. Diagnostic tooling is
unchanged, while documentation adds 276 lines. The trusted review seam is the
fixed-sequence singleton recognizer, one first-present payload fold, and one
canonical typed fallback in `relation.py`; all rejected shapes use the old
path. The larger test delta is outside the TCB and supplies exhaustive
fixed-order, guard, provenance, metadata, bound, cap, and StageGraph evidence.
At that checkpoint Milestone 67 next targeted q24's restricted
`Optional<Utf8>` `Unicode::ToUpper` export gap.

The completed post-M67 physical-line audit uses reviewed-UDF refactor
`bed31799d83`, exporter implementation `0ca4097d444`, q24 formula policy
`a26d42bdba8`, and cast-gate clarification `f929a36b59c` for code and tests,
plus this documentation update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 13,562 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 12,112 |
| **Proof-producing code total** | **25,674** |
| Tests, outside the TCB | 62,916 |
| Diagnostic/orchestration tools, outside the TCB | 5,245 |
| Documentation, outside the TCB | 10,096 |

Relative to the post-M66 audit, Milestone 67 adds no trusted Python, 182 C++
lines, 182 proof-producing lines, and 392 test lines. Diagnostic tooling is
unchanged; documentation adds 280 lines. The trusted review seam is one
immutable reviewed-UDF specification, one exact fail-closed host recognizer,
and one lowering to existing `if_present`, bound, nullable-opaque, and typed-NULL
IR. The larger test delta is outside the TCB and supplies the exact JSON,
thirty-two isolated near misses, and binding-depth boundary. q64's independent
8,192-row join-output guard remains a larger construction problem.

The completed post-M68 physical-line audit uses compiled-LIKE commits
`b39a8b47b46` and `84515a6c887`, grouped count-distinct commit
`636201f517f`, exact pushed-coalesce commit `658b98e1eee`, and compact exact
coalesce commit `719e0a0d3e5` for code and tests, plus this policy and
documentation update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 13,619 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 13,133 |
| **Proof-producing code total** | **26,752** |
| Tests, outside the TCB | 65,415 |
| Diagnostic/orchestration tools, outside the TCB | 5,318 |
| Documentation, outside the TCB | 10,385 |

Relative to the post-M67 audit, Milestone 68 adds 57 trusted Python lines,
1,021 C++ exporter lines, 1,078 proof-producing lines, and 2,499 test lines.
Diagnostic tooling is unchanged. The 73-line increase corrects the post-M67
audit's omission of four tracked package/CLI files; documentation adds 289
lines. This is a comparatively large exporter slice, so the review unit is the
three closed recognizers and their fail-closed
near-miss matrices, not the raw file as a whole. The Python theorem seam stays
narrow: existing `if_present`, bound, literal, opaque-function, integer
equality, null-safe grouping, and aggregate machinery are reused. No separate
`starts_with` theorem rule was needed.

The completed post-M69 physical-line audit uses delayed-compaction
commit `743643cc20f`, scheduler commit `ad3613f1816`, and policy commit
`7f75c4f2bb0`:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 13,921 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 13,133 |
| **Proof-producing code total** | **27,054** |
| Tests, outside the TCB | 66,015 |
| Diagnostic/orchestration tools, outside the TCB | 5,318 |
| Documentation, outside the TCB | 10,646 |

Relative to the completed post-M68 audit, Milestone 69 adds 302 trusted Python
and proof-producing lines and 600 test lines. The C++ exporter and diagnostic
tooling are unchanged; documentation adds 261 lines. The new proof-producing
review unit is the one fail-closed delayed-Filter recognizer, its stable factor
scheduler, reuse of the independently gated direct unique-RHS compactor, and
exact restoration of the original input column layout.

The completed post-M70 physical-line audit uses mixed-prefix comparison
commit `02b067a8d27` and checked-Project implementation commit
`3d74b1eadcf` for code and tests, plus this policy and documentation update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 14,147 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 13,441 |
| **Proof-producing code total** | **27,588** |
| Tests, outside the TCB | 68,238 |
| Diagnostic/orchestration tools, outside the TCB | 5,320 |
| Documentation, outside the TCB | 10,951 |

Relative to the completed post-M69 audit, the comparison prerequisite and
Milestone 70 add 226 trusted Python lines, 308 C++ exporter lines, 534
proof-producing lines, 2,223 test lines, and two diagnostic lines.
Documentation adds 305 lines. The trusted Python review seam is one exact
mixed-prefix equality construction plus marker validation and outcome-error
composition. The C++ seam is one closed nullable-String recognizer, exact type
materialization, and a two-topology demand gate. The substantially larger test
delta remains outside the TCB and supplies exhaustive schema, type, topology,
subplan, error-composition, exporter, and real-runtime evidence.

The completed post-M71 physical-line audit uses topology-aware routed-copy
implementation commit `d2979a0e459` and policy commit `a754b499484`, plus
this evidence and documentation update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 14,175 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 13,441 |
| **Proof-producing code total** | **27,616** |
| Tests, outside the TCB | 68,377 |
| Diagnostic/orchestration tools, outside the TCB | 5,320 |
| Documentation, outside the TCB | 11,206 |

Relative to the completed post-M70 audit, Milestone 71 adds 28 trusted Python
and proof-producing lines and 139 test lines. The C++ exporter and diagnostic
tooling are unchanged, and documentation adds 255 lines. The review seam is one
existing occurrence/fact-gated exact quotient plus three explicit
topology-specific representation choices; the larger test delta remains
outside the TCB and covers their semantic and resource boundaries.

The completed post-M72 physical-line audit uses concrete-atom implementation
commit `9b9133fda8c` and policy commit `a4a82dd6f7e`, plus this evidence and
documentation update:

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 14,214 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 13,441 |
| **Proof-producing code total** | **27,655** |
| Tests, outside the TCB | 68,567 |
| Diagnostic/orchestration tools, outside the TCB | 5,320 |
| Documentation, outside the TCB | 11,444 |

Relative to the completed post-M71 audit, Milestone 72 adds 39 trusted Python
and proof-producing lines and 190 test lines. The C++ exporter and diagnostic
tooling are unchanged, and documentation adds 238 lines. The trusted review
seam is one Script-local reverse atom map, one partial equality helper, and one
scalar equality call path; the larger test delta remains outside the TCB and
exhausts identity, symbolic fallback, cross-family, NULL-envelope, and sealing
boundaries.

The completed post-M73 physical-line audit uses fixture commit `e55c37f967f`,
factor-rejection commit `7cf4a049f61`, dead-slot commit `1e53b1fb9a0`,
seed-rebase commit `8c5eca71246`, policy commit `a96c2a0be8f`, and this
evidence and documentation update. The fixture is outside the verification
subtree and changes none of the rows below.

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 14,358 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 13,441 |
| **Proof-producing code total** | **27,799** |
| Tests, outside the TCB | 69,271 |
| Diagnostic/orchestration tools, outside the TCB | 5,320 |
| Documentation, outside the TCB | 11,799 |

Relative to the completed post-M72 audit, Milestone 73 adds 144 net trusted
Python and proof-producing lines and 704 test lines. The C++ exporter and
diagnostic tooling are unchanged. Documentation adds 355 lines. The
proof-producing increase is approximately 0.52% of the complete
trusted code. Of those net lines, two expose the existing IR column walk and
142 are localized in `relation.py`; the review units are one factor-local
row-filtering path, one common literal-dead Join-input identity, and one narrow
scheduler rebase branch. The complete proof core remains a medium-sized
27,799-line subsystem rather than one short script, but M73 does not spread a
new semantic contract across the exporter, decoder, solver, or diagnostics.
Its larger independent test surface remains outside the TCB.

The completed post-M74 physical-line audit uses implementation commits
`99557229439`, `36b7dd75d96`, and `8c9c29ceaf7`, policy commits
`26c6d0387b3` and `1545921b5d1`, and this documentation closeout.

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 14,620 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 13,952 |
| **Proof-producing code total** | **28,572** |
| Tests, outside the TCB | 71,181 |
| Diagnostic/orchestration tools, outside the TCB | 5,320 |
| Documentation, outside the TCB | 12,095 |

Relative to the completed post-M73 audit, Milestone 74 adds 262 trusted Python
lines, 511 C++ exporter lines, and 1,910 test lines. The net 773-line
proof-producing increase is approximately 2.78% of the post-M73 core.
Diagnostic tooling is unchanged, while documentation adds 296 net lines. The
new trusted surface is localized to raw
Decimal distinct equality, the closed staged-carrier certificate, and its
independent decoder validation; the larger mutation and differential surface
remains outside the TCB.

The completed post-M75 physical-line audit uses implementation commit
`cda99a952cb`, policy commit `c6fbadcc9a8`, intervening verifier repair
`daab603c2f1`, optimizer-fix commits `c2c66fb1d7b` and `564010e2e4e`, and
this documentation closeout.

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 15,003 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 14,097 |
| **Proof-producing code total** | **29,100** |
| Tests, outside the TCB | 72,842 |
| Diagnostic/orchestration tools, outside the TCB | 5,320 |
| Documentation, outside the TCB | 12,610 |

Relative to the completed post-M74 audit, the current tree adds 383 trusted
Python lines, 145 C++ exporter lines, and 1,661 test lines. The net 528-line
proof-producing increase is approximately 1.85% of the post-M74 core.
`cda99a952cb` contributes 383 trusted Python, 150 C++ exporter, and 1,255 test
lines. The earlier repaired-Concat commit removes five net exporter lines and
adds 31 test lines; the TopSort and computed-projection optimizer fixes add
346 and 29 verifier regression-test lines respectively. Diagnostic tooling is
unchanged. Documentation adds 515 net lines. The new trusted seam is restricted
to audited checked-Concat provenance, one shared value/failure pair, and its
producer-demand certificate; the larger topology, mutation, and workload
surfaces remain outside the TCB.

The completed post-M76 physical-line audit uses semantic implementation
commit `dbdae0a107f`, StageGraph routing commit `70ab3d3631c`,
metadata-transport repair `a7095c6a797`, schema-v5 policy commit `1da14eb637b`,
test-lifetime commit `78e255b5e1f`, and this documentation pass. Its formula,
proof, and full integration evidence is recorded above rather than inferred
from these code counts.

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 15,364 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 14,683 |
| **Proof-producing code total** | **30,047** |
| Tests, outside the TCB | 75,118 |
| Diagnostic/orchestration tools, outside the TCB | 5,332 |
| Documentation, outside the TCB | 13,294 |

Relative to the completed post-M75 audit, M76 adds 361 trusted Python lines,
586 C++ exporter lines, and 2,276 test lines. The net 947-line proof-producing
increase is approximately 3.25% of the post-M75 core. Diagnostic tooling adds
12 lines from the inspector update, while documentation adds 684 net lines.
The trusted increase is confined to the exact raw-window grammar,
independent `window_sum` IR/dataflow validation, and relation-local SUM
semantics. Production stage assignment and its common-key/gather decision are
outside this proof-producing line total, as is the later 93-line production
metadata-transport repair. The 356 post-routing test lines comprise 38 lines
for its q51 regression, 317 benchmark-C++ and two policy-JSON lines for
schema-v5 pair-floor coverage, less one line from test-lifetime cleanup in
`78e255b5e1f`. Their assumptions and independent tests are stated separately
above and below.

The post-M77 physical-line audit uses semantic implementation commit
`3f9b9c8b2c6`, policy commit `adfe48088f5`, and this documentation closeout.
Its focused and complete evidence is recorded above; physical size is not a
substitute for those semantic gates.

| Area | Physical lines |
|---|---:|
| Ten trusted Python semantic modules | 15,530 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 14,926 |
| **Proof-producing code total** | **30,456** |
| Tests, outside the TCB | 76,999 |
| Diagnostic/orchestration tools, outside the TCB | 5,359 |
| Documentation, outside the TCB | 13,933 |

Relative to the completed post-M76 audit, M77 adds 166 trusted Python lines,
243 C++ exporter lines, and 1,881 test lines. The net 409-line proof-producing
increase is approximately 1.36% of the post-M76 core. Diagnostic tooling adds
27 lines, while documentation adds 639 net lines by recursive Markdown count.
The trusted increase is localized to ordered one-through-four-key `window_avg`
export, decoding, and validation, task-local exact Decimal AVG state/finish
semantics, and exact nullable Decimal Abs. The
production metadata-transport widening and movement/routing barriers remain
outside the proof-producing count; the larger mutation, real-host, and workload
evidence remains outside the TCB.

The post-M78 physical-line audit compares the completed M77 tree at
`8ad4068` with exact policy HEAD `e926958d96c`. It uses raw physical `wc -l`
counts over the same tracked implementation/test/tool sets and a recursive
Markdown count for documentation. The proof-producing implementation is the
combined q49 work in `0f12406f6c4`; production routing and robustness commits
`27e3f260017`, `97a03c64ab9`, and `68eb64102c7` remain outside that trusted
total.

| Area | M77 physical lines | M78 physical lines | Delta |
|---|---:|---:|---:|
| Ten trusted Python semantic modules | 15,530 | 15,990 | +460 |
| C++ exporter (`semantic_snapshot.cpp`, `.h`, and `read_range_predicate_impl.h`) | 14,926 | 15,742 | +816 |
| **Proof-producing code total** | **30,456** | **31,732** | **+1,276** |
| Tests, outside the TCB | 76,999 | 79,098 | +2,099 |
| Diagnostic/orchestration tools, outside the TCB | 5,359 | 5,390 | +31 |
| Documentation, outside the TCB | 13,933 | 14,512 | +579 |

The 1,276-line trusted increase is 4.1897% of the M77 proof-producing core.
It is localized to the closed q49 Rank grammar, exact Decimal rescale,
independent IR checks, relation/window semantics, and their proof construction;
the production optimizer fixes, tests, diagnostics, and documentation remain
outside the TCB.

The post-M79 physical-line audit compares code, tests, and tools at exact M78
policy HEAD `e926958d96c` with M79 through policy `4609f334b0c`. Its
documentation baseline is the completed M78 closeout `bf64a1dc9db`; the M79
documentation count includes the audit-isolation update in `26f2210d0d7` and
this closeout. Counts are raw physical `wc -l` over the same tracked sets, with
a recursive Markdown count for documentation. Production window transport,
routing, and required-key range robustness remain outside the proof-producing
implementation total.

| Area | M78 physical lines | M79 physical lines | Delta |
|---|---:|---:|---:|
| Ten trusted Python semantic modules | 15,990 | 16,603 | +613 |
| C++ exporter (M78 core files; M79 also includes both private window headers) | 15,742 | 16,632 | +890 |
| **Proof-producing code total** | **31,732** | **33,235** | **+1,503** |
| Tests, outside the TCB | 79,098 | 82,083 | +2,985 |
| Diagnostic/orchestration tools, outside the TCB | 5,390 | 5,432 | +42 |
| Documentation, outside the TCB | 14,512 | 14,953 | +441 |

The 1,503-line trusted increase is 4.7365% of the M78 proof-producing core. It
is localized to strict q51 metadata decoding and validation, exact
ROWS-prefix Decimal SUM/MAX evaluation, task-local peer choices and routing,
and the closed exporter/topology audits. Production optimizer changes, tests,
diagnostics, and documentation remain outside the TCB.

The post-M80 physical-line audit compares the completed M79 documentation and
policy tree at `0f2ac692a86` with proof-policy commit `ebb5c8806fc` plus this
closeout. The commit changes only the benchmark policy and its C++ regression
fixtures; the trusted implementation and diagnostic tooling are byte-for-byte
unchanged. Counts use the same tracked raw `wc -l` sets as M79.

| Area | M79 physical lines | M80 physical lines | Delta |
|---|---:|---:|---:|
| Ten trusted Python semantic modules | 16,603 | 16,603 | 0 |
| C++ exporter (including both private window headers) | 16,632 | 16,632 | 0 |
| **Proof-producing code total** | **33,235** | **33,235** | **0** |
| Tests, outside the TCB | 82,083 | 82,085 | +2 |
| Diagnostic/orchestration tools, outside the TCB | 5,432 | 5,432 | 0 |
| Documentation, outside the TCB | 14,953 | 15,136 | +183 |

M80 therefore changes the policy obligation set and its tests, not the trusted
semantics. The documentation delta records the four-file proof-promotion
closeout plus the q51 trusted-responsibility terminology correction above.

The post-M82 physical-line audit compares the completed M81 documentation at
`3523737aaf3` with proof-policy commit `42a879e19f5` plus this closeout. The
commit changes only the benchmark policy and its C++ regression fixtures; the
trusted implementation and diagnostic tooling are byte-for-byte unchanged.
Counts use the same tracked raw `wc -l` sets as M79, with a recursive Markdown
count for documentation.

| Area | M81 physical lines | M82 physical lines | Delta |
|---|---:|---:|---:|
| Ten trusted Python semantic modules | 16,603 | 16,603 | 0 |
| C++ exporter (including both private window headers) | 16,632 | 16,632 | 0 |
| **Proof-producing code total** | **33,235** | **33,235** | **0** |
| Tests, outside the TCB | 82,085 | 82,087 | +2 |
| Diagnostic/orchestration tools, outside the TCB | 5,432 | 5,432 | 0 |
| Documentation, outside the TCB | 15,225 | 15,431 | +206 |

M82 therefore changes only proof policy depth, its regression fixtures, and
the four-file closeout. No trusted semantic implementation grows.

The post-M83 physical-line audit compares the completed M82 documentation at
`e4520879eb1` with proof-policy commit `cc85514862d` plus this closeout. The
commit changes only the benchmark policy and its C++ regression fixtures; the
trusted implementation and diagnostic tooling are byte-for-byte unchanged.
Counts use the same tracked raw `wc -l` sets as M82, with a recursive Markdown
count for documentation.

| Area | M82 physical lines | M83 physical lines | Delta |
|---|---:|---:|---:|
| Ten trusted Python semantic modules | 16,603 | 16,603 | 0 |
| C++ exporter (including both private window headers) | 16,632 | 16,632 | 0 |
| **Proof-producing code total** | **33,235** | **33,235** | **0** |
| Tests, outside the TCB | 82,087 | 82,089 | +2 |
| Diagnostic/orchestration tools, outside the TCB | 5,432 | 5,432 | 0 |
| Documentation, outside the TCB | 15,431 | 15,696 | +265 |

M83 therefore changes only proof policy depth, its regression fixtures, and
the four-file closeout. No trusted semantic implementation grows. The
q21/q56/q60 result records future proof-reduction motivation but adds no
trusted implementation.

The post-M84 physical-line audit compares exact M83 policy HEAD
`cc85514862d` and its completed 15,696-line documentation with semantic commit
`476f2ea38f4` plus this four-file closeout. Counts are raw physical `wc -l`
over the same tracked sets as M83: the ten trusted Python modules enumerated
above, the five C++ exporter files, every tracked file under `ut/`, `*_ut/`,
and `prefix_capture/ut/`, the remaining diagnostic/orchestration source, and
every tracked Markdown file under this directory.

| Area | M83 physical lines | M84 physical lines | Delta |
|---|---:|---:|---:|
| Ten trusted Python semantic modules | 16,603 | 16,931 | +328 |
| C++ exporter (including both private window headers) | 16,632 | 16,632 | 0 |
| **Proof-producing code total** | **33,235** | **33,563** | **+328** |
| Tests, outside the TCB | 82,089 | 83,225 | +1,136 |
| Diagnostic/orchestration tools, outside the TCB | 5,432 | 5,432 | 0 |
| Documentation, outside the TCB | 15,696 | 16,068 | +372 |

The trusted increase is confined to `relation.py` (+281 net physical lines)
and `stages.py` (+47); it is approximately 0.9869% of the M83 proof-producing
core. The 1,136-line test increase is confined to the already-registered
`test_sort.py` and `test_stage_compaction.py` modules. The C++ exporter, wire
schema, strict IR, policy, and diagnostic tooling are unchanged. Physical size
does not replace the semantic, differential, packaging, and workload evidence
above.

The post-M85 physical-line audit compares completed M84 documentation commit
`5f86f3b11e0` with semantic commit `67655eaa786`, policy commit
`95182b541fb`, and this four-file closeout. Counts use the same raw tracked
`wc -l` sets as M84: the ten trusted Python modules, five C++ exporter files,
every tracked file under `ut/`, `*_ut/`, and `prefix_capture/ut/`, the remaining
diagnostic/orchestration source, and every tracked Markdown file under this
directory.

| Area | M84 physical lines | M85 physical lines | Delta |
|---|---:|---:|---:|
| Ten trusted Python semantic modules | 16,931 | 17,218 | +287 |
| C++ exporter (including both private window headers) | 16,632 | 16,632 | 0 |
| **Proof-producing code total** | **33,563** | **33,850** | **+287** |
| Tests, outside the TCB | 83,225 | 84,167 | +942 |
| Diagnostic/orchestration tools, outside the TCB | 5,432 | 5,432 | 0 |
| Documentation, outside the TCB | 16,068 | 16,586 | +518 |

The trusted increase is confined to `relation.py` (+275 net physical lines)
and `verify.py` (+12), approximately 0.8551% of the M84 proof-producing core.
The semantic tests add 940 lines in the already-registered `test_sort.py` and
`test_verify.py`; the q21 policy fixture adds another two net test lines. The
C++ exporter, wire schema, strict IR, diagnostic tooling, row/task bound, and
canonical emitted formula are unchanged. Physical size does not replace the
exact-cover proof, packaging, focused workload, repeat, and complete-gate
evidence above.

The post-M86 physical-line audit compares completed M85 documentation commit
`d2535e30390` with semantic commit `374f8fb65df` plus this four-file closeout.
Counts use the same raw tracked `wc -l` sets as M85: the ten trusted Python
modules, five C++ exporter files, every tracked file under `ut/`, `*_ut/`, and
`prefix_capture/ut/`, the remaining diagnostic/orchestration source, and every
tracked Markdown file under this directory.

| Area | M85 physical lines | M86 physical lines | Delta |
|---|---:|---:|---:|
| Ten trusted Python semantic modules | 17,218 | 17,605 | +387 |
| C++ exporter (including both private window headers) | 16,632 | 16,632 | 0 |
| **Proof-producing code total** | **33,850** | **34,237** | **+387** |
| Tests, outside the TCB | 84,167 | 85,165 | +998 |
| Diagnostic/orchestration tools, outside the TCB | 5,432 | 5,432 | 0 |
| Documentation, outside the TCB | 16,586 | 17,103 | +517 |

The trusted increase is confined to `decimal.py` (+127 net physical lines),
`relation.py` (+187), `scalar.py` (+20), and `stages.py` (+54), offset by the
one-line unused-import removal in `verify.py`; it is approximately 1.1433% of
the M85 proof-producing core. Tests add 165 lines in `test_decimal.py`, 163 in
`test_stage_compaction.py`, and 670 in `test_verify.py`. The C++ exporter,
snapshot/IR wire contract, policy, diagnostic tools, bounds, solver schedule,
and mismatch theorem are unchanged. Physical size does not replace the
composition lemma, fail-closed lineage/fallback review, full-subtree chronology,
focused solver evidence, or complete gates above.

The post-M87 physical-line audit compares completed M86 documentation commit
`ac957ea3d5f` with structural-sharing commit `0077c196ea8`, scope-cap commit
`d9be39ad01b`, and this four-file closeout. Counts use the same raw tracked
`wc -l` sets as M86: the ten trusted Python modules, five C++ exporter files,
every tracked file under `ut/`, `*_ut/`, and `prefix_capture/ut/`, the remaining
diagnostic/orchestration source, and every tracked Markdown file under this
directory.

| Area | M86 physical lines | M87 physical lines | Delta |
|---|---:|---:|---:|
| Ten trusted Python semantic modules | 17,605 | 17,788 | +183 |
| C++ exporter (including both private window headers) | 16,632 | 16,632 | 0 |
| **Proof-producing code total** | **34,237** | **34,420** | **+183** |
| Tests, outside the TCB | 85,165 | 85,634 | +469 |
| Diagnostic/orchestration tools, outside the TCB | 5,432 | 5,432 | 0 |
| Documentation, outside the TCB | 17,103 | 17,578 | +475 |

The trusted increase is confined to `smt.py` (+183 net physical lines), about
0.5345% of the M86 proof-producing core. Tests add 469 net lines in
`test_smt.py`. The exporter, snapshot/IR, semantic evaluator, solver schedule,
policy, bounds, and diagnostic tooling are unchanged. Physical size does not
replace the exact-key/scope argument, cap/fallback differential, adversarial
complexity probe, solver equivalence, package gate, or workload evidence above.

The post-M88 physical-line audit compares completed M87 documentation commit
`bde0d7acdf5` with policy/fixture commit `b3ce77ab1d2` plus this four-file
closeout. Counts use the same raw tracked `wc -l` sets as M87: the ten trusted
Python modules, five C++ exporter files, every tracked file under `ut/`,
`*_ut/`, and `prefix_capture/ut/`, the remaining diagnostic/orchestration
source, and every tracked Markdown file under this directory.

| Area | M87 physical lines | M88 physical lines | Delta |
|---|---:|---:|---:|
| Ten trusted Python semantic modules | 17,788 | 17,788 | 0 |
| C++ exporter (including both private window headers) | 16,632 | 16,632 | 0 |
| **Proof-producing code total** | **34,420** | **34,420** | **0** |
| Tests, outside the TCB | 85,634 | 85,638 | +4 |
| Diagnostic/orchestration tools, outside the TCB | 5,432 | 5,432 | 0 |
| Documentation, outside the TCB | 17,578 | 17,926 | +348 |

M88's only tracked non-document changes are the coverage-policy entry updates
and their test fixtures; their test-set net is four physical lines. Trusted
Python, the C++ exporter, the semantic theorem, the snapshot/IR wire, bounds,
solver schedule, and diagnostic tooling do not grow. Physical size does not
replace the stale-evidence review, repeatability check, policy diff audit, or
authoritative proof gates above.

## External assumptions

The production optimizer claim additionally relies on facts not established by
the SMT obligation itself:

- the host invokes the initial hook after `TOpRoot` construction and parent
  computation but before the first new-RBO stage, and invokes the final hook
  after the last stage/property recomputation but before physical generation;
- the shared captured catalog and exported initial/final roots are the plans
  actually present at those boundaries, and instrumentation does not alter
  optimization;
- each accepted exporter encoding and Python semantic rule agrees with the
  corresponding YQL, RBO, KQP task-construction, and runtime behavior;
- each accepted concrete String or Utf8 literal denotes exactly its recorded
  strict-UTF-8 byte sequence, and runtime equality across those two families is
  raw byte equality without normalization;
- the accepted nullable Unicode-uppercase bridge identifies the deterministic
  built-in `Unicode.ToUpper`; `SafeCast` from String to Utf8 deterministically
  returns NULL for invalid bytes and otherwise supplies that UDF with the
  decoded value, so equal raw String inputs have equal combined nullable
  results;
- each accepted compiled-LIKE program is exactly the deterministic,
  case-sensitive YQL/RE2 `PatternFromLike` plus `Match` operation represented
  by its audited pattern/options fingerprint; the generic and pushed
  `KqpOlapApply` spellings have the same NULL-to-false behavior, and pushed
  outer NOT has the explicit Boolean semantics retained in the snapshot;
- each accepted pushed Boolean coalesce evaluates its `Optional<Bool>` left
  operand once, returns its present payload, and otherwise returns the recorded
  non-null Boolean literal; the compact `is-null OR value` / `not-null AND
  value` encoding is exact for the corresponding true/false fallback;
- each accepted checked nullable-String projection returns the direct source
  payload for a present value and raises an observable query error for a
  present NULL; a main result-root Project is demanded, and the accepted
  private keyed `left_semi` RHS is evaluated as the eager/build side even when
  the left input is empty;
- for an accepted compact ordered singleton, a `sequence` relation without an
  ordinal vector is in the exact fixed runtime row order represented by its
  tuple; each enumerated StageGraph Merge outcome establishes one fixed global
  order before `Limit`, while unordered gather does not preserve `sequence`,
  and runtime `Limit(1)` returns the first present row in that order;
- each accepted `outer_bind` represents one fresh correlated scalar invocation
  with no hidden row-selection, ordering, error, or nondeterministic choice
  semantics beyond the explicitly modeled root;
- each accepted relational `EXISTS` descriptor represents one Boolean presence
  test over the recorded inner root; its ordered dependency values come from
  one outer row, its retained predicate has the strict comparison/NULL behavior
  described above, and it has no hidden row selection, ordering, error,
  coercion, correlation, or fanout beyond the admitted form;
- each accepted dynamic-`IN` descriptor represents one uncorrelated
  existential membership test over the recorded lookup/result columns; for an
  independently nullable fixed-width-integral or Date pair, the binding occurs
  only as a direct positive top-level Filter conjunct where FALSE and UNKNOWN
  both reject the outer row; Date values obey the recorded bounded domain, and
  there is no hidden coercion, correlation, cardinality-error, or fanout
  semantics in the `IN` operator itself;
- each scalar binding consumed inside an accepted dynamic-`IN` root is exactly
  the recorded uncorrelated scalar plan, is demanded at the recorded immediate
  unary consumer, and has no hidden dependency, invocation, choice, error, or
  cardinality semantics beyond the ordinary scalar-subplan model;
- each leaf `IN` binding consumed inside an accepted dynamic-`IN` root is
  exactly the recorded second uncorrelated membership test, has no hidden
  subplan dependency or correlation, and preserves the same NULL, Filter-truth,
  caching, error, and row-pair semantics at that nested consumer;
- each accepted nullable-annotated Date item in a raw static-`SqlIn` tuple is
  the recorded direct literal `SafeCast`, MiniKQL parsing proves the runtime
  value present, and replacing it with the emitted non-null Date literal is
  exact;
- each accepted literal-only String `Concat` evaluates by ordered byte
  concatenation, has no hidden failure within the audited type, metadata,
  source-size, and allocation bounds, and therefore equals the emitted
  canonical literal;
- each catalog bound carried into a restricted stored-String `Concat` is a true
  runtime upper bound: Datashard enforces 16 MiB per value and validated Arrow
  `BinaryType` storage bounds one Olap cell by `INT32_MAX`; MiniKQL concatenates
  the audited leaves in fingerprint order, returns that deterministic byte
  string when every intermediate sum fits `UINT32_MAX`, and raises an observable
  deterministic query error when a sum exceeds it, with no modeled failure
  depending on stage, task, evaluation count, or ambient allocation pressure;
- every row counted by an accepted unstaged checked-Concat corridor is demanded
  when its selector is nonbinding at the declared row bound, and a checked
  Project at the staged result root is demanded for every present result row;
- each accepted `opaque_double` denotes the recorded deterministic q83
  average/deviation expression over exactly three direct nullable Int64
  arguments, its fingerprint preserves the complete reviewed callable,
  literal, type, and ordered-use identity, and transporting the result as a
  non-key payload does not inspect or alter its runtime Double value;
- each certified `ExpandMultiDistinct` physical
  `Nothing(Optional<Tuple<Decimal(35,s),Uint64>>)` pad denotes an absent
  logical Decimal AVG lane, identity `UnionAll` and payload-only StageGraph
  transport do not inspect or alter that state, and the matching Final AVG
  consumes the intermediate `(sum,count)` states with count-weighted semantics;
- each accepted directly linked intermediate/final Decimal `SUM` ignores NULL
  original inputs, materializes exactly the visible intermediate scalar
  represented by the private state, and applies the same special-aware
  aggregate operation to non-NULL partial scalars at the final phase. A
  non-nullable empty intermediate materializes present zero; nullable empty
  input remains NULL. Within the verifier-checked finite headroom, flattening
  selected original bags and composing their partial states are therefore
  observationally identical;
- each accepted integral `AVG` trait denotes exactly the recorded
  `Optional<Int64> -> Optional<Double>` runtime aggregate, intermediate
  `(count,min,max)` is an exact summary of its original non-NULL inputs and
  composes exactly across the directly linked final phase, and non-NULL count
  at most two makes that summary sufficient to identify the unordered input
  multiset;
- each accepted `integral_avg_rank_v1` Sort or Merge key is the recorded
  completed integral AVG carried through only the certified direct dataflow,
  and runtime binary64 ordering of those results is a total preorder whose
  equivalence classes can be embedded in the verifier's integer rank;
- each accepted fixed-width integral `MIN`/`MAX` trait uses the recorded
  signed/unsigned comparison for its exact input/output type, ignores NULL
  inputs, and combines intermediate values with the same associative extremum
  operation;
- the producer observer sees every successful completed integral-AVG result
  before any parent transformation, and the completed certificate has no
  runtime value or downstream transport semantics beyond constructing the
  model-domain exclusion;
- each accepted repeated-source Map rename copies the same runtime input value
  into every declared distinct output, suppresses the original source once,
  and preserves the recorded Map-element order without hidden computation;
- each accepted side-explicit JoinKey names the actual left and right runtime
  values, each admitted shared-IU semi/anti join exposes only its selected
  side, and StageGraph occurrences with equal IU spellings remain distinct
  runtime streams;
- source placement and HashShuffle routing assign every present row to exactly
  one modeled task, so opposite task facts are exhaustive and mutually
  exclusive; a Broadcast gather occurs before runtime replication, while
  HashShuffle gathers producer tasks and then routes each reconstructed
  present row once, preserving the modeled connection multiplicity;
- each accepted `YqlAggWin(sum|avg)` spelling evaluates over the rows visible
  in its current runtime task, groups the complete ordered nullable partition
  tuple with SQL `IS NOT DISTINCT FROM`, and ignores NULL Decimal inputs. SUM
  returns NULL for an all-NULL partition and otherwise uses the exact Decimal
  aggregate addition. AVG additionally carries exact widened Decimal SUM and
  `Uint64` count state, returns NULL for count zero, and for positive count uses
  runtime Decimal division with nearest/even rounding plus same-scale
  narrowing;
- each accepted q49 `YqlWin(rank)` spelling evaluates its complete unpartitioned
  task input using the recorded ascending/null-first Decimal key, ordinary
  Decimal equality for peers, SQL rank gaps, and one independent unstable-sort
  ordinal family per leaf; raw Decimal order is `-Inf < finite < +Inf < NaN`,
  separate NaN rows need not be peers, `CalcOverWindow` publishes no output
  sequence, and the final TopSort supplies the observed order;
- each accepted q51 `YqlAggWin(sum|max)` spelling evaluates over exactly the
  rows visible in its current task; SUM partitions required Int64 item values
  and outer MAX partitions nullable Int64 item values, both by `IS NOT DISTINCT
  FROM`; each orders nullable Date ascending with NULLs first and aggregates the
  ROWS prefix from unbounded preceding through the current row;
  each of the four leaves has an independent legal order for equal Date peers,
  SUM and MAX ignore NULL inputs, SUM uses exact Decimal addition within the
  verifier-derived finite-headroom bound, MAX uses raw Decimal order including
  specials and consumes the recorded distinct typed input columns, and no
  Project publishes the window's internal sequence;
- each accepted Decimal `Abs` propagates NULL and applies the MiniKQL Decimal
  builtin's raw signed-`TInt128` rule: `SafeNeg` exactly for a negative code,
  leaving positive infinity and the positive NaN sentinel unchanged;
- runtime HashV2 routes equal nullable keys, including two NULL keys, to the
  same task; a nonparallel `UnionAll` consumer has one task, and no later
  physical rule erases or bypasses the explicit Aggregate-to-window stage
  boundary installed by `70ab3d3631c`;
- each exported catalog unique key denotes the runtime at-most-one constraint
  over present base rows, and an accepted direct scan plus task routing
  preserves each source cell exactly while only strengthening its presence
  guard;
- each accepted direct Uint64 `Just` is always present at runtime and the
  synthetic typed-NULL branch preserves its exact static Optional schema;
- each accepted scalar-final Uint64 `unwrap` denotes the physical builder's
  coalesce-to-zero result and therefore has a non-null effective output for
  empty, all-NULL, and populated inputs;
- each accepted scalar or grouped fixed-width integer or nullable Decimal
  count-distinct uses the runtime raw aggregate equality of its exact input
  type, including self-equality for the Decimal NaN code, ignores absent rows
  and NULL values, partitions rows by the modeled null-safe grouping-key
  equality, and returns one non-null Uint64 count without overflow within the
  declared relation-row bound;
- each accepted `yql-datetime-year-v1` shape denotes the same complete
  Date-to-Timestamp cast and deterministic, total Split/GetYear operation on
  the bound Date payload;
- each accepted Date-`Unwrap` SafeCast spelling converts Int32 zero to a
  present Date zero, exactly like the accepted `Just(Date(0))` spelling, so
  the normalized missing branch is exact and the runtime error path is
  unreachable;
- each accepted `cast_decimal` `source_type` names the actual runtime source,
  weak integral-to-Decimal `SafeCast` propagates source NULL but saturates
  present overflow to signed infinity, and same-scale non-decreasing-precision
  Decimal `SafeCast` preserves every finite and special encoded value; the six
  accepted q49 `Decimal(35,2)`-to-`Decimal(15,4)` casts multiply finite raw
  coefficients by 100, saturate at the target precision boundary, preserve
  specials, and propagate NULL;
- every propagated Decimal integral-arithmetic bound covers all finite runtime
  outputs under the recorded integer type domain; NULL and Decimal specials do
  not create an additional finite result outside that bound;
- opaque fingerprints identify the same runtime function exactly when
  intended, and every admitted opaque expression is deterministic, total, and
  safe to model as an uninterpreted function;
- symbolic Merge input ordinals describe each producer's runtime sequence:
  strict ordinal order must be preserved for two present rows, while equal
  ordinals impose no relative order;
- the fixed two-task routing, hashing, connection, ordering, multiplicity, and
  error semantics agree with the runtime for the admitted StageGraph subset;
- the pinned Z3 executable correctly decides the emitted SMT-LIB formula, and
  the process and output parser return its result without corruption.

Changing a capture point, runtime semantic rule, supported operator field,
opaque positive list, hash/task rule, or solver version therefore requires a
trust-boundary review even when the Python API is unchanged.

## Explicitly outside the proof TCB

The following components supply evidence, diagnostics, preservation, or
workflow automation, but do not contribute clauses to a normal
start-to-finish `UNSAT` result:

- `rbo_verifier/cli.py`, command wrappers under `*_bin/`, and build metadata;
- `inspector/` and `inspect_ut/`;
- `replay/`, `confirmation/`, and their tests;
- `tools/`, `bisect_bin/`, `bisect_ut/`, and diagnostic `prefix_capture/`;
- `benchmark_ut/`, its policy, and workload reports;
- `ut/`, `cpp_ut/`, `integration_ut/`, `runtime_ut/`, and all other tests;
- this documentation, optimizer trace renderers, Explain JSON, and retained
  repro artifacts.

Tests and replay are essential confidence and finding-classification
boundaries, but trusting them is not necessary for the bounded theorem above.
Conversely, passing them cannot repair an unsound exporter or semantic encoder.
The normal host hook placement remains an external assumption, not a
diagnostic-tool responsibility.

## Slice-by-slice audit procedure

For each new semantic slice:

1. State the exact accepted runtime shape and observable semantics, including
   NULLs, errors, bags/order, duplicates, shared-DAG behavior, task locality,
   and nondeterministic choices.
2. Trace every relevant C++ field through serialized JSON, strict IR decoding,
   evaluator terms, result-family comparison, and the final SMT assertion.
3. Review every near-miss and resource boundary. Missing evidence, malformed
   shape, unknown setting, or exceeded ceiling must fail closed.
4. Compare the symbolic encoding with an independent concrete reference on
   small exhaustive domains where feasible. The reference must not reuse the
   encoder's decision logic.
5. Add cross-language exporter mutations and a real-host boundary case when
   the shape originates in optimizer state.
6. Inspect emitted SMT for a minimal identity case and a one-field semantic
   mutation; require the former to be `UNSAT` and the latter to expose a
   mismatch when the bound permits it.
7. Run the focused suites before the full verifier, C++, inspector, integration,
   and workload gates. Workload coverage is the last check, not the semantic
   oracle.

## Conformance matrix

This matrix identifies the primary review path and independent evidence for
each slice. It is an audit checklist, not a claim that tests are exhaustive.

| Slice | Trusted path to review | Primary independent evidence |
|---|---|---|
| Capture, catalog, root schema | host hook assumption; `semantic_snapshot.*`; `ir.py`; `verify.py` | `cpp_ut/semantic_snapshot_exporter_ut.cpp`; `integration_ut/optimizer_snapshot_pair_ut.cpp`; schema-mutation tests |
| Types, NULLs, scalar functions | `semantic_snapshot.cpp`; `ir.py`; `types.py`; `scalar.py`; `decimal.py`; `string_order.py` | `ut/test_scalar.py`; `test_checked_concat.py`; `test_decimal.py`; `test_string_order.py`; `test_string_proof.py`; `test_sql_in.py`; `test_project_error.py`; `test_window_sum.py`; `test_window_avg.py`; `test_window_rank.py`; concrete same/different/cross-Script/symbolic String-atom equality and exhaustive ordinary/null-safe NULL matrices; canonical literal-only and restricted stored-String `Concat`, String-predicate, generic/pushed compiled-LIKE, pushed Boolean-coalesce, Date-year, dynamic Date-shift, nullable String-to-Utf8 `Unicode.ToUpper`, proven-total Date-`Unwrap`, checked nullable-String `Unwrap`, direct-Uint64-`Just`, exact Decimal weak-`SafeCast` including the fixed q49 rescale, exact nullable Decimal Abs including specials, proven-present raw-tuple Date-`SafeCast`, restricted whole-floating-predicate, exact whole-partition Decimal-window and global-rank leaves, passive-Double carrier, and exact literal-wrapper mutations; checked-Concat type/root-fingerprint/direct-argument mutations and shared value/failure identities; compiled-LIKE cross-dialect fingerprint/NOT/descriptor mutations and exhaustive compact-coalesce truth table; q24 exact JSON, 32 isolated Map/cast/lambda/UDF mutations, and 63/64 binding-depth boundary; checked-projection source/result/type/topology/error-composition mutations and direct-root plus empty-left-semi runtime boundaries; integral-right Decimal finite-bound boundary/special and two-row aggregate tests; passive-carrier identity/mutation and non-key Sort/Merge passenger proofs; `source_type`, NULL, overflow, widening-special, q49 scale/saturation/special, and fail-closed references; synthetic real-host proofs; exporter near-miss mutations |
| Logical bags, order, limits, errors | `semantic_snapshot.cpp`; `ir.py`; `smt.py`; `sort_network.py`; `relation.py`; `verify.py` | `ut/test_logical_reference.py`; `test_limit.py`; `test_sort.py`; `test_checked_concat.py`; checked-Concat present-row error composition, discarded-row eagerness, exact corridor, forbidden consumers, staged-root demand, producer-spine rejection, 2/3-row cardinality boundary, shared-stage proof, and dropped-error counterexample; exhaustive network topology/prefix/nullable/mixed-order/Merge-hole/AVG-state tests; completed-integral-AVG rank identity/order/mutation and provenance-forgery tests; fixed-sequence singleton-`Limit` permutations/presence masks, nullable payloads, tied-ordinal fallback, dead padding, metadata rejection, Decimal bounds, audit cap, and StageGraph Merge compaction; null-safe certificate validation, complete/incomplete-key exact-reference comparisons, nullable String and composite keys, choice-free predecessor/network paths, and TopSort/Limit preservation; preferred keyed-cover sparse nullable-composite exhaustive reference, independent Z3 equivalence, per-mismatch SAT branches, positional near misses, and 64-branch/256-comparison cap boundaries; packed-layout, declaration-structure, present-prefix equality, and cap tests; deep stack-safe rendering plus 3,000-DAG byte differential; focused concrete differential tests |
| Aggregates and subplans | `semantic_snapshot.cpp`; `ir.py`; `decimal.py`; `scalar.py`; `relation.py`; `verify.py` | exact whole-partition Decimal SUM/AVG source grammar, AVG-only ordered one-through-four nullable String/Int64 partition keys (SUM retains one nullable String key), private Aggregate/Project topology, named duplicate-SUM carrier selection, NULL partition/input behavior, multiplicity, split-state lineage, SUM/count overflow headroom, task locality, exact positive-count tie rounding, and malformed/fanout/subplan mutations; aggregate/DistinctAll/count-distinct/unwrap exporter and IR mutations; grouped-Aggregate and `DistinctAll` nullable-composite `K` derivation, direct-alias and partition-key remapping, and computed/missing/`error_on_null` Project drops; nullable Decimal count-distinct NULL/duplicate/NaN/raw-code references and type/nullability mutations; direct and staged Decimal-AVG topology, one-producer/pad/alias/fanout/exposure/descriptor mutations and weighted-state differentials; integral-AVG strict contract, one/two/three-row semantics, split-state mutation, central producer-observe/parent-strip lifecycle, model-domain SAT/UNKNOWN/UNSAT protocol, and projected/sorted/limited/staged observation tests; fixed-width signed/unsigned integral-extrema boundary, NULL/group/split, odd-width exhaustive, and solver-mutation checks; exhaustive scalar/grouped fixed-width count-distinct duplicates, nullable grouping keys, `DistinctAll(group,value) -> count` differential, and full candidate-group triangular cap; scalar-final unwrap empty/all-NULL/present references; Decimal-extrema raw-code differential, routing, and solver-mutation checks; nullable composite-key differential and staged-routing checks; `ut/test_subplans.py`; cardinality, demand, NULL, duplicate, error, exact scalar- and one-level `IN`-inside-`IN` ownership/nesting/cache/choice checks, nested finite references and sequential-semi solver differentials, correlated outer-binding, one- and exact two-dependency `EXISTS` ordering/shape/semi/anti checks, dynamic-`IN` mapping/cache/pair-cap and positive-nullable integral/Date-context checks, real-host Decimal-AVG and correlated-`EXISTS`, and non-null/nullable `IN`-to-`left_semi` cases |
| Split Decimal-SUM summary | `decimal.py`; `scalar.py`; `relation.py`; `stages.py` | `ut/test_decimal.py`, `test_stage_compaction.py`, and `test_verify.py`; flattened-versus-composed NULL/finite/NaN/infinity references; direct/staged grouped/ungrouped exhaustive two-row/two-task differential; maximum-precision type and strict headroom boundaries; exact intermediate/final lineage plus root/subplan/fanout/key/phase/function/type/distinct/unwrap/duplicate-use near misses; authoritative scalar/NULL/bound reconstruction; missing and malformed all-scalar fallback; nullable/non-nullable empty partials; quantified-choice dependency registration; exclusive task-copy lane selection and invalid-alternative drop; accepted final-term non-redecoding check; focused q56/q60 formula and branch-size differential |
| Global Decimal Rank | `semantic_snapshot.cpp`; `ir.py`; `decimal.py`; `scalar.py`; `relation.py`; `stages.py`; `verify.py` | `ut/test_window_rank.py`; six-leaf q49 exact JSON and topology/type/name/order/frame mutations; finite/infinity/NaN peer, gap, NULL, unstable-ordinal, and no-published-order references; q49 exact formula and 60-second `UNKNOWN`; fixed-database q49 serial control, hash-routing counterexample, and concrete trace; committed synthetic universal serial proof and hash-routing counterexample; focused real-host capture with physical `YqlWin` failure |
| Ordered Decimal ROWS windows | `semantic_snapshot.cpp`; `window_expression_export_impl.h`; `window_projection_audit_impl.h`; `ir.py`; `decimal.py`; `scalar.py`; `relation.py`; `stages.py`; `verify.py` | `ut/test_window_rows.py`; exact q51 four-leaf/three-Project JSON; C++ binder plus cross-language names, local orders, frame, types, SUM-Aggregate provenance, distinct MAX-input, topology, mixed-family, fanout, and subplan mutations; concrete required/nullable item partition, NULL input, peer-order, prefix-SUM/MAX, Decimal-special/headroom, task-local, and no-published-order references; item-only HashV2 and Date-liveness routing checks; focused formula and 60-second `UNKNOWN`; focused real-host exact Initial/Final capture with later physical `YqlAggWin` failure |
| StageGraph, reads, joins, and routing | `semantic_snapshot.cpp`; `read_range_predicate_impl.h`; `ir.py`; `scalar.py`; `stages.py`; `relation.py` | exact q9 point and q45 finite-set `ComputeNode` references; exhaustive range-grammar/key/annotation/pointer-identity mutations; pushed-range-plus-OLAP conjunction; `OriginalPredicate` irrelevance and `ComputeNode` sensitivity; synthetic window full-group-key `COUNTEREXAMPLE` and synthetic partition-only `VERIFIED_BOUNDED`, with production q12 post-fix `UNKNOWN`; nullable-key routing, global/disjoint/untracked/malformed gather, rename-history/current-input checks, and `cpp_ut/stage_assignment_rules_ut.cpp`; staged Decimal-AVG carrier payload transport plus HashShuffle-key/Merge-order rejection; tagged integral-AVG Merge propagation/mismatch tests; `ut/test_stagegraph_reference.py`; `test_stage_compaction.py`; same-occurrence/opposite-fact gating, ordinary eight-row threshold, forced eligible Broadcast compaction, HashShuffle eight-cell/ten-cell boundary, conditional hash-key ITE and opposite new routing facts, NULL/Decimal/integral-AVG state preservation, and overlapping Broadcast multiplicity; `P ⊆ K` global promotion, broken-certificate alternatives, Broadcast non-promotion, cross-task duplicate rejection, and choice-free Merge-network cap boundary; shared-IU semi/anti exhaustive execution; JoinKey budget/mutation checks; direct unique-RHS exhaustive bags, composite/extra keys, cross-type coercion rejection, provenance/schema/predicate/Project/limit/metadata mutations, row/pair caps, and Broadcast/gather equivalence; delayed Filter/Cross single, reversed, composite, residual, factor-local rejection/NULL/order/outcome/work-cap, deferred-factor scheduling, certified seed rebase and three-factor continuation, explicit reordered-inner equivalence, mutation, exact column-restoration, cap, override, shared-producer, subplan, choice, and StageGraph-gate tests; literal-false Join inputs across all kinds, poisoned payloads, symbolic-presence retention, sequence-metadata erasure, and exact cap accounting; C++ topology/task mutations; real-host integration |
| SMT construction and verdict | `smt.py`; `verify.py` | `ut/test_smt.py`; `test_verify.py`; product ownership, closed-definition, free-symbol, nullary-capture, and foreign-declaration rejections; emitted-SMT inspection; exact independently rebuilt structural sharing and class/sort/atom/operation/child-order near misses; Script-owner isolation; global, shadowed, nested, and sibling quantifier scopes; definition-parameter scopes; deterministic hygienic aliases; deep and colliding-hash DAGs; real and forced 16,384/16,385 cap boundaries; byte-exact identity fallback and nested structural re-entry; Z3 equivalence; identity and semantic-mutation obligations; preferred metadata/canonical-formula invariance, soundness-first protocol, canonical-skip versus ordinary canonical-first scheduling, all-branch proof, winning-branch replay, shared decreasing deadline, untried-branch rejection, first-UNKNOWN preservation, and empty-portfolio rejection |
| Workload reach and regressions | no additional trusted code | `benchmark_ut/`, coverage policy, TPCH/TPC-DS reports including focused q97/q88/q99/q21/q15/q19, the clean M80 20/20, M82 21/21, M83 22/22, M85/M86/M87 23/23, and M88 25/25 TPC-DS proof gates, the M83/M84/M85 q21/q56/q60 batches, M86 q56/q60 batch, q21 and q15/q19 repeats, canonical-formula stability, Decimal-SUM and M87 structural-CSE formula-size reductions, q56/q60 preferred payload localization, the superseded uncapped TPCH performance diagnostic, capped TPCH recovery, all 101 formula rows, inspector, and replay for candidates |
