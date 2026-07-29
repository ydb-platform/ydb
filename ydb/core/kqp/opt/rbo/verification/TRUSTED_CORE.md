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

The canonical formula is retained as one grouped mismatch assertion. Solver
execution first sets that check's SMT timeout to at most three quarters of the
one global solver deadline. If it returns `UNKNOWN`, the verifier replaces
only that assertion with an exact distributive cover: no enabled left
language, no enabled right language, then one guarded unmatched-result
predicate for each normalized left and right outcome. The verdict is
`VERIFIED_BOUNDED` only when the canonical assertion is `UNSAT`, or every
branch is `UNSAT` before the same deadline. Any branch `SAT` wins immediately;
an unresolved or untried branch prevents a proof. Model extraction reruns the
exact winning assertion without resetting the deadline.

The critical construction invariant is:

```text
canonical mismatch = OR(all exact solver branches)
```

`relation.py` assembles both forms by local Boolean distribution. A
solver-backed quantified 2-by-2 representative regression checks their
equivalence and fixed branch order. A future edit to either representation
requires reviewing the construction itself, not merely rerunning that example.
`--emit-smt` deliberately writes the canonical monolithic formula for stable
inspection. It is the exact theorem but not a transcript of the internal
portfolio, so solving that one file can have different performance or return
`UNKNOWN` where the portfolio succeeds.

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
| `semantic_snapshot.cpp` | Mechanical catalog and plan export; scalar normalization and safety gates, including exact literal-only String `Concat` folding, the passive-Double constructor, and strict phase-linked integral-AVG admission; operator, exact scalar- and one-level `IN`-inside-`IN` nesting, subplan, correlated outer-binding, StageGraph, topology, task, and resource validation; exact read-range integration; deterministic JSON serialization. |
| `read_range_predicate_impl.h` | Closed q9/q45 point and finite point-set `RangeInfo::ComputeNode` grammar, physical-key/catalog binding, extractor-cap and node-identity validation, and lowering to existing equality/static-`IN` predicate IR. Included exactly once inside `semantic_snapshot.cpp`'s anonymous namespace. |
| `rbo_verifier/ir.py` | Strict JSON decoding, version/schema validation, normalized IR, expression typing, tagged aggregate-state contracts including phase-linked integral AVG, passive-Double use confinement, all-plan-root virtual-binding confinement, exact scalar- and one-level `IN`-inside-`IN` plus correlated-subplan shape checks, and operator/StageGraph invariants. |
| `rbo_verifier/types.py` | Supported scalar identities, exact domains, opaque-carrier family, and compatibility predicates. |
| `rbo_verifier/smt.py` | Typed immutable SMT terms, script-owned one-constructor product datatypes, closed quantifier-free exact function definitions, quantifier-safe sharing, deterministic canonical rendering, exact marked-obligation substitution, and solver-output parsing primitives. |
| `rbo_verifier/string_order.py` | Finite exact bounded quotient for String/Utf8 equality and unsigned byte ordering. |
| `rbo_verifier/decimal.py` | Decimal representation, domains, comparison, arithmetic, extrema, specials, and proof bounds. |
| `rbo_verifier/scalar.py` | Nullable values, SQL three-valued predicates, exact scalar evaluation, conservative Decimal finite-coefficient propagation, tagged `AverageMetadata`, the shared cardinality-certified integral-AVG carrier, typed opaque functions, and the domain-free passive carrier encoding. |
| `rbo_verifier/sort_network.py` | Audited power-of-two bitonic compare-exchange topology and exact construction cost. |
| `rbo_verifier/relation.py` | Symbolic database, unique-key constraints, logical operators, aggregate ghost state and node-local integral-AVG certificates, per-row scalar subplans, bags/sequences, packed exact Sort/Merge transport with concrete or symbolic producer order, exact present-prefix equality, errors, choices, result-family equality, and the exact mismatch cover. |
| `rbo_verifier/stages.py` | Two-task StageGraph execution, routing, connection semantics, per-task evaluation, and root gathering. |
| `rbo_verifier/verify.py` | Boundary/catalog/schema checks, shared model construction, producer-local integral-AVG observation, mandatory model-domain precheck, canonical/branch solver portfolio, one-deadline status interpretation, and witness decoding. |

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
producer before returning the family to any parent. The observer builds an
exact bounded reachability predicate over successful present non-NULL rows;
the certificate is then removed before downstream projection, sorting,
limiting, compaction, or routing. Sorting-network and StageGraph paths enforce
that lifecycle rather than transporting completed certificates. This design
keeps one `AverageMetadata` union instead of parallel hidden fields and makes
the proof-domain dependency explicit at its origin.

The solver protocol first proves that no completed integral average with count
greater than two is reachable. `SAT` and `UNKNOWN` both become verifier
`UNKNOWN`. After exclusion `UNSAT`, an uninterpreted-carrier semantic `SAT`
also becomes `UNKNOWN`, because distinct summaries can yield the same rounded
binary64 value; exact binary64 replay is required before treating it as a
runtime candidate. Only semantic `UNSAT` proves equivalence. The raw formula's
top-level OR is intentional and auditable, but its standalone `SAT` result is
not a counterexample.

Focused two-row/two-task TPC-DS q7/q13/q26 runs all emit formulas after
194/1,181, 247/1,830, and 204/1,122 ms, with combined report SHA-256
`721507f60df911e5906865fb26710ed98772338b5aa74afc93532dad63881853`.
Their separate 60-second solver results are `UNKNOWN` at branch 4/28, 4/4,
and 4/28. This raises focused measured coverage to 59 TPC-DS and 77/121
combined formulas, but adds no proof or optimizer finding; the proof floor
remains twenty-seven. The policy pins all three at preparation plus formula
construction.

Commits `8d3e44f59a6` and `abe190f6344` record the completed implementation and
coverage policy. The final suites pass 608/608 Python verifier, 237/237 C++
exporter, 47/47 inspector, and 14/14 policy tests. The complete semantic
partition is TPCH 18 formula / 2 unsupported / 2 no-pair and TPC-DS 59 / 22 /
18, with preparation 20/2 and 73/26. The 20 TPCH plus 81 TPC-DS exact pairs
produce 18 plus 66 verifier entrants. q35 is the fourth new TPC-DS entrant and
rejects unsupported integral `MAX` in Python; q7/q13/q26 are the three new
formulas. Coverage is therefore 77/121 corpus, 77/101 exact-pair, 77/93
preparation-success, and 77/84 verifier-entry formulas.

The complete TPCH dashboard spends 3,273/37,511 ms in preparation/verifier
work and has report SHA-256
`f7430b2bc2e0dc3779b939831afa163d7fa7b45a7c12eeadae761117f3517b8f`;
TPC-DS spends 76,727/851,301 ms and has report SHA-256
`c37f457d0335a8b94ee10d48a5e15bffb86d6ec671050fba4538297e89688867`.
Its q7/q13/q26 rows spend 210/1,258, 279/2,049, and 224/1,361 ms. Slice A adds
no proof or optimizer finding; the proof floor remains twenty-seven.

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
tasks. The current complete policy gate independently returns 11/11 TPCH and
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

The canonical String-predicate bridge adds no evaluator-specific truth table.
`semantic_snapshot.cpp` alone must establish the narrow generic and OLAP
grammars, catalog type/nullability, positive-filter coalesce handling, and the
one-to-one mapping from `EndsWith`/`StringContains` to their two stable
fingerprints. Existing `ir.py`/`scalar.py` opaque-function validation then
shares each deterministic total function and its ordered column/literal
arguments across both plans. Cross-dialect exporter mutations and a
solver-backed real-host fixture are the independent evidence.

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
q97, or q59 is a bounded proof.

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
Every other `Unwrap`, including the remaining String shape in TPC-DS q8,
fails closed. C++ near-miss mutations, independent Python NULL/present
references and semantic mutations, real-host initial/final normalization, and
the q38/q87 bounded proofs cover this vertical path.

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
rows enumerate the trusted files in the table above. Tests are source files
under `ut/`, `*_ut/`, and `prefix_capture/ut/`; documentation is every tracked
Markdown file under this verification directory. The diagnostic row is the
remaining non-test, non-document,
non-TCB source. Build/configuration metadata (`ya.make`, `.gitignore`, and the
coverage policy) is excluded. These figures are a review baseline, not a
generated invariant. The trusted core is a medium-sized verification
subsystem, so it should be audited by vertical semantic slice rather than
treated as one small script.

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
summary/certificate lifecycle, and `verify.py`'s model-domain protocol. The
next recommended exact slice is fixed-width integral `MIN`/`MAX` for q35,
before derived-`Double` ordering for q22/q85.

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
- each accepted `opaque_double` denotes the recorded deterministic q83
  average/deviation expression over exactly three direct nullable Int64
  arguments, its fingerprint preserves the complete reviewed callable,
  literal, type, and ordered-use identity, and transporting the result as a
  non-key payload does not inspect or alter its runtime Double value;
- each accepted integral `AVG` trait denotes exactly the recorded
  `Optional<Int64> -> Optional<Double>` runtime aggregate, intermediate
  `(count,min,max)` is an exact summary of its original non-NULL inputs and
  composes exactly across the directly linked final phase, and non-NULL count
  at most two makes that summary sufficient to identify the unordered input
  multiset;
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
- each accepted direct Uint64 `Just` is always present at runtime and the
  synthetic typed-NULL branch preserves its exact static Optional schema;
- each accepted scalar-final Uint64 `unwrap` denotes the physical builder's
  coalesce-to-zero result and therefore has a non-null effective output for
  empty, all-NULL, and populated inputs;
- each accepted direct Int64 count-distinct uses ordinary Int64 equality,
  ignores absent/NULL rows as modeled, and returns one non-null Uint64 count
  without overflow within the declared relation-row bound;
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
  Decimal `SafeCast` preserves every finite and special encoded value;
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
| Types, NULLs, scalar functions | `semantic_snapshot.cpp`; `ir.py`; `types.py`; `scalar.py`; `decimal.py`; `string_order.py` | `ut/test_scalar.py`; `test_decimal.py`; `test_string_order.py`; `test_string_proof.py`; `test_sql_in.py`; canonical literal-only String-`Concat`, String-predicate, Date-year, proven-total Date-`Unwrap`, direct-Uint64-`Just`, exact Decimal weak-`SafeCast`, proven-present raw-tuple Date-`SafeCast`, restricted whole-floating-predicate, passive-Double carrier, and exact literal-wrapper mutations; integral-right Decimal finite-bound boundary/special and two-row aggregate tests; passive-carrier identity/mutation and non-key Sort/Merge passenger proofs; `source_type`, NULL, overflow, widening-special, and fail-closed references; synthetic real-host proofs; exporter near-miss mutations |
| Logical bags, order, limits, errors | `semantic_snapshot.cpp`; `ir.py`; `smt.py`; `sort_network.py`; `relation.py` | `ut/test_logical_reference.py`; `test_limit.py`; `test_sort.py`; exhaustive network topology/prefix/nullable/mixed-order/Merge-hole/AVG-state tests; packed-layout, declaration-structure, present-prefix equality, and cap tests; focused concrete differential tests |
| Aggregates and subplans | `semantic_snapshot.cpp`; `ir.py`; `decimal.py`; `scalar.py`; `relation.py`; `verify.py` | aggregate/DistinctAll/count-distinct/unwrap exporter and IR mutations; integral-AVG strict contract, one/two/three-row semantics, split-state mutation, node-local certificate, model-domain SAT/UNKNOWN/UNSAT protocol, and projected-result observation tests; exhaustive count-distinct duplicates and triangular cap; scalar-final unwrap empty/all-NULL/present references; Decimal-extrema raw-code differential, routing, and solver-mutation checks; nullable composite-key differential and staged-routing checks; `ut/test_subplans.py`; cardinality, demand, NULL, duplicate, error, exact scalar- and one-level `IN`-inside-`IN` ownership/nesting/cache/choice checks, nested finite references and sequential-semi solver differentials, correlated outer-binding, one- and exact two-dependency `EXISTS` ordering/shape/semi/anti checks, dynamic-`IN` mapping/cache/pair-cap and positive-nullable integral/Date-context checks, real-host Decimal-AVG and correlated-`EXISTS`, and non-null/nullable `IN`-to-`left_semi` cases |
| StageGraph, reads, joins, and routing | `semantic_snapshot.cpp`; `read_range_predicate_impl.h`; `ir.py`; `scalar.py`; `stages.py`; `relation.py` | exact q9 point and q45 finite-set `ComputeNode` references; exhaustive range-grammar/key/annotation/pointer-identity mutations; pushed-range-plus-OLAP conjunction; `OriginalPredicate` irrelevance and `ComputeNode` sensitivity; `ut/test_stagegraph_reference.py`; `test_stage_compaction.py`; shared-IU semi/anti exhaustive execution; JoinKey budget/mutation checks; C++ topology/task mutations; real-host integration |
| SMT construction and verdict | `smt.py`; `verify.py` | `ut/test_smt.py`; `test_verify.py`; product ownership, closed-definition, free-symbol, nullary-capture, and foreign-declaration rejections; emitted-SMT inspection; identity and semantic-mutation obligations |
| Workload reach and regressions | no additional trusted code | `benchmark_ut/`, coverage policy, TPCH/TPC-DS reports, inspector and replay for candidates |
