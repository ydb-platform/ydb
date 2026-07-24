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

The command-line default is two row slots per table. Stage execution has a
fixed bound of two tasks; a modeled stage may use one or two. Explicit type,
value, expression, relation, choice, and construction ceilings are also part of
the accepted subset. Crossing a ceiling returns `UNSUPPORTED`; it does not
silently weaken the formula.

This is not unbounded SQL equivalence. It does not cover unsupported semantics,
inputs with more than `N` rows per table, execution with more than two tasks,
`ConvertToPhysical`, the execution engine, optimizer optimality, or error
codes/text beyond the modeled success/error distinction. `FORMULA_EMITTED` and
`UNKNOWN` are not proofs. A satisfiable formula is a symbolic candidate and may
need real-YDB replay because admitted opaque values can over-approximate
runtime behavior.

## Proof-producing trusted code

A defect in these files can turn inequivalent supported plans into
`VERIFIED_BOUNDED`.

| File | Trusted responsibility |
|---|---|
| `semantic_snapshot.h` | Version-one catalog, snapshot, boundary, and fail-closed exporter contract. |
| `semantic_snapshot.cpp` | Mechanical catalog and plan export; scalar normalization and safety gates; operator, subplan, correlated outer-binding, StageGraph, topology, task, and resource validation; deterministic JSON serialization. |
| `rbo_verifier/ir.py` | Strict JSON decoding, version/schema validation, normalized IR, expression typing, correlated-subplan shape checks, and operator/StageGraph invariants. |
| `rbo_verifier/types.py` | Supported scalar identities, domains, families, and compatibility predicates. |
| `rbo_verifier/smt.py` | Typed immutable SMT terms, declarations, quantifier-safe sharing, deterministic canonical rendering, exact marked-obligation substitution, and solver-output parsing primitives. |
| `rbo_verifier/string_order.py` | Finite exact bounded quotient for String/Utf8 equality and unsigned byte ordering. |
| `rbo_verifier/decimal.py` | Decimal representation, domains, comparison, arithmetic, extrema, specials, and proof bounds. |
| `rbo_verifier/scalar.py` | Nullable values, SQL three-valued predicates, exact scalar evaluation, and typed opaque functions. |
| `rbo_verifier/relation.py` | Symbolic database, unique-key constraints, logical operators, per-row scalar subplans, bags/sequences, errors, choices, result-family equality, and the exact mismatch cover. |
| `rbo_verifier/stages.py` | Two-task StageGraph execution, routing, connection semantics, per-task evaluation, and root gathering. |
| `rbo_verifier/verify.py` | Boundary/catalog/schema checks, shared model construction, canonical/branch solver portfolio, one-deadline status interpretation, and witness decoding. |

`Term` caches its structural hash when the immutable SMT DAG node is
constructed. The cached field is excluded from equality; equality still checks
the complete `(sort, operation, arguments, atom)` structure, and Python
dictionary and set lookup still resolves hash collisions with that equality.
Equality uses an iterative identity-pair worklist, checks exact runtime classes
and ordered arguments, and therefore does not turn Python recursion depth into
a verifier limit. The cache changes the cost of repeated routing-fact and set
lookups, not the formula or proof obligation. Deep independently constructed
shared-DAG regressions check both equal-key coalescing and separation of unequal
terms with deliberately colliding hashes.

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
14/14 TPC-DS `VERIFIED_BOUNDED`, confirming all 25/25 curated obligations.
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
`AddDependencies`, observable `EnsureAtMostOne`, fanout, nesting, staging,
tuples, coercions, nullable `String`, `Utf8`, Bool, Decimal, mismatched
identities, and nullable `NOT`/`OR`/embedded uses fail closed.

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
reaches the later nested-subplan rejection, so this slice changes neither the
formula count nor the proof policy.

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
verifier's exact construction cap; none of these results is a bounded proof.

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

The post-nullable-Decimal-`SafeCast` 2026-07-24 physical-line audit records
implementation, test, and diagnostic rows at source `5dafcc79a4e`;
documentation includes this evidence update:

| Area | Physical lines |
|---|---:|
| Nine trusted Python semantic modules | 10,896 |
| C++ exporter (`semantic_snapshot.cpp` and `.h`) | 9,005 |
| **Proof-producing code total** | **19,901** |
| Tests, outside the TCB | 48,872 |
| Diagnostic/orchestration tools, outside the TCB | 5,182 |
| Documentation, outside the TCB | 6,282 |

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
  there is no hidden coercion, correlation,
  cardinality-error, or fanout semantics;
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
- opaque fingerprints identify the same runtime function exactly when
  intended, and every admitted opaque expression is deterministic, total, and
  safe to model as an uninterpreted function;
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
| Types, NULLs, scalar functions | `semantic_snapshot.cpp`; `ir.py`; `types.py`; `scalar.py`; `decimal.py`; `string_order.py` | `ut/test_scalar.py`; `test_decimal.py`; `test_string_order.py`; `test_string_proof.py`; `test_sql_in.py`; canonical String-predicate, Date-year, proven-total Date-`Unwrap`, direct-Uint64-`Just`, and exact Decimal weak-`SafeCast` mutations; `source_type`, NULL, overflow, widening-special, and fail-closed references; synthetic real-host proofs; exporter near-miss mutations |
| Logical bags, order, limits, errors | `semantic_snapshot.cpp`; `ir.py`; `relation.py` | `ut/test_logical_reference.py`; `test_limit.py`; `test_sort.py`; focused concrete differential tests |
| Aggregates and subplans | `semantic_snapshot.cpp`; `ir.py`; `decimal.py`; `scalar.py`; `relation.py` | aggregate/DistinctAll/count-distinct/unwrap exporter and IR mutations; exhaustive count-distinct duplicates and triangular cap; scalar-final unwrap empty/all-NULL/present references; Decimal-extrema raw-code differential, routing, and solver-mutation checks; nullable composite-key differential and staged-routing checks; `ut/test_subplans.py`; cardinality, demand, NULL, duplicate, error, correlated outer-binding, one- and exact two-dependency `EXISTS` ordering/shape/semi/anti checks, dynamic-`IN` mapping/cache/pair-cap and positive-nullable integral/Date-context checks, real-host Decimal-AVG and correlated-`EXISTS`, and non-null/nullable `IN`-to-`left_semi` cases |
| StageGraph, joins, and routing | `semantic_snapshot.cpp`; `ir.py`; `scalar.py`; `stages.py`; `relation.py` | `ut/test_stagegraph_reference.py`; `test_stage_compaction.py`; shared-IU semi/anti exhaustive execution; JoinKey budget/mutation checks; C++ topology/task mutations; real-host integration |
| SMT construction and verdict | `smt.py`; `verify.py` | `ut/test_smt.py`; `test_verify.py`; emitted-SMT inspection; identity and semantic-mutation obligations |
| Workload reach and regressions | no additional trusted code | `benchmark_ut/`, coverage policy, TPCH/TPC-DS reports, inspector and replay for candidates |
