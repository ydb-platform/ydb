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

The dynamic-`IN` slice adds one explicit typed `in` subplan descriptor. C++ and
Python independently require exactly one lookup column from the sole Filter
consumer and one result column from the inner root, with the same non-null
fixed-width integral type. The binding is non-null `Bool`, uncorrelated, and
virtual; `OuterBind`, `AddDependencies`, observable `EnsureAtMostOne`, fanout,
nesting, staging, tuples, coercions, nullable values, and nonintegral
identities fail closed.

`relation.py` evaluates membership per present outer row as the OR of present
non-null inner values equal to the non-null lookup value. Thus duplicates
collapse, empty input is false, and consumer negation implements `NOT`.
Repeated references reuse the cached subplan family, while root errors remain
eager even with no present outer row. A shared preflight rejects more than
16,384 outer/inner membership pairs cumulatively across alternatives and
nested evaluation. The optimized side is still evaluated as
the ordinary final StageGraph; there is no dynamic-`IN` equivalence shortcut.
Independent duplicate/empty/negation, cache, left-semi/left-anti, inherited
error, mapping-mutation, descriptor-boundary, pair-cap, exporter, inspector,
and real-host `IN`-to-`left_semi` tests cover the vertical path.

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

The canonical String-predicate bridge adds no evaluator-specific truth table.
`semantic_snapshot.cpp` alone must establish the narrow generic and OLAP
grammars, catalog type/nullability, positive-filter coalesce handling, and the
one-to-one mapping from `EndsWith`/`StringContains` to their two stable
fingerprints. Existing `ir.py`/`scalar.py` opaque-function validation then
shares each deterministic total function and its ordered column/literal
arguments across both plans. Cross-dialect exporter mutations and a
solver-backed real-host fixture are the independent evidence.

Decimal `MIN` crosses `ir.py`, `decimal.py`, and `relation.py`. The decoder
admits only exact same-type Decimal input/output with phase-aware nullability;
the kernel reduces non-NULL values in raw signed-code order and preserves a
lone NaN; the relational layer supplies NULL for an emitted group with no
non-NULL value, including scalar empty input, and carries the same scalar state
through undefined, intermediate, and final phases.
Independent exhaustive guarded-code and concrete aggregate references, staged
routing, wrong-shuffle checks, and a final-min-to-max solver mutation cover the
path.

The post-dynamic-IN 2026-07-24 physical-line audit recorded:

| Area | Physical lines |
|---|---:|
| Nine trusted Python semantic modules | 10,577 |
| C++ exporter (`semantic_snapshot.cpp` and `.h`) | 8,394 |
| **Proof-producing code total** | **18,971** |
| Tests, outside the TCB | 44,453 |
| Diagnostic/orchestration tools, outside the TCB | 5,131 |
| Documentation, outside the TCB | 5,098 |

These figures are a review baseline, not a generated invariant. The trusted
core is a medium-sized verification subsystem, so it should be audited by
vertical semantic slice rather than treated as one small script.

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
- each accepted dynamic-`IN` descriptor represents one uncorrelated
  existential membership test over the recorded lookup/result columns, with
  no hidden NULL, coercion, correlation, cardinality-error, or fanout
  semantics;
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
| Types, NULLs, scalar functions | `semantic_snapshot.cpp`; `ir.py`; `types.py`; `scalar.py`; `decimal.py`; `string_order.py` | `ut/test_scalar.py`; `test_decimal.py`; `test_string_order.py`; `test_string_proof.py`; `test_sql_in.py`; canonical String-predicate dialect mutations and real-host proof; exporter near-miss mutations |
| Logical bags, order, limits, errors | `semantic_snapshot.cpp`; `ir.py`; `relation.py` | `ut/test_logical_reference.py`; `test_limit.py`; `test_sort.py`; focused concrete differential tests |
| Aggregates and subplans | `semantic_snapshot.cpp`; `ir.py`; `decimal.py`; `relation.py` | aggregate/DistinctAll exporter and IR mutations; Decimal-extrema raw-code differential, routing, and solver-mutation checks; nullable composite-key differential and staged-routing checks; `ut/test_subplans.py`; cardinality, demand, NULL, duplicate, error, correlated outer-binding, dynamic-`IN` mapping/cache/pair-cap, real-host Decimal-AVG, and `IN`-to-`left_semi` cases |
| StageGraph and routing | `semantic_snapshot.cpp`; `ir.py`; `stages.py`; `relation.py` | `ut/test_stagegraph_reference.py`; `test_stage_compaction.py`; C++ topology/task mutations; real-host integration |
| SMT construction and verdict | `smt.py`; `verify.py` | `ut/test_smt.py`; `test_verify.py`; emitted-SMT inspection; identity and semantic-mutation obligations |
| Workload reach and regressions | no additional trusted code | `benchmark_ut/`, coverage policy, TPCH/TPC-DS reports, inspector and replay for candidates |
