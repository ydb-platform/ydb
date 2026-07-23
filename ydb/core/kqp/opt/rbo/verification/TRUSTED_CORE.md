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
| `semantic_snapshot.cpp` | Mechanical catalog and plan export; scalar normalization and safety gates; operator, subplan, StageGraph, topology, task, and resource validation; deterministic JSON serialization. |
| `rbo_verifier/ir.py` | Strict JSON decoding, version/schema validation, normalized IR, expression typing, operator and StageGraph invariants. |
| `rbo_verifier/types.py` | Supported scalar identities, domains, families, and compatibility predicates. |
| `rbo_verifier/smt.py` | Typed immutable SMT terms, declarations, quantifier-safe sharing, deterministic SMT-LIB rendering, and solver-output parsing primitives. |
| `rbo_verifier/string_order.py` | Finite exact bounded quotient for String/Utf8 equality and unsigned byte ordering. |
| `rbo_verifier/decimal.py` | Decimal representation, domains, comparison, arithmetic, specials, and proof bounds. |
| `rbo_verifier/scalar.py` | Nullable values, SQL three-valued predicates, exact scalar evaluation, and typed opaque functions. |
| `rbo_verifier/relation.py` | Symbolic database, unique-key constraints, logical operators, subplans, bags/sequences, errors, choices, and result-family equality. |
| `rbo_verifier/stages.py` | Two-task StageGraph execution, routing, connection semantics, per-task evaluation, and root gathering. |
| `rbo_verifier/verify.py` | Boundary/catalog/schema checks, shared model construction, negated-equivalence assertion, solver invocation, status interpretation, and witness decoding. |

The 2026-07-23 physical-line audit, including the hardening changes that added
this map, recorded:

| Area | Physical lines |
|---|---:|
| Nine trusted Python semantic modules | 9,375 |
| C++ exporter (`semantic_snapshot.cpp` and `.h`) | 7,492 |
| **Proof-producing code total** | **16,867** |
| Tests, outside the TCB | 39,573 |
| Diagnostic/orchestration tools, outside the TCB | 5,082 |
| Documentation, outside the TCB | 4,400 |

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
| Types, NULLs, scalar functions | `semantic_snapshot.cpp`; `ir.py`; `types.py`; `scalar.py`; `decimal.py`; `string_order.py` | `ut/test_scalar.py`; `test_decimal.py`; `test_string_order.py`; `test_string_proof.py`; `test_sql_in.py`; exporter near-miss mutations |
| Logical bags, order, limits, errors | `semantic_snapshot.cpp`; `ir.py`; `relation.py` | `ut/test_logical_reference.py`; `test_limit.py`; `test_sort.py`; focused concrete differential tests |
| Aggregates and subplans | `semantic_snapshot.cpp`; `ir.py`; `decimal.py`; `relation.py` | aggregate/exporter mutations; `ut/test_subplans.py`; cardinality, demand, NULL, duplicate, and error cases |
| StageGraph and routing | `semantic_snapshot.cpp`; `ir.py`; `stages.py`; `relation.py` | `ut/test_stagegraph_reference.py`; `test_stage_compaction.py`; C++ topology/task mutations; real-host integration |
| SMT construction and verdict | `smt.py`; `verify.py` | `ut/test_smt.py`; `test_verify.py`; emitted-SMT inspection; identity and semantic-mutation obligations |
| Workload reach and regressions | no additional trusted code | `benchmark_ut/`, coverage policy, TPCH/TPC-DS reports, inspector and replay for candidates |
