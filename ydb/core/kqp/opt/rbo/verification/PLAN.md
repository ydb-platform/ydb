# Current semantic contract

The normal obligation compares Initial logical RBO with Final pre-physical
StageGraph. The [bounded theorem](contracts/THEOREM.md) is authoritative for
result-language equality, solver scheduling, model-domain exclusions, and the
limits of the claim. [External assumptions](contracts/EXTERNAL_ASSUMPTIONS.md)
state the accepted runtime behavior that the formula itself does not prove.

This document describes the current design, not a milestone backlog. Earlier
implementation notes remain in the [historical plan](history/pre-audit-refactor/PLAN.md).

## Boundary and observation

Initial capture occurs after `TOpRoot` construction and parent computation,
before the first RBO stage. Final capture occurs after the final stage and
property recomputation, before `ConvertToPhysical`. Exactly two boundary
results must arrive in Initial-then-Final order. A later preparation failure
does not invalidate an already captured pair; preparation is a separate axis.

Snapshots carry catalog constraints, the operator DAG, ordered output columns,
typed scalar expressions, subplans, stages, and every connection occurrence.
Explain JSON, costs, estimates, and trace text are not semantic inputs.
The C++ exporter and Python decoder reject unmodeled fields/shapes instead of
dropping them. Normal verification requires a logical Initial and complete
staged Final; transformation-prefix mode has a separately tagged boundary contract.

Successful unordered results are bags: duplicate multiplicity matters.
Ordered results are sequences. Result languages include all modeled legal
choices; shared-DAG choices remain correlated and independent task executions
remain independent. Equality distinguishes success from query error, but not
error text/code beyond the modeled distinction. Root names, order, types, and
nullability are checked before formula construction.

## Operator review unit

Each operator should have one locally readable semantic entry point and a
separate, explicit encoding choice. An optimization is not a new SQL operator.
Its eligibility checks, representation, and equivalence argument stay together.
Moving an optimized encoder to another module does not make it untrusted.

For each operator or admitted scalar slice, maintain this small audit card:

1. **Denotation:** input/output schema, presence, NULL, multiplicity, observable
   order, errors, and legal choices.
2. **Admission:** exact types/shapes and required catalog/provenance evidence;
   malformed evidence or exceeded resource ceilings must fail closed.
3. **Composition:** how the shared family machinery transports enabled outcomes,
   errors, choice identities, and private certificates.
4. **Encoding equivalence:** why baseline and optimized representations denote
   the same observable outcomes under the stated premises.
5. **Evidence:** independent tiny-domain reference, same-input encoding
   differential, semantic mutation, and admission/cap near misses.

A differential test is confidence on its enumerated domain, not a universal
proof of the encoder. Both the reference path used for production verdicts and
every optimized clause-producing path belong to the trusted code.

### Join example

For input slots `i,j`, define:

```text
match(i,j) = left_present(i) and right_present(j)
             and SQL_TRUE(all side-explicit key equalities)
             and SQL_TRUE(residual predicate)
```

Inner/Cross emits matching pairs. Semi emits each retained-side row once when
any match exists; anti emits it once when none exists. Left/Right/Full adds
the appropriate unmatched rows padded with NULLs. Exclusion emits unmatched
rows from both sides. An absent slot's payload is unobservable. Distinct
occurrences with equal values still contribute distinct bag multiplicities.

The baseline relation builder and family composition must remain independently
reviewable. Unique-RHS compaction additionally requires an exact direct scan,
a complete non-null catalog key compared without collapsing coercions, and
validated source-slot/payload/presence provenance. Its at-most-one premise
justifies selecting one RHS value per left row. Delayed Filter/Cross factoring
must also retain the original predicate, output order, errors, and choices.

Existing independent evidence is in
[logical references](ut/test_logical_reference.py) and the
[unique-RHS/delayed-Cross tests](ut/test_verify.py); these include NULL/absent
slots, duplicates, all admitted Join kinds, shared names, key/provenance
near misses, and construction ceilings. Do not replace those references with
expectations computed by the production strategy selector.

`join.reference_rows` is the baseline row-emission rule. `_join` accepts
`encoding="auto"`, `"baseline"`, or `"compact"`; forced compaction still requires
validated admission. `_filter` similarly supports `"auto"`, `"baseline"`, and
`"factored"` while always applying the complete predicate. `sort_family` has
explicit `"enumerated"`, `"ordinals"`, `"unique"`, and `"network"` encodings
beside `"auto"`; the pure `sort_strategy.choose` selector checks measured costs
and certificates before the dispatcher constructs symbols or results. These
are internal review/test seams, not extra SQL syntax.

## Deliberately restricted semantics

The admitted subset is defined by the exporter gates and strict IR validation,
not by a workload query number. A benchmark fixture motivates a slice but
does not authorize neighboring shapes.

- Fixed-width integers preserve signedness, width, overflow, NULLs, and the
  exact admitted casts/division. Date operations accept only reviewed shapes.
- Decimal arithmetic and aggregates preserve encoded finite/special values,
  scale, NULL behavior, and phase contracts. Finite aggregate headroom is
  mandatory; potentially non-associative overflow is not silently assumed away.
  Private SUM/AVG summaries require validated lineage and exact reconstruction.
- Integral AVG is admitted only through its explicit contract and producer-local
  count certificate. The solver must exclude successful non-NULL completed
  results with count above two. Its abstract carrier/rank is not general Double
  arithmetic; semantic SAT in that restricted model remains UNKNOWN.
- Passive Double payloads and opaque scalar functions use closed fingerprints
  with exact argument/type identity. General floating arithmetic, arbitrary
  Double consumers, and unreviewed callable envelopes remain unsupported.
  Over-approximate opaque functions can make symbolic candidates spurious.
- Checked projections and scalar subplans model demanded local errors and eager
  inherited errors. No general assumption of eager projection or lazy subplan
  evaluation replaces the admitted topology checks.
- Sort/Limit/Merge represent the complete bounded result language, including
  ties and independent choices. Ordinals, networks, keyed covers, and row
  compaction are exact encodings with explicit admission premises.
- Window leaves require their exact frame, partition/order, type, and topology
  contracts. Task locality is semantic: global windows cannot silently become
  independent per-task windows.
- StageGraph models one or two tasks, source placement, HashShuffle, Broadcast,
  Map/Union/Merge connections, occurrence multiplicity, and routing consistency.
  Equal spellings do not identify different streams; opposite routing facts
  justify compaction only with certified occurrence identity.

The detailed current runtime premises are retained in
[external assumptions](contracts/EXTERNAL_ASSUMPTIONS.md). Unsupported inputs,
resource ceilings, more rows/tasks, physical lowering, and real execution are
outside the bounded theorem.

## Verdicts

| Status | Meaning |
|---|---|
| `VERIFIED_BOUNDED` | The required model-domain and semantic obligations are UNSAT within the declared bounds. |
| `FORMULA_EMITTED` | The canonical formula was constructed; no proof is claimed. |
| `COUNTEREXAMPLE` | A semantic mismatch is satisfiable; extraction may still lack a witness, and opaque models require confirmation. |
| `UNKNOWN` | Required solving or model-domain certification did not establish a proof/candidate classification. |
| `SCHEMA_MISMATCH` | Observable root schemas differ; no solver witness is required. |
| `UNSUPPORTED` | A semantic/admission/construction boundary was not met. |
| `SOLVER_ERROR` | Solver execution or protocol failed; no semantic conclusion follows. |

[Benchmarks](BENCHMARK_COVERAGE.md), inspection, replay, and localization do not
add clauses to the normal theorem or turn formula coverage into proof coverage.
