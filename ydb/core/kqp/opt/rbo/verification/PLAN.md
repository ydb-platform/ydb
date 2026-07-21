# New RBO bounded equivalence verifier

## Objective

Build a small, auditable correctness checker for the new RBO. For a bounded
symbolic database, it searches for an input on which the initial RBO operator
tree and the final pre-physical StageGraph program return different results.

The normal check is start-to-finish. Rule-by-rule inspection is diagnostic
machinery and must remain outside the verifier kernel.

## Verification contract

For deterministic plans the checker solves:

```text
schema constraints
and bounded symbolic input tables
and Eval(initial RBO plan) != Eval(final StageGraph program)
```

Unordered Limit, Sort ties, and Merge interleavings make evaluation a finite
family of enabled bags or sequences. Equality is mutual inclusion of the two
families. Shared-DAG choices remain correlated, while distinct stage-task
executions are independent.

Results have five distinct meanings:

- `VERIFIED_BOUNDED`: no counterexample exists at the declared row and task
  bounds, for the modeled semantics.
- `COUNTEREXAMPLE`: Z3 produced a candidate input database. An opaque scalar
  function may make the candidate spurious, so concrete replay is required.
- `UNKNOWN`: the solver timed out or could not decide the formula.
- `SCHEMA_MISMATCH`: the root result names, order, types, or nullability differ;
  this is a definite correctness failure and requires no solver model.
- `UNSUPPORTED`: the input uses semantics the checker does not model.

An `UNSAT` result is never described as unbounded query equivalence.

## Snapshot boundaries

The C++ side will export a purpose-built, versioned semantic snapshot. Explain
JSON is not an input to the verifier because it intentionally omits details.

- Initial snapshot: after conversion to `TOpRoot` and `ComputeParents()`, before
  the first new-RBO stage.
- Final snapshot: after the final stage and property recomputation, immediately
  before `ConvertToPhysical`.

The exporter is part of the trusted path. It must be mechanical, preserve every
semantic field, use stable IDs rather than addresses, and fail when it encounters
an operator or expression it cannot represent.

The snapshot contains only semantic data:

- table columns, types, nullability, keys, and relevant partitioning metadata;
- operator DAG, exact output IU order, root column order, and subplan references;
- canonical scalar-expression structure, types, constants, and ordered IU uses;
- stages and every connection occurrence, including producer/consumer stage,
  duplicate-edge occurrence, producer output index, effective consumer-input
  ordinal, shuffle keys, hash function, broadcast/map/union mode, and merge
  ordering as `(IU, ascending, nulls-first)`;
- explicit assumptions required by a physical decision such as shuffle
  elimination.

Costs, estimates, trace strings, and pointer identities are excluded.

## Trusted kernel

The trusted Python code is deliberately split into small semantic modules:

```text
rbo_verifier/ir.py          strict, versioned snapshot decoding
rbo_verifier/smt.py         typed SMT terms and deterministic SMT-LIB output
rbo_verifier/scalar.py      nullable values, SQL Bool3, scalar UFs
rbo_verifier/relation.py    bounded bag/sequence operator semantics
rbo_verifier/stages.py      two-task StageGraph and connection semantics
rbo_verifier/verify.py      one counterexample formula and verdict decoding
```

The kernel has no YDB client, optimizer tracing, benchmark discovery, or
rule-bisection logic. The kernel emits inspectable SMT-LIB and invokes an
explicit Z3-compatible solver executable; it does not import ambient Python
packages. A pinned Z3 binary will be vendored as a separate build-integration
step.

## Scalar expressions

The explicit scalar core initially contains:

- column access, typed literal, and typed NULL;
- SQL/YQL three-valued `AND`, `OR`, and `NOT`;
- ordinary nullable equality and null-safe equality;
- filter truth conversion.

Every other deterministic, total scalar subtree is represented as a typed
uninterpreted function:

```text
opaque<canonical AST shape, literals, types, settings>(ordered IU values)
```

The function identity is not merely its input-column list. IU names are
alpha-normalized through lineage, while callable shape, constants, argument
positions, repeated arguments, and types remain part of the identity. The same
fingerprint is shared between both plans.

Volatile, stateful, observably failing, evaluation-count-sensitive, or otherwise
unsupported expressions produce `UNSUPPORTED`. New concrete scalar semantics are
added only in response to real optimizer transformations or spurious witnesses.

Version-one strings have equality-only uninterpreted-atom semantics. Literals
receive deterministic integer atom IDs, while other integer IDs decode to
collision-checked placeholder strings for replay. This avoids placing Z3's
version-dependent SMT string parser in the trusted path; string operations stay
opaque until explicitly modeled. Because those atoms have no YDB ordering,
`Sort` and `Merge` on `String` or `Utf8` fail closed during snapshot validation.

## Relational semantics

Each base table has a fixed number of symbolic row slots. A slot contains a
presence Boolean and one nullable value per column. Plans produce fixed vectors
of guarded rows.

Implementation sequence:

1. M1: one-row empty source, scan, exact projection, and filter;
2. M1: inner, cross, left/right/full outer, semi, and anti/only joins;
3. M1: logical bag `UnionAll`;
4. M1: root projection and column order;
5. M4: common aggregates and unordered literal Limit;
6. M4: Sort/TopSort, ordered literal Limit, and ordered Merge;
7. later: subplans, distinct expansion, range reads, and other OLAP pushdowns.

The C++ exporter lowers an RBO map mechanically to an exact projection:
all expressions read the input row, rename sources are removed, untouched input
IUs pass through, and map targets are appended in operator order. Exporter tests
cover that normalization before it enters the trusted path. The projection also
records `TOpMap::Ordered`. Both values currently have the same sequence-preserving
runtime semantics because RBO lowers Map through its streaming WideMap builder;
the field remains explicit so that contract cannot change silently.

Unordered results are compared by symbolic tuple multiplicity. Ordered results
are compared as sequences where order is observable. Root output names and
their order are an external schema contract and must match exactly; the exporter
may add a mechanical final projection when internal IU IDs differ.

Limit count/offset, TopSort limit, and pushed scan limits are exact non-null
`Uint64` literals in v1. An unordered Limit enumerates every row mask of the
required result cardinality. On an ordered stream, Limit takes the exact
`offset:offset+count` slice of the compressed present-row sequence. A pushed
column-scan limit runs after source partitioning and therefore applies once per
task. Exact reuse of one unordered Limit node remains correlated. Distinct
Limit observers of one shared unordered stream remain unsupported until a
common latent-order model is added. Ordered Limit is deterministic, while
Aggregate, Join, and UnionAll establish new unordered streams.

Sort enumerates every permutation of the bounded row slots and enables exactly
those satisfying the lexicographic `(column, ascending, nulls-first)` order.
This preserves every legal tie ordering. A non-null Sort limit is TopSort and
applies the same ordered prefix semantics after sorting. Sort and Limit phases
are preserved but do not independently change the modeled runtime semantics.
If the initial root is ordered, results are compared as compressed sequences;
otherwise they are compared as bags.

All explicit families fail closed above 256 alternatives. This cap includes
unordered-Limit choices, Sort permutations, Merge interleavings, latent
sequence expansion, and family products/gathers. Cross-plan equality fails
closed above 4096 outcome pairs. Neither cap is approximated.

## StageGraph semantics

A stage output is a vector of per-task bags or sequences. Distribution checks
use at least two source/producer tasks; one source task masks shuffle and
broadcast mistakes. Consumer task counts follow connection semantics rather
than being fixed uniformly.

```text
Map:          preserve producer task count and output[i] = input[i]
HashShuffle:  route each row once into the consumer task count
Broadcast:    copy all source rows to every task of the consumer stage
UnionAll:     gather all producer partitions into one consumer task
Parallel UA:  route producer-task streams round-robin to consumer tasks
Merge:        one consumer task; exact bounded merge by (IU, asc, nulls-first)
```

Stage-local operators execute independently on each task. The final collection
then projects `TOpRoot::ColumnOrder`.

Merge requires every producer task to carry an order compatible with the edge
order. It enumerates exactly the sorted interleavings that preserve each
producer sequence; incompatible metadata and unordered inputs fail closed.

Logical `TOpUnionAll` and a `TUnionAllConnection` are different IR nodes and
receive different semantics.

Shuffle elimination and source co-partitioning are rejected until the snapshot
contains enough source-distribution information to verify them.

## Diagnostics outside the kernel

- `tools/inspect.py` renders normalized plans, solver formulas, candidate base
  rows, and a per-operator concrete evaluation trace.
- `tools/replay.py` consumes witness JSON, creates the bounded database in a real
  YDB instance, and compares new-RBO execution with a trusted baseline.
- `tools/bisect.py` reruns the optimizer with a stop-after-application debug hook
  and invokes the unchanged verifier on each prefix.

The bisection unit is a dynamic rule-application ordinal, not merely a rule name,
because stages may iterate to a fixpoint.

## Validation strategy

Before treating optimizer findings as credible:

1. Compare every symbolic operator encoding with an independent concrete
   evaluator on exhaustively enumerated tiny databases.
2. Mutation-test the checker by deleting a filter, changing a join kind/key,
   dropping a UnionAll branch, changing a shuffle key/hash, corrupting an
   aggregate phase, moving Limit across Filter, changing Sort direction, NULL
   placement, key order, or TopSort limit, and corrupting split Limit phases.
3. Preserve and replay every solver witness.
4. Run the supported subset of `TPCH_YQL` and `TPCDS_YQL` as a coverage dashboard;
   report unsupported features separately from failures.

The first useful bound is two row slots per referenced table and two tasks.
Larger bounds are query-specific because multiway joins grow rapidly.

## Milestones

### M1: executable logical kernel — implemented

- Strict v1 snapshot decoder.
- Empty source, scan, project, filter, joins, and logical UnionAll.
- Nullable exact YQL Bool, integer-width, String, and Utf8 identities with
  structurally identified scalar UFs. Solver domains may be shared, but snapshot
  type identity is never collapsed.
- Bag-equivalence formula, deterministic SMT-LIB, witness decoding, CLI, and
  mutation tests.

### M2: semantic C++ snapshots — implemented for the supported subset

- Initial/final exporter hooks.
- Stable operator, IU, expression, stage, and edge IDs.
- C++ unit tests proving semantically relevant fields survive export.
- End-to-end new-RBO comparisons. Integration tests drive a real `IKqpHost`,
  capture both boundaries, and pass them through the normal CLI; solving is
  explicit until M2b supplies the hermetic Z3 target.

### M2b: hermetic solver packaging — pending

- Vendor a reviewed, pinned MIT-licensed Z3 release in `contrib`.
- Build a command-line solver target without linking it into `ydbd`.
- Make integration tests locate that binary explicitly through `ya`.

### M3: StageGraph routing — implemented for the supported subset

- Two-producer-task Map, HashShuffle, Broadcast, and UnionAll with
  connection-derived consumer counts.
- Local join execution and final gather.
- Wrong-shuffle and wrong-broadcast mutation tests.
- Exact bounded Merge execution with input-order validation, tie-preserving
  sorted interleavings, and wrong-order mutation tests.

### M4: benchmark coverage — in progress

- Grouped/scalar count and integer sum, including split intermediate/final
  execution, NULLs, and exact 64-bit numeric behavior; distinct variants remain.
- Unordered literal Limit/offset, split per-task execution, and column-source
  pushed limits, with exhaustive and mutation tests.
- Sort/TopSort, ordered Limit, and Merge, with exhaustive concrete differential
  tests, family-cap tests, and order/limit/phase mutation tests.
- A real-host ordered test captures logical Sort+Limit and the transformed
  per-task TopSort+Merge+final-Limit program, then constructs or solves the
  normal equivalence obligation.
- TPCH/TPCDS coverage and timeout report.
- Explicit unsupported-feature inventory.

### M5: confirmation and localization — pending

- Real-YDB replay tool.
- Rule-application snapshot hook and separate bisection driver.
- CI policy for confirmed counterexamples and bounded verification coverage.

## Non-goals

- Proving CBO optimality.
- Checking after every rule in normal runs.
- Treating bounded verification as a general SQL-equivalence theorem.
- Proving `ConvertToPhysical`, task construction, or execution-engine correctness;
  those require a later boundary check and real-YDB replay.
- Growing the verifier into a second optimizer or expression simplifier.
