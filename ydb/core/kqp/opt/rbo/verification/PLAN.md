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
- `COUNTEREXAMPLE`: the obligation is satisfiable and normally includes a
  candidate input database. If a second solver run cannot extract the model,
  the verdict remains a counterexample but carries a reason and no witness. An
  opaque scalar function may make a candidate spurious, so concrete replay is
  required.
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
- ordinary nullable equality, null-safe equality, and integer ordering when
  YQL's common integer type preserves both operands exactly;
- same-type signed and unsigned integer `+`, `-`, and `*`, with strict NULL
  propagation and exact fixed-width modular/two's-complement overflow;
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

The arithmetic node is deliberately narrow: both operands and the result must
have exactly the same integer identity, and result nullability must be the OR of
operand nullability. Mixed-width arithmetic remains opaque instead of asking
the verifier to reproduce YQL's promotion rules.

YQL does not expose a complete determinism-and-totality annotation. The v1 C++
exporter therefore uses a reviewed positive list for opaque subtrees: integer
`+`, `-`, and `*` forms that do not meet the structural gate; scalar
comparisons; `Just`, `Exists`, `Coalesce`, and `If`; `SafeCast`; and `Convert`
only when YQL's cast analysis says it cannot fail. Unknown callables, UDF/PG
calls, division, strict casts, `Unwrap`, free variables, position-aware or
unordered nodes, and side-effecting/CSE-unsafe nodes fail closed. Expanding this
list requires an explicit totality review and tests.

The persisted fingerprint is collision-free canonical text rather than a
machine hash. It length-prefixes node kind, callable and atom bytes, normalized
atom flags, exact formatted types, child counts, and ordered children. Direct
input-row Members become first-use ordinals; the corresponding unique IU values
are emitted as ordered UF arguments. Source positions, allocations, IU names,
and DAG sharing are deliberately absent. The exporter caps this representation
at 256 expanded nodes, nesting depth 64, and 64 KiB.

Version-one strings have equality-only uninterpreted-atom semantics. Literals
receive deterministic integer atom IDs, while other integer IDs decode to
collision-checked placeholder strings for replay. This avoids placing Z3's
version-dependent SMT string parser in the trusted path; string operations stay
opaque until explicitly modeled. Because those atoms have no YDB ordering,
`Sort` and `Merge` on `String` or `Utf8` fail closed during snapshot validation.

Version-one `Date` and canonical parameterized `Decimal(p,s)` are also
equality-only atoms, initially for benchmark columns that are carried but not
actively transformed. Exact type identity and NULLs are preserved, while
literals and ordering fail closed. Their unbounded carrier is an over-approximate
domain: it can make a witness spurious, but cannot make an inequivalent bounded
plan verify.

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
7. M4: actual column-store filter pushdown from the executed OLAP dialect;
8. later: subplans, distinct expansion, range reads, and other OLAP pushdowns.

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

- `kqp_rbo_inspect plan` renders every normalized plan and StageGraph field in
  deterministic line-oriented text.
- `kqp_rbo_inspect witness` rebuilds the unchanged start-to-finish obligation
  and renders candidate base rows plus every enabled per-operator, stage-task,
  connection, and root-boundary result from one solver model. Optional
  read-only observers collect immutable terms; definitional model aliases are
  added only after normal formula construction and are audit-capped.
- `kqp_rbo_replay` consumes the inspector's concrete trace, creates the bounded
  database under generated namespaces in two isolated YDB targets, verifies the
  optimizer mode through explain metadata, and compares new-RBO execution with
  a trusted baseline. It uses BulkUpsert for setup so candidate writes do not
  pass through the optimizer under test, rejects observably nondeterministic
  traces, and retains every created namespace for diagnosis.
- `tools/bisect.py` reruns the optimizer with a true stop-after-application
  debug hook and invokes the same formula kernel under an explicit diagnostic
  rule-prefix boundary contract. Every such verdict is labeled
  `comparison_scope: RULE_APPLICATION_PREFIX` so it cannot be confused with a
  whole-optimizer result.

The bisection unit is a dynamic rule-application ordinal, not merely a rule name,
because stages may iterate to a fixpoint. Prefixes are inspected sequentially:
equivalence is not monotonic across rule applications, so binary search would
not soundly identify the first bad transformation.

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
  structurally identified scalar UFs, plus equality-only passive Date and exact
  parameterized Decimal identities. Solver domains may be shared, but snapshot
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
- Reviewed deterministic total scalar subtrees are exported as canonical typed
  opaque functions. Unit tests cover IU alpha-renaming, first-use argument order,
  repeated arguments, structural/literal/callable mutations, DAG-sharing
  independence, nullability, and fail-closed safety gates.
- Same-type fixed-width integer `+`, `-`, and `*` are exported structurally and
  evaluated with exact strict-NULL and modular overflow semantics. Synthetic
  exporter and Python tests cover all widths, malformed schemas, and overflow;
  a real-host typed-`Int64` query verifies through the normal obligation.
- TPC-DS q88 exposed why that concrete extension was needed: opaque source
  additions did not constrain optimizer-folded literals. Its regenerated
  obligation has no opaque scalar functions and no longer returns the spurious
  counterexample; Z3 currently returns `UNKNOWN` at the 60-second bound.
- Actual pushed column-store filters are decoded from `OlapFilterLambda`, not
  optimizer statistics metadata. The supported Boolean/comparison subset is
  evaluated before per-task pushed limits, rejects projections and unknown
  process operations, and is covered by a real-host bounded proof.
- The exact new-RBO `TPCDS_YQL` q96 schema and query pass strict initial/final
  export and produce `VERIFIED_BOUNDED` at two rows per table and two tasks. This
  covers passive Date/Decimal catalog columns, canonical `Void` for `COUNT(*)`,
  four scans, three joins, split aggregation, TopSort/Merge/Limit, and
  Map/Broadcast/UnionAll StageGraph routing.
- The real-host dashboard runs all 22 `TPCH_YQL` and 99 `TPCDS_YQL` sources,
  writes a structured timeout-aware report, and preserves diagnostic artifacts
  for every correctness, unknown, schema, or solver outcome.
- [BENCHMARK_COVERAGE.md](BENCHMARK_COVERAGE.md) records the exact setup,
  commands, formula-only baseline, solver-backed q96 proof, q88 investigation,
  and explicit unsupported/optimizer-failure inventory.

### M5: confirmation and localization — in progress

- Separate normalized-plan and exact concrete-counterexample inspector.
- Separate real-YDB replay tool for deterministic, range-valid inspector
  witnesses, with strict dual-target mode preflight and typed BulkUpsert setup;
  multi-result TPC-DS q14, q23, and q39 remain an explicit replay extension.
- Explicit diagnostic rule-prefix verifier boundary, committed-rule snapshot
  hook, strict real-host capture command, and separate sequential localization
  driver are implemented. Constant folding and hash propagation still need
  explicit atomic transformation checkpoints before localization is complete.
- Formula-construction coverage has a checked-in regression floor. Hermetic
  solver-backed proof policy and replay-confirmed counterexample policy remain.

## Non-goals

- Proving CBO optimality.
- Checking after every rule in normal runs.
- Treating bounded verification as a general SQL-equivalence theorem.
- Proving `ConvertToPhysical`, task construction, or execution-engine correctness;
  those require a later boundary check and real-YDB replay.
- Growing the verifier into a second optimizer or expression simplifier.
