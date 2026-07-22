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
rbo_verifier/decimal.py     exact Decimal values, comparison, arithmetic, and ordering
rbo_verifier/scalar.py      nullable values, SQL Bool3, scalar UFs
rbo_verifier/relation.py    bounded bag/sequence operator semantics
rbo_verifier/stages.py      two-task StageGraph and connection semantics
rbo_verifier/verify.py      one counterexample formula and verdict decoding
```

The kernel has no YDB client, optimizer tracing, benchmark discovery, or
transformation-prefix localization logic. The kernel emits inspectable SMT-LIB and invokes an
explicit Z3-compatible solver executable; it does not import ambient Python
packages. Hermetic tests resolve the separately built, pinned Z3 executable;
the solver is not linked into `ydbd`.

## Scalar expressions

The explicit scalar core initially contains:

- column access, typed literal, and typed NULL;
- SQL/YQL three-valued `AND`, `OR`, and `NOT`;
- ordinary nullable equality, null-safe equality, integer ordering when YQL's
  common integer type preserves both operands exactly, and same-type `Date`
  ordering; exact Decimal equality and ordering use YDB `DataCompare`
  alignment for Decimal/Decimal and Decimal/integer operands;
- same-type signed and unsigned integer `+`, `-`, and `*`, with strict NULL
  propagation, exact typed input domains, and fixed-width
  modular/two's-complement overflow;
- canonical Decimal `+` and `-` with same-type operands, plus `DecimalMul` with
  a same-type Decimal or integer right operand, all with exact `NDecimal`
  specials, rounding, overflow, and strict NULL propagation;
- restricted static `IN`: a direct raw tuple or `AsList` containing 1..512
  recursively supported, non-null expressions of one item type; that type is
  identical to the lookup or uses the same lossless common-integer gate as
  ordinary equality, evaluated as the SQL three-valued OR of that equality;
- filter truth conversion.

The static-`IN` result must be `Bool` and nullable exactly when its lookup is
nullable. `ansi`, `warnNoAnsi`, `isCompact`, and `nullsProcessed` are erased
only under that semantic gate. `tableSource`, dynamic, empty, oversized,
nullable-item, heterogeneous-item, lossy or non-integer mixed-type,
malformed-option, unknown-option, and duplicate-option forms fail closed.
Decimal membership is deliberately outside this static-`IN` subset.

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

Integer arithmetic is deliberately narrow: both operands and the result must
have exactly the same integer identity, and result nullability must be the OR of
operand nullability. Mixed-width arithmetic remains opaque instead of asking
the verifier to reproduce YQL's promotion rules. Integer literals, source
cells, and non-null opaque results are constrained to their exact signed or
unsigned width, so a model cannot manufacture an out-of-range arithmetic
witness.

Decimal arithmetic has a separate canonical gate. Binary `+` and `-` require
both operands and the result to have one exact canonical `Decimal(p,s)` type.
Binary `DecimalMul` requires the left operand and result to have that type; its
right operand is either the same Decimal type or one signed/unsigned integer
width. In every case result nullability is exactly the OR of operand
nullability, and the expression must pass the same closed-world scalar audit as
an opaque expression. The normalized snapshot node is `add`, `sub`, or `mul`;
an integer is never admitted on the left at this boundary.

YQL does not expose a complete determinism-and-totality annotation. The v1 C++
exporter therefore uses a reviewed positive list for opaque subtrees: integer
`+`, `-`, and `*` forms that do not meet the structural gate; scalar
comparisons; `Just`, `Exists`, `Coalesce`, and `If`; `SafeCast`; and `Convert`
only when YQL's cast analysis says it cannot fail. Unknown callables, UDF/PG
calls, division, strict casts, `Unwrap`, free variables, position-aware or
unordered nodes, and side-effecting/CSE-unsafe nodes fail closed. Expanding this
list requires an explicit totality review and tests.

One cast shape is normalized before opaque fallback: when YQL cast analysis
reports a complete conversion from a non-null integer literal to a non-null
Decimal, the exporter evaluates it and emits the resulting Decimal literal.
General casts, nullable cast shapes, and non-integer Decimal cast sources fail
closed.

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

Version-one `Date` is the exact unsigned day-since-epoch domain
`[0, NUdf::MAX_DATE)`. Numeric literals are range-checked, source slots and
non-null opaque Date results receive explicit domain constraints, and same-type
comparison, Sort, and Merge use integer day ordering.

Canonical `Decimal(p,s)` uses YDB's scaled-integer representation. Finite
values satisfy `-10^p < code < 10^p`; negative infinity, positive infinity, and
NaN are the only other legal codes. Snapshot literals tag these four cases
explicitly, with a canonical signed-integer string only on `finite`. Ordinary
equality and ordering are strict on NULL, NaN makes every ordinary comparison
false, and infinities are ordered. Null-safe equality is accepted only for the
same exact Decimal type and compares encoded non-null values, including NaN.
Decimal/Decimal and Decimal/integer comparison alignment mirrors YDB
`DataCompare`, including scale increase, integer decimal widths, the precision
35 cap, and conversion saturation. Any alignment requiring an invalid
zero-precision type fails closed.

Decimal `add` and `sub` operate on same-scale coefficients with exact
`NDecimal` NaN/infinity algebra and saturation at the result precision.
Same-type `mul` divides the coefficient product by `10^s` using nearest,
ties-to-even rounding for either sign. `DecimalMul` with an integer right
operand does not rescale, so it preserves the left Decimal scale. NaN,
infinity-times-zero, signed infinity, and finite overflow—including a finite
result that numerically collides with the in-band NaN code—are handled before
the result is decoded.

Decimal Sort, TopSort, and Merge use the MiniKQL/DQ runtime comparator,
not ordinary `DataCompare`: raw signed 128-bit codes form the total non-null
order `-Inf < finite values < +Inf < NaN`, reversed for descending. Raw code
equality makes two NaNs a sort tie. One order item retains one exact canonical
`Decimal(p,s)` identity without scale alignment; separate tuple keys may have
different Decimal identities. NULL placement continues to use the pre-physical
snapshot's explicit `nulls_first` field. Decimal division, general casts, `IN`,
and aggregate functions remain unsupported.

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
8. M4: exact Decimal literals, domains, comparison, and constant-cast
   normalization;
9. M4: exact canonical Decimal `+`, `-`, and `DecimalMul`;
10. M4: exact Decimal Sort, TopSort, and Merge ordering;
11. later: subplans, distinct expansion, range reads, and other OLAP pushdowns.

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
- `tools/bisect.py` reruns the optimizer with a true stop-after-transformation
  debug hook and invokes the same formula kernel under an explicit diagnostic
  transformation-prefix boundary contract. Every such verdict is labeled
  `comparison_scope: OPTIMIZER_TRANSFORMATION_PREFIX` so it cannot be confused
  with a whole-optimizer result.

The localization unit is a dynamic transformation-event ordinal. Events cover
both committed rule applications and atomic mutating non-rule stage commits;
rules may occur repeatedly while a stage iterates to a fixpoint. Prefixes are
inspected sequentially because equivalence is not monotonic across optimizer
transformations, so binary search would not soundly identify the first bad one.

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

Independent exhaustive concrete references now cover EmptySource,
scan/project/filter, logical UnionAll, root projection, and every admitted join
kind, including NULL/absence payload independence and duplicate multiplicity.
Separate concrete references cover aggregate, Limit, Sort/Merge, pushed OLAP
filters, and two-task StageGraph Map, HashShuffle, Broadcast, serial/parallel
UnionAll, and local-join routing combinations. The StageGraph reference checks
also distinguish wrong hash functions, shuffle keys, broadcasts, and UnionAll
modes. Decimal tests exhaust every finite value for all `Decimal(p,s)` with
`p <= 2`, plus all specials, against an independent rational-value comparison
reference. Arithmetic uses the same independent reference: multiplication is
exhausted at every scale, while addition/subtraction are exhausted once per
precision and checked structurally scale-independent. Adversarial cases cover
ties-to-even for both signs, every integer width, special values, precision
overflow, and finite collisions with the NaN code. Existing C++ literal and
alignment tests use `NDecimal` as an oracle, while arithmetic exporter tests
audit the signature gates. Normal verifier tests prove unchanged
`add`/`sub`/`mul` across a staged Map and require operation mutations to produce
solver counterexamples.
Decimal ordering is exhaustively checked on every legal finite code through
precision two plus specials, directions, explicit NULL placement, NaN ties,
TopSort prefixes, and two-task Merge. A C++ oracle locks the stated total order
to `NUdf::CompareValues<Decimal>`. A real-host Decimal query verifies the
bounded two-row/two-task pre-physical logical Sort+Limit to staged
TopSort+Merge+final-Limit transformation pair.
All integer-width endpoints are checked independently for literals, source
cells, and opaque results; a solver regression proves that `Decimal * i` and
`Decimal * (i + 0)` cannot be distinguished by an out-of-range integer model.

The first useful bound is two row slots per referenced table and two tasks.
Larger bounds are query-specific because multiway joins grow rapidly.

## Milestones

### M1: executable logical kernel — implemented

- Strict v1 snapshot decoder.
- Empty source, scan, project, filter, joins, and logical UnionAll.
- Nullable exact YQL Bool, integer-width, String, and Utf8 identities with
  structurally identified scalar UFs, exact bounded and ordered Date, plus exact
  parameterized Decimal identity (initially carried without active Decimal
  transformations). Solver domains may be shared, but snapshot type identity is
  never collapsed.
- Bag-equivalence formula, deterministic SMT-LIB, witness decoding, CLI, and
  mutation tests.

### M2: semantic C++ snapshots — implemented for the supported subset

- Initial/final exporter hooks.
- Stable operator, IU, expression, stage, and edge IDs.
- C++ unit tests proving semantically relevant fields survive export.
- End-to-end new-RBO comparisons. Integration tests drive a real `IKqpHost`,
  capture both boundaries, and pass them through the normal CLI and hermetic
  solver.

### M2b: hermetic solver packaging — implemented

- Reviewed Z3 4.16.0 sources and deterministic generated files are pinned in
  `contrib/tools/z3`, with exact archive, source-list, and generated-tree hashes.
- A standalone command-line-only `PROGRAM(z3)` target has no library target or
  YDB `PEERDIR`, so it cannot add a solver link dependency to `ydbd`.
- Python, inspector, benchmark, and real-host integration tests declare the
  binary through `DEPENDS` and resolve its exact build output through `ya`.

### M3: StageGraph routing — implemented for the supported subset

- Two-producer-task Map, HashShuffle, Broadcast, and UnionAll with
  connection-derived consumer counts.
- Local join execution and final gather.
- Wrong-shuffle and wrong-broadcast mutation tests.
- Exact bounded Merge execution with input-order validation, tie-preserving
  sorted interleavings, and wrong-order mutation tests.
- Independent exhaustive concrete routing references for every admitted
  non-Merge connection and representative local-join combinations.

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
- Restricted static `IN` is exported as an explicit node and evaluated with SQL
  three-valued membership semantics. Independent exhaustive small-domain
  same-type references, representative lossless mixed-integer cases,
  dictionary-path heterogeneity rejection, mutation and boundary tests, and a
  real-host query with nullable String and `Int64` lookups cover the gate and
  prove the normal obligation with the hermetic solver.
- Decimal literals are tagged as finite, negative infinity, positive infinity,
  or NaN; source and opaque values use the exact legal typed domain. Ordinary
  equality/order, exact-type null-safe equality, Decimal/Decimal and
  Decimal/integer `DataCompare` alignment, precision-cap saturation, and
  complete non-null integer constant casts are modeled. Canonical same-type
  Decimal `+`/`-` and `DecimalMul` with a same-type Decimal or integer right
  operand have exact `NDecimal` special, ties-to-even, scale, and overflow
  semantics. Sort, TopSort, and Merge use the distinct raw-code total order,
  including ordered NaN and exact Decimal key identity. General casts, Decimal
  division, `IN`, and aggregate functions still fail closed. Exhaustive
  rational and ordering references, adversarial arithmetic cases, signature
  and mutation tests, and green real-host Decimal filter, arithmetic, and
  ordered obligations cover this boundary.
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
  covers exact Date and typed Decimal catalog columns, canonical `Void` for
  `COUNT(*)`,
  four scans, three joins, split aggregation, TopSort/Merge/Limit, and
  Map/Broadcast/UnionAll StageGraph routing.
- The real-host dashboard runs all 22 `TPCH_YQL` and 99 `TPCDS_YQL` sources,
  writes a structured timeout-aware report, and preserves diagnostic artifacts
  for every correctness, unknown, schema, or solver outcome.
- The Decimal milestone moves TPC-DS q48 through strict initial/final export and
  emits its two-row/two-task SMT obligation. This is formula coverage, not a
  solver proof; the focused formula-only run took 365116 ms and no Z3 executable
  was present.
- Focused Decimal-arithmetic and ordering corpus reruns move the affected TPCH
  and TPC-DS queries past `DecimalMul`, `+`, `-`, and Decimal order keys. TPCH
  q3 and TPC-DS q3, q52, q55, q71, and q93 now reach the narrower verifier-level
  Decimal `sum` gap; String keys remain fail-closed. No additional formula is
  emitted: the complete floors remain TPCH 0/22 and TPC-DS 3/99.
- [BENCHMARK_COVERAGE.md](BENCHMARK_COVERAGE.md) records the exact setup,
  commands, formula-only baseline, solver-backed q96 proof, q88 investigation,
  and explicit unsupported/optimizer-failure inventory.

### M5: confirmation and localization — in progress

- Separate normalized-plan and exact concrete-counterexample inspector.
- Separate real-YDB replay tool for deterministic, range-valid inspector
  witnesses, with strict dual-target mode preflight and typed BulkUpsert setup;
  legal Decimal specials are rendered as `-inf`, `inf`, and `nan`; multi-result
  TPC-DS q14, q23, and q39 remain an explicit replay extension.
- Explicit diagnostic transformation-prefix verifier boundary, committed-rule
  and atomic-stage snapshot hooks, strict real-host capture command, and
  separate sequential localization driver are implemented.
- Formula-construction coverage has a checked-in regression floor. A
  corpus-level solver-backed proof floor and replay-confirmed counterexample
  policy remain.

## Non-goals

- Proving CBO optimality.
- Checking after every rule in normal runs.
- Treating bounded verification as a general SQL-equivalence theorem.
- Proving `ConvertToPhysical`, task construction, or execution-engine correctness;
  those require a later boundary check and real-YDB replay.
- Growing the verifier into a second optimizer or expression simplifier.
