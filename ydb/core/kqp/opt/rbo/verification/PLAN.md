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

Unordered Limit produces a finite family of enabled bags. Small ordered choices
may also stay as explicit sequence families, while larger Sort, Merge, and latent
sequence choices use bounded symbolic row ordinals. Equality is mutual inclusion
of the two result languages: one side supplies a candidate sequence and the
other side's ordinal choices are quantified when testing membership. Shared-DAG
choices remain correlated, while distinct stage-task executions are independent.
Every ordinal ranges only over the fixed candidate-row vector, so this removes
factorial construction without broadening the bounded verification claim.

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
rbo_verifier/string_order.py exact finite String/Utf8 byte-order quotient
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

SMT terms form an immutable DAG. The renderer gives each quantifier body its own
scope and emits repeated compound terms once through hygienic, dependency-ordered
SMT `let` bindings. It never lifts a term across a quantifier that binds one of
its symbols. This preserves the direct mathematical obligation while avoiding
textual duplication in large ordered queries; the sharing transformation is an
exact rendering step, not a solver hint or semantic approximation. A separate
iterative post-order interner assigns equal IDs exactly when complete SMT term
structure is equal. It uses object identity only to traverse the DAG and never
recursively hashes a deep term; grouped-key classification and deferred
String-domain compaction consume those exact structural IDs.

## Scalar expressions

The explicit scalar core initially contains:

- column access, typed literal, and typed NULL;
- SQL/YQL three-valued `AND`, `OR`, and `NOT`;
- ordinary nullable equality, null-safe equality, and ordering across every
  signed/unsigned 8/16/32/64-bit integer pair, cross-identity `String`/`Utf8`
  equality and unsigned raw-byte ordering, and same-type `Date` ordering; exact
  Decimal equality and ordering use YDB `DataCompare` alignment for
  Decimal/Decimal and Decimal/integer operands;
- same-type signed and unsigned integer `+`, `-`, and `*`, with strict NULL
  propagation, exact typed input domains, and fixed-width
  modular/two's-complement overflow;
- canonical Decimal `+` and `-` with same-type operands, plus `DecimalMul` with
  a same-type Decimal or integer right operand, all with exact `NDecimal`
  specials, rounding, overflow, and strict NULL propagation;
- exact direct non-null String/Utf8-literal `SafeCast` to `Optional<Date>`,
  folded to an existing Date literal or typed NULL in both generic expressions
  and the executed OLAP-filter dialect;
- exact constant Optional-Date `+`/`-` folding for a direct String/Utf8-literal
  `SafeCast` and the strict normalized `DateTime2.IntervalFromDays` UDF shape;
- restricted static `IN`: a direct raw tuple or `AsList` containing 1..512
  recursively supported, non-null expressions of one item type; that type is
  identical to the lookup or uses a deliberately separate lossless
  common-integer gate, evaluated as the SQL three-valued OR of that equality;
- exact `Exists`, scalar `If`, and unary `IfPresent`; optional payloads use
  lexically scoped de Bruijn bindings, and the optimizer's exact
  identity-key/Void-payload `(One, Auto)` static `ToDict` membership shape is
  normalized to the same explicit `in` node;
- filter truth conversion.

The static-`IN` result must be `Bool` and nullable exactly when its lookup is
nullable. `ansi`, `warnNoAnsi`, `isCompact`, and `nullsProcessed` are erased
only under that semantic gate. `tableSource`, dynamic, empty, oversized,
nullable-item, heterogeneous-item, lossy or non-integer mixed-type,
malformed-option, unknown-option, and duplicate-option forms fail closed.
Decimal membership is deliberately outside this static-`IN` subset.

`Exists(x)` returns non-null `Bool(!x.is_null)`. Scalar `If` requires a
`Bool`/`Optional<Bool>` condition and branch scalar types matching the result;
its result is nullable exactly when the condition or either branch is nullable,
and a NULL condition produces NULL without selecting a branch. Unary
`IfPresent` requires exactly one `Optional<Data>` input and a one-argument
handler whose argument is the corresponding non-null Data value. Both branches
exactly match the result type and nullability. Snapshot `bound(depth)` nodes are
valid only inside the handler subtree, with depth zero naming the nearest
handler. Nested scopes are alpha-normalized by depth, and no more than 64
handler bindings may be live. The special new-RBO
membership normalization admits only
`Contains(ToDict(List(items), x -> x, x -> Void(), (One, Auto)), bound)` with
1..512 non-null exact-type items; generic dictionaries and `Contains` remain
unsupported.

Three reviewed optimizer-generated wrapper normalizations reuse those existing
exact nodes.
`Coalesce(predicate, false)` lowers to `if_present` only when the first child
is either one direct nullable ordinary comparison or exactly binary
`Or(member == literal, member == literal)`/`And(member != literal, member != literal)`.
The binary form requires the same direct `Optional<String>` member and a
non-null `String` literal in each leaf. The fallback is exact non-null
`Bool(false)`, and the result is exact non-null `Bool`. Larger Boolean trees,
other fallbacks, and different Optional shapes remain opaque.
`Coalesce(member, zero)` lowers to `if_present(member, bound(0), zero)` only
for one direct visible `Optional<Decimal(p,s)>` member, a non-null matching
Decimal result, and either a canonical Decimal zero or a complete
`SafeCast(Int32("0"), Decimal(p,s))` fallback. `Just(decimal)` lowers to
`if(true, decimal, typed-null)` only when its child is a direct canonical
Decimal literal, a complete integer-literal `SafeCast`/`Convert` to canonical
Decimal, or that exact Decimal Coalesce-zero form, and its result is the
matching `Optional<Decimal>`.
The unreachable typed-NULL branch preserves the Optional schema while the
constant-true condition preserves `Just` runtime presence. Incomplete,
mismatched, dynamic, nonzero, or broader safe near-matches remain opaque.
All three gates retain
the full closed-world scalar safety validation and shared normalized-node,
source-depth, and live-binding limits.

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

Ordinary integer `DataCompare` accepts all 64 ordered pairs of signed and
unsigned 8-, 16-, 32-, and 64-bit identities for equality, null-safe equality,
and ordering. MiniKQL compares their sign-aware mathematical values rather than
applying a wrapping unsigned conversion. Existing exact per-identity domains on
literals, source cells, and non-null opaque results therefore make an SMT
integer comparison exact: width `w` uses `[-2^(w-1), 2^(w-1)-1]` when signed
and `[0, 2^w-1]` when unsigned. Ordinary comparisons are strict on SQL NULL;
null-safe equality is two-valued with the usual both-NULL/one-NULL cases.
Static `IN` intentionally keeps its narrower lossless-common-type gate: equal
signedness, or a signed width greater than the unsigned width.

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
width. Binary `DecimalDiv` has the same closed operand gate as `DecimalMul`.
In every case result nullability is exactly the OR of operand nullability, and
the expression must pass the same closed-world scalar audit as an opaque
expression. The normalized snapshot node is `add`, `sub`, `mul`, or `div`; an
integer is never admitted on the left at this boundary.

YQL does not expose a complete determinism-and-totality annotation. The v1 C++
exporter therefore uses a reviewed positive list for opaque subtrees: integer
`+`, `-`, and `*` forms that do not meet the structural gate; scalar
comparisons; `Just` and `Coalesce`; `SafeCast`; and `Convert`
only when YQL's cast analysis says it cannot fail. The exact workload form
`Substring(Optional<String>, constant Uint32, constant Uint32)` is also
admitted, including direct in-range integer-literal conversions in its two
bound positions. Its constants remain in the canonical fingerprint and only
the String input is an external UF argument. Unknown callables, UDF/PG
calls, generic division, strict casts, `Unwrap`, free variables, position-aware
or unordered nodes, and side-effecting/CSE-unsafe nodes fail closed.
`DecimalDiv` is the one explicitly audited total division callable. Expanding
this list requires an explicit totality review and tests.

One cast shape is normalized before opaque fallback: when YQL cast analysis
reports a complete conversion from a non-null integer literal to a non-null
Decimal, the exporter evaluates it and emits the resulting Decimal literal.
An explicit `cast_decimal` node separately models `SafeCast` from a non-null
exact signed or unsigned 8/16/32/64-bit integer expression to a non-null
canonical `Decimal(p,s)`. The target descriptor and annotation must agree with
the result, and the target must retain at least one integral digit. Its exact
runtime meaning is the integer coefficient scaled by `10^s`, with strict
`Decimal(p,s)` bounds and signed-infinity saturation. Complete literals remain
normalized literals; incomplete literals and non-constant admitted expressions
remain explicit casts. `Convert`, `StrictCast`, nullable source/target/result
shapes, zero-integral-digit targets, and non-integer Decimal sources fail closed
outside the existing complete-literal normalization.

A separate fixed conversion normalizes only
`SafeCast(String|Utf8 literal, OptionalType(Decimal(p,s))) ->
Optional<Decimal(p,s)>`. The source must be a direct non-null literal containing
non-empty 7-bit ASCII. Result, descriptor, outer annotation, and nested non-null
Decimal item annotation must agree exactly, and YQL must classify the
source-to-item cast as `MayFail | MayLoseData`. The exporter calls
`NDecimal::FromStringEx`: `IsError` becomes typed NULL; successful finite values
retain round-half-to-even parsing and must be normal at precision `p`; NaN and
signed infinity remain tagged specials; overflow saturates to signed infinity;
and underflow may round to zero. A successful nonnormal result fails closed.
The fold emits the existing Decimal literal or typed-NULL shape, so it requires
no Python IR or evaluator extension. Dynamic, nullable, empty, non-ASCII,
misannotated, `Convert`, and `StrictCast` forms remain unsupported.

An explicit `cast_integral` node models only partial integer `SafeCast` pairs.
The source may be nullable or non-null and must have one exact signed or
unsigned 8/16/32/64-bit identity. YQL cast analysis must classify conversion to
the optional integer target as `MayFail`; the target descriptor, its outer and
item annotations, and the result type must agree exactly. The value is NULL
when the source is NULL or outside the target's exact integer domain, and is
otherwise the unchanged mathematical integer. NULL results use a canonical
zero payload. The complete expression also passes the closed-world opaque
safety audit. Complete integer conversions remain opaque; `Convert`,
`StrictCast`, non-integer pairs, and non-optional partial results do not enter
this exact node.

The persisted fingerprint is collision-free canonical text rather than a
machine hash. It length-prefixes node kind, callable and atom bytes, normalized
atom flags, exact formatted types, child counts, and ordered children. Direct
input-row Members become first-use ordinals; the corresponding unique IU values
are emitted as ordered UF arguments. Source positions, allocations, IU names,
and DAG sharing are deliberately absent. The exporter caps this representation
at 256 expanded nodes, nesting depth 64, and 64 KiB.

Independently of that hidden opaque-fingerprint budget, every complete
normalized scalar expression tree is capped at 1,024 expanded node occurrences
and structural depth 128, with the root at depth one. Repeated source-DAG uses
count once per emitted occurrence. Each scan or filter predicate, projection
expression, limit count, and offset is a separate root; all generated join keys
and residuals share the final synthesized join predicate budget, and every
pushed OLAP filter shares the final assembled scan-predicate budget. C++ charges
normalized occurrences before expansion, guards source recursion, and audits
the completed JSON iteratively; Python independently charges the same tree
while parsing. The C++ recursion ceiling is intentionally conservative when a
source wrapper normalizes away. The exact normalized budget still admits the
514-node full 512-item static-`IN` form with leaf lookup and items, while the
64-live-`IfPresent` binding limit remains separate.

Version-one `String` and `Utf8` values share one exact bounded integer-rank
quotient of YDB's unsigned UTF-8/raw-byte lexicographic order, without collation
or Unicode normalization. This keeps Z3 string theory and parsing outside the
trusted path. Ordinary and null-safe equality and ordinary ordering accept
either identity on either side. HashShuffle uses the same symbolic hash family
for both identities because the runtime hashes their raw bytes identically;
their snapshot type identities remain distinct. Static `IN` deliberately keeps
its narrower exact-type string gate.

The SMT script first collects every strict-UTF-8 literal and every distinct
observable nonliteral string term in both plans. Given `M` such terms, the
quotient keeps `M` valid-UTF-8 concrete representatives in every infinite open
literal interval and `min(M, interval size)` representatives in the only finite
byte-order gaps: below NUL prefixes and between a prefix and its NUL extensions.
Sorting distinct assigned values within each interval and mapping them to those
representatives proves preservation of all observed equalities and comparisons;
the converse holds because every rank has one listed concrete representative.
NUL extensions of complete UTF-8 literals keep witness representatives valid
UTF-8 and replayable, including for equivalence classes containing arbitrary
`String` bytes.

The universe is built only when SMT rendering seals the script. Sealing fixes
literal ranks, bounds every registered term, and exposes the complete rank-to-
representative map to witness decoding; later registration and out-of-universe
ranks fail closed. Construction is preflight-capped at 65,536 representatives,
64 MiB of total encoded representative bytes, and 1,000,000 bytes per value.
The per-value cap is shared with inspection and replay. A string-valued term
depending on a sequence ordinal that family comparison may rebind under a
quantifier also fails closed: a top-level finite-domain assertion would not
constrain all rebound valuations.

The same choice-independence audit applies to every top-level source, catalog,
and opaque-result domain invariant: any such invariant that depends on a
rebound sequence ordinal fails closed, not only String rank bounds.
Global invariants render before the ordinary counterexample obligation even
when deferred String sealing registers them later.

Version-one `Date` is the exact unsigned day-since-epoch domain
`[0, NUdf::MAX_DATE)`. Numeric literals are range-checked, source slots and
non-null opaque Date results receive explicit domain constraints, and same-type
comparison, Sort, and Merge use integer day ordering.

The exporter now evaluates a direct non-null `String` or `Utf8` literal under
`SafeCast` to exactly `Optional<Date>` in both generic scalar expressions and
the executed OLAP-filter dialect. The result and target descriptor, outer and
nested annotations, and reviewed `MayFail` cast classification must agree.
MiniKQL `ValueFromString` is the parser oracle: a valid value becomes an
existing Date literal and parser failure or an out-of-domain value becomes
existing typed Date NULL. The generic path retains the opaque expression
encoder's closed-world safety and totality audit. Dynamic, nullable, malformed,
differently annotated, and non-`SafeCast` forms fail closed. No Date-cast IR or
Python evaluator operation is added.

The exporter additionally normalizes one complete constant Date expression,
rather than introducing Interval into the snapshot IR. The left operand must be
a direct non-null String/Utf8 literal `SafeCast` whose result, target descriptor,
outer annotation, and item annotation are exactly `Optional<Date>` and whose
YQL cast classification is `MayFail`. The right operand must be an `Apply` of
the strict normalized eight-child `DateTime2.IntervalFromDays` UDF to a direct
non-null `Int32` literal in `[-49672, 49672]`. Its callable/cached annotations,
AutoMap flag, Void run configuration and user types, empty type configuration
and file alias, and ordered `blocks, strict` settings must all agree exactly.
Only Optional-Date `+` and `-` with that operand order enter the gate.

MiniKQL `ValueFromString` is the parser oracle. A valid Date plus the signed day
offset becomes an existing Date literal; parser failure or a result outside
`[0, NUdf::MAX_DATE)` becomes existing typed Date NULL. The pushed OLAP
`just` wrapper is erased only around a direct valid non-null Date literal.
Dynamic, nullable, malformed, differently annotated, or otherwise noncanonical
forms fail closed. Because the whole expression is evaluated by the exporter,
no Interval node or Python evaluator semantics are added.

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

Same-type `div` multiplies the left coefficient by `10^s` before division;
`DecimalDiv` with an integer right operand divides the coefficient directly and
therefore preserves the left Decimal scale. Both reproduce `NDecimal::Div`'s
current signed-remainder behavior exactly: positive divisors round to nearest
with ties to even, while negative-divisor non-ties truncate toward zero and
exact ties still round to even. Division by zero, NaN, signed infinities,
global 35-digit saturation, result-precision saturation, and a finite quotient
that collides with the reserved NaN code are explicit.

Decimal Sort, TopSort, and Merge use the MiniKQL/DQ runtime comparator,
not ordinary `DataCompare`: raw signed 128-bit codes form the total non-null
order `-Inf < finite values < +Inf < NaN`, reversed for descending. Raw code
equality makes two NaNs a sort tie. One order item retains one exact canonical
`Decimal(p,s)` identity without scale alignment; separate tuple keys may have
different Decimal identities. NULL placement continues to use the pre-physical
snapshot's explicit `nulls_first` field. Generic division, casts outside the
exact integral-`SafeCast` and constant-normalization gates,
dynamic or otherwise non-core `IN`, and aggregate functions outside the
modeled subset below remain unsupported.

`sum(Decimal(p,s))` widens inputs, partial state, and result to
`Decimal(35,s)`. MiniKQL/DQ combines them with saturating `AggrAdd`, which is not
associative when finite overflow is possible. Each modeled Decimal value can
therefore retain a conservative absolute finite-code bound. A sum is admitted
only when the sum of all candidate bounds is strictly less than `10^35`; that
guarantees every input order and partial/final parenthesization agrees. In this
domain the compact exact result is NULL for no non-NULL input, NaN for any NaN
or both infinity signs, the sole infinity sign when present, and otherwise the
raw scaled-integer total. Partial states preserve the tighter bound through
aliases and StageGraph connections. Missing provenance falls back to the full
declared-type bound and can only make verification fail closed.

The bound invariant covers every non-NULL finite Decimal coefficient and makes
no numeric claim about exact NaN/infinity value terms. Finite literals seed
their absolute coefficient; typed NULL and special literals seed a vacuous zero;
same-type `+`/`-` use a precision-capped triangle bound; and exact non-null
integral casts use the complete source-type domain, target scale, and saturation
point. `If`/`IfPresent` select the maximum known alternative, while any unknown
operand remains unknown. Focused scalar tests cover finite, NULL, special,
additive, conditional, and signed/unsigned 8/64-bit cast cases. Relation tests
consume literal arithmetic and integral-cast bounds through a two-row Decimal
`SUM`, check special/NULL semantics, and retain the strict `10^35` rejection.

Decimal `avg` is admitted only when its input and output are the identical
canonical `Decimal(p,s)`. The exporter records the physical state hidden by the
logical RBO IU type as
`{sum_type: "Decimal(35,s)", count_type: "Uint64", nullable:
<input-nullability>}`; non-AVG traits omit this field. The strict decoder
requires one direct intermediate-to-final aggregate lineage with identical
ordered keys and state metadata. Each intermediate state IU must have exactly
one matching final AVG use, cannot be used as an ordinary scalar or key, and
may cross StageGraph routing only as payload.

Undefined and intermediate phases accumulate one `Decimal(35,s)` sum and one
`Uint64` count over non-NULL inputs. The final phase adds both components, so
unequal task partitions are weighted by their counts rather than averaging
partial averages. A group with no non-NULL input produces NULL. Existing
Decimal special algebra makes NaN absorbing, opposite infinities NaN, and a
sole infinity sign stable. Division by the positive count reproduces signed
round-to-nearest/ties-to-even behavior, after which the exact same-scale narrow
cast preserves specials and saturates finite overflow to signed infinity.
Verification fails closed unless the finite sum bound is strictly inside the
35-digit accumulator and the count bound is below `2^64`, avoiding any claim
across non-associative sum overflow or count wrap.

Inspector traces expose the optional physical `{sum,count}` value, its types,
and its conservative proof bounds on every intermediate state cell. The state
terms are always probed, and partial state rendering fails closed.

Independent exhaustive small-domain differential tests cover finite values,
NULL, NaN, signed infinities, positive and negative ties, grouped/scalar
aggregation, and unequal split-task counts. Decoder mutations cover every
state field, type/nullability mismatch, non-Decimal AVG, state leakage, and
broken phase lineage. Focused C++ exporter tests pass 3/3 and the complete C++
exporter suite passed 147/147 at this Decimal-AVG milestone.

An isolated manual real-YDB diagnostic exercises the rejected overflow domain
without weakening that gate. For the same three `Decimal(35,0)` rows it observes
`M` with one column-table partition and `inf` with two partitions under both the
new-RBO and legacy optimizers. This is a confirmed shared aggregation/runtime
partition-sensitivity witness, not a new-RBO-only optimizer counterexample; the
ordinary verification target remains green and the intentionally failing
diagnostic stays manual and separate.

## Relational semantics

Each base table has a fixed number of symbolic row slots. A slot contains a
presence Boolean and one nullable value per column. Plans produce fixed vectors
of guarded rows. Rows also carry structural occurrence provenance and routing
facts used only for exact StageGraph normalization; neither annotation changes
SQL values or multiplicity.

Each relational outcome also carries one explicit Boolean query-error status.
Ordinary relational operators preserve an input error, family products combine
input errors with Boolean OR, and error is observable even when an operator such
as `Limit 0` produces no rows. Two outcomes are equal when both error, or when
both succeed and their result relations are equal; an error and a successful
result are never equal. The result relation attached to an error outcome is
therefore diagnostic data, not an observable value. Inspector traces render the
status explicitly. Version one distinguishes error from success but does not
compare error categories, codes, or text. Error-aware real-YDB replay remains a
separate extension and currently fails closed on such a trace.

Implementation sequence:

1. M1: one-row empty source, scan, exact projection, and filter;
2. M1: inner, cross, left/right/full outer, semi, and anti/only joins;
3. M1: logical bag `UnionAll`;
4. M1: root projection and column order;
5. M4: common aggregates and unordered literal Limit;
6. M4: Sort/TopSort, ordered literal Limit, and ordered Merge;
7. M4: actual column-store filter pushdown, including exact presence tests,
   from the executed OLAP dialect;
8. M4: exact Decimal literals, domains, comparison, and constant-cast
   normalization;
9. M4: exact non-null integral `SafeCast` to Decimal;
10. M4: exact partial integral `SafeCast` to an optional integer;
11. M4: exact canonical Decimal `+`, `-`, `DecimalMul`, and `DecimalDiv`;
12. M4: exact Decimal Sort, TopSort, and Merge ordering;
13. M4: exact headroom-bounded Decimal `sum` and partial-state combination;
14. M4: occurrence-aware routing compaction and scalable symbolic ordinals for
    Sort, Merge, and latent sequences;
15. M4: quantifier-scoped shared-term SMT rendering;
16. M4: exact bounded String/Utf8 comparison, ordering, and hash compatibility;
17. M4: exact all-pairs ordinary integral `DataCompare`;
18. M4: exact direct String/Utf8-literal `SafeCast` to optional Decimal;
19. M4: exact same-type Decimal aggregate `max`;
20. M4: exact direct String/Utf8-literal `SafeCast` to optional Date in generic
    and executed OLAP-filter expressions;
21. M4: exact constant String/Utf8-to-Date plus-or-minus
    `DateTime2.IntervalFromDays` normalization and direct Date-literal OLAP
    `just` erasure;
22. M4: provenance- and allocation-bounded stored-String `Concat` at a Map-body
    root;
23. M4: exact phase-aware Decimal `avg` with explicit hidden state and direct
    intermediate-to-final lineage;
24. M4: subplan-aware initial catalog capture and exact ordered logical
    `UnionAll`;
25. M4: exact captured uncorrelated scalar subplans that are statically at most
    one row, including task-aware no-op admission for final
    `EnsureAtMostOne` markers;
26. M4: explicit query-error outcomes, exact `EnsureAtMostOne`, and general
    uncorrelated scalar subplans with consumer-demanded local cardinality
    errors and eager inherited errors;
27. next: exact restricted relational `IN`/`EXISTS`;
28. later: equality-correlated scalar and `EXISTS` subplans;
29. later: proof scaling, distinct expansion, range reads, and other OLAP
    pushdowns.

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
Aggregate and Join establish new unordered streams. Unordered UnionAll does the
same; ordered UnionAll independently orders each input and concatenates the
complete left sequence before the right.

Sort enumerates every permutation only when every family outcome has at most
three candidate rows and their combined permutations fit the ordinary outcome
cap. Any outcome with four or more rows instead assigns one bounded integer
ordinal to every row slot, even when its factorial would still fit. Present rows
have in-range, pairwise-distinct ordinals; key comparisons constrain their
relative ordinals, while ties remain unconstrained. Absent rows do not occupy a
compressed position. This is the same finite sequence language with quadratic
constraints rather than factorial outcomes. A non-null Sort limit is TopSort
and applies an exact prefix by compressed ordinal rank. The same selector is
used when an unordered bag needs a latent sequence. Sort and Limit phases are
preserved but do not independently change the modeled runtime semantics. If the
initial root is ordered, results are compared as compressed sequences;
otherwise they are compared as bags.

Every materialized relation fails closed above 4096 candidate rows. Join
matching/output, UnionAll, and grouped-aggregate sizes are checked before their
large intermediates are allocated. Sort, Merge, and latent-sequence encodings
likewise fail closed above 16384 candidate-row pairs before computing factorials
or allocating ordinals. Explicit outcome families separately fail closed above
256 alternatives; that cap applies to unordered-Limit masks, small enumerated
ordered choices, and family products/gathers. Large ordered choices switch to
the exact ordinal representation within the row-pair bound. Cross-plan equality
fails closed above 4096 explicit outcome pairs. None of these caps is
approximated.
Grouped aggregation assigns an exact structural signature to every complete
ordered group-key value. For `N` input rows and `K` distinct signatures, the
repeated-class representation is eligible only when `K < N`, its `K*N`
memberships fit the pair ceiling, and its `K*(K+1)/2` symmetric class
comparisons separately fit the ceiling. It is selected when the directional
`N^2` square exceeds the ceiling, or below the ceiling only when the sum of
those two class costs is strictly smaller than `N^2`. Aggregate membership
still ranges over all original rows, preserving duplicates. Presence and
first-representative suppression remain directional; only composite null-safe
key equality is shared over the class upper triangle. Singleton classes retain
provenance, multi-member classes retain only their common partition facts, and
no SQL row is deduplicated. If classes are ineligible, the established
directional formula is retained while it fits and the exact singleton-class
upper-triangle fallback is used above that point. This is exact representation
selection, not a raised cap or an assumption that row presence is symmetric.

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

Stage task inference is completed as part of strict StageGraph validation before
any staged physical property is admitted. Every Limit snapshot explicitly
records `ensure_at_most_one`; legacy snapshots may omit it only as the
well-defined default `false`. When set, the evaluator observes an error exactly
when the post-Skip/post-Take relation contains more than one present row.
Stage-local checks run independently in each task, and a later connection
propagates any task error. The exporter no longer erases the marker through a
structural cardinality proof or treats a single producer task as proof that the
check is inert.

Merge requires every producer task to carry an order compatible with the edge
order. Small cases may enumerate sorted producer-order-preserving interleavings.
Larger cases assign result ordinals and constrain them by both sort keys and the
input ordinals within each producer. Incompatible metadata and unordered inputs
fail closed.

Source placement and HashShuffle create guarded task copies of one logical row
occurrence. At a non-Merge multi-task gather, opposite facts for the same routing
choice prove those copies mutually exclusive, so the evaluator can coalesce them into
one guarded occurrence and use exact conditional values when task-local state
differs. Broadcast copies have no contradictory routing fact and retain their
bag multiplicity. Unknown provenance also remains uncompacted. This
occurrence/routing normalization removes task-copy blow-up without identifying
rows that can coexist.

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

### Finding preservation and commit policy

Every solver candidate is retained as a reproducible case: exact query,
initial and final snapshots, byte-exact raw verdict, their SHA-256 bindings,
and the emitted SMT formula when available. Inspection adds the pinned witness
and operator/stage trace; confirmation adds exact child commands, streams, and
the retained real-YDB namespaces. A localized case additionally retains the
transformation-prefix captures. Temporary minimized queries are promoted to a
focused durable test before a production fix is considered complete.
Real-host replay may instead reclassify a symbolic discrepancy as a
verifier-model error. Such a case becomes a model regression, but the optimizer
must still be audited independently because a model bug and a real execution
divergence can coexist.

Verifier semantics or exporter changes are committed separately from optimizer
changes so review can audit the model independently. An optimizer correction
and its focused regression normally form one atomic commit. Semantic and
finding notes may be updated with that fix, but numerical coverage reports
change only after a complete corpus rerun. The history does not intentionally
retain a red commit merely to demonstrate the bug: the pre-fix failure is
documented by the preserved repro and by showing that the regression fails
against the parent revision.

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
   report unsupported features separately from failures, and keep a hermetic
   solver-backed regression floor for every curated workload proof.

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
exhausted at every scale, division is checked against a structurally separate
literal transcription of `NDecimal::Div`, and addition/subtraction are
exhausted once per precision and checked structurally scale-independent.
Adversarial cases cover ties-to-even for both signs, negative-divisor non-ties,
every integer width, special values, precision overflow, and finite collisions
with the NaN code. Existing C++ literal, alignment, and division tests use
`NDecimal` as an oracle, while arithmetic exporter tests audit the signature
gates. Normal verifier tests prove unchanged `add`/`sub`/`mul`/`div` across a
staged Map and require operation mutations to produce solver counterexamples.
Decimal ordering is exhaustively checked on every legal finite code through
precision two plus specials, directions, explicit NULL placement, NaN ties,
TopSort prefixes, and two-task Merge. A C++ oracle locks the stated total order
to `NUdf::CompareValues<Decimal>`. A real-host Decimal query verifies the
bounded two-row/two-task pre-physical logical Sort+Limit to staged
TopSort+Merge+final-Limit transformation pair.
Decimal sum is exhaustively checked across finite values, specials, NULLs,
grouping, scalar empty input, and every two-row/two-task partial-state routing.
A C++ `NDecimal::Add` oracle locks the overflow non-associativity that requires
the headroom gate, the exporter test locks `Decimal(7,2)` to `Decimal(35,2)`
widening across intermediate/final phases, and a real-host query proves the
split two-row/two-task aggregate obligation.
Symbolic-order tests compare the represented sequence sets with exhaustive
finite enumeration, exercise 48-slot construction, and retain direction, NULL,
tie, producer-order, TopSort, and mutation cases. Routing-compaction tests keep
distinct occurrences and broadcast multiplicity while checking exact guarded
values for exclusive task copies. SMT tests lock typed quantifier shadowing,
hygienic dependency-ordered `let` bindings, and the rule that shared terms in a
quantified body stay inside that scope.
String-order tests exhaust small byte alphabets and literal/term bounds, finite
prefix/NUL gaps, Unicode normalization distinctions, valid-UTF-8 replay
representatives, sealing, budget rejection, and quantified-choice fail-closed
behavior. C++ runtime oracles lock the shared unsigned-byte comparator and
type-independent String/Utf8 hash contract.
Direct String/Utf8-literal Decimal-cast tests use `FromStringEx` as the runtime
oracle for both source identities, exponent syntax, both signs of
round-half-to-even boundaries, NaN/infinities, overflow saturation, underflow,
parser errors, and successful nonnormal rejection. Structural mutations cover
the complete annotation, descriptor, source, ASCII, and cast-classification
gate. The actual pushed-OLAP dialect has direct exporter coverage, while
real-host TPC-DS q65 confirms that both snapshot boundaries now pass export.
Constant Date/Interval tests use MiniKQL `ValueFromString` as the parser oracle
for String and Utf8, both arithmetic signs, negative day literals, zero and
both Date-domain boundaries, invalid dates, and result underflow/overflow.
Structural mutations cover every result/descriptor annotation, UDF name and
eight-child metadata field, cached callable, setting order/presence, argument
type/nullability, day-range boundary, and arithmetic operator. OLAP tests admit
only direct valid non-null Date literals under `just`, and a real-host
column-store `BETWEEN` obligation proves the logical filter to pushed-filter
pair at the normal two-row/two-task bound.
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
  explicit or symbolic producer-order-preserving interleavings, and wrong-order
  mutation tests.
- Occurrence- and routing-aware gather compaction for mutually exclusive source
  and shuffle task copies, while broadcast multiplicity and distinct occurrences
  remain explicit.
- Independent exhaustive concrete routing references for every admitted
  non-Merge connection and representative local-join combinations.

### M4: benchmark coverage — in progress

- Grouped/scalar count, integer sum, and headroom-bounded Decimal sum, including
  split intermediate/final execution, NULLs, exact 64-bit integer behavior,
  Decimal specials, partial-state bound provenance, same-type Decimal MAX, and
  phase-aware Decimal AVG with explicit `(sum,count)` state; distinct variants
  remain.
- Unordered literal Limit/offset, split per-task execution, and column-source
  pushed limits, with exhaustive and mutation tests.
- Sort/TopSort, ordered Limit, and Merge, with exhaustive concrete differential
  tests, bounded symbolic ordinals beyond the small explicit-family threshold,
  and order/limit/phase mutation tests.
- A real-host ordered test captures logical Sort+Limit and the transformed
  per-task TopSort+Merge+final-Limit program, then constructs or solves the
  normal equivalence obligation.
- `String` and `Utf8` use an exact bounded unsigned-byte-order quotient for
  cross-identity equality and scalar ordering and for Sort, TopSort, and Merge.
  HashShuffle shares their type-independent raw-byte hash family. Exhaustive
  quotient/reference tests and C++ comparator/hash oracles cover NUL-prefix
  gaps, non-normalized Unicode, arbitrary `String` bytes, replayable witnesses,
  resource caps, deferred sealing, and quantified-choice fail-closed behavior.
  A focused run moves TPC-DS q42 and q50 through formula construction and proves
  q42. The remaining former String blockers now reach deeper construction caps
  (q4, q11, q25, q29, q46, q64, q68, and q91).
- Reviewed deterministic total scalar subtrees are exported as canonical typed
  opaque functions. Unit tests cover IU alpha-renaming, first-use argument order,
  repeated arguments, structural/literal/callable mutations, DAG-sharing
  independence, nullability, and fail-closed safety gates.
- The workload `Substring` form is admitted only for an optional String and two
  constant `Uint32` bounds. Direct, exact integer-literal conversion to
  `Uint32` is allowed only in those bound positions; type, range, arity, and
  dynamic-bound mutations fail closed. A real-host obligation covers the
  normalized converted-literal form. This moves TPC-DS q15, q19, q62, q79, and
  q99 through formula construction; q8 then reaches its mixed-width integral
  comparison at both snapshot boundaries.
- Ordinary integral equality, null-safe equality, and ordering admit all 64
  ordered pairs of signed/unsigned 8/16/32/64-bit identities. MiniKQL's
  sign-aware mathematical comparison, exact integer domains, ordinary SQL NULL
  propagation, and two-valued null-safe equality have all-pair exporter and
  decoder tests, plus signed/unsigned endpoint and fail-closed mutation tests.
  A dedicated real-host `COUNT(*) > 1` fixture captures `Uint64 > Int32` at
  both snapshots and returns `VERIFIED_BOUNDED`. Static `IN` retains its
  independent lossless-common-type audit. A focused real-host q8 run now passes
  the former mixed-width boundary and fails closed on unsupported scalar
  callable `Unwrap` in both snapshots after 480 ms of preparation and 0 ms of
  verifier work; formula and proof counts are unchanged.
- Partial integer `SafeCast` is exported as `cast_integral` only for exact
  signed/unsigned 8/16/32/64-bit source and optional target identities whose
  YQL cast classification is `MayFail`. Descriptor and nested annotation
  agreement, the closed-world safety audit, all source/target width pairs,
  signed/unsigned boundaries, source NULL propagation, and canonical NULL
  payloads have fail-closed or exact tests. Complete conversions and other cast
  families remain outside this exact node.
- A direct non-null String/Utf8 literal `SafeCast` to an exactly matching
  optional canonical Decimal is evaluated with `FromStringEx` and folded to an
  existing tagged Decimal literal or typed NULL. The gate is restricted to
  non-empty 7-bit ASCII and the exact `MayFail | MayLoseData` classification;
  finite half-even parsing, specials, saturation, underflow, and nonnormal
  rejection are locked by runtime-oracle and fail-closed tests. No Python IR
  change is needed. Before Date/Interval folding, focused TPC-DS q21 and q40
  reached initial `Interval` and final OLAP `just`; at that historical point
  q65 exported both snapshots and reached unsupported aggregate `avg` after
  231 ms of preparation and 255 ms of verifier work. This Decimal-cast
  milestone itself added no formula.
- Exact direct non-null String/Utf8-literal `SafeCast` to `Optional<Date>` now
  folds through MiniKQL parsing in generic and executed OLAP-filter expressions.
  Valid text becomes an existing Date literal and invalid text becomes typed
  Date NULL; descriptor, annotation, cast-classification, safety, and totality
  gates fail closed around that exact shape. Focused exporter tests passed 4/4,
  the complete `cpp_ut` run passed 144/144 at that Date-cast milestone, and the
  q5-shaped actual-host integration passed 1/1 with `VERIFIED_BOUNDED`.
  Regenerated q5/q77
  obligations return `UNKNOWN` instead of rediscovering either old candidate.
  The q5 witness is refuted; q77's old witness remains unconfirmed because its
  corrected fixed-witness diagnostic was also `UNKNOWN`. Formula and proof
  floors at that milestone remained 37/121 and 13/121.
- Exact constant Date/Interval normalization admits only a direct non-null
  String/Utf8 literal `SafeCast` to exactly `Optional<Date>`, followed by `+` or
  `-` with the strict normalized eight-child
  `DateTime2.IntervalFromDays` UDF applied to a direct non-null `Int32` literal
  in `[-49672, 49672]`. MiniKQL parses the Date; invalid input or a result outside
  `[0, 49673)` becomes typed Date NULL. The related pushed-OLAP `just` is erased
  only around a direct valid non-null Date literal. Runtime-oracle, structural
  mutation, Date/day boundary, and real-host pushed-filter tests cover the
  complete gate without adding Interval to the snapshot IR. This moved TPC-DS
  q37, q40, and q82 through formula construction, for 21/99 TPC-DS and 23/121
  total workload formulas (19.0%) at that milestone. Formula construction is
  not a proof. q37 and q82 return `UNKNOWN` at a 60-second solver budget. A
  separate non-gating q40 scaling experiment retains a 97,319,076-byte formula
  and reports `SOLVER_ERROR` after the external solver exceeds its 15.0-second
  deadline; that focused `ya` experiment fails on the status as designed. The
  proof floor remained ten at that milestone. Before restricted stored-String
  `Concat` was added, TPC-DS q5, q80, and q84 stopped at that callable; other
  deeper blockers include `Double` for q21, a noncanonical dynamic Date fold
  for q72, and verifier-side Decimal-SUM headroom for q77.
- Direct numeric Date/Interval normalization accepts only an exact non-null
  `Date` left operand and `Interval` right operand under an `Optional<Date>`
  `+` or `-`. It
  reproduces MiniKQL's microsecond scaling, signed arithmetic, scaled-domain
  validation, and final day truncation; exact type-range premises prove the
  intermediate cannot overflow `i64`. Malformed shapes and invalid Interval
  atoms fail closed, while an out-of-Date-range result becomes typed NULL.
  Synthetic boundary/fractional-day tests and a real-host pushed-filter proof
  cover the gate. TPCH q1 now passes both exporters and reaches verifier-side
  aggregate `avg`; formula and proof counts are unchanged.
- Restricted stored-String `Concat` is admitted only at a Map-body root as a
  binary non-null String tree. Its leaves are canonical String literals, one or
  two catalog-backed stored String occurrences, or exactly
  `Coalesce(nullable member, String(""))`; every other placement, type, leaf,
  or fallback fails closed. Provenance begins only at catalog-confirmed
  Datashard and Olap tables, excludes system views and generated or computed
  values, and follows Map pass-through/rename, Filter, Limit, Sort, aggregate
  group keys, value-preserving join sides, and UnionAll. Outer/exclusion joins
  widen the affected side, semi/anti joins drop the absent side, UnionAll ORs
  nullability from both inputs, and the final Member annotation must match that
  carried catalog nullability. The auditor carries Datashard's enforced 16 MiB
  value cap or the `INT32_MAX` logical-cell bound imposed by Olap's validated
  Arrow `BinaryType` representation, charges it per occurrence plus exact
  literal bytes, and proves that MiniKQL's `ui32` allocation-growth calculation
  cannot wrap. One generic Olap occurrence can pass when its exact literals fit
  the remaining allocation headroom; any two generic Olap occurrences fail
  closed. Only then does it encode the whole syntax tree as one opaque function
  whose fingerprint retains structure, literal bytes, order, and repetition.
  Focused tests cover the
  grammar, provenance failures, and all ten join kinds; a real-host
  initial/final one-Olap-occurrence obligation is `VERIFIED_BOUNDED`, and a
  two-Olap-occurrence case fails closed. At that milestone TPC-DS q5 reached
  the Decimal-SUM headroom gate and q80 reached the 82,944-pair
  grouped-aggregate construction cap. q84 had two Olap String occurrences and
  stopped at the allocation-totality gate. The formula slice remained 23/121
  (19.0%) and the proof floor remained ten.
- Constant DateTime2 calendar-shift normalization admits only the exact
  optional-Date `Map(Shift(Split(Date), Int32), MakeDate)` tree generated for
  `ShiftYears` and `ShiftMonths`. Date, Int32, `DateTime2.TM`, callable and
  cached descriptors, UDF user types, Void fields, settings, AutoMap flags,
  optionality, unary lambda shape, and binder identity must all match the
  reviewed normalized form. One five-row reviewed signature table covers
  `IntervalFromDays`, `Split`, both shifts, and `MakeDate`; shared validators
  enforce their normalized UDF envelopes, cached types, and `Apply` nodes.
  MiniKQL Date split/make tables reproduce leap-day
  and month-end clamping plus the runtime signed month quotient/remainder
  sequence. Potential wrap of TM's unsigned 12-bit year field fails closed; a
  valid shifted calendar value outside the Date domain becomes typed NULL.
  Synthetic result, boundary, mutation, binder, and wrap tests plus a real-host
  pushed-filter proof cover the gate. The complete TPCH dashboard now emits
  formulas for q5, q6, q10, and q14 after 170/113,378, 55/222, 108/7,886, and
  53/267 ms of preparation/verifier work, respectively. At that milestone q12
  passed the shift fold and exposed unordered scalar children at both snapshot
  boundaries. This raised TPCH formula coverage to 6/22 and total formula
  coverage to 27/121 (22.3%). Focused solver experiments at that milestone
  left the proof floor unchanged: q5 was `SOLVER_ERROR` after 180/230,982 ms
  of preparation/verifier work and the 65-second external-process watchdog,
  q10 was `UNKNOWN` after 142/74,871 ms, and q6/q14 produced symbolic
  counterexamples after 54/788 and 87/1,046 ms. Inspection indicates verifier
  false positives caused by
  equivalent predicate/Decimal lowerings receiving distinct opaque forms;
  neither was replay-confirmed or evidence of an optimizer bug.
- Exact wrapper normalization resolves those q6/q14 verifier-modeling gaps
  without globally erasing either wrapper. Only a nullable direct comparison
  under exact `Coalesce(..., false)` becomes schema-preserving `if_present`, and
  only a direct Decimal literal or complete integer-literal Decimal cast under
  matching `Just` becomes `if(true, value, typed-null)`. Structural, type,
  nullability, safety, source-depth, normalized-node, and live-binding tests
  fail closed outside those forms. The policy-backed TPCH proof-floor run now
  returns `VERIFIED_BOUNDED` for q6 after 72/749 ms and q14 after 97/33,152 ms
  of preparation/verification. Their former candidates disappear under the
  exact model, so both enter the proof floor and neither is an optimizer bug.
- Exact q12 membership/complement normalization accepts only
  `Coalesce(Or(member == literal, member == literal), false)` and
  `Coalesce(And(member != literal, member != literal), false)`, with both leaves
  comparing the same direct `Optional<String>` member with a non-null `String`
  literal. It reuses schema-preserving `if_present`; broader Boolean trees
  remain opaque. The fresh complete TPCH dashboard records q12 as
  `FORMULA_EMITTED` after 109/5,343 ms of preparation/verifier work; an earlier
  focused formula run recorded 108/5,816 ms. Focused and policy-floor solver
  runs return `VERIFIED_BOUNDED` after 108/38,880 and 106/40,602 ms,
  respectively. At that milestone TPCH formula coverage was 7/22, total
  formula coverage was 28/121 (23.1%), TPCH had five proofs, and the workload
  proof floor was 13/121 (10.7%). No proof produced a candidate, so replay was
  not invoked and no optimizer correctness bug was found.
- Exact direct Decimal Coalesce-zero normalization accepts only one visible
  `Optional<Decimal>` member and either a matching canonical zero or a complete
  Int32-zero `SafeCast`, including the matching `Just` wrapper. It reuses
  `if_present` and `if` without erasing nullability. At that milestone the complete
  TPC-DS dashboard moved q43 through formula construction after 145/4,760 ms
  and moved q77 past finite Decimal `SUM` headroom to the 25,600-pair grouped
  aggregate cap after 2,063/442 ms. TPC-DS reached 22/99 formulas and the
  workload 29/121 (24.0%); q77 remained unsupported. q43 is `UNKNOWN` at the
  60-second solver budget after 147/69,391 ms, so the proof floor remains
  thirteen. The first complete run caught incomplete Decimal zero casts in
  q40/q80 entering the strict exact-cast path; classification now leaves those
  near-matches opaque, focused regressions cover both bare and wrapped forms,
  and the repeated complete dashboard restores q40's formula and q80's
  verifier-entry result. No candidate or optimizer bug arose.
- At the preceding grouped-comparison milestone, sharing only the symmetric
  null-safe group-key upper triangle above the directional cap moved q25/q29
  from 65,536 to 32,896 comparisons, q80 from 82,944 to 41,616, and q77 through
  both aggregates to a 51,360-pair Sort. That dashboard still had 22/99
  TPC-DS formulas and 29/121 workload formulas; these are historical
  intermediate blockers, not current results.
- The exact representation-selector milestone first assigned nonrecursive
  bottom-up structural IDs to complete SMT terms. Grouped aggregates partition
  candidates whose ordered `(type, is-null term, value term)` group keys are
  structurally identical, retain one result candidate per exact key class, and
  continue to range aggregate membership over every original row. The class
  form is used only when it reduces the candidate count, its membership and
  upper-triangle comparison counts fit, and it is required by the directional
  cap or is strictly cheaper below that cap. Independently, Sort and latent
  sequences enumerate only at-most-three-row outcomes; four or more rows use
  exact bounded symbolic ordinals. Exhaustive differential, provenance,
  partition-fact, selector, three/four-row boundary, and deep-term regressions
  remain green.
  The complete dashboard at that milestone emitted 30/99 TPC-DS formulas,
  with 40 unsupported queries and 29 optimizer failures. The new formulas are q5,
  q25, q29, q46, q68, q77, q80, and q91, measured at 1,588/2,653,
  249/11,564, 263/4,142, 284/2,574, 276/2,301, 2,122/3,323, 1,810/42,847,
  and 227/3,754 ms of preparation/verifier work, respectively. With TPCH's
  then-current seven formulas, coverage was 37/121 (30.6%); the
  thirteen-query proof floor is unchanged. Formula construction is not a
  solver proof. Regenerated full TPC-DS solver runs return `UNKNOWN` for q5
  and q77 after 1,552/64,916 and 2,035/66,344 ms of
  preparation/verification. Focused 60-second runs return `UNKNOWN` for q25,
  q29, q46, q68, q80, and q91 after 302/86,108, 272/68,174, 313/64,717,
  293/64,427, 1,784/121,558, and 221/67,811 ms, respectively. None is a proof
  or current candidate divergence.
- Before exact direct literal-to-Date normalization, focused 60-second solver
  runs returned `COUNTEREXAMPLE` for q5 after
  1,576/10,150 ms and q77 after 2,062/36,163 ms of preparation/verification;
  those historical inputs and verdicts are SHA-bound in retained artifacts. A
  fixed-witness q5 inspector run reproduced the symbolic root mismatch, with
  six present logical rows and one staged row. Follow-up audit proves q5 is a
  verifier false positive: three initial String-literal-to-optional-Date lower
  bounds were shared zero-argument opaque functions, permitting witness days
  10,441 and 10,457 outside the real 10,442..10,456 range, while the pushed
  final scans used Date literal 10,442. Pinning those three results to 10,442
  made the fixed obligation `UNSAT` in about two seconds. At that point q77's
  candidate remained diagnostically unresolved: a 180-second inspector run
  reached the 185-second deadline, witness day 10,472 was in range, and
  narrowed diagnostics were `UNKNOWN`.
- Exact direct String/Utf8-literal `SafeCast` to `Optional<Date>` is now folded
  through the runtime parser in generic and executed OLAP-filter expressions.
  Focused exporter tests passed 4/4, the complete `cpp_ut` run passed 144/144
  at that Date-cast milestone, and the q5-shaped actual-host integration passed
  1/1 with `VERIFIED_BOUNDED`. Regeneration produces the current q5/q77 `UNKNOWN`
  results above instead of rediscovering either candidate. The exact cast
  refutes q5's saved witness. q77's corrected fixed-witness diagnostic also
  remained `UNKNOWN`, so its old witness is historical and unconfirmed rather
  than proved false. Neither query enters the proof floor or provides evidence
  of an optimizer bug; replay remains mandatory if q77 is reproduced by the
  corrected model.
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
- Exact `Exists`, scalar `If`, and unary `IfPresent` are compositional scalar
  nodes. `IfPresent` uses lexically scoped de Bruijn bindings, and the one
  optimizer-generated identity-key/Void-payload `ToDict` membership shape is
  normalized to explicit `in`. Type, nullability, scoping, lambda, dictionary,
  and safety gates fail closed. TPCH q19 passes both real-host boundaries,
  emits a formula, and is `VERIFIED_BOUNDED`. At that milestone TPC-DS q68
  passed export and reached the 32,640-pair Merge construction gate; the later
  exact representation selector moves q68 through formula construction.
- Decimal literals are tagged as finite, negative infinity, positive infinity,
  or NaN; source and opaque values use the exact legal typed domain. Ordinary
  equality/order, exact-type null-safe equality, Decimal/Decimal and
  Decimal/integer `DataCompare` alignment, precision-cap saturation, and
  complete non-null integer constant casts are modeled. Exact `cast_decimal`
  additionally covers non-null integral `SafeCast` expressions for every
  signed and unsigned 8/16/32/64-bit width when the non-null canonical Decimal
  target/result agree and retain an integral digit; runtime scale multiplication
  and signed-infinity saturation are explicit. Canonical same-type
  Decimal `+`/`-`, `DecimalMul`, and `DecimalDiv` with a same-type Decimal or
  integer right operand have exact `NDecimal` special, scale, rounding, and
  overflow semantics, including the current negative-divisor asymmetry. Sort,
  TopSort, and Merge use the distinct raw-code total order,
  including ordered NaN and exact Decimal key identity. Decimal `sum` widens to
  `Decimal(35,s)` and is exact whenever its carried finite bound proves that
  saturating partial addition cannot overflow; unsafe bounds fail closed.
  Same-type Decimal `max` ignores NULL and reduces the raw signed codes in the
  runtime's total order, `-Inf < finite < +Inf < NaN`, with the same scalar
  state in logical, intermediate, and final phases. Same-type Decimal `avg`
  uses the explicit weighted `(sum,count)` phase contract above. Casts outside
  those gates, generic division, non-core `IN`, and aggregate functions outside
  this subset remain unsupported. Exhaustive cast, rational, ordering, and
  aggregate references, adversarial arithmetic and accumulator-overflow cases,
  signature and mutation tests, and green real-host Decimal filter,
  integral-cast, arithmetic, ordered, and aggregate obligations cover this
  boundary. TPC-DS q90 exercises two
  `Uint64` count expressions cast to `Decimal(15,4)` and is
  `VERIFIED_BOUNDED` at the standard two-row/two-task bound.
- Finite Decimal literals, specials, typed NULL, and complete non-null integral
  casts now seed conservative finite-coefficient bounds. Exact same-type
  `+`/`-` and `If`/`IfPresent` propagate them without treating special values as
  finite numbers; unknown operands remain unknown. Scalar and relation tests
  cover signed/unsigned 8/64-bit domains, saturation, typed-null wrappers, and
  two-row `SUM` consumption. At that milestone a focused TPC-DS q5 run cleared
  its former Decimal-SUM headroom rejection and reached the deeper 32,896-pair
  Merge construction cap after 1,720/42,044 ms; the later exact representation
  selector moves q5 through formula construction.
- Exact Decimal-only `max` has independent raw-code, NULL, grouped/global,
  split-state, wrong-shuffle, type, and phase-nullability tests. At that
  milestone focused TPC-DS q74 passed its former aggregate blocker and reached
  the 65,536-pair join-matching preflight after 463 ms of preparation and
  375 ms of verifier work. Current q74 reaches a deeper Sort construction cap;
  it remains unsupported and changed neither the formula slice nor the proof
  floor.
- TPC-DS q79 initially returned a symbolic counterexample with
  `d_year = 1998`. The initial plan compared its nullable `Int64` directly with
  `Int32` membership constants; the final plan used an opaque
  `SafeCast(Int64 -> Int32)` before the membership test, so the model could
  incorrectly choose NULL for the in-range value. Exact `cast_integral`
  semantics remove that witness. A focused direct-membership versus
  `Exists`/cast/`IfPresent` lowering is `VERIFIED_BOUNDED`; the full q79 solver
  run is `UNKNOWN` at 60000 ms. This was a verifier-modeling false positive,
  not a confirmed optimizer bug.
- TPC-DS q88 exposed why that concrete extension was needed: opaque source
  additions did not constrain optimizer-folded literals. Its regenerated
  obligation has no opaque scalar functions and no longer returns the spurious
  counterexample; Z3 currently returns `UNKNOWN` at the 60-second bound.
- Actual pushed column-store filters are decoded from `OlapFilterLambda`, not
  optimizer statistics metadata. The supported Boolean/comparison subset is
  evaluated before per-task pushed limits. Exact two-child
  `TKqpOlapFilterUnaryOp` tuples require an Atom operator tag: `exists(x)` maps
  to the scalar presence node and `empty(x)` to its negation. Unknown or
  non-Atom tags, malformed tuples, and unavailable physical columns fail
  closed; the separate Date gate admits `just` only around a direct valid
  non-null Date literal. `Coalesce(predicate, false)` is erased only at a
  positive filter position propagated through AND/OR; the same node beneath
  NOT, a comparison, or a unary presence operation fails closed. Exporter
  safety tests and real-host `IS NULL`/`IS NOT NULL` obligations cover these
  boundaries. This exact lowering moves TPC-DS q76 through formula
  construction.
- The exact new-RBO `TPCDS_YQL` q96 schema and query pass strict initial/final
  export and produce `VERIFIED_BOUNDED` at two rows per table and two tasks. This
  covers exact Date and typed Decimal catalog columns, canonical `Void` for
  `COUNT(*)`,
  four scans, three joins, split aggregation, TopSort/Merge/Limit, and
  Map/Broadcast/UnionAll StageGraph routing.
- Exact phase-aware Decimal `avg` records the hidden
  `(Decimal(35,s) sum, Uint64 count)` state in every AVG trait, validates one
  direct matching intermediate-to-final lineage, and combines weighted partial
  sums/counts before ties-to-even division and same-scale narrowing. NULLs,
  specials, finite headroom, and count-wrap guards follow the exact contract
  above. Exhaustive independent small-domain and malformed-lineage tests remain
  green; focused C++ exporter tests passed 3/3 and the full C++ exporter suite
  passed 147/147 at that milestone. TPCH q1 emits a formula after 111/998 ms;
  its non-gating 60-second solver experiment returns `UNKNOWN` after
  159/63,937 ms. TPC-DS q65 emits a formula after 687/30,318 ms. Neither result
  extends the proof floor or confirms an optimizer correctness bug.
- The real-host dashboard runs all 22 `TPCH_YQL` and 99 `TPCDS_YQL` sources,
  writes a structured timeout-aware report, and preserves diagnostic artifacts
  for every correctness, unknown, schema, or solver outcome.
- Its strict version-three input policy and independently versioned evaluation
  enforce three monotonic depths: TPCH q1 and TPC-DS q5, q65, and q80 must reach
  the verifier, the 41-query formula floor must keep constructing SMT, and the
  fifteen-query hermetic proof floor must remain `VERIFIED_BOUNDED`. A verifier-side
  `UNSUPPORTED` result satisfies only the first tier; later formulas and proofs
  satisfy every weaker tier without pinning brittle blocker text.
- Occurrence/routing compaction, scoped shared-term rendering, nonrecursive
  structural IDs, exact grouped-key classes, and the at-most-three-row
  enumeration/symbolic-ordinal selector remove the former factorial and
  repeated-structure construction gates. The latest complete suite
  measurements reran on 2026-07-23 before the explicit general scalar-error
  commits. They emit TPCH q1, q3, q5,
  q6, q10, q11, q12, q14, q15, and q19 (10/22) and TPC-DS q3, q5, q15, q19,
  q25, q29, q37, q40, q42, q43, q46, q48, q50, q52, q55, q61, q62, q65, q68,
  q71, q76, q77, q79, q80, q82, q88, q90, q91, q93, q96, and q99 (31/99),
  for 41/121 workload queries (33.9%). TPCH has nine unsupported and three
  optimizer-failure results; TPC-DS has 40 unsupported and 28
  optimizer-failure results, for 49 unsupported and 31 optimizer failures
  across both suites. Complete preparation/verification totals are
  2,754/6,017 ms for TPCH and 54,058/178,875 ms for TPC-DS, or
  56,812/184,892 ms together. Within TPC-DS, q9 is the sole category change
  from the preceding complete run: preparation now succeeds and exposes
  unsupported final Read range/ordering semantics, moving it from optimizer
  failure to unsupported without changing formula coverage. Formula emission
  confirms end-to-end model coverage at two rows per referenced table and two
  tasks; it is not a proof by itself. TPCH q7 and q8 now reach the deeper
  generic `Map` exporter blocker and remain unsupported.
- Construction preflights cap every materialized relation at 4096 candidate
  rows and each unshared quadratic construction or shared symmetric comparison
  triangle at 16384 candidate-row pairs. The remaining verifier-side
  construction blockers are q4's 20,736-pair join match, q64's 8,192-row join
  output, and TPC-DS q11/q31/q74's 8,386,560-pair Sort constructions. q1, q5,
  q25, q29, q46, q65, q68, q77, q80, and q91 now construct complete formulas
  instead of stopping at their historical aggregate, Sort, or Merge gates.
- A shared expanded-node/depth budget now caps every complete exact scalar tree
  at 1,024 normalized occurrences and depth 128. Independent C++ and Python
  checks cover exact 1,024/1,025-node and 128/129-depth boundaries, expanded DAG
  occurrences, per-projection resets, assembled OLAP filters, synthesized join
  predicates, decoder recursion, and the unchanged 512-item `IN` and
  64-live-`IfPresent` limits. Opaque fingerprints retain their independent
  256-node/64-depth/64-KiB budget.
- A checked-in hermetic solver floor returns `VERIFIED_BOUNDED` for TPCH q3,
  q6, q11, q12, q14, q15, and q19 plus TPC-DS q3, q42, q48, q52, q55, q90,
  q93, and q96
  with a fixed 60-second per-query budget. The latest TPCH run recorded
  116/3,168 ms of preparation/verification for q3, 61/775 ms for q6,
  158/6,585 ms for q11, 88/2,085 ms for q12, 91/34,457 ms for q14,
  199/2,750 ms for q15, and 118/872 ms for q19. The latest TPC-DS run recorded
  112/5,049 ms for q3, 101/4,726 ms for q42, 192/4,006 ms for q48,
  99/4,599 ms for q52, 116/4,194 ms for q55, 280/8,542 ms for q90,
  102/2,319 ms for q93, and 117/508 ms for q96.
  These are
  fifteen curated proofs (12.4% of the workload). q50 emits a formula but its
  solver experiment ended `SOLVER_ERROR` after the external process exceeded its
  65.0-second deadline; it is not part of the proof floor. TPC-DS q15, q61, q62,
  q76, q79, and q88 return `UNKNOWN` at the 60-second solver budget. q43 likewise
  returns `UNKNOWN` after 147/69,391 ms. q61's
  1,572,871-byte formula recorded 955 ms of preparation and 63,897 ms of
  verification. Focused q76 formula construction recorded 391 ms of
  preparation and 14,169 ms of verification; its solver experiment recorded 419
  ms and 88,305 ms before `UNKNOWN`. q71's 118,276,852-byte formula recorded
  83,339 ms in the verifier/formula-emission phase of the complete run before a
  focused solver
  attempt reached the external process deadline. q76 is formula-covered but is
  not one of the fifteen proofs. The Date additions q37 and q82 return `UNKNOWN`
  at the 60-second solver budget after 63,782 and 63,078 ms of verifier work; their
  retained formulas are 4,201,832 and 2,841,844 bytes. A separate non-gating
  q40 scaling experiment used a 10-second solver budget, prepared in 178 ms,
  retained a 97,319,076-byte formula, and spent 104,804 ms in verifier
  processing before reporting `SOLVER_ERROR` because the external solver
  exceeded its 15.0-second process deadline. That focused `ya` experiment fails
  on `SOLVER_ERROR` as designed; q40 is formula-covered but neither proved nor
  a counterexample. No optimizer correctness bug is confirmed by these runs.
- [BENCHMARK_COVERAGE.md](BENCHMARK_COVERAGE.md) records the exact setup,
  commands, complete formula-only baseline, proof-floor evidence, q6/q14 and
  q79/q88 investigations, and explicit unsupported/optimizer-failure inventory.

The subplan inventory of the then-catalog-blocked slice contains 32 source
subqueries across seven TPCH and thirteen TPC-DS queries: fifteen scalar
expressions and seventeen `EXISTS` predicates. Twenty-five are correlated, only
seven are uncorrelated, and none is a dynamic `IN` subplan. Only TPCH q11/q15
and TPC-DS q24/q54 are fully uncorrelated; q6 and q22 mix uncorrelated and
correlated forms.

The catalog prerequisite follows every ordered subplan root with one
deduplicating traversal, validates the `OrderedList`/`PlanMap` registry, and
captures tables referenced only by a subplan. The semantic snapshot now adds an
ordered scalar-subplan descriptor for each used binding: stable binding name,
kind, root node, selected output, exact type/nullability, dependency list, and
explicit consumer-node list. Export fails closed on an unregistered binding,
duplicate or colliding binding, bad topology, consumer mismatch, nested or
staged subplan, correlation, unsupported kind, or a binding whose physical
placement cannot be represented.

Exact ordered logical UnionAll is also implemented as a prerequisite for scalar
lowering. Each unordered input denotes every legal local sequence; the operator
then concatenates the complete left sequence before the right. Symbolic
ordinals use input-specific choice scopes and compressed branch offsets, so an
ordered UnionAll followed by `Limit 1` selects a real scalar row before the NULL
fallback without correlating independent input orderings.

The first auditable initial-boundary milestone accepted uncorrelated
scalar bindings with no dependencies, nullable result type, explicit
Project/Filter consumers, and a root statically known to produce at most one
row. The static proof admitted `EmptySource`, an eligible ungrouped aggregate, a
literal `Limit <= 1`, and Project/Filter/Sort wrappers over an admitted child.
It rejected Join, UnionAll, grouped/intermediate/`DistinctAll` aggregation, and
every shape not covered by that small structural proof. Root plans may carry
auxiliary columns, but only the declared result output becomes the scalar
binding.

That historical slice moved TPCH q11 and q15 through formula construction and
into the proof floor. Final scalar lowering retained physical
`EnsureAtMostOne` Limits, and the then-current staged export proved those
markers inert. q11 and q15 each crossed a serial UnionAll from a one-task
aggregate producer; their recorded complete formula rows took 176/558 and
152/462 ms, and the post-hardening proof floor returned `VERIFIED_BOUNDED` in
158/6,585 and 199/2,750 ms. Those measurements predate general scalar-error
modeling and are not silently reclassified.

Commit `b2cd6e3c5bb` introduces the explicit outcome algebra, and
`f930f1352e7` introduces general uncorrelated scalar subplans. Each binding
retains the source family's relational decisions and choices: zero present
rows produce typed NULL, one produces the selected value, and more than one
produces a new cardinality-error term. Commit `1aaf281c07a` gives enumerated
latent-sequence alternatives a stable scoped decision, so the same cached
scalar family cannot select different sequence permutations at different
consumers. Binding types remain lexically available only in declared
Project/Filter consumers and cannot leak into physical outputs.

The binding's newly generated more-than-one-row error is demanded only when
that binding's immediate Project/Filter consumer has at least one present input
row. Once such a row exists, the binding is evaluated even when it appears
under a dead scalar-expression branch. An error already inherited from
evaluating the subplan root is different: it remains observable and is not
gated again by an empty outer consumer. This distinction composes through
nested scalars. An inner binding demanded by its own nonempty consumer may
error; an enclosing binding inherits that error even when the enclosing
binding's top-level consumer is empty. An intrinsic error already raised while
evaluating the producer is eager in the same way.

Model-correction commit `125962c87df` keeps the inherited `Outcome.error` and
the binding-local `cardinality_error` as separate terms until it combines them
at that immediate consumer.

Commit `9e50d234264` correctly gates the cardinality observation generated for
one scalar binding with one outer row. CBO could then commute the
order-sensitive synthetic Cross: physical Cross drains its right input first,
so putting the empty outer input on the right allowed execution to finish
without draining the scalar side and its inherited error. Optimizer-fix commit
`cab0dd1e89c` marks both synthetic scalar Crosses `PreserveInputOrder`, makes
BuildInitial/Expand CBO treat the marked join as a barrier while still
optimizing its sides, and prevents filter absorption from rewriting that
barrier.

Every Limit now serializes its marker, including across one- and multi-task
producer stages, and the evaluator checks it exactly after Skip/Take in each
task. The focused `*AtMostOneMarker*` exporter matrix passes 3/3 for direct,
multi-task-producer, and single-task-producer serialization. After the model
and optimizer corrections, the full `cpp_ut` passes 165/165 and the affected
Python verifier/inspector/bisect/replay/confirmation gates pass 507/507.
TPC-DS q24 still reaches the independent blocker, `Unsupported scalar callable
Map`. The recorded q54 dashboard reasons belong to the preceding static-proof
snapshot; no fresh full-dashboard metric is claimed for the general
implementation.

Exact relational `EXISTS`, restricted dynamic `IN`, and correlated forms remain
separate later slices. Every slice must preserve output type/nullability, empty
behavior, correlation metadata, and every final StageGraph consumer.
Solver/formula-size work follows coverage and promotes a query only after a
reproducible `VERIFIED_BOUNDED` run.

The milestone audit has independently found seven production optimizer defects.
First, an unrelated earlier `NOT` left stale state while the simple-subplan rule
searched later conjuncts, so a positive `EXISTS` could be lowered as
`NOT EXISTS`; the focused regression and per-conjunct reset are committed in
`95a2afad1d3`. Second, new RBO selected the first row of a multirow scalar
subquery instead of raising the required error; the `EnsureAtMostOne`
enforcement and real-YDB regressions are committed in `e1e3419012c`. Third,
`TOpMap::GetSubplanIUs()` called `AddUnique` with its source and destination
reversed. Fourth, direct projection lambdas skipped `RemoveSubplans`, leaving
the inner query plan embedded in a scalar Map expression instead of replacing
it with the registered binding. Fifth, the empty branch of a direct
nonaggregate YQL scalar projection attempted `Nothing<Int64>` instead of
producing `Nothing<Optional<Int64>>`, so a valid zero-row scalar subquery failed
type annotation rather than returning NULL. Commit `52a1d7c4084` fixes the
last three together and retains direct aggregate, plain singleton, computed,
zero-row, and multirow real-YDB regressions.

Sixth, a multirow uncorrelated scalar raised `PRECONDITION_FAILED` under new
RBO even when the outer consumer produced no rows, while legacy execution
returned the required empty result. The direct `EnsureAtMostOne` check was
materialized eagerly in an independent scalar producer instead of being
conditioned on consumer demand. Commit `9e50d234264` bounds the scalar side,
gates the generated check with one outer row, preserves colliding outer/scalar
IUs through an explicit rename, and retains both the empty-consumer and
same-name real-host regressions. Its symbolic regression proves the gated
lowering for demanded and empty consumers and distinguishes both a missing
check and the former eager check. At that production-fix checkpoint,
`ScalarSubplanEvaluationTest` passed 14/14, and
`KqpRboYql::ExpressionSubquery` passed 1/1 with the new empty-consumer and
same-IU cases plus the existing scalar-cardinality cases. The prerequisite
shared-input repair is separate in `a51c2459ad5`; its two direct rule tests pass
2/2 and prevent Limit pushdown into a shared Read or Sort. As noted above, the
scalar patch addresses the directly generated cardinality error, while
inherited producer errors require the separate treatment below.

Seventh, a reliable warmed paired real-host probe of
`nested_empty_outer.sql` found both a verifier-model bug and a production
divergence. The inner scalar has a nonempty immediate consumer and more than one
row, while the top-level consumer is empty. Legacy execution raises
`PRECONDITION_FAILED` with “More than one row in a scalar subquery”; two warmed
default-CBO new-RBO runs instead deterministically exited successfully with an empty
result JSON beginning `{"columns":[{"name":"value",...}]}`. Commit
`125962c87df` corrects the model's inherited/local error split, and
`1aaf281c07a` preserves shared enumerated-sequence choices. The corrected model
exposes the production mismatch.

The production root cause was CBO commuting an order-sensitive synthetic scalar
Cross. Because physical Cross drains the right input first, the commuted empty
outer input could finish the join before the inherited scalar error was
evaluated. Commit `cab0dd1e89c` fixes the defect with the
`PreserveInputOrder` barriers described above. Its two direct order-sensitive
join rule tests pass 2/2, `KqpRboYql::ExpressionSubquery` passes 1/1 including
the CBO2 nested regression, the full `cpp_ut` passes 165/165, and the affected
Python gates pass 507/507. Defect seven is fixed.

An additional legacy probe with an intrinsic
`Ensure(foo.id, false, "inner scalar error")` inside the scalar producer raises
`PRECONDITION_FAILED` despite an empty top-level consumer, confirming that the
eager inherited-error contract is not specific to nested cardinality checks.

The focused
`test_inherited_scalar_error_is_observed_without_a_consumer_input_row`
regression locks the corrected boundary.

### M5: confirmation and localization — implemented for replayable single-result witnesses

- Separate normalized-plan and exact concrete-counterexample inspector.
- Separate real-YDB replay tool for deterministic, range-valid inspector
  witnesses, with strict dual-target mode preflight and typed BulkUpsert setup;
  legal Decimal specials are rendered as `-inf`, `inf`, and `nan`; multi-result
  TPC-DS q14, q23, and q39 remain an explicit replay extension.
- Version-four benchmark reports preserve the exact assembled query, both
  snapshots, and byte-exact raw verifier verdict with SHA-256 bindings. The raw
  verdict artifact is authoritative for the witness; the report's parsed
  verdict contains metadata only and omits the witness to prevent loss of wide
  Decimal integers during JSON re-encoding. The separate confirmation driver
  processes every `COUNTEREXAMPLE` deterministically, pins inspection to the
  database decoded directly from that raw artifact, invokes real-YDB replay
  with explicit isolated targets, and retains every input, child command,
  stream, classification, and digest. A missing or changed witness,
  nondeterminism, setup failure, multi-result query, or protocol error is
  `UNRESOLVED`; symbolic candidates are never promoted beyond symbolic evidence
  without a successful replay divergence, and exact StageGraph attribution
  remains a separate localization step.
- Explicit diagnostic transformation-prefix verifier boundary, committed-rule
  and atomic-stage snapshot hooks, strict real-host capture command, and
  separate sequential localization driver are implemented.
- Formula construction and the fifteen curated workload proofs have separate
  checked-in regression floors. Every future solver witness has a mandatory,
  automatic all-candidates confirmation command; the external target mutation
  remains outside recursive tests and the verifier kernel.
- A separate manual real-YDB Decimal `SUM` diagnostic checks one- versus
  two-partition execution of identical rows in both optimizer modes. It
  currently confirms the shared `M` versus `inf` mismatch and is intentionally
  excluded from normal recursive tests until the runtime aggregate state is
  fixed.

## Non-goals

- Proving CBO optimality.
- Checking after every rule in normal runs.
- Treating bounded verification as a general SQL-equivalence theorem.
- Proving `ConvertToPhysical`, task construction, or execution-engine correctness;
  those require a later boundary check and real-YDB replay.
- Growing the verifier into a second optimizer or expression simplifier.
