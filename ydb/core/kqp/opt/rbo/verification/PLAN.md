# New RBO bounded equivalence verifier

## Objective

Build a focused, auditable correctness checker for the new RBO. For a bounded
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

Unordered Limit produces an exact finite language of enabled bags. Its
nontrivial `Take(1)` case uses at most one bounded symbolic row selector and one
conditional row per source outcome; other successful cardinalities use exact
bag masks. Small ordered choices may also stay as explicit sequence families,
while larger Sort, Merge, and latent-sequence choices use bounded symbolic row
ordinals. Equality is mutual inclusion of the two result languages: one side
supplies a candidate result and the other side's bounded choices are quantified
when testing membership. Shared-DAG choices remain correlated, while distinct
stage-task executions are independent. Each choice has a declared finite
domain. Symbolic ordinals are allocated only for syntactically live candidate
slots, so fixed-false padding consumes neither choices nor pair budget. These
representations remove factorial construction without broadening the bounded
verification claim.

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

Successful whole-query preparation is not a precondition for auditing a pair
that was already captured at these boundaries. Initial and Final boundary
results own their versioned JSON or unsupported diagnostic independently; a
later `SyncPrepareDataQuery` failure is recorded on a separate preparation
axis and does not discard them. Reports therefore partition preparation and
semantic verification separately. The proof obligation ends before
`ConvertToPhysical`; later physical generation remains outside this contract.

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

The trusted Python code is deliberately split into explicit semantic modules:

```text
rbo_verifier/ir.py          strict, versioned snapshot decoding
rbo_verifier/types.py       supported type identities, domains, and compatibility
rbo_verifier/smt.py         typed SMT terms and deterministic SMT-LIB output
rbo_verifier/string_order.py exact finite String/Utf8 byte-order quotient
rbo_verifier/decimal.py     exact Decimal values, comparison, arithmetic, and ordering
rbo_verifier/scalar.py      nullable values, SQL Bool3, scalar UFs
rbo_verifier/sort_network.py audited bitonic compare-exchange topology
rbo_verifier/relation.py    bounded bag/sequence operator semantics
rbo_verifier/stages.py      two-task StageGraph and connection semantics
rbo_verifier/verify.py      one counterexample formula and verdict decoding
```

The current proof-producing boundary, external assumptions, audited physical
size, and vertical-slice review procedure are maintained in
[TRUSTED_CORE.md](TRUSTED_CORE.md). The subsystem is audited by semantic slice;
the compact top-level obligation builder is not used as a proxy for total
trusted-code size.

The kernel has no YDB client, optimizer tracing, benchmark discovery, or
transformation-prefix localization logic. The kernel emits inspectable SMT-LIB and invokes an
explicit Z3-compatible solver executable; it does not import ambient Python
packages. Hermetic tests resolve the separately built, pinned Z3 executable;
the solver is not linked into `ydbd`.

SMT terms form an immutable DAG. Each node caches its complete structural hash
once from the already-constructed child hashes; equality remains exact
structural equality, so collisions are resolved by equality and the cache
changes lookup cost only. Equality uses an iterative pair worklist with
identity-pair sharing, preserving ordered structure and exact runtime classes
without Python recursion depth becoming a verifier limit. The renderer gives
each quantifier body its own scope
and emits repeated compound terms once through hygienic, dependency-ordered SMT
`let` bindings. It never lifts a term across a quantifier that binds one of its
symbols. This preserves the direct mathematical obligation while avoiding
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
- exact nullable
  `Date -> Timestamp -> DateTime2.Split -> DateTime2.GetYear` projection:
  one direct visible `Optional<Date>` member, a complete cast, and the reviewed
  unary UDF chain normalize to an explicit NULL lift around the shared
  `yql-datetime-year-v1` typed opaque function;
- exact nullable
  `Optional<String> -> SafeCast(Optional<Utf8>) -> Unicode.ToUpper`
  projection for one direct visible member: the complete reviewed Map/lambda/UDF
  envelope normalizes to an explicit NULL lift around the shared nullable
  `yql-string-to-utf8-unicode-upper-v1` opaque function;
- exact constant Optional-Date `+`/`-` folding for a direct String/Utf8-literal
  `SafeCast` and the strict normalized `DateTime2.IntervalFromDays` UDF shape;
- restricted static `IN`: a direct raw tuple or `AsList` containing 1..512
  recursively supported expressions of one item type; items are non-null
  except that a raw tuple may contain an exact direct String/Utf8-literal
  `SafeCast` to `Optional<Date>` whose runtime parse proves it present and
  folds it to a non-null Date literal; the item type is identical to the
  lookup or uses a deliberately separate lossless common-integer gate, and
  membership is evaluated as the SQL three-valued OR of that equality;
- exact `Exists`, scalar `If`, and unary `IfPresent`; optional payloads use
  lexically scoped de Bruijn bindings, and the optimizer's exact
  identity-key/Void-payload `(One, Auto)` static `ToDict` membership shape is
  normalized to the same explicit `in` node;
- filter truth conversion.

The static-`IN` result must be `Bool` and nullable exactly when its lookup is
nullable. `ansi`, `warnNoAnsi`, `isCompact`, and `nullsProcessed` are erased
only under that semantic gate. `tableSource`, dynamic, empty, oversized,
other nullable-item, nullable-`AsList`, heterogeneous-item, lossy or
non-integer mixed-type, malformed-option, unknown-option, and duplicate-option
forms fail closed.
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

Reviewed optimizer-generated wrapper normalizations reuse those existing exact
nodes.
`Coalesce(predicate, false)` lowers to `if_present` only when the first child
is either one direct nullable ordinary comparison, exactly binary
`Or(member == literal, member == literal)`/`And(member != literal, member != literal)`,
or the canonical String predicate described below.
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
matching `Optional<Decimal>`. Independently, `Just(member)` lowers to the same
explicit constant-true `if` shape only for one direct visible, exact non-null
`Uint64` input member and an exact `Optional<Uint64>` result.
An additional always-present literal gate evaluates a complete `Convert` from
one direct non-null integer literal to a non-null integer result as the exact
target-typed literal. `Just(Date literal)` and
`Just(complete integer-literal Convert)` then lower to the same
`if(true, value, typed-null)` shape when the Optional result type, target
descriptor, and nested annotations all agree. A direct integer-literal `Just`
without that reviewed conversion, an incomplete or dynamic conversion, and
mismatched wrapper/result types remain opaque or fail closed.
The unreachable typed-NULL branch preserves the Optional schema while the
constant-true condition preserves `Just` runtime presence. Incomplete,
mismatched, dynamic, nonzero, or broader safe near-matches remain opaque.
All wrapper gates retain
the full closed-world scalar safety validation and shared normalized-node,
source-depth, and live-binding limits.

The canonical String-predicate bridge maps generic
`EndsWith`/`StringContains` and executed OLAP
`ends_with`/`string_contains` to one stable typed opaque identity per
operation. Its exact gate is one direct `Optional<String>` member or catalog
column, one non-null `String` literal, and an `Optional<Bool>` result with
matching descriptor/nullability. Ordered operands remain explicit. This is a
shared deterministic-total uninterpreted function, not a reimplementation of
the byte predicate, so it can prove preservation of the same operation and
arguments across dialect lowering. Other types, arities, operand orders,
computed operands, and catalog/descriptor mismatches fail closed.

The reviewed compiled-LIKE bridge maps the complete generic
`Apply(AssumeStrict(Re2.Match), Optional<String>)` program and its pushed
`KqpOlapApply` spelling to one stable
`yql-re2-pattern-from-like-match-v1` opaque identity. The exporter validates
the exact cached callable/run-config/type descriptors for `Re2.Match`,
`Re2.PatternFromLike`, and `Re2.Options`, a case-sensitive ASCII pattern of at
most 4,096 bytes, canonical options, and a direct nullable String member or
read column. Source NULL lowers to false; a present source invokes the shared
non-null Boolean opaque function under `if_present`. The pushed form also
admits one exact outer `Not`. Other regex programs, options, patterns, columns,
annotations, lambda shapes, or safety metadata fail closed.

The restricted floating-predicate bridge does not add `Double` values or
floating arithmetic to the snapshot IR. It accepts only an
`Optional<Int64>` left expression under one `Optional<Bool>` ordering
comparison paired with the exact reviewed non-null `Double` constant/operator
combinations `>= 2/3`, `<= 3/2`, `> 1.2`, and `< 0.9`. The two fractional
constants may use their reviewed source divisions or the exact folded literal
spelling; all four constants receive stable fingerprints containing their
exact IEEE-754 binary64 bits. The comparison may additionally appear under the
existing exact `Coalesce(..., false)` envelope. The exporter audits the full
left subtree and keeps the complete comparison as one typed opaque Boolean
function over its ordered visible-IU arguments. Every other floating constant,
operator pairing, arithmetic use, result shape, or wrapper outside the
separate passive-carrier slice fails closed.

The passive-Double bridge is a distinct, narrower data-carrier contract, not
floating arithmetic in the scalar evaluator. It accepts exactly the four q83
result expressions: three deviation roots
`((member / (member + member + member)) / Double("3.0")) * Int32("100")`
and one average root
`(member + member + member) / Double("3.0")`. The sum is left-associated,
all three members are distinct direct `Optional<Int64>` columns, the deviation
numerator is one of them, every floating result is `Optional<Double>`, and the
two constants have exact type, value, flags, and safety metadata.

The whole expression lowers to
`opaque_double<fingerprint>(three ordered IU values)` with the
`yql-passive-double-v1` prefix. Python independently requires that prefix,
three distinct direct `Optional<Int64>` arguments, and an `Optional<Double>`
result. Its NULL lane and payload are conservative deterministic
uninterpreted functions, represented by Boolean and integer SMT terms with no
floating domain. `Double` is rejected in table metadata, subplans, scalar
consumers, comparisons, join keys, aggregate keys/inputs/results, sort keys,
and routing keys. It can flow through relational operators and StageGraph only
by direct, uninspected column pass-through; q83's observed downstream path is
Project, non-key Sort, Limit, and Merge. This admits unchanged expression
identity across the optimizer while remaining incomplete for algebraic
rewrites or every broader Double use.

Every other deterministic, total scalar subtree is represented as a typed
uninterpreted function:

```text
opaque<canonical AST shape, literals, types, settings>(ordered IU values)
```

The function identity is not merely its input-column list. IU names are
alpha-normalized through lineage, while callable shape, constants, argument
positions, repeated arguments, and types remain part of the identity. The same
fingerprint is shared between both plans.

Milestone 75 adds one deliberately partial member of that fingerprinted
family. The existing restricted stored-String `Concat` audit still proves the
same closed grammar, storage provenance, exact literal bytes, and worst-case
result length. A tree whose maximum fits `UINT32_MAX` remains an ordinary total
`opaque` expression. An otherwise identical audited tree whose maximum exceeds
that runtime result bound is instead serialized as non-null String
`checked_concat`. Its successful value uses the ordinary shared opaque function;
a separate shared Boolean function represents whether that exact fingerprint
and ordered nullable argument tuple raises the observable Concat error. Both
functions are arbitrary, so the model includes the concrete deterministic
runtime interpretation and may add spurious `SAT` or `UNKNOWN` cases, but
cannot make an inequivalent supported pair prove `UNSAT`.

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

Integer `+`, `-`, and `*` arithmetic is deliberately narrow: both operands and
the result must have exactly the same integer identity, and result nullability
must be the OR of operand nullability. Mixed-width arithmetic remains opaque
instead of asking the verifier to reproduce YQL's promotion rules. Integer
literals, source cells, and non-null opaque results are constrained to their
exact signed or unsigned width, so a model cannot manufacture an out-of-range
arithmetic witness.

Integral `/` has a separate exact partial-arithmetic gate. Both operands and
the result must have exactly one fixed-width signed or unsigned integer
identity, and the result must be Optional even when both operands are
non-optional. Operand NULL, a zero divisor, and signed `MIN / -1` overflow
produce NULL. Every other quotient truncates toward zero: the model divides
nonnegative magnitudes and restores the sign instead of using SMT integer
division's negative rounding. Mixed-type, mixed-width, non-Optional-result,
and floating-point forms fail closed as standalone expressions. The restricted
whole-predicate bridge above may audit such syntax only inside its one opaque
Boolean identity, and the passive-carrier bridge may audit only its two exact
q83 roots inside `opaque_double`; neither exposes floating division to the
scalar evaluator.

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
the String input is an external UF argument. The restricted whole
floating-predicate bridge above is a separate pointer-scoped positive audit:
only its exact comparison root and constant bypass ordinary `Double` type
admission. Unknown callables, UDF/PG calls, floating-point or mixed-type
division outside that whole-predicate bridge, strict casts, `Unwrap`, free
variables, position-aware or unordered nodes, and side-effecting/CSE-unsafe
nodes fail closed. `DecimalDiv` and exact same-type fixed-width integral `/`
are the explicitly audited total division callables in the scalar core.
Expanding this list requires an explicit totality review and tests.

One cast shape is normalized before opaque fallback: when YQL cast analysis
reports a complete conversion from a non-null integer literal to a non-null
Decimal, the exporter evaluates it and emits the resulting Decimal literal.
An explicit `cast_decimal` node separately models weak `SafeCast` for two exact
source families. The first is a signed or unsigned 8/16/32/64-bit integer
expression. A source NULL produces target NULL; otherwise the integer
coefficient is scaled by `10^s`, with strict `Decimal(p,s)` bounds and
signed-infinity saturation. In particular, present integral overflow produces
`-Inf` or `+Inf`, not NULL. The second family is canonical Decimal with the
same scale and no greater precision than the result. That widening preserves
the encoded value exactly, including finite values, both infinities, and NaN,
and propagates NULL. Milestone 78 adds one disjoint exact source/target pair:
`Decimal(35,2) -> Decimal(15,4)`. It multiplies a finite raw coefficient by
100, saturates to signed infinity when the source magnitude reaches `10^13`,
preserves both infinities and NaN, and propagates source NULL.

For both families, source and result nullability must match, the canonical
target descriptor and every target annotation must agree with the result, and
the target must retain at least one integral digit. The exporter serializes the
actual `source_type`; the Python decoder independently requires it to equal the
argument type before selecting integral-cast or Decimal-widening semantics.
That field is a required cross-language audit seam rather than redundant
metadata. Complete integer literals remain normalized literals; other admitted
expressions remain explicit casts. Missing or mismatched `source_type`,
`Convert`, `StrictCast`, nullability mismatch, every other Decimal narrowing or
scale change, non-integral/non-Decimal sources, and zero-integral-digit targets
fail closed outside the existing complete-literal normalization.

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
nonliteral string-generating root in both plans. Derived `if`, `IfPresent`, and
row-selector terms are pure selections of registered roots or literals and do
not generate another value, so they are not charged separately. A generating
root independent of bounded plan choices contributes one to `M`; a dependent
root contributes the product of their registered positive bounds. Summing
those capacities after exact structural compaction bounds how many distinct
values the formula can observe across legal choice valuations. Given that `M`, the
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
literal ranks, universally range-guards each choice-dependent term bound over
the choices it actually uses, and exposes the complete rank-to-representative
map to witness decoding; later value registration and out-of-universe ranks
fail closed. Construction is preflight-capped at 65,536 representatives,
64 MiB of total encoded representative bytes, and 1,000,000 bytes per value.
The per-value cap is shared with inspection and replay.

Opaque integer, Date, and Decimal result domains use the same universally
range-guarded invariant mechanism. The shared uninterpreted functions remain
global, preserving determinism and congruence across choice valuations and
plans. Raw top-level global assertions must still be choice-independent.
Family comparison audits that every observable registered-choice dependency is
carried by its outcome, adds each carried range to effective enablement, and
requires disjoint left/right choice symbols before quantification.

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

The separate dynamic Date-shift gate accepts only exact binary `+` or `-` with
an `Optional<Date>` result, a direct visible `Optional<Date>` input member on
the left, and a literal `IntervalFromDays` on the right. The Initial boundary
must apply the reviewed eight-child UDF envelope above to an `Int32` literal. The
Final boundary may instead use exact `Just(Interval literal)`, but only when
the literal is a whole-day multiple and decodes to the same bounded
`[-49672, 49672]` day domain. Other wrappers, dynamic intervals, fractional
days, commuted operands, and Date variants fail closed.

The normalized IR is
`if_present(column(Date), opaque(bound Date), NULL<Date>)`. The outer
`if_present` models source NULL exactly. The present branch is a versioned
nullable-Date opaque operation keyed by both `+`/`-` and the decoded day
literal; it conservatively represents the full result as any in-domain Date or
NULL, including Date overflow. The same
fingerprint and bound argument share one deterministic operation across both
plans. Extra opaque outcomes can prevent a proof but cannot create a false
`UNSAT` equivalence result. No Interval type or new Python evaluator operation
is added.

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
snapshot's explicit `nulls_first` field. Floating-point, mixed-type, and other
division forms outside the restricted whole-predicate bridge, casts outside the
exact weak-`SafeCast` and constant-normalization gates,
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
same-type `+`/`-` use a precision-capped triangle bound; exact integral casts
use the complete source-type domain, target scale, and saturation point; and
same-scale widening preserves the input bound or conservatively derives it
from the source precision. `If`/`IfPresent` select the maximum known
alternative, while any unknown operand remains unknown. Focused scalar tests
cover finite, NULL, special, additive, conditional, signed/unsigned 8/64-bit
cast, and Decimal-widening cases. Relation tests consume literal arithmetic
and integral-cast bounds through a two-row Decimal `SUM`, check special/NULL
semantics, and retain the strict `10^35` rejection.

Same-type Decimal `min` and `max` retain the input type and use MiniKQL's raw
signed-code order, `-Inf < finite < +Inf < NaN`, rather than ordinary
`DataCompare`. They ignore NULL and respectively select the least or greatest
non-NULL value; an emitted group with no non-NULL value, including scalar empty
input, is NULL, while a lone NaN remains NaN. Undefined, intermediate, and
final phases carry the same scalar state, so split-task combination is exact.
Non-Decimal extrema, mismatched types, and phase/nullability mismatches fail
closed. Distinct and unwrap traits fail closed except for the two exact
contracts below.

Direct `count(distinct x)` is admitted on scalar or grouped,
phase-`undefined`, non-`DistinctAll` Aggregate when `x` is one exact non-null
fixed-width signed or unsigned integer, or one canonical nullable Decimal;
the result is exact non-null `Uint64`, `unwrap` is false, and the Aggregate
contains at most one direct distinct trait. For each emitted group and present
input row, the evaluator counts a non-NULL value exactly when no earlier
present row in the same group has the same raw aggregate value code. The raw
equality is ordinary integer equality for fixed-width inputs and MiniKQL
Decimal code equality for Decimal, so Decimal NaN deduplicates with itself.
Nullable grouping keys retain null-safe group equality. Before building value
equalities it charges
`candidate_groups * N*(N-1)/2` against the 16,384-pair ceiling in every
relation representation. Other input types, phases, multiple direct-distinct
traits, and distinct/unwrap combinations fail closed.

Physical scalar aggregate unwrap is admitted only for one keyless final,
non-`DistinctAll`, non-distinct `sum(Optional<Uint64>)` whose raw snapshot
output is `Optional<Uint64>`. The physical builder's coalesce contract makes
the effective result non-null: it is zero for empty or all-NULL input and the
ordinary wrapped `Uint64` sum otherwise. The decoder therefore retains the raw
snapshot annotation for validation but exposes a non-null result column to
downstream semantics. Every other unwrap shape fails closed.

Decimal `avg` is admitted only when its input and output are the identical
canonical `Decimal(p,s)`. The exporter records the physical state hidden by the
logical RBO IU type as
`{sum_type: "Decimal(35,s)", count_type: "Uint64", nullable:
<input-nullability>}`; non-AVG traits omit this field. The strict decoder
requires either one direct intermediate-to-final aggregate lineage with
identical ordered keys and state metadata or the closed staged carrier form
below. Each intermediate state IU must have exactly one matching final AVG
use, cannot be used as an ordinary scalar or key, and may cross StageGraph
routing only as payload.

The staged form is one keyless plain Final aggregate over a binary unordered
identity-`UnionAll` tree. Every leaf is one unordered Project directly over
one keyless one-trait Intermediate aggregate. Exactly one leaf has the direct
same-name matching AVG producer. At the C++ boundary every other leaf must
contain the exact physical
`Nothing(Optional<Tuple<Decimal(35,s),Uint64>>)` pad; the exporter certifies
that expression and normalizes only that pad to the existing logical
nullable-Decimal NULL. Generic tuple `Nothing` stays unsupported. The Python
decoder independently accepts only the corresponding normalized logical NULL
leaf and rechecks the logical carrier topology, ownership, and routing. Across
the two boundaries, the checks require unique consumers, trace the carrier
through every Aggregate, Project, Union, and StageGraph hop, and reject
aliases, project chains, direct aggregate leaves, ordered carriers, fanout,
extra producers, malformed descriptors, scalar or root exposure, and use as a
HashShuffle key or Merge order.

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
therefore diagnostic data, not an observable value. A cardinality-checked Limit
may consequently quotient alternative payloads in its greater-than-one error
region while retaining the exact zero- and one-row result language. Inspector
traces render the status explicitly. Version one distinguishes error from
success but does not compare error categories, codes, or text. Error-aware
real-YDB replay remains a separate extension and currently fails closed on such
a trace.

A `checked_concat` Project ORs its shared failure predicate into the outcome
error for every present input row. Its result payload is irrelevant on the
error path and its error composes with the same inherited, subplan, and
cardinality errors. This eager projection rule is admitted only through the
demand corridor described below; it is not generic eager evaluation for Map
expressions.

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
14. M4: occurrence-aware routing compaction and scalable bounded choices for
    unordered singleton Limit, Sort, Merge, and latent sequences;
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
27. M4: exact uncorrelated and one-equality-correlated relational `EXISTS`;
28. M4: trusted-core map and independent C++/Python auditability review;
29. M4: mechanical C++ subplan-exporter phase separation;
30. M4: typed C++ subplan-descriptor variant with explicit kind states;
31. M4: repeatable proof-depth sweep over the newly admitted formulas;
32. M4: exact canonical-first, per-outcome mismatch decomposition under one
    solver deadline;
33. M4: exact one-equality-correlated scalar aggregation with an explicit
    per-invocation outer binding;
34. M4: exact row-level `DistinctAll` aggregation, beginning with TPC-DS q6;
35. M4: canonical generic-to-OLAP `EndsWith`/`StringContains` bridge;
36. M4: exact same-type Decimal aggregate `min`;
37. M4: exact uncorrelated non-null integral dynamic `IN` and exact nullable
    `Date -> Timestamp -> DateTime2.Split -> DateTime2.GetYear` projection;
38. M4: side-explicit join keys for one-sided joins with shared IUs, exact
    direct `Just(Uint64 member)`, scalar final Uint64 SUM unwrap, and direct
    scalar `COUNT(DISTINCT Int64)`;
39. M4: exact same-type non-null String dynamic `IN`;
40. M4: exact proven-total Date `Unwrap`;
41. M4: exact independently nullable same-type fixed-width integral dynamic
    `IN` at a direct positive top-level Filter conjunct;
42. M4: exact nullable integral-to-Decimal weak `SafeCast` and same-scale,
    non-decreasing-precision Decimal weak `SafeCast`;
43. M4: exact pushed-OLAP physical/full/short output-IU resolution with
    referenced-ambiguity rejection;
44. M4: exact ordered two-dependency relational `EXISTS` with one strict direct
    equality and one strict direct inequality in separate conjuncts;
45. M4: exact uncorrelated same-type `Date` dynamic `IN`, with independent
    lookup/output nullability only at a direct positive top-level Filter
    conjunct;
46. M4: exact Map projection when one source IU is copied to multiple distinct
    output IUs;
47. M4: cached immutable structural hashes for SMT terms without changing
    structural equality;
48. M4: stack-safe iterative exact equality for deep SMT DAGs, including
    independently constructed equal terms and unequal hash collisions;
49. M4: exact same-type fixed-width integral division with Optional invalid
    arithmetic, q73 formula/proof coverage, and q78 verifier-entry coverage;
50. M4: bounded exact Sort/TopSort/Merge sorting networks with coherent
    whole-row transport, finite tie ranks, producer-order-preserving Merge,
    and TPCH q2 formula coverage;
51. M4: one-constructor packed-row sorting-network transport, one exact shared
    comparator definition per network outcome, exact present-prefix sequence
    equality, and TPC-DS q59/q78 formula coverage;
52. M4: exact uncorrelated scalar bindings consumed from expression-level
    dynamic-`IN` roots, with flat descriptors and independently checked
    main/subplan-root ownership;
53. M4: exact fixed or symbolic producer-order Merge sorting networks, moving
    TPC-DS q58 through formula construction;
54. M4: exact one-level closed dynamic-`IN` nesting, moving q83's initial
    snapshot to its later scalar boundary;
55. M4: proven-present String/Utf8-literal Date `SafeCast` items in raw
    static-`SqlIn` tuples, moving both q83 snapshots to the later `Double`
    boundary;
56. M4: restricted whole floating predicates over `Optional<Int64>` with
    exact IEEE-bit constant identities, moving TPC-DS q21/q34/q75 through
    formula construction and q34 into the proof floor;
57. M4: exact always-present direct Date-literal and complete integer-literal
    `Convert` wrappers, preserving Optional schema and removing q21's spurious
    candidate;
58. M4: exact passive `Optional<Double>` carriers for the four reviewed q83
    output expressions, with conservative UF identity, derived-only/passive-only
    dataflow gates, and q83 formula construction;
59. M4: exact literal-only non-null String `Concat` folding at a Map-body root,
    conservative finite-coefficient bounds for Decimal multiplication and
    division by an integral right operand, and q66 formula construction;
60. M4: audited point and finite point-set `RangeInfo::ComputeNode` lowering
    for q9/q45, moving q45 through formula construction and q9 to verifier
    entry;
61. M4: cardinality-certified integral `avg` for the strict
    `Optional<Int64> -> Optional<Double>` contract, with exact non-NULL
    cardinality at most two, a mandatory model-domain exclusion, and TPC-DS
    q7/q13/q26 formula construction;
62. M4: exact same-output-type fixed-width integral `min`/`max`, stack-safe
    SMT occurrence/level/term rendering, central producer-observe-before-strip
    integral-AVG certificate lifecycle, and TPC-DS q35 formula construction;
63. M4: narrowly tagged ordering for completed integral-`AVG`
    `Optional<Double>` outputs, with independently derived producer provenance,
    exact alias/pass-through propagation, explicit Sort/StageGraph Merge tags,
    and TPC-DS q22/q85 formula construction;
64. M4: exact dynamic `Optional<Date>` plus/minus literal `IntervalFromDays`
    normalization for TPC-DS q72, accepting the Initial `Apply` and Final
    folded whole-day `Just(Interval)` spellings, preserving source NULL, and
    moving q72 through both exporters to verifier entry;
65. M4: exact unique-key-aware at-most-one direct right-side join compaction
    for q72, without raising either global construction bound;
66. M4: exact fixed-sequence ordered singleton-`Limit` compaction for q9's
    `UnionAll(real-or-error, NULL fallback) -> Limit(1) -> Cross` scalar
    lowering, moving q9 through formula construction and into the bounded
    proof floor;
67. M4: the exact q24 nullable String-to-Utf8 `Map`/`Unicode.ToUpper`
    normalization, moving q24 through formula construction without adding
    Unicode or cast semantics to the Python verifier;
68. M4: one reviewed compiled-LIKE identity shared by generic expressions and
    pushed `KqpOlapApply`, grouped fixed-width integer `count(distinct)`, and
    exact value-preserving pushed Boolean `?? true/false` lowering, moving
    TPCH q13 and q16 through formula construction and bounded proof. No
    separate `starts_with` extension is needed for the observed q16 plan;
69. M4: exact branch-local scheduling of delayed direct unique-RHS equalities
    over one private logical left-deep Cross spine, retaining the original
    Filter and restoring its input column order, moving TPC-DS q64 through
    formula construction without raising either global construction bound;
70. M4: exact checked nullable-String `Unwrap` projection outcomes at the
    result root or a private keyed `left_semi` RHS, moving TPC-DS q8 through
    formula construction and into the bounded proof floor;
71. M4: exact topology-aware representation selection for mutually exclusive
    routed task copies: Broadcast always attempts eligible compaction before
    fan-out, HashShuffle adds a more-than-eight transported-cell trigger, and
    root/Union gathers retain the existing row heuristic. This moves TPC-DS
    q31 through formula construction without raising a cap; its focused solver
    result remains `UNKNOWN`;
72. M4: exact early equality for registered concrete String atoms,
    eliminating impossible sale-type Cross branches and moving TPC-DS q11/q74
    through formula construction. q4's different predicate-aware Cross
    factorization remains separate and is implemented by item 73;
73. M4: exact delayed-Cross factor-local static rejection, literal-false join-slot
    erasure, and certified innermost unique-seed rebasing, moving TPC-DS q4
    through formula construction without raising a global construction bound;
74. M4: exact nullable Decimal count-distinct with raw aggregate-code equality
    plus the independently certified staged Decimal AVG carrier, moving TPC-DS
    q28 through both exporters, formula construction, and bounded proof;
75. M4: a demand-aware partial stored-String `Concat` outcome: the existing
    restricted grammar and fingerprint supply a shared successful value and a
    separate shared failure predicate, while independent topology and
    row-bound gates restrict eager projection to q84's result corridor. q84
    now constructs a formula; its 60-second solver result is `UNKNOWN`, so it
    does not join the proof floor;
76. M4: exact unordered whole-partition Decimal-window semantics plus an
    explicit StageGraph boundary which colocates every accepted partition.
    TPC-DS q12, q20, and q98 now construct formulas despite failed physical
    preparation. A pre-routing-fix q12 obligation exposed a task-local window
    mismatch; the routing repair gives those windows a partition-key HashV2
    boundary, while q12's post-fix 60-second solver result is `UNKNOWN` rather
    than a proof. A strict metadata-transport repair restores q51's exact
    unsupported pair, and schema-v5 now pins 96 formulas plus effective exact
    pair floors of 20 TPCH/81 TPC-DS. Both complete formula gates are green,
    and fresh proof gates verify all 32/32 obligations. M4 remains current.
77. M4: exact unordered whole-partition nullable-Decimal `AVG` over one through
    four ordered nullable String/Int64 partition keys, plus the exact unary
    nullable-Decimal `Abs` needed by the q53/q63/q89 result expressions. Those
    three failed-preparation exact pairs now construct formulas. The formula
    floor is 99 and the supplemental pair-only set is reduced to q49/q51;
    neither the 32-obligation proof floor nor the 101-pair floor changes. Both
    complete dashboards and all 32 fresh proof obligations are green below. M4
    remains current.
78. M4: exact q49 global Decimal `Rank` semantics, including its one reviewed
    `Decimal(35,2) -> Decimal(15,4)` rescale, six independently unstable
    empty-partition Rank definitions, their closed Aggregate/ratio/Rank
    topology, and a mandatory serial gather before every global-Rank Project.
    Production commits `27e3f260017`, `97a03c64ab9`, and `68eb64102c7` retain
    hidden order dependencies, preserve immutable source metadata, and close an
    untracked-window alias-rewrite loop; verifier commit `0f12406f6c4` models
    the exact slice. Policy commit `e926958d96c` promotes q49, leaving q51 as
    the sole supplemental pair-only row. The authoritative floor is 100
    formulas, 101 exact pairs, and the unchanged 32 bounded proofs; all four
    complete dashboard/proof gates are green below. M4 remains current.
79. M4: exact TPC-DS q51 ordered contextual Decimal `ROWS` windows. Four
    distinct leaves are confined to three private Projects: web/store
    running SUM definitions use local order zero, followed by independent
    running MAX definitions with local orders zero and one. Each partitions by
    item, orders nullable Date ascending with NULLs first, and uses ROWS from
    unbounded preceding through the current row. The model preserves exact
    task-local NULL-ignoring Decimal aggregation, independent peer orders, and
    an item-only HashV2 boundary; Date remains live as a window-order input but
    is not a window shuffle key. Production range robustness commits `9faa9c19a82` and
    `877d65c8f12`, window transport commits `43af260c8a6`, `a749e7800be`, and
    `82fc8b25bd2`, exporter isolation commit `26f2210d0d7`, and verifier/export
    commits `beb329debc8` and `d2965d0a765` implement the slice. Policy commit
    `4609f334b0c` promotes q51, making the supplemental pair-only sets empty and
    pinning formulas for all 101/101 exact pairs. The proof floor remains 32;
    both dashboards and both proof gates are green below. M4 remains current.
80. M4: promote already-supported TPC-DS q97 into the checked bounded-proof
    floor. This is a policy-and-regression-test change only: no exporter,
    verifier, model, bound, formula, exact-pair, verifier-entry, or preparation
    floor changes. Focused q97 is `VERIFIED_BOUNDED` at the existing two-row,
    two-task, 60-second contract, and policy commit `ebb5c8806fc` adds it as
    TPC-DS obligation twenty. A clean full TPC-DS proof-floor rerun verifies
    all 20/20 obligations. Together with the unchanged 13-query TPCH floor,
    the M80 checked-in floor became 33/33. M4 remains current.
81. M4: record a focused bounded-solver checkpoint for already-supported
    TPC-DS q33. At unchanged code HEAD `3d1d99a953c`, the exact 2x2 obligation
    reaches the solver but returns `UNKNOWN` at the global deadline before
    branch 4/28. This is neither a proof nor a policy promotion; the floor
    remains 33. Further q33 work should reduce the exact semantic outcome
    structure, not merely tune the timeout. M4 remains current.
82. M4: promote already-supported TPC-DS q88 into the checked bounded-proof
    floor. Two focused runs on unchanged proof-producing code reproducibly return
    `VERIFIED_BOUNDED` at the unchanged two-row, two-task, 60-second contract.
    Policy commit `42a879e19f5` changes only the proof obligation and its
    regression fixtures; no exporter, verifier, model, bound, formula,
    exact-pair, verifier-entry, or preparation floor changes. Fresh complete
    gates verify all 13/13 TPCH and 21/21 TPC-DS obligations, raising the
    checked floor to 34/34. M4 remains current.
83. M4: promote already-supported TPC-DS q99 into the checked bounded-proof
    floor. Two focused runs on unchanged proof-producing code reproducibly
    return `VERIFIED_BOUNDED` at the unchanged two-row, two-task, 60-second
    contract. Policy commit `cc85514862d` changes only the proof obligation and
    its regression fixtures; no exporter, verifier, model, bound, formula,
    exact-pair, verifier-entry, or preparation floor changes. Fresh complete
    gates verify all 13/13 TPCH and 22/22 TPC-DS obligations, raising the
    checked floor to 35/35. Fresh q21/q56/q60 `UNKNOWN` results make a derived,
    exact unique-key ordering certificate the next bounded-proof model target;
    they do not justify an implicit tie-break or a larger timeout. M4 remains
    current.
84. M4: derive exact private ordering certificates for grouped results. Commit
    `476f2ea38f4` adds no snapshot field, IR node, exporter rule, policy
    obligation, or runtime assumption. `Relation` instead carries a validated
    nonempty null-safe unique key `K` and task-partition key `P`, both subsets
    of its visible schema. Grouped Aggregate and `DistinctAll` mint `K`;
    Filter and exact null-preserving aliases retain or rename certificates;
    computed, missing, or `error_on_null` key projections and row-combining
    operators drop them. Hash routing supplies `P`, and a multi-task gather
    promotes task-local `K` only when every task agrees and `P ⊆ K`.
    A Sort or Merge whose complete comparator contains `K` is total: the
    ordinary path uses exact predecessor counts, while eligible compact-prefix
    and large-Merge networks use concrete row-index tie ranks. Neither path
    adds decisions or bounded choices, and the incomplete-key behavior is
    unchanged. Independent semantic and packaging audits found no blocker;
    the registered Python package passes 760/760. The focused q21/q56/q60
    batch now has one outcome on each side and zero bounded order choices, but
    all three remain `UNKNOWN`: the deadline is reported before branch 4/4
    (`right_outcome_0_unmatched`) after earlier solver work. That label names
    an unattempted branch, not a localized cause. This is a substantial
    proof-shape reduction, not a proof-floor promotion: the floor stays 35/35,
    and q21's deterministic singleton-family comparison is the next exact
    target. M4 remains current.
85. M4: use an exact certificate-gated keyed mismatch cover and schedule that
    cover branch-first. Semantic commit `67655eaa786` changes only the trusted
    Python comparison and solver protocol. The preferred cover is admitted
    only for ordered singleton result families with no decisions or choices,
    position-compatible schemas, the same nonempty positional null-safe unique
    key, and the same positional order signature covering that key. It checks
    possible language absence, asymmetric errors, bidirectional per-row key
    absence, and non-key payload mismatch from the side with fewer live slots.
    Null-safe uniqueness makes that cover equivalent to the unchanged canonical
    mismatch; branch-count 64 and comparison-work 256 are inclusive ceilings,
    with ordinary canonical-first comparison as the fallback. The canonical
    SMT artifact, general exact decomposition, model-domain checks, and theorem
    are unchanged. Eligible queries skip the canonical probe and spend one
    monotonic global deadline on the preferred branches; every branch must be
    `UNSAT`, and model extraction reuses a winning `SAT` branch. The first
    branch `UNKNOWN` is retained instead of being overwritten by a later
    deadline message.

    Before the final scheduler-diagnostic regression, the packaged checkpoint
    passed 776/776: 754 Python tests, 21 lint checks, and one import check. The
    earlier keyed-cover slice passed 7/7. On the frozen tree, the lowercase
    `preferred` slice collected 755 Python tests, selected 11, deselected 744,
    and passed all 11; no post-regression full-package run is claimed. At exact
    semantic HEAD, q21 proves twice at 2x2 in
    216/47,563 and 199/47,320 ms of preparation/verification. q56 and q60
    remain `UNKNOWN` after 1,561/61,589 and 1,609/61,586 ms, both preserving
    branch 7/8
    `preferred_left_row_0_column_1_payload_mismatch` as the first unresolved
    branch. Policy commit `95182b541fb` promotes only q21; policy validation
    passes 16/16, and fresh gates prove 13/13 TPCH plus 23/23 TPC-DS. The
    checked floor is now 36/36. M86 begins with summary-state diagnosis of the
    q56/q60 payload branch; it promises neither a new reduction nor a policy
    promotion before that branch is understood. M4 remains current.

More than two dependencies, broader correlations, coercing and nullable-String
dynamic `IN`, broader range grammars, and other OLAP pushdowns remain.

The exact read-range slice is a closed exporter grammar, not a general
expression rewriter. It accepts only a column-store StageGraph source with
`SortDir::None`, one catalog non-null `Int64` physical primary key, and exactly
one emitted read mapping for that key. Runtime consumes
`RangeInfo::ComputeNode`, so that node is the semantic source;
`OriginalPredicate` is intentionally ignored. `KeyColumns` is checked as
descriptive evidence and must resolve to the same emitted physical-key IU.
q9's admitted point form is
`RangeFinalize(RangeMultiply(10000, RangeUnion(RangeFor(...))))`. q45's
admitted finite form normalizes one typed static tuple through
`Just`/`Map`/`AsList`/`Nth`, then checks the exact
`IfPresent`/`FlatMap`/`RangeMultiply`/`Collect`/`Take`/`IfStrict`/
full-range-fallback/`RangeUnion` extractor program. Literal types, binders,
indices, pointer identities, the 10,000 and 10,001 caps, descriptor
nullability, and `ExpectedMaxRanges` must all agree. Generated prephysical
nodes may lack annotations; any present annotation must agree with the exact
syntax. Duplicate or adjacent points remain exact membership, and a pushed
OLAP filter is conjoined with the lowered range predicate.

The 929-line matcher is isolated in `read_range_predicate_impl.h`, included
exactly once inside `semantic_snapshot.cpp`'s anonymous namespace. It reuses
the existing scalar-safety, catalog, output-IU, and JSON helpers and exports
only existing equality/static-`IN` IR. This keeps the vertical audit seam
separate without duplicating helpers, adding a second public exporter API, or
adding Python trusted semantics.

The completed cardinality-certified integral-`AVG` slice is deliberately not a
general floating-point model. C++ accepts only
`Optional<Int64> -> Optional<Double>` with one exact
`integral_double_v1` descriptor and directly linked undefined/intermediate and
final traits. Python independently checks the same phase, type, key, lineage,
and state contract. Undefined and intermediate aggregates summarize the
original non-NULL inputs as ghost `(count,min,max)` state; final aggregation
combines those summaries. One function shared across both snapshots maps
`(count,min,max)` to the result carrier. For count one or two, that tuple
uniquely identifies the unordered input multiset, so preservation of the tuple
proves preservation of the runtime result. Count zero remains the ordinary
NULL result.

Every successful completed aggregate carries a node-local
`IntegralAverageCertificate(count)`. A node observer consumes it immediately
at the aggregate producer, before a parent can project, sort, limit, compact,
route, or discard the output, and constructs the existential model-domain
exclusion “a present non-NULL result with count greater than two is reachable.”
The completed certificate is then dropped; only intermediate
`IntegralAverageState` is transportable hidden state. This keeps
`AverageMetadata` tagged and prevents a completed proof certificate from
becoming accidental relational payload.

Solver execution checks the model-domain exclusion first. A `SAT` exclusion
or an unresolved exclusion returns `UNKNOWN` and no semantic mismatch is
classified. Only exclusion `UNSAT` permits the semantic obligation. The shared
carrier still over-approximates binary64 equality between different certified
summaries, so semantic `SAT` also returns `UNKNOWN` and requires exact binary64
replay; semantic `UNSAT` alone produces `VERIFIED_BOUNDED`. The emitted raw
formula is `semantic mismatch OR model-domain exclusion`. Its standalone
`SAT` result can therefore be either an out-of-domain valuation or an abstract
carrier mismatch and is never, by itself, a counterexample.

The completed derived-ordering slice does not add general binary64 semantics.
C++ and Python independently derive a Boolean provenance fact only for a
completed integral-AVG `Optional<Double>` result. It propagates through exact
direct Map/Project aliases and pass-through Filter/Limit/Sort/AddDependencies,
through retained Join payloads, and through a positional `UnionAll` only when
every branch is certified. Sort and StageGraph Merge require the exact
`integral_avg_rank_v1` tag and matching provenance. Base, computed, passive,
intermediate-state, untagged, or forged `Double` ordering, and every `Double`
hash, group, join, or predicate use remain unsupported.

Ordering reuses the completed AVG's shared `(count,min,max) -> Int` carrier as
an abstract rank, so equality, prefix equality, Sort, and Merge observe one
coherent term. Once the mandatory model-domain check excludes count greater
than two, each concrete binary64 AVG equivalence class can be assigned an
integer rank. The unconstrained function also admits collisions, separations,
and reversed ranks that a concrete runtime cannot produce; these enlarge the
model and may cause `UNKNOWN`, but cannot create a false `UNSAT` proof.

The C++ exporter lowers an RBO map mechanically to an exact projection:
all expressions read the input row, rename sources are removed, untouched input
IUs pass through, and map targets are appended in operator order. Source
removal is set-valued: one existing source may be copied to multiple distinct,
nonempty targets, each receiving the same exact value, while missing sources
and duplicate or empty target names fail closed. Exporter tests cover that
normalization before it enters the trusted path. The projection also records
`TOpMap::Ordered`. Both values currently have the same sequence-preserving
runtime semantics because RBO lowers Map through its streaming WideMap builder;
the field remains explicit so that contract cannot change silently.

Join equi-keys are side-explicit snapshot descriptors
`{"left": left_iu, "right": right_iu}` rather than ordinary column expressions.
The residual `predicate` contains only `JoinFilters`; matching is the
conjunction of every left/right key equality and that residual. This preserves
operand identity when the two inputs contain the same IU name. Version-one
snapshots that predate `keys` decode it as the empty list.

Shared input IUs are admitted only for left/right semi and left/right anti
joins, whose result exposes exactly one side. At that boundary the exporter
requires no `JoinFilters`, and strict decoding independently requires a
literal-true residual; joins that output both sides still fail closed. Every
key must exist on its declared side and have equality-compatible types. Key
matching reads the two row maps separately before any output merge, so equal
names cannot collapse one operand. The existing 1,024-node/128-depth exact
scalar budget remains unchanged: each side-explicit key charges its equality
and two leaves, the effective key/residual conjunction charges one node, and
the residual begins one level deeper. Exhaustive left/right semi/anti and
StageGraph routing tests cover shared names, NULLs, duplicates, source-task
placement, and HashShuffle connection occurrences.

An exact compact representation is available for `inner` and `left` joins
whose right plan child is an unfiltered, unlimited direct `Scan`. The residual
must be the exact non-null literal `true`, and side-explicit equalities must
cover one complete declared non-null right-table unique key with identical
left/right scalar types. Exact types are a soundness condition: a coercing
Decimal comparison can collapse distinct catalog values to the same infinity.
The runtime gate independently checks the right schema, distinct base-table
occurrences and slots, source-presence implication, and exact `Value` payload
identity. A Project, predicate, limit, nullable key, partial key, coercion,
forged occurrence, or changed payload falls back to the generic join.

The compact result has exactly one candidate slot per left slot. A nested ITE
selects the unique matching right payload over a typed NULL fallback; inner
presence additionally requires a match, while left presence remains the
original left guard. Selector guards deliberately exclude task-local left
presence so routed copies of an absent logical row retain identical
unobservable payloads and StageGraph gather can coalesce them. The result keeps
only left-local partition facts and derives occurrence from the left row.
Catalog uniqueness plus the runtime gate proves that at most one selector can
hold in every satisfying model.

Unordered results are compared by symbolic tuple multiplicity. Ordered results
are compared as sequences where order is observable. Root output names and
their order are an external schema contract and must match exactly; the exporter
may add a mechanical final projection when internal IU IDs differ.

Limit count/offset, TopSort limit, and pushed scan limits are exact non-null
`Uint64` literals in v1. For each nontrivial unordered `Take(1)` source outcome,
at most one bounded selector and one conditional output row represent every
legal result; empty and single-candidate cases require no selector. The row is
retained exactly when the present-row count exceeds the offset; when retained,
the selector must name a syntactically live present slot. The same selector
chooses the typed value, NULL term, and hidden Decimal AVG state. Static bounds
are joined conservatively, occurrence becomes unknown, and only partition facts
common to every syntactically live candidate survive. Other unchecked
nontrivial unordered cardinalities use exact row masks.

With `ensure_at_most_one`, zero- and one-row results retain their exact output
language. In the unordered mask representation, every greater-than-one mask
for one source outcome is quotiented into one error outcome with an unobservable
all-false payload. For zero offset and count greater than one, masks are not
constructed: applying the check directly to the input family is exact because
successful inputs already contain at most one row and every larger input
produces the same observable error. On an ordered stream, Limit takes the exact
`offset:offset+count` slice of the compressed present-row sequence. A pushed
column-scan limit runs after source partitioning and therefore applies once per
task. Bounded choices travel with their outcomes, so exact reuse of one
unordered Limit node remains correlated while distinct stage-task executions
choose independently. Distinct Limit observers of one shared unordered stream
remain unsupported until a common latent-order model is added. Ordered Limit
is deterministic, while Aggregate and Join establish new unordered streams.
Unordered UnionAll does the same; ordered UnionAll independently orders each
input and concatenates the complete left sequence before the right.

Sort enumerates every permutation only when every shaped family outcome has at
most three row slots and their combined permutations fit the ordinary outcome
cap. Moderate larger cases assign a bounded integer ordinal only to
syntactically live slots—those whose guard is not the literal `false`. A
fixed-false padding slot uses constant ordinal zero and consumes no choice; a
symbolically guarded slot still counts as live and is forced to zero only when
its guard evaluates false. Present-row ordinals are in range and pairwise
distinct; key comparisons constrain their relative ordinals, while ties remain
unconstrained. Absent rows do not occupy a compressed position. This is the
same finite sequence language with quadratic constraints rather than factorial
outcomes.

When that pair representation is too large, or a TopSort prefix must be
materialized before a downstream Merge, an audited bitonic sorting network
uses one finite permutation rank per candidate. SQL key order dominates the
rank and the rank orders exact ties; present rows dominate absent rows. Each
nontrivial outcome encodes every live row as a value of one shared
one-constructor SMT datatype containing its presence lane, every nullable flag
and scalar value, and every hidden Decimal AVG sum/count lane. One
quantifier-free `define-fun` per outcome contains the complete SQL key
comparison over two payloads and their tie ranks. Each compare-exchange output
consequently selects one whole payload with one `ite` and its rank with one
`ite`, instead of rebuilding the same condition for every scalar lane.
Power-of-two false padding is unobservable, and the retained output slots form
a proven present prefix, so ordered Limit can take its literal
`offset:offset+count` slice directly. When both result sequences have that
invariant, equality compares aligned slots and requires any unmatched suffix to
be absent; this is the exact compressed-sequence equality without the
quadratic rank matrix. Merge uses the same network for fixed or symbolic
producer ordinals. A fixed producer order adds adjacent tie-rank chains in its
semantic order. A symbolic producer order adds one presence-guarded direction
equivalence per unordered producer pair; equal input ordinals impose no edge,
while the existing ordinal invariant makes two present input ordinals
distinct. Both encodings therefore yield exactly the legal cross-producer
interleavings. The symbolic producer-pair count is preflighted cumulatively
against the 16,384 relation-pair cap. Small explicit permutation selection
deliberately continues to use the full shaped row vector. The bounded-ordinal
representation remains the latent-order model for unordered bags. Sort and
Limit phases are preserved but do not independently change the modeled runtime
semantics. If the initial root is ordered, results are compared as compressed
sequences; otherwise they are compared as bags.

Ordered `Limit(1)` at offset zero has one additional exact representation for
a fixed input sequence (`ordinals is None`). It is considered before the
ordinary `within_limit` no-op, requires a shaped relation with more than one
slot, one through three syntactically live rows, and no hidden AVG metadata,
and otherwise leaves the established ordered-Limit path unchanged. The compact
relation has one present-prefix row whose presence is the OR of the candidate
presences. Each cell uses a right-to-left `ite` fold over the original row
guards, so the first present fixed-sequence row wins, with a canonical typed
absent payload as the final fallback. Decimal finite bounds are combined
conservatively, partition facts retain only the facts common to every
candidate, and alternative occurrence identity is discarded. The source
`Outcome` error, decisions, and choices are preserved unchanged. Count,
offset, sequence, ordinal, size, or metadata near misses use the prior exact
path rather than weakening a gate.

Every materialized relation fails closed above 4096 candidate rows. Join
matching/output, UnionAll, and grouped-aggregate sizes are checked before their
large intermediates are allocated. The direct unique-RHS join still charges
the complete `|L|*|R|` match matrix against the pair ceiling, but its proven
at-most-one output charges exactly `|L|` rows; no global cap was raised.
Sort, Merge, and latent-sequence pair
preflights charge only syntactically live slots. Sort and fixed- or symbolic-order Merge
may instead use the exact network above, bounded by 32768 comparators, 131072
logical packed-payload cells, and 64 ordering columns. The payload metric is
live input rows times scalar lanes; it is an auditably stable logical-width
gate, not a Python-memory or formula-byte estimate. If neither exact
representation fits, construction fails closed above 16384 pairs before
allocating permutations or ordinals. Network Merge also replaces the later
directional producer-order encoding when only that combined cost exceeds the
pair cap. Representation selection and small explicit
permutations/interleavings remain based on the full shaped row vector.
Explicit outcome families separately fail closed above 256
alternatives; that cap applies to non-singleton unordered-Limit masks,
canonical checked-error outcomes, small enumerated ordered choices, and family
products/gathers.
Nontrivial unordered `Take(1)` instead uses at most one bounded choice per
source outcome, and large ordered choices switch to the exact ordinal
representation within the live pair bound. Cross-plan equality fails closed
above 4096 explicit outcome pairs. None of these caps is approximated.
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
Larger cases assign result ordinals only to syntactically live slots and
constrain them by both sort keys and the input ordinals within each producer.
Incompatible metadata and unordered inputs fail closed.
An enumerated Merge outcome is a fixed sequence and may therefore use the
ordered singleton-`Limit` representation above. A non-Merge gather deliberately
drops sequence semantics, so it cannot enter that representation merely
because its inputs happened to be ordered.

Source placement and HashShuffle create guarded task copies of one logical row
occurrence. At a non-Merge multi-task gather, opposite facts for the same routing
choice prove those copies mutually exclusive, so the evaluator may coalesce
them into one guarded occurrence and use exact conditional values when
task-local state differs. Equal-valued eligible copies always coalesce. A
pre-Broadcast gather requests conditional-value compaction for every eligible
producer occurrence before replication. HashShuffle requests it when the
ordinary more-than-eight-row trigger fires or the explicit transported payload
exceeds eight candidate cells; exactly eight cells remain explicit. Root and
serial/parallel Union gathers keep only the ordinary row trigger. Replicated
Broadcast copies have no contradictory routing fact and retain their bag
multiplicity. Unknown occurrences, distinct occurrences, and copies without
pairwise contradictory facts also remain uncompacted. This topology-aware
normalization removes task-copy blow-up without identifying rows that can
coexist or changing a construction cap.

Logical `TOpUnionAll` and a `TUnionAllConnection` are different IR nodes and
receive different semantics.

Shuffle elimination and source co-partitioning are rejected until the snapshot
contains enough source-distribution information to verify them.

## Diagnostics outside the kernel

- Production RBO diagnostic renderers traverse plans as DAGs. `PlanToString`
  expands each operator body once and marks later occurrences `[shared]`; the
  optimizer HTML trace emits later occurrences as leaf nodes with
  `Shared=true`. Explain and execution JSON emit one CTE-style definition per
  shared operator or stage and connection-shaped `CTE Name` references
  thereafter. This keeps diagnostic size linear without changing the
  occurrence-sensitive semantic snapshot; Explain JSON remains outside the
  verifier input.
- `kqp_rbo_inspect plan` renders every normalized plan and StageGraph field in
  deterministic line-oriented text.
- `kqp_rbo_inspect witness` rebuilds the unchanged start-to-finish obligation
  and renders candidate base rows plus every enabled per-operator, stage-task,
  connection, and root-boundary result from one solver model. Each enabled
  outcome and unmatched root record includes every bounded plan choice as a
  concrete `{value,bound}` pair. These valuations are diagnostic model data,
  not part of the stable verifier witness. Optional read-only observers collect
  immutable terms; definitional model aliases are added only after normal
  formula construction and are audit-capped. Trace decoding treats an outcome
  with an out-of-range bounded choice as disabled. Raw global invariants remain
  choice-independent; opaque-result domains use the guarded quantified form
  described above.
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
4. Run the supported subset of `TPCH_YQL` and `TPCDS_YQL` as a coverage
   dashboard; report semantic unsupported outcomes and preparation failures as
   independent axes, preserve their overlap when an exact pair survives, and
   keep separate preparation-success, verifier-entry, formula-construction,
   and hermetic solver-backed proof floors.

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
Integral division has its own independent concrete reference: `Int8` and
`Uint8` are exhaustively enumerated, every wider signed and unsigned width
covers zero, signed overflow, and both truncation signs, and operand-NULL plus
safe-denominator cases are explicit. C++ signature mutations fail closed.
A solver differential verifies an unchanged division pair and finds a bounded
counterexample when operand order is reversed.
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
Decimal extrema use independent exhaustive guarded raw-code and concrete
grouped/scalar references across NULLs, specials, and split tasks. Routing and
solver mutations cover wrong shuffle keys and a final `min` changed to `max`;
the latter has a two-row counterexample.
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
- Occurrence- and routing-aware gather compaction only for one logical
  occurrence with pairwise contradictory task facts. Broadcast requests it
  before fan-out, HashShuffle adds the audited transported-cell trigger, and
  root/Union gathers retain the ordinary row heuristic; replicated Broadcast
  multiplicity and distinct or unknown occurrences remain explicit.
- Independent exhaustive concrete routing references for every admitted
  non-Merge connection and representative local-join combinations.

### M4: benchmark coverage — in progress

- Grouped/scalar count, integer sum, headroom-bounded Decimal sum, and
  row-level `DistinctAll`, including
  split intermediate/final execution, NULLs, exact 64-bit integer behavior,
  Decimal specials, partial-state bound provenance, same-type Decimal MIN/MAX,
  phase-aware Decimal AVG with explicit `(sum,count)` state, and
  the closed staged Decimal AVG carrier with one producer and certified
  physical NULL pads, plus
  cardinality-certified integral AVG with exact `(count,min,max)` ghost state
  for non-NULL count at most two, plus exact same-output-type fixed-width
  signed/unsigned integral MIN/MAX. Integral extrema use a guarded, balanced,
  sentinel-free reduction and preserve scalar/grouped NULL and split-phase
  behavior; Decimal extrema are unchanged. The integral-AVG result certificate
  is observed at its producer, contributes a mandatory solver-first
  model-domain exclusion, and is centrally removed before the family is cached
  or returned to a parent. Intermediate aggregate state remains transportable.
  A separate `integral_avg_rank_v1` provenance tag admits only completed
  integral-AVG `Optional<Double>` results in Sort and StageGraph Merge, using
  the shared carrier as a conservative abstract rank.
  `DistinctAll`
  accepts exact positional aliases of a nonempty ordered key tuple, deduplicates
  null-safely, and remains task-local across intermediate/final phases;
  direct per-trait distinct remains fail closed except for the exact
  fixed-width non-null integer and canonical nullable Decimal
  `COUNT(DISTINCT ...)` contracts.
- Side-explicit join-key descriptors preserve left/right operands even when a
  one-sided semi/anti join receives the same IU name from both inputs. Shared
  IUs require an empty `JoinFilters` list and literal-true residual; joins that
  output both sides and broader residuals remain fail closed. The effective
  key/residual expression retains the shared exact node/depth budget.
- Exact direct `Just(non-null Uint64 member)`, scalar final
  `sum(Optional<Uint64>)` unwrap-to-zero, and scalar
  `COUNT(DISTINCT non-null Int64)` complete the reviewed q95 aggregate path.
  The distinct comparison triangle is preflighted before construction, and
  every nearby type, nullability, phase, key, trait-count, and unwrap mutation
  fails closed.
- Unordered literal Limit/offset, including a bounded symbolic singleton
  selector and exact checked-error quotient, split per-task execution, and
  column-source pushed limits, with exhaustive and mutation tests.
- Sort/TopSort, ordered Limit, and Merge, with exhaustive concrete differential
  tests, syntactically live bounded symbolic ordinals beyond the small
  explicit-family threshold, exact fixed-sequence ordered singleton compaction,
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
  q42. At that checkpoint, the remaining former String blockers reached
  deeper construction caps (q4, q11, q25, q29, q46, q68, and q91).
- Reviewed deterministic total scalar subtrees are exported as canonical typed
  opaque functions. Unit tests cover IU alpha-renaming, first-use argument order,
  repeated arguments, structural/literal/callable mutations, DAG-sharing
  independence, nullability, and fail-closed safety gates.
- The separate `opaque_double` constructor admits only q83's three deviation
  outputs and one average output over three distinct direct
  `Optional<Int64>` arguments. C++ audits the exact roots and constants; Python
  independently confines `Double` to a derived `Optional<Double>` UF carrier
  and direct uninspected payload pass-through. Base data, subplans, scalar
  consumers, comparisons, join keys, aggregate keys/inputs/results, ordering,
  and routing keys remain closed.
  Inspector output preserves the constructor and fingerprint. Focused q83
  formula construction is `FORMULA_EMITTED`; the separate solver result is
  `UNKNOWN`, so the proof floor is unchanged.
- Generic `EndsWith`/`StringContains` and executed OLAP
  `ends_with`/`string_contains` share one narrow
  `yql-string-predicate-v1` opaque identity per operation. Only a direct
  nullable String column, non-null String literal, and matching nullable Bool
  result are admitted; ordered arguments and the generic coalesce-false NULL
  behavior remain explicit. Cross-dialect exporter mutations and a real-host
  column-store fixture cover the bridge, and the fixture is
  `VERIFIED_BOUNDED`.
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
  independent lossless-common-type audit. At that checkpoint, a focused
  real-host q8 run passed the former mixed-width boundary and failed closed on
  unsupported scalar callable `Unwrap` in both snapshots after 480 ms of
  preparation and 0 ms of verifier work; formula and proof counts were
  unchanged. Milestone 70 later adds the checked nullable-String form.
- Exact proven-total Date `Unwrap` recognizes only the reviewed shape occurring
  in q38/q87. Inside
  `Unwrap(Coalesce(Optional<Date> member, fallback))`, the observed initial
  fallback is `SafeCast(Int32(0), Optional<Date>)` and the observed final
  fallback is `Just(Date(0))`; either exact spelling normalizes to the existing
  non-null `if_present` IR. Callable
  metadata, types, nullability, arity, child order, direct-member visibility,
  cast category, literal value, and expression budget must all match; every
  near miss fails closed. At this Date-only checkpoint, q8's String `Unwrap`
  remained unsupported; Milestone 70 later adds its separately checked
  Project-error contract. This slice changes no production Python semantic
  module.
  Independent C++ mutation coverage, Python solver mutations, and a real-host
  initial/final capture lock the boundary. TPC-DS q38 and q87 now construct
  formulas and return `VERIFIED_BOUNDED` in the checked-in proof floor after
  333/1,115 and 324/1,052 ms of preparation/verification, respectively.
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
  literal bytes, and proves that the complete result fits in `UINT32_MAX`.
  Commit `82cfcd837f4` clamps MiniKQL's half-spare capacity calculation instead
  of allowing `newSize + newSize / 2` to wrap; commit `daab603c2f1` aligns the
  verifier gate with that repaired runtime bound. One generic Olap occurrence
  can pass when its exact literals fit, and two maximum Olap cells without a
  literal also fit. q84's two maximum cells plus its two-byte `", "` literal
  total `UINT32_MAX + 1` and still fail closed. Only then does the exporter
  encode the whole syntax tree as one opaque function
  whose fingerprint retains structure, literal bytes, order, and repetition.
  Focused tests cover the
  grammar, provenance failures, and all ten join kinds; a real-host
  initial/final one-Olap-occurrence obligation is `VERIFIED_BOUNDED`, and a
  two-maximum-cell case is admitted while adding `", "` fails closed. At that
  milestone TPC-DS q5 reached
  the Decimal-SUM headroom gate and q80 reached the 82,944-pair
  grouped-aggregate construction cap. q84 had two Olap String occurrences and
  stopped at the allocation-totality gate. The formula slice remained 23/121
  (19.0%) and the proof floor remained ten.
- Milestone 75 retains that proven-total branch and admits only an over-bound
  tree which has already passed the identical restricted grammar, provenance,
  type, scalar-safety, and `ui64` length audit. It emits exactly one top-level
  non-null String `checked_concat` Project expression with the canonical
  root-`Concat` fingerprint and one or two distinct direct stored-String
  arguments. The normal value is the existing fingerprinted opaque function;
  a second shared Boolean function of the same ordered NULL/value envelopes is
  the possible runtime error. A present producer row contributes that predicate
  to the observable query-error outcome.

  The checked expression must be private to the main plan, have one producer
  and one returned output, and may reach an unstaged root only through a
  single-consumer chain of direct Project transports, non-key Sort, and
  offset-free non-error Limit or TopSort. When that corridor contains a
  selector, the verifier independently bounds its producer through only Scan,
  Filter, Cross, and Inner Join. Every selector must be nonbinding at the
  requested row bound; q84's six scans give at most `2^6 = 64` producer rows
  at bound two beneath `LIMIT 100`, while
  bound three gives `3^6 = 729` and fails closed. In a staged snapshot the
  checked Project must be the result root after every materializing edge.
  Subplans, nesting, multiple checked expressions, fanout, computed consumers,
  sort-key use, offsets, error-bearing Limits, other operators in a
  selector-bounded producer spine, and non-result outputs remain unsupported.
  C++ validates the source expression and structural corridor; Python
  independently validates the serialized
  expression, physical Project inputs, topology, and requested-bound premise.
  Focused q84 formula and solver evidence is recorded in the current
  checkpoint below.
- A disjoint literal-only `Concat` gate accepts exactly a non-null String
  binary tree at a Map-body root whose leaves are canonical String literals.
  It audits every source node's scalar safety metadata, exact type, arity,
  literal encoding, shared node/depth limit, and MiniKQL allocation bound, then
  folds the ordered bytes to the existing canonical literal IR. Stored members,
  computed leaves, `Utf8`, unsafe metadata, malformed trees, and oversized
  source or result shapes fail closed or return to the separate stored-member
  gate as appropriate. No Python expression kind or equivalence axiom is
  added.
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
  remain opaque. At that milestone the complete TPCH dashboard recorded q12 as
  `FORMULA_EMITTED` after 109/5,343 ms of preparation/verifier work; an earlier
  focused formula run recorded 108/5,816 ms. Focused and then-current
  policy-floor solver runs returned `VERIFIED_BOUNDED` after 108/38,880 and
  106/40,602 ms,
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
  prove the normal obligation with the hermetic solver. A raw tuple has the
  additional exact proven-present Date-cast exception described above;
  positive and fail-closed C++ mutations cover that exporter-only fold.
- Exact `Exists`, scalar `If`, and unary `IfPresent` are compositional scalar
  nodes. `IfPresent` uses lexically scoped de Bruijn bindings, and the one
  optimizer-generated identity-key/Void-payload `ToDict` membership shape is
  normalized to explicit `in`. Type, nullability, scoping, lambda, dictionary,
  and safety gates fail closed. TPCH q19 passes both real-host boundaries,
  emits a formula, and is `VERIFIED_BOUNDED`. At that milestone TPC-DS q68
  passed export and reached the 32,640-pair Merge construction gate; the later
  exact representation selector moves q68 through formula construction.
- Exact uncorrelated dynamic `IN` has one typed lookup column, one typed
  inner-result column, and one Filter consumer. Non-null lookup/output columns
  may have the same fixed-width integral identity, exact `String` type, or
  Date. Fixed-width integral or Date lookup/output columns may instead be
  independently nullable while retaining the same underlying identity, but
  only when the binding appears only in direct positive top-level conjuncts of
  that Filter.
  At this positive truth boundary, SQL membership is exactly existential
  equality over a non-NULL lookup and a present, non-NULL inner value:
  duplicates collapse,
  empty input is false, and an unmatched NULL never passes the Filter.
  `NOT`, `OR`, embedded nullable uses, nullable `String`, coercions, and other
  nullable identities fail closed because false and SQL UNKNOWN are
  distinguishable outside the positive Filter-truth position. `OuterBind`,
  `AddDependencies`, observable `EnsureAtMostOne`, staging, fanout, tuples,
  `Utf8`, Bool, Decimal, and mismatched identities also fail closed. Non-null
  consumer `NOT` continues to supply anti-membership, repeated references
  share one cached subplan family, and inherited root errors remain eager. Its
  root may reference closed uncorrelated scalar bindings and closed leaf `IN`
  bindings. Each nested `IN` consumes no subplan binding. C++ and Python
  independently require every consumer operator to belong to exactly one main
  or subplan root. Structural subplan-root nesting, a correlated scalar,
  another nesting owner/kind, a cycle, or depth greater than one fails closed.
  The nested scalar retains ordinary cached
  zero/one/many-row semantics and demand; the nested `IN` retains the same
  membership and nullable positive-Filter gates. The membership product is
  cumulatively capped at 16,384 outer/inner pairs across alternatives and both
  nesting levels. Real-host integer, String, and nullable-Date cases prove
  initial dynamic `IN` equivalent to final `left_semi` at two rows and two
  tasks; TPC-DS q33 exercises the nullable positive contract, q58 exercises
  three admitted scalar-inside-`IN` pairs, and q83 exercises one-level
  `IN`-inside-`IN`.
- Exact nullable Date-year projection accepts only an
  `Optional<Uint16>` `Map` over a complete `SafeCast` from one direct visible
  `Optional<Date>` member to `Optional<Timestamp>`. Its unary non-null
  Timestamp lambda must be exactly
  `DateTime2.GetYear(DateTime2.Split(argument))`; all UDF names, callable and
  cached descriptors, user types, AutoMap flags, settings, annotations, and
  lambda identity are checked. Near-miss shapes fail closed. The snapshot
  preserves source NULL with `if_present`, applies the stable non-null typed
  opaque function `yql-datetime-year-v1` to the bound Date payload, and uses an
  explicit constant-true `if` with typed NULL to lift that payload result to
  `Optional<Uint16>`. Focused C++ mutations cover the material
  Date-year-specific gates while shared tests retain generic UDF-envelope and
  scalar-safety checks; Python tests cover NULL propagation plus
  fingerprint/argument mutations, and a real-host nullable-Date projection is
  `VERIFIED_BOUNDED`. At that checkpoint, complete validation passed 183/183
  C++ tests, 493/493 Python verifier tests, 46/46 inspector tests, and 32/32
  real-host integration tests.
- Decimal literals are tagged as finite, negative infinity, positive infinity,
  or NaN; source and opaque values use the exact legal typed domain. Ordinary
  equality/order, exact-type null-safe equality, Decimal/Decimal and
  Decimal/integer `DataCompare` alignment, precision-cap saturation, and
  complete non-null integer constant casts are modeled. Exact `cast_decimal`
  additionally covers nullable or non-null integral weak `SafeCast`
  expressions for every signed and unsigned 8/16/32/64-bit width when the
  canonical Decimal target/result agree and retain an integral digit. Source
  NULL propagates; a present out-of-range integer saturates to signed infinity
  rather than becoming NULL. Canonical Decimal sources normally require
  same-scale, non-decreasing-precision widening, which preserves every finite
  and special encoded value. The one additional exact pair is q49's
  `Decimal(35,2) -> Decimal(15,4)` rescale: finite raw codes are multiplied by
  100 and source magnitudes at least `10^13` saturate to signed infinity, while
  specials are preserved. Result nullability must equal source nullability,
  and the independently checked serialized `source_type` selects those
  semantics across the C++/Python boundary. Canonical same-type
  Decimal `+`/`-`, `DecimalMul`, and `DecimalDiv` with a same-type Decimal or
  integer right operand have exact `NDecimal` special, scale, rounding, and
  overflow semantics, including the current negative-divisor asymmetry. Sort,
  TopSort, and Merge use the distinct raw-code total order,
  including ordered NaN and exact Decimal key identity. Decimal `sum` widens to
  `Decimal(35,s)` and is exact whenever its carried finite bound proves that
  saturating partial addition cannot overflow; unsafe bounds fail closed.
  Same-type Decimal `min`/`max` ignore NULL and reduce the raw signed codes in
  the runtime's total order, `-Inf < finite < +Inf < NaN`, with the same scalar
  state in logical, intermediate, and final phases. Same-type Decimal `avg`
  uses the explicit weighted `(sum,count)` phase contract above. Decimal scale
  changes or narrowing, source/result nullability mismatch, `StrictCast`,
  `Convert` outside constant normalization, other casts, floating-point,
  mixed-type, and other division forms, non-core `IN`, and aggregate functions
  outside this subset remain unsupported. Exhaustive cast, rational, ordering,
  and aggregate references,
  adversarial arithmetic and accumulator-overflow cases, signature and
  mutation tests, and green real-host Decimal filter, integral-cast,
  arithmetic, ordered, and aggregate obligations cover this boundary. TPC-DS
  q90 exercises two
  `Uint64` count expressions cast to `Decimal(15,4)` and is
  `VERIFIED_BOUNDED` at the standard two-row/two-task bound.
- Finite Decimal literals, specials, typed NULL, and complete non-null integral
  casts seed conservative finite-coefficient bounds. Exact same-type
  `+`/`-`, `If`/`IfPresent`, and stage alternatives propagate them without
  treating special values as finite numbers. A Decimal product with an
  integral right operand multiplies the known left bound by the full signed or
  unsigned type-domain magnitude and caps it at the largest finite result;
  integral-right division retains the left bound. A zero divisor and special
  left value cannot produce an unaccounted finite result. Unknown and
  same-Decimal multiplicative operands remain unknown and can only make a later
  aggregate fail closed. Scalar and relation tests cover signed/unsigned
  8/64-bit domains, saturation, division by zero, specials, typed-null wrappers,
  and two-row `SUM` consumption. At the earlier additive-bound milestone a
  focused TPC-DS q5 run cleared its former Decimal-SUM headroom rejection and
  reached the deeper 32,896-pair Merge construction cap after 1,720/42,044 ms;
  the later exact representation selector moves q5 through formula
  construction. Combined with literal-only `Concat`, the integral
  multiplicative bounds move q66 through formula construction.
- Exact Decimal-only `min`/`max` have independent raw-code, NULL,
  grouped/global, split-state, wrong-shuffle, type, and phase-nullability tests.
  A staged final `min` changed to `max` has a two-row solver witness. At the
  earlier MAX milestone, focused TPC-DS q74 passed its aggregate blocker and
  reached the 65,536-pair join-matching preflight after 463 ms of preparation
  and 375 ms of verifier work; later work reached an 8,126,496-pair Sort
  preflight, and Milestone 72 now moves q74 through formula construction. The
  MIN extension lets TPCH q2 pass verification setup after its
  canonical `EndsWith` lowering, but q2 then failed closed at a 32,640-pair
  Merge construction above the 16,384-pair cap. At that checkpoint neither
  query joined the formula or proof floor; the later exact sorting network now
  moves q2 through formula construction.
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
  counterexample; at that historical checkpoint Z3 returned `UNKNOWN` at the
  60-second bound. M82's fresh focused and complete-gate proofs supersede only
  q88's current proof status, without attributing the different solver result
  to any particular intervening change.
- Actual pushed column-store filters are decoded from `OlapFilterLambda`, not
  optimizer statistics metadata. The supported Boolean/comparison subset is
  evaluated before per-task pushed limits. Exact two-child
  `TKqpOlapFilterUnaryOp` tuples require an Atom operator tag: `exists(x)` maps
  to the scalar presence node and `empty(x)` to its negation. Unknown or
  non-Atom tags, malformed tuples, and unavailable read columns fail closed.
  Physical read names, full output-IU names, and short output-IU names resolve
  to the corresponding logical scan output. A referenced spelling that denotes
  distinct outputs fails closed as ambiguous; unused ambiguity is accepted
  because it cannot affect the predicate. The separate Date gate admits `just`
  only around a direct valid
  non-null Date literal. An exact four-child pushed Boolean
  `KqpOlapFilterBinaryOp("??")` requires a non-null Bool result descriptor, an
  explicitly typed `Optional<Bool>` binary left predicate, and a direct
  non-null Bool literal fallback. It lowers without contextual erasure to
  `if_present(optional, bound(0), fallback)`, preserving both `false` and
  `true` beneath `NOT`, comparisons, AND/OR, and other value-sensitive
  positions. Every recursively represented OLAP scalar node is audited for
  safety metadata; only commutative `KqpOlapAnd`/`KqpOlapOr` may carry
  unordered children. The same pushed dialect admits the exact compiled-LIKE
  `KqpOlapApply` contract above. Exporter safety tests and real-host
  `IS NULL`/`IS NOT NULL` obligations cover the surrounding boundaries. The
  earlier presence lowering moves TPC-DS q76 through formula construction;
  exact coalesce and compiled LIKE move TPCH q13/q16.
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
- Exact two-dependency relational `EXISTS` extends the existing uncorrelated
  and one-equality forms without making correlation generic. The dependency
  list contains exactly two ordered, distinct outer columns. Each dependency
  occurs in its own conjunct: exactly one strict direct equality and one strict
  direct inequality compare against distinct direct inner columns. Every
  residual conjunct is inner-only. Each outer/inner pair has the same base type
  while nullability may differ; ordinary strict comparison and Filter truth
  preserve the NULL behavior.

  C++ serializes source `!=` as normalized JSON `not(eq)`. Python independently
  accepts that exact predicate shape, retains dependency order, and binds both
  values from the same outer row. The C++ exporter checks the complete
  `AddDependencies` output schema, order, and types; Python independently
  checks the serialized ordered dependencies and predicate contract. Limit,
  TopSort, and scan `pushed_limit` fail closed for a correlated root because
  their row choice would require a fresh decision per invocation. On a
  one-sided witness join, a dropped `Void` is admitted only when the retained
  side has the same-name `Void`; unmatched dropped `Void` and `Void` join keys
  remain unsupported.

  At the two-dependency milestone, focused coverage passed 17/17 Python
  `EXISTS` tests, 527/527 complete Python verifier tests, 6/6 C++ `EXISTS`
  tests, 203/203 complete exporter tests, 46/46 inspector tests, and the new
  real-host case 1/1. Solver differentials
  return `VERIFIED_BOUNDED` for the exact `left_semi` and negated
  `left_anti` lowerings, while omitting the second correlation returns
  `COUNTEREXAMPLE`. TPCH q21 and TPC-DS q16/q94 now construct formulas and
  return `VERIFIED_BOUNDED` at two rows per table and two tasks. These are
  bounded theorems only, not unbounded query-equivalence claims.
- Exact same-type integral `/` requires one fixed-width integer identity for
  both operands and its Optional result. Operand NULL, a zero divisor, and
  signed `MIN / -1` overflow produce NULL; otherwise magnitude division plus
  sign restoration truncates toward zero. All mixed-type, mixed-width,
  non-Optional-result, and floating-point forms fail closed.

  At the preceding integral-division milestone, TPC-DS q73 emitted a formula
  after 252/760 ms in the complete run. q78 passed both exporters and reached
  the verifier, then failed closed after
  1,075/27,987 ms at a 52,326-pair Sort construction above the 16,384-pair
  audit cap. The policy pins q73 at preparation, formula construction, and
  bounded proof, and q78 at preparation plus verifier entry. That milestone's
  complete validation passed 537/537 Python verifier tests, 207/207 C++
  exporter tests, and 14/14 coverage-policy tests. A focused solver
  differential passes 1/1:
  the unchanged pair is `VERIFIED_BOUNDED`, while reversing the operands
  produces `COUNTEREXAMPLE`. Focused workload q73 is `VERIFIED_BOUNDED` after
  239/8,940 ms and produced report SHA-256
  `2c9dd4e765f4507bd952189055d67a0db5cf818ecb84abe188bfcdd8a15122e0`.
- Exact Sort/TopSort/Merge networks replace quadratic ordinal construction
  only within explicit comparator, logical payload-width, and key-width
  budgets. Finite distinct tie ranks, present-before-absent comparison,
  coherent whole-row swaps, and concrete producer-order rank chains make the
  representation exact. Present-prefix metadata is preserved only through
  audited row-preserving operations and lets ordered Limit compact a TopSort
  before Merge.

  At the initial per-lane carrier milestone, focused TPCH q2 returned
  `FORMULA_EMITTED` at two rows and two tasks:
  problem construction takes 11.469 seconds, canonical rendering takes 15.931
  seconds, and the formula is 62,274,331 bytes. Its two 128-row local networks
  each cost 37,632 comparator/column transports and its 200-row Merge costs
  96,768 under that carrier's cap. The policy added q2 to preparation and
  formula floors but not to bounded proof.

  The subsequent packed carrier removes that width multiplier from every
  compare-exchange. Each nontrivial network outcome declares one
  one-constructor product datatype over the complete row lanes and one closed,
  quantifier-free `define-fun` for SQL ordering over two payloads and two tie
  ranks. A cell then transports one payload and one rank. Layout validation
  independently checks lane sorts, Decimal bounds, and complete AVG state; it
  retains only occurrence and partition metadata common to every live input.
  Zero- and one-live-row outcomes allocate neither declaration.

  The separate 131,072-cell safety gate now charges live rows times packed
  scalar lanes, not comparator count times column count and not estimated
  memory. The observed q59 256-row/139-column initial shape costs 71,424 cells,
  its 1,024-row/36-column final shape costs 74,752, and q78's
  324-row/22-column shape costs 14,580. All remain below the logical payload
  cap; the independent 32,768-comparator and 64-order-column gates remain.
  Exact aligned present-prefix equality also avoids reconstructing a quadratic
  compressed-rank matrix at the final comparison.

  Focused production-host runs now return `FORMULA_EMITTED` for both TPC-DS
  rows. q59 takes 44.66 seconds, peaks at 1,707,844 KiB RSS, and emits a
  116,879,360-byte formula with four datatypes and four comparator definitions
  (SHA-256
  `3a140fcb1b5d6a5145c4aa30cbcd817167a27f21bed94d85ef969223dce73c8e`).
  q78 takes 57.06 seconds, peaks at 2,006,884 KiB RSS, and emits a
  202,469,546-byte formula with three datatypes and three comparator definitions
  (SHA-256
  `fb0eaebb95ea9bdfb3b0f815f5078a70d1c2e3765ed5d6675be1c4f06b8249c4`).
  These are exact bounded obligations, but they were not solved: both results
  are formula coverage only, add no bounded proof, and reveal no optimizer
  correctness bug. The complete hermetic verifier target passes 564/564 tests,
  and the updated coverage-policy target passes 14/14.
- The real-host dashboard runs all 22 `TPCH_YQL` and 99 `TPCDS_YQL` sources,
  writes a structured timeout-aware version-five report, and preserves
  diagnostic artifacts for every captured boundary, correctness, unknown,
  schema, or solver outcome. Preparation status and semantic classification
  are independent; a later failed preparation does not erase an exact captured
  pair.
- Its strict version-five input policy and independently versioned
  version-four evaluation enforce one orthogonal preparation-success floor
  and four monotonic semantic depths. The exact-pair floor unions every formula
  and explicit verifier-entry row with supplemental TPC-DS
  q51, yielding 20 TPCH plus 81 TPC-DS pairs because q49/q53/q63/q89 now
  belong to the formula tier. TPCH q1, q13,
  and q16 plus TPC-DS q5, q8, q9, q59, q65, q72, q78, and q80 have explicit
  verifier-entry requirements, the 100-query formula floor must keep
  constructing SMT, and the 32-query hermetic
  proof floor must remain
  `VERIFIED_BOUNDED`. A verifier-side `UNSUPPORTED` result satisfies only the
  exact-pair tier unless separately pinned at entry; later formulas and proofs
  satisfy every weaker semantic tier
  without pinning brittle blocker text.
- Exact concrete-String equality, occurrence/routing compaction, scoped
  shared-term rendering, nonrecursive
  structural IDs, exact grouped-key classes, the small
  enumeration/symbolic-ordinal selector, and bounded exact sorting networks
  remove the former factorial and repeated-structure construction gates.
  SMT occurrence discovery, dependency levels, and term emission are also
  iterative: deeply nested exact obligations no longer depend on Python's
  recursion limit, while 3,000 randomized shared and quantified DAGs preserve
  the preceding renderer's bytes exactly.

  The historical Milestone 69 checkpoint includes the preceding q72, q9, and q24
  formula checkpoints plus reviewed generic/pushed compiled LIKE, exact
  pushed Boolean coalesce, and grouped fixed-width integer count-distinct, as
  well as delayed direct unique-RHS factor scheduling for q64 and the
  literal-`Concat`/Decimal-bound q66 and exact point/finite-point
  `RangeInfo::ComputeNode`, integral-AVG, integral-extrema, and derived-ordering
  milestones.
  TPCH's semantic partition is 20 formulas, no unsupported query, and two
  no-pair optimizer failures; TPC-DS has 66 formulas, 15 unsupported queries,
  and 18 no-pair optimizer failures. Preparation succeeds for 20/22 TPCH and
  73/99 TPC-DS queries and fails for the other 2 and 26. TPCH retains 20 exact
  pairs and 20 verifier entrants; TPC-DS retains 81 exact pairs and 70
  verifier entrants. Eight failed TPC-DS preparations retain exact pairs and
  overlap the unsupported inventory.
  Across both suites, 86/121 construct formulas (71.1%), 86/101 exact pairs do
  so (85.1%), 86/93 do so within the preparation-successful subset (92.5%),
  and 86/90 verifier entrants do so (95.6%). The 15 unsupported rows split by
  primary terminal layer into 11 initial-export, zero final-export, and four
  verifier results; secondary boundary diagnostics remain recorded.

  The completed post-M69 TPCH dashboard confirms 20 formula / 0 unsupported /
  2 no-pair after 2,928/93,582 ms, with report SHA-256
  `3e36c25a277c81ef0b817452c3b1fddad1ff96bfbb32d0096023af580682fb32`.
  TPC-DS confirms 66 / 15 / 18 after 64,878/750,968 ms, with report SHA-256
  `28ac807523973e4b963c2f9eef2a5271437d81a7d573377e5a9f35d887d4bb7b`.
  Both complete reports satisfy the preparation and formula floors with no
  policy violation.
  The unchanged proof floor is freshly green: 13/13 TPCH after
  1,523/60,015 ms (report SHA-256
  `4788bd065a9e0cb7e58b0c2d2be851e50d5eb411abe8216ea0df5f2fcc890ed5`)
  and 17/17 TPC-DS after 12,465/100,402 ms (report SHA-256
  `325c5970a4decedf961bb8958a6871cf965f1f7237d4406e500b449981327194`),
  all `VERIFIED_BOUNDED`.
  Validation passes 647/647 Python verifier tests, 14/14 policy tests, and the
  5/5 proof-floor target. M69 changes no exporter code; the last complete C++
  exporter gate remains M68's 259/259.

  The preceding q66 complete TPCH formula dashboard spent 2,880/31,400 ms in
  preparation/verifier work and produced report SHA-256
  `59382f89eee68d48601d5bd102350b656e42cda17173970ce412dc83da40bdac`;
  TPC-DS spent 64,372/683,109 ms and produced
  `6aaaed8da14d46ecee9f6b3cfa31544077df5652c7c6168a01ee4ba93f0ac595`.
  Those timings and hashes are historical q66 evidence. Within that TPC-DS
  run, q66 emitted after 2,138/35,635 ms of preparation/verifier work.

  The preceding read-range complete dashboards spent 2,895/30,992 ms for TPCH and
  64,296/706,547 ms for TPC-DS in preparation/verifier work. Their report
  SHA-256 values are
  `9a6c562fc3c8ef7d9d56dacf2411f1c87cc35d966e7ac538e4a947add9dded56` and
  `1eff186049cceb773f6710ce29504bde5065b098d9d7aec1d692bf05f8f5fbec`.
  Within TPC-DS, q9 spent 8,951/5,293 ms and q45 spent 511/14,367 ms.

  A separate q66 solver experiment is `UNKNOWN` after 2,134/98,339 ms: the
  60-second global deadline expires before branch 1/4
  (`left_language_empty`). Its report SHA-256 is
  `60f9efb2609555474d6cd082c0a75e953f605c1a17ed821db71ca1da4c27c27e`;
  the 97,279,426-byte canonical formula has SHA-256
  `dcebfec17d3373e376f78ae0992aa45eb9a1e2006ea6598766ab3210379b83e5`.
  q66 adds formula coverage only, no bounded proof, counterexample, or optimizer
  finding. Validation passes 593/593 Python verifier tests, 227/227 C++
  exporter tests, and 14/14 coverage-policy tests.

  Focused production-host q45 now constructs a formula; a separate solver run
  is `UNKNOWN`, not a proof or counterexample. q9 passes both exporters and
  enters the verifier, then fails closed because an 8,192-row join output
  exceeds the 4,096-row construction audit bound. This slice adds no optimizer
  correctness finding and leaves the twenty-seven-query proof floor unchanged.
  Validation passes 232/232 C++ exporter tests, 593/593 Python verifier tests,
  and 14/14 coverage-policy tests. Those counts belong to the preceding
  read-range checkpoint.

  Cardinality-certified integral `AVG` is now complete as Slice A. Focused
  row-bound-two/task-bound-two formula-only runs move TPC-DS q7, q13, and q26
  to `FORMULA_EMITTED` after 194/1,181, 247/1,830, and 204/1,122 ms of
  preparation/verifier work. Their combined report SHA-256 is
  `721507f60df911e5906865fb26710ed98772338b5aa74afc93532dad63881853`.
  Separate 60-second solver runs are all `UNKNOWN`: q7 at branch 4/28, q13 at
  branch 4/4, and q26 at branch 4/28. No bounded proof, counterexample, or
  optimizer bug is claimed. The policy pins all three at preparation plus
  formula construction.

  Slice A raises TPC-DS formula coverage from 56 to 59 and combined
  coverage from 74 to 77/121 (63.6%). It gives 77/101 (76.2%) exact-pair
  coverage, 77/93 (82.8%) preparation-success coverage, and 77/84 (91.7%)
  verifier-entrant coverage. Entrants rise from 80 to 84: q35 is the fourth
  new TPC-DS entrant and at that checkpoint rejects unsupported integral `MAX`
  in Python, while q7/q13/q26 add formulas. TPC-DS unsupported
  outcomes fall from 25 to 22, optimizer failures remain 18, and preparation
  remains 73 success / 26 failure. The twenty-seven-query proof floor is
  unchanged.

  Commits `8d3e44f59a6` and `abe190f6344` record the implementation and policy.
  The Slice A suites passed 608/608 Python verifier, 237/237 C++ exporter, 47/47
  inspector, and 14/14 coverage-policy tests. The complete TPCH dashboard
  spends 3,273/37,511 ms in preparation/verifier work and has report SHA-256
  `f7430b2bc2e0dc3779b939831afa163d7fa7b45a7c12eeadae761117f3517b8f`;
  TPC-DS spends 76,727/851,301 ms and has report SHA-256
  `c37f457d0335a8b94ee10d48a5e15bffb86d6ec671050fba4538297e89688867`.
  Its q7/q13/q26 rows spend 210/1,258, 279/2,049, and 224/1,361 ms.

  Exact fixed-width integral `MIN`/`MAX` now moves TPC-DS q35 from verifier
  rejection to formula construction. Its baseline rejected `max(Int64)` at
  `n16.aggregates[2]` after 598/265 ms, report SHA-256
  `829ff76b7d3fb9849db3a13b86bac9a604bca84eaa7f64c939517560822d50b1`.
  The first exact semantics run exposed a Python `RecursionError` in the SMT
  renderer, a verifier bug rather than an optimizer finding; the preserved
  report SHA-256 is
  `d19f0e233fad50d4b6be279eaaa8fc9fdac2d48a01fb23f79ba7a33cc30cd7e1`.
  The stack-safe renderer fixes that tool defect without changing canonical
  bytes. A focused q35 run is `FORMULA_EMITTED` after 542/120,515 ms, report
  SHA-256
  `b312b43d1ba4d20aeeb615c2fe75b54b8baeed87cfdb54bea85aa4a0e9ccc9b5`.
  Its separate 60-second solver run is `UNKNOWN` after 614/199,928 ms because
  the solver cannot rule out an integral-AVG count greater than two; report
  SHA-256
  `164398b163725598b676c231349a19c30f161fdb012dc61f951934c89676f2e4`.

  Commits `b6c8e8863bb`, `cb50a1ee896`, `7785d8dd23c`, `90a7abd2334`, and
  `a39863e5b33` record the exact extrema, stack-safe renderer, q35 policy,
  odd-width exhaustive regression, and producer-local certificate cleanup.
  The integral-extrema suites pass 615/615 Python verifier, 237/237 C++ exporter,
  47/47 inspector, and 14/14 coverage-policy tests. The complete TPCH dashboard
  spends 3,207/36,148 ms in preparation/verifier work and has report SHA-256
  `499e0098afda7bed5198b2cb4cc2dfe35ca81e24252aa15c8e7b1803f26e2b3f`;
  TPC-DS spends 70,746/858,347 ms and has report SHA-256
  `8b194da2b89d4da4dbd9fd088bf8cc07e5224239e1b656322e3cfa43198d662a`.
  Its q35 row is `FORMULA_EMITTED` after 565/121,012 ms. This checkpoint adds
  formula coverage only: the proof floor remains twenty-seven, and it found no
  optimizer bug or counterexample.

  Derived integral-AVG ordering is complete at implementation commit
  `e8abaff7ff4` and policy commit `3e91814d64e`. The suites pass 619/619 Python
  verifier, 242/242 C++ exporter, 50/50 inspector, and 14/14 coverage-policy
  tests. Focused formula-only TPC-DS q22/q85 emit after 324/1,704 and
  347/8,346 ms of preparation/verifier work, with report SHA-256
  `0a7612f430d9dbff68d60afdcd79cf3a7cf97170d54a5287e315be9270ba954e`.
  Separate 60-second solver runs are both `UNKNOWN`: q22 spends 348/61,611 ms
  before the global deadline at branch 2/4 (`right_language_empty`), and q85
  spends 315/71,360 ms before failing to exclude integral-AVG count greater
  than two. Their report SHA-256 is
  `6fbe29825c3e2863ad8c3a7d92ea661bd655e7245d455fcbb1db207dcd1e258c`.

  The complete TPCH dashboard remains 18 formula / 2 unsupported / 2 no-pair,
  spends 2,947/33,310 ms, and has report SHA-256
  `8a231a04398f6ca176286bd9d4d658e7d836c36c34ddcc4d43dfde54cc413a4b`.
  TPC-DS is 62 / 19 / 18, spends 68,923/846,363 ms, and has report SHA-256
  `64fbda391ca5b50698aceaa2a38ba2210617fd0c1c0071bcb7c5c7967b260ecd`.
  The two new formulas move combined coverage to 80/121 (66.1%), exact-pair
  coverage to 80/101 (79.2%), preparation-success coverage to 80/93 (86.0%),
  and verifier-entrant coverage to 80/86 (93.0%). The proof floor remains
  twenty-seven. This checkpoint found no optimizer bug or counterexample.

  Milestone 64 is complete at implementation commit `97f103ce060` and policy
  commit `aa01e609499`. TPC-DS q72 now exports at both boundaries and is pinned
  at successful preparation plus verifier entry, not formula construction.
  At the normal limits it rejects a 4,608-row join output above the 4,096-row
  relation cap. A disposable increase to 8,192 rows was fully reverted after
  showing the next blocker: a 10,619,136-pair grouped aggregate above the
  16,384-pair construction cap after 12.151 seconds. Therefore the next
  logical step is an exact unique-key-aware proof that the relevant right side
  contributes at most one row, followed by join compaction using that fact;
  increasing a global construction cap is not the plan.

  q72 changes only the entry denominator: combined formula coverage remains
  80/121, exact-pair coverage 80/101, and preparation-success coverage 80/93,
  while verifier-entry coverage becomes 80/87 (92.0%). TPC-DS has 69 entrants
  and the unsupported split becomes 14 initial / 0 final / 7 verifier.
  The complete post-q72 TPC-DS dashboard spends 67,551/841,054 ms and has
  report SHA-256
  `8fa6661f88bbbc3f45b8bbee7fec73c4262608f5b2755736e5b1425bce15ec15`;
  q72 spends 371/535 ms before the join guard.
  Formula and proof floors remain 80 and twenty-seven. No optimizer bug or
  counterexample was found.

  Milestone 65 is complete at implementation commit `0b0025f2a11`, naming
  polish `a55f3ecba73`, and policy commit `aa004084427`. The verifier now uses
  catalog uniqueness only after its runtime gate has re-established a direct,
  unfiltered RHS scan, exact non-null same-type key coverage, distinct source
  slots, source-presence implication, and exact payload identity. It retains
  the `|L|*|R|` match-pair preflight but emits one candidate per left slot.
  Every rejected shape continues through the generic join.

  Focused q72 now reaches `FORMULA_EMITTED` after 376/1,359 ms; its report
  SHA-256 is
  `3e6875016128af45b91b41a2fdc427fcfc7a3ef9817fa51441dd28c3636bdc8f`.
  A separate 60-second solver attempt is `UNKNOWN` after 366/61,896 ms because
  the global deadline expires before branch 4/4
  (`right_outcome_0_unmatched`); report SHA-256 is
  `5bdd19a912dbdbfb5c02d865547a692690f93f8c9cd2bac83db180dd18aeea1e`.
  This is formula coverage, not a bounded proof or counterexample.

  The complete post-M65 TPCH dashboard remains 18 formula / 2 unsupported /
  2 no-pair, spends 2,938/90,617 ms, and has report SHA-256
  `67aff9f9ce4404ca52b720d5155a05a6fc7943061ab20d6ad3cc8773d2e4017e`.
  TPC-DS becomes 63 / 18 / 18, spends 64,770/786,154 ms, and has report
  SHA-256
  `5ca1acabd6e83cf99476cdce6547be427368c74edf0d2ea8b829cc8826d9dd62`.
  Combined formula coverage is 81/121, the proof floor stays twenty-seven,
  and no optimizer bug was found. Validation passes 629/629 Python verifier
  tests and 14/14 policy tests. At that checkpoint Milestone 66 next targeted
  q9's ordered singleton-`Limit` representation; q64's independent 8,192-row
  join blocker remained.

  Milestone 66 is complete at implementation commit `e8b81982299`, formula
  policy commit `6804f459df7`, proof-policy commit `0cba3c9262e`, direct
  left-biased encoding commit `66db625c092`, and proof-fixture update
  `0d2fc858b70`. It adds no q9-specific IR or expression rule: the entire
  trusted change is a 138-line net addition in `relation.py`, isolated behind
  the exact fixed-sequence gate documented above. More than 500 focused test
  lines independently exercise the semantics and near misses. No global cap,
  snapshot schema, exporter, StageGraph evaluator, or solver rule changed.

  An independent pre-commit soundness review rejected an earlier proposal to
  admit symbolic ordinals. Two present rows may legally reach this local
  representation with tied ordinal terms; the old compressed-rank Limit can
  then retain both rank-zero rows, while singleton compaction would retain
  only one. The committed gate therefore requires `ordinals is None`, and a
  tied-symbolic-ordinal regression requires the established path. Enumerated
  StageGraph Merge outcomes are safe because each outcome has a fixed sequence;
  gather drops sequence semantics and cannot enter the gate. This was a
  verifier-design issue caught before commit, not an optimizer finding.

  Focused q9 formula construction is `FORMULA_EMITTED` after 8,926/5,796 ms
  of preparation/verifier work, with report SHA-256
  `1e188e3624d93d67439459eaaea112a263558b464f05a1d584ed20a7d174ab76`.
  Its separate 60-second run is `VERIFIED_BOUNDED` after 8,710/22,517 ms,
  with report SHA-256
  `c997e678ed070e12278b6425f2b8b90bfee2fa3d961a9abae4e8f4f7de3f46e0`.
  The first sound implementation materialized mutually exclusive selected-row
  guards. It kept q9 exact but made the pre-existing q15 proof time out. The
  equivalent direct presence OR plus right-to-left per-cell fold reduces q15's
  canonical formula from roughly 930 KiB to 881 KiB and restores its
  `VERIFIED_BOUNDED` result in 13.434 seconds, while q9 remains proved in
  22.517 seconds.

  The complete post-M66 TPCH dashboard remains 18 formula / 2 unsupported /
  2 no-pair, spends 2,773/92,364 ms, and has report SHA-256
  `f56b6f3e402c83489331479b3a8c9a2337eb0c15b7ec5ec9904ec05f61476c83`.
  TPC-DS becomes 64 / 17 / 18, spends 64,839/660,548 ms, and has report
  SHA-256
  `8ba769a6bccb8e1a6b1a75821ee041810311ceb5ac2e48ec04ae5cf41e42efa8`.
  Combined formula coverage is 82/121 (67.8%); the exact-pair,
  preparation-success, and verifier-entrant ratios are respectively 82/101
  (81.2%), 82/93 (88.2%), and 82/87 (94.3%). The primary unsupported split is
  14 initial / 0 final / 5 verifier. q9 raises the bounded proof floor to
  twenty-eight. The focused Limit suite passes 52/52, the complete verifier
  suite passes 634/634, the coverage-policy suite passes 14/14, and the full
  proof-floor gate passes 5/5 while confirming all 28 obligations. No
  counterexample or optimizer bug was found. At that checkpoint q24's
  `Optional<Utf8>` Map/`Unicode::ToUpper` exporter boundary remained next;
  q64 independently remained at the 8,192-row join guard.

  Milestone 67 is complete at semantics-neutral reviewed-UDF refactor commit
  `bed31799d83`, exporter commit `0ca4097d444`, formula-policy commit
  `a26d42bdba8`, and audit-comment commit `f929a36b59c`. It accepts only
  `Map(SafeCast(direct visible Optional<String> member, Optional<Utf8>),
  lambda Utf8 -> Apply(reviewed Unicode.ToUpper Udf, the identical binder))`
  with an `Optional<Utf8>` result. The gate independently checks the complete
  normalized eight-child UDF envelope, callable and cached type descriptors,
  one `AutoMap` Utf8 argument, zero optional arguments, Void configuration,
  `(blocks, strict)` settings, scalar-safety metadata, the reviewed
  String-to-Utf8 `MayFail` conversion, and the 64-binding audit limit.

  The accepted expression becomes
  `if_present(source, nullable opaque(bound(0)), null Utf8)`. Source NULL is
  therefore exact. For a present String, fingerprint
  `yql-string-to-utf8-unicode-upper-v1` jointly represents UTF-8 validation
  failure and deterministic uppercase output. This conservative
  over-approximation may make an obligation `SAT` or `UNKNOWN`, but cannot
  manufacture a false `UNSAT`. It reuses existing bound-value,
  `if_present`, typed-NULL, and nullable opaque-function semantics; no IR kind
  or Python theorem rule changed. Thirty-two independent malformed-shape
  mutations and the 63-accepted/64-rejected binding boundary keep the exporter
  fail closed.

  Focused q24 formula construction is `FORMULA_EMITTED` after 1,380/5,179 ms,
  with report SHA-256
  `f9266984538cbb4ddfb7e0cbb8fa196d8137be1bb136841c185e23ef7f47c445`.
  Its 14,998,792-byte, 1,319-line SMT artifact has SHA-256
  `cf0057462b5dce47c7ccc345e59286fe76cd93a59d4b174a338a13f4715f9e9d`.
  The separate 60-second solver run is `UNKNOWN` after 1,366/65,594 ms because
  the global deadline expires at branch 3/4
  (`left_outcome_0_unmatched`); report SHA-256 is
  `83a4dd5c32b0c009d18245c7368a3cee23aba9130fc0575f25ec122566c52ce2`.
  It is not a bounded proof or counterexample.

  The complete post-M67 TPCH dashboard remains 18 formula / 2 unsupported /
  2 no-pair, spends 2,850/95,985 ms, and has report SHA-256
  `584cf92e304f0ef8ebc67937b4ed9ea80d0bd5d1bcbe7ae944a9da7a27978917`.
  TPC-DS becomes 65 / 16 / 18, spends 64,487/673,727 ms, and has report
  SHA-256
  `ab8422fcc7f830a6dd314cd4edd7f0203072846c9fcc73df0f5a36b922ce7d44`.
  Combined formula coverage is 83/121 (68.6%); the exact-pair,
  preparation-success, and verifier-entrant ratios are respectively 83/101
  (82.2%), 83/93 (89.2%), and 83/88 (94.3%). The primary unsupported split is
  13 initial / 0 final / 5 verifier. The proof floor remains twenty-eight.
  Validation passes 634/634 Python verifier tests, 247/247 C++ exporter tests,
  14/14 policy tests, and the 5/5 proof-floor gate. That gate confirms 11/11
  TPCH obligations after 1,351/56,500 ms (report SHA-256
  `9fcc4b6967736dd408760b16469380aeb03871518e2dea9ea850ec08c3f1e563`)
  and 17/17 TPC-DS obligations after 12,900/88,785 ms (report SHA-256
  `c59c5e08f836e0a619969fa9afeac984f0c1715bd61a42e0c0f151001a86105c`).
  No optimizer bug or counterexample was found.

  Milestone 68 closes the two remaining TPCH export gaps through three narrow
  seams. The compiled-LIKE recognizer audits both the generic expression and
  pushed `KqpOlapApply` program, including cached descriptors, canonical
  case-sensitive RE2 options, bounded ASCII pattern, nullable String input,
  outer NOT, and scalar resource limits. Both lower to the same existing
  deterministic-total opaque fingerprint. Pushed Boolean `?? true/false`
  retains exact typed `if_present` semantics in every value position; the
  evaluator uses only an algebraically equivalent compact term for the
  identity-bound case. Ordinary direct integer count-distinct admits one
  scalar or grouped phase-undefined trait over a non-null fixed-width
  signed/unsigned input. Group guards and null-safe key equality are exact,
  and every relation representation preflights
  `candidate_groups * N*(N-1)/2` before constructing comparisons.

  Focused TPCH q13/q16 canonical formulas are 206,870/383,335 bytes with
  SHA-256
  `f5b95c739c8d32944249c865bac64a44f4fb70c3f78082c0e9ec3db27cc995b4`
  and
  `6f97871a39cd1d1aaefcefa61bca2018159895e21b200156a9f480ea1218c588`.
  The complete post-M68 TPCH dashboard is 20 formula / 0 unsupported /
  2 no-pair after 2,859/93,926 ms (report SHA-256
  `9bf3c81dbeef81094ed2df0350acad101ebc6613c10cebbe6c44deb8484391a2`).
  TPC-DS remains 65 / 16 / 18 after 64,187/669,357 ms (report SHA-256
  `d6fd489bc6c24af3d5670eb4054bc5e7feadc5c1e25047d18b8b399773e0b5bb`).
  Combined formula coverage is 85/121 (70.2%), with 85/101 exact pairs
  (84.2%), 85/93 successful preparations (91.4%), and 85/90 verifier entrants
  (94.4%). Unsupported primary terminal layers split 11 initial / 0 final /
  5 verifier.

  The hermetic proof gate confirms 13/13 TPCH after 1,633/56,327 ms (report
  SHA-256
  `d641e3445696fce0f0a367a4f586aa20543d73cb6408a7cf0fefe49f42d64b47`)
  and 17/17 TPC-DS after 12,286/97,337 ms (report SHA-256
  `b6005eb3ed976d111756976959571916ccafa29072f329f33c2eb6f166f27278`).
  q13/q16 are `VERIFIED_BOUNDED` there after 106/2,570 and 105/3,626 ms.
  Exact coalesce preservation initially made q15 return solver `UNKNOWN`; the
  compact exact identity encoding restores it to `VERIFIED_BOUNDED` in 7,575
  ms without reverting value semantics. Validation passes 638/638 Python
  verifier tests, 259/259 C++ exporter tests, 14/14 policy tests, and the 5/5
  proof-floor target. Commits `b39a8b47b46`, `84515a6c887`,
  `636201f517f`, `658b98e1eee`, and `719e0a0d3e5` record the semantic slices
  and compact exact encoding. No new optimizer bug or counterexample was
  found.

  Milestone 69 is implemented by commits `743643cc20f` and `ad3613f1816`.
  It adds no snapshot, exporter, scalar, or StageGraph semantics. The logical
  evaluator recognizes only a Filter whose input is a private left-deep spine
  of keyless Cross joins with literal non-null `TRUE` residuals. The snapshot
  must have no StageGraph; the Filter may have no consumer subplan or edge
  input; each spine node must have exactly its next spine node, or the Filter,
  as its sole parent. Cached or edge-supplied spine nodes, explicit overrides
  of the seed or any spine factor, and subplan roots all retain the original
  path.

  The admitted predicate is either one direct equality or a flat top-level
  conjunction containing ordinary, non-null-safe direct column equalities.
  Nested conjunctions, computed operands, cross-type equality, and every
  nonconjunctive context are ignored. A pending right factor may move earlier
  only when all left key columns are already available and the existing
  static direct unique-RHS gate proves that the factor is an unfiltered,
  unlimited direct Scan whose same-type equality columns cover a declared
  non-null catalog unique key. The scheduler repeatedly takes the first such
  factor; if none is ready, it takes the first pending Cross factor unchanged.
  The existing runtime gate still rechecks exact schema, payload, occurrence,
  slot, and presence provenance before compacting. A runtime near miss executes
  the original Cross, not a weakened join.

  Scheduling is a private evaluation representation: the captured plan and
  node identities are unchanged, every seed and right relation still uses the
  ordinary evaluator/cache path, and the original Filter remains intact with
  all residual conjuncts. Cross reassociation preserves its bag, a promoted
  equality only removes pairs that the retained Filter necessarily rejects,
  and the final map restores the original Filter-input columns and value order
  exactly. The 4,096-row and 16,384-pair construction audits remain in force.

  Focused row-bound-two/task-bound-two q64 evidence is `FORMULA_EMITTED` after
  7,356/121,306 ms of preparation/verifier work. Its report SHA-256 is
  `858677db83fd7af634fc96982214c3a4d4d2db3eba2aa6f968a7ac007a22e2ec`.
  The canonical SMT artifact is 279,504,238 bytes and 3,589 lines, has SHA-256
  `478b4d0b72cef35684ef2c418afc11fa3d55ae9fbd21b8a0af866ca0f676c124`,
  and the focused process tree peaks at 2.24 GiB. This raises the checked-in
  formula floor to 86; no solver was run, so it adds no bounded proof,
  counterexample, or optimizer correctness finding.

  Milestone 70 is implemented by commit `3d74b1eadcf`. The exporter recognizes
  only a non-rename physical Map expression whose complete lambda body is
  `Unwrap` of one direct physical `Optional<String>` input member and whose
  exact output is non-null `String`. It keeps the normal expression as a
  direct column and adds optional projection metadata
  `error_on_null: true`; omission means false. Type annotations, physical
  provenance, safety metadata, and the exact Map output type are all checked.
  Python independently requires the same direct nullable-String source and
  non-null result. Evaluation carries the source payload but makes it
  non-null only behind an outcome error equal to the disjunction of
  `row.present AND source.is_null` for every marked projection; successful
  results therefore cannot observe the placeholder payload. Existing
  inherited, subplan, and cardinality errors compose through the ordinary
  result-family path.

  Checked projection errors are admitted at only two demand-safe topologies:
  the Project is the main result root and every marked output is returned, or
  it is a private direct sole-consumer RHS of one `left_semi` Join and every
  marked output is an exact RHS key. Subplan descendants, fanout, a left input,
  another join kind, an unkeyed marked output, or another consumer fail closed.
  A `Limit` consumer need not demand every projected row, so that topology is
  rejected rather than assuming global projection eagerness. The retained
  empty-left keyed-semi runtime case confirms the q8 RHS eager/build-side
  premise.

  Production TPC-DS q8 prepares successfully in 695 ms, retains both
  snapshots, constructs the bounded formula, and returns
  `VERIFIED_BOUNDED` at row/task bounds 2/2 after 2,041 ms with a 60,000 ms
  solver timeout. The post-M70 semantic partition is TPCH 20 formula /
  0 unsupported / 2 no-pair and TPC-DS 67 / 14 / 18. Across both suites,
  87/121 queries construct formulas (71.9%), as do 87/101 exact captured
  pairs (86.1%), 87/93 preparation successes (93.5%), and 87/91 verifier
  entrants (95.6%). The remaining primary unsupported results split into ten
  initial-export, zero final-export, and four verifier cases. The bounded
  proof floor is 31/121 (25.6%), 31/87 constructed formulas, and 31/31 curated
  obligations.

  The complete post-M70 formula dashboards are TPCH 20 / 0 / 2 after
  3,068/91,127 ms (report SHA-256
  `9c83253534089d26e0c17a3a049e3411e5e4720707cdadf57fb9bc3db09a2d01`)
  and TPC-DS 67 / 14 / 18 after 64,964/698,120 ms (report SHA-256
  `350b349fa618e016f3a485a7c614566694b6d947c202a0bc762d0dbfcaddf47b`);
  both policy evaluations are valid with no violation. At that checkpoint,
  the proof-floor reports were 13/13 TPCH after 1,540/61,083 ms (SHA-256
  `dfefd2bfd26a5013bc42d7d22a3f60620ece8aec8cbeaa271686a249aa0afca7`)
  and 18/18 TPC-DS after 13,313/103,833 ms (SHA-256
  `ea06c1e3e9072c5a9f9241233647e9c05696ec730cb1879e2ae291e891c4d214`),
  all `VERIFIED_BOUNDED`; the complete proof-floor target passes 5/5.
  Validation also passes 673/673 Python tests, 264/264 C++ exporter tests,
  51/51 inspector tests, the 14/14 policy target, and the 1/1
  `RealRuntimeStringUnwrapEagerBoundaries` runtime regression. No new
  optimizer bug was found in M70; nine production optimizer bugs remain the
  cumulative historical total.

  Milestone 71 is implemented by `d2979a0e459`, with the preparation/formula
  floor recorded by `a754b499484`. The audit first reproduced q31's exact
  construction failure: each final local Sort received 2,048 candidates and
  rejected 2,096,128 unordered pairs above the 16,384-pair cap; the network
  alternative required 67,584 comparators. Bypassing only that check would
  have exposed a 4,096-candidate, 8,386,560-pair Merge.

  The surviving construction adds no Sort primitive, uniqueness certificate,
  or cap. It reuses the existing exact quotient for one non-NULL logical
  occurrence whose task copies carry pairwise contradictory routing facts.
  Presence is ORed; values, NULL lanes, and hidden Decimal/integral-AVG state
  are ITE-selected; Decimal bounds join conservatively; and only common facts
  survive. Broadcast requests this representation for every eligible
  producer-task group before fan-out. HashShuffle requests it when either the
  existing more-than-eight-row trigger fires or the explicit transported
  payload exceeds eight candidate cells. Root and serial/parallel Union
  gathers retain the existing row heuristic. Replicated Broadcast copies,
  unknown occurrences, and non-contradictory copies remain explicit.

  q31 consequently has 64 candidates and 2,016 pairs in each local Sort, then
  128 candidates at Merge. The exact ordinal Merge costs 8,128 global pairs
  plus 8,064 producer-order constraints, or 16,192 below the unchanged 16,384
  cap. Focused formula-only evidence is `FORMULA_EMITTED` after 626/74,700 ms
  (SHA-256
  `3ee9274960c93c0bb42c014bd722e7ac9e194aefdf3150567a09ebd7b3b51a4e`).
  The pinned 60-second solver experiment is `UNKNOWN` after 771/140,345 ms:
  the deadline expires before branch 1/4 `left_language_empty` (SHA-256
  `77142a4284c5ddc51ce5a7fbe6180ace15b511da608aeeb0588255436c2a1059`).
  It adds no proof, counterexample, replay, or optimizer bug.

  The post-M71 semantic partition is TPCH 20 formula / 0 unsupported /
  2 no-pair and TPC-DS 68 / 13 / 18. Across both suites, 88/121 queries
  construct formulas (72.7%), as do 88/101 exact captured pairs (87.1%),
  88/93 preparation successes (94.6%), and 88/91 verifier entrants (96.7%).
  Primary unsupported outcomes split ten initial / zero final / three
  verifier. The proof floor remains 31/121 (25.6%), 31/88 formula-covered
  queries (35.2%), and 31/31 curated obligations.

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
  all `VERIFIED_BOUNDED`; the proof-floor target is policy-valid and passes
  5/5.

  Mixed-prefix equality commit `02b067a8d27` remains an independently exact
  sequence-scaling improvement, with exhaustive sparse/prefix, NULL, ordinal,
  capacity, and operand-orientation differentials. The q31 audit showed that
  it is not exercised here: both compacted roots retain sparse ordinals. It was
  therefore not the actual prerequisite for this construction.

  Milestone 72 is implemented by `9b9133fda8c`, with preparation/formula floors
  recorded by `a4a82dd6f7e`. A Script now remembers the concrete strict-UTF-8
  value behind each of its own `string_atom()` terms by term identity. Its
  partial equality theorem returns TRUE or FALSE only when both inputs are
  those owned atoms. A value lane that is not one of those exact retained
  objects—including source, opaque, unregistered, or foreign-Script
  terms—gets no concrete-atom fact and follows the existing generic SMT
  equality path. Cross-Script term mixing is outside the Script-owned verifier
  invariant.
  String and Utf8 share raw-byte atoms, while ordinary/null-safe SQL NULL
  envelopes remain unchanged. Ordering, `IN`, relational `not_distinct`, and
  generic SMT equality are outside this slice.

  The Initial plans for q11 and q74 each Cross four aliases of an eight-row
  `UnionAll`. Deferred literal ranks formerly kept 4,032 of 4,096 combinations
  live and reached an 8,126,496-pair Sort preflight. Exact atom equality leaves
  256 live rows and carries both complete final StageGraphs through formula
  construction without raising a cap. Focused formula-only q11 is
  `FORMULA_EMITTED` after 853/19,725 ms and q74 after 570/31,719 ms (combined
  SHA-256
  `cf2106b2ae0e2d08dad1d59640fdde7fe0b51a5bde74e517fa116ddf0eab0018`).
  Separate pinned 60-second runs both return `UNKNOWN` before branch 2/4
  `right_language_empty`: q11 after 757/79,751 ms and q74 after 529/91,782 ms
  (combined SHA-256
  `76fffb3c86848126cd8d88473ae069476e2e63940c1b9c1eec0dcfe9fd51f467`).
  They add no proof, counterexample, replay, or optimizer bug.

  At the historical post-M72 checkpoint, the semantic partition was TPCH
  20 formula / 0 unsupported / 2 no-pair and TPC-DS 70 / 11 / 18. Across both
  suites, 90/121 queries construct formulas (74.4%), as do 90/101 exact
  captured pairs (89.1%), 90/93 preparation successes (96.8%), and 90/91
  verifier entrants (98.9%).
  Primary unsupported outcomes split ten initial / zero final / one verifier.
  The proof floor remains 31/121 (25.6%), 31/90 formula-covered queries
  (34.4%), and 31/31 curated obligations. The full verifier passes 680/680
  tests and policy passes 14/14.

  The historical complete post-M72 formula dashboards are TPCH 20 / 0 / 2 after
  2,856/91,743 ms (report SHA-256
  `355d7d23510c7a239ac8cd25200e3af1572375ec2e3e9e109bb50bbb61c36528`)
  and TPC-DS 70 / 11 / 18 after 65,567/707,008 ms (report SHA-256
  `e56362daf059e0869496d27f06eedfae94b1703bc5ea7c57a480c1436e187c1b`);
  both formula policies are valid. Fresh proof-floor reports verify 13/13
  TPCH after 1,511/60,760 ms (SHA-256
  `42f292e6e8968457064acf858ccb2bfaf1a2ab5f76c5577620f32f494ed47d67`)
  and 18/18 TPC-DS after 13,518/105,096 ms (SHA-256
  `68fbd23bac77cf75a00bf83a2bcddf0e6e9877ebdea116e6c7bdf9c83a8917c4`),
  all `VERIFIED_BOUNDED`; the proof-floor target is policy-valid and passes
  5/5.

  Milestone 73 is implemented by `7cf4a049f61`, `1e53b1fb9a0`, and
  `8c5eca71246`, with the q4 preparation/formula floor recorded by
  `a96c2a0be8f`. It combines three independently exact reductions. First,
  within the existing private delayed-Cross gate, a Filter conjunct owned by
  exactly one factor may reject only rows for which the row-presence guard
  conjoined with SQL-is-true of that conjunct is the canonical `FALSE` term;
  every other row and the complete original Filter are retained. Second, the
  common Join entry erases only slots whose presence guard is canonical
  `FALSE`, before pair/output audits. Third, the scheduler may commute only
  its innermost Cross and rebase onto its former right factor when the former
  seed is an unfiltered, unlimited direct Scan whose same-type equality
  columns cover a declared non-null unique key. The existing runtime
  certificate is still rechecked, later factors use the unchanged scheduler,
  and original output-column order is restored.

  The RBO TPC-DS workload fixture required a separate correction:
  `e55c37f967f` restores q4's canonical web-sales discriminator from the
  accidental `'s'` to `'w'`. The erroneous fixture made the captured query
  statically empty; this was a workload defect, not an optimizer correctness
  bug. The corrected pair now returns `FORMULA_EMITTED`. Its canonical
  269,969,712-byte, 2,032-line formula has SHA-256
  `d4740aeb93d18e9b2e1338bcb9db58d98eedd1e93d3d5f91cf02a38bc7f0a92d`
  and took 70.16 seconds to construct from the captured snapshots. No solver
  result is inferred from construction alone. A separate two-row/two-task run
  with a 60-second global Z3 deadline is `UNKNOWN` after 1,689/200,603 ms:
  the deadline expires before branch 1/4 (`left_language_empty`). Its report
  has SHA-256
  `c2280dd7284f7ae7593c4a0fb8121d7830646c8d0db83c3f681973faf461dc38`.
  Normalizing the harness cluster identity and timeout makes its formula
  byte-identical to the canonical obligation. It produced no model, witness,
  candidate, counterexample, replay, proof, or optimizer bug; the cumulative
  historical optimizer-defect count remains nine.

  The post-M73 semantic partition is TPCH 20 formula / 0 unsupported /
  2 no-pair and TPC-DS 71 / 10 / 18. Across both suites, 91/121 queries
  construct formulas (75.2%), as do 91/101 exact captured pairs (90.1%),
  91/93 preparation successes (97.8%), and all 91/91 verifier entrants.
  Primary unsupported outcomes split ten initial / zero final / zero verifier,
  so no exact pair currently stops at verifier-side formula construction.
  The proof floor remains 31/121 (25.6%), 31/91 formula-covered queries
  (34.1%), and 31/31 curated obligations.

  A fresh complete TPCH formula dashboard remains 20 / 0 / 2 after
  3,001/95,255 ms (report SHA-256
  `19d2d5b34053889df31905fe0811a212defdfc0b8abb9bd846f088fdd452d22f`).
  The fresh policy-bound complete TPC-DS dashboard is 71 / 10 / 18 after
  67,223/814,378 ms (report SHA-256
  `dfb7976c5207a9fdf2fd31d79fc950fedb0d2473b899526c28ab2ff8c2305e41`).
  Its embedded policy requires and observes all 71 formula rows, satisfies all
  71 pinned preparation rows, and reports zero violations.
  Fresh proof-floor reports verify 13/13 TPCH after 1,569/62,282 ms
  (SHA-256
  `28079d5aaf1af70d6badd77f14652d28893f1b149acdcc0d6fda96f1fc590246`)
  and 18/18 TPC-DS after 13,463/103,079 ms (SHA-256
  `1b34081c5e98dcf9b7f6bfb82d59491d7862b40b4c14a1a3a45711bcfadbefa6`),
  all `VERIFIED_BOUNDED`; the proof floor is unchanged.

  Milestone 74 is implemented by `99557229439`, `36b7dd75d96`, and
  `8c9c29ceaf7`, with q28 formula policy in `26c6d0387b3` and proof policy in
  `1545921b5d1`. It adds raw aggregate-code equality for canonical nullable
  Decimal count-distinct and the closed staged Decimal AVG-carrier theorem
  defined above. C++ alone certifies each exact physical tuple-`Nothing` pad
  and normalizes it to logical nullable-Decimal NULL. Python independently
  validates that normalized logical NULL topology. Together the checks require
  one producer, a binary unordered identity-Union tree, direct
  Intermediate/Project leaves, keyless matching aggregate traits, unique
  ownership, carrier-only routing, and no ordering, hashing, fanout, aliasing,
  malformed descriptor, or state exposure.

  Validation passes 693/693 Python checks (676 functional, 16 lint, and one
  import), 267/267 C++ exporter tests, 14/14 policy tests, and the 5/5
  proof-floor target. The preserved pre-policy focused q28 formula report
  returns `FORMULA_EMITTED` after 696/1,196 ms (report SHA-256
  `69fa31b540190c36e08d6b92a10df9a44e66004fd206c9889704d3ade2855c49`)
  but is formula-construction evidence only: its embedded policy does not
  require q28. The later focused solver report is `VERIFIED_BOUNDED` after
  686/11,496 ms (report SHA-256
  `7b52eaf52dbd17ccded3eb9cb7565a78a9ec1216ffef63b9097be7cfafe5e7a4`).
  A deliberately corrupted snapshot returns `COUNTEREXAMPLE`, but the
  production pair proves within the two-row/two-task bound; this is harness
  evidence, not an optimizer finding or replay, and the cumulative historical
  defect count remains nine.

  The post-M74 semantic partition is TPCH 20 formula / 0 unsupported /
  2 no-pair and TPC-DS 72 / 9 / 18. Across both suites, 92/121 queries
  construct formulas (76.0%), as do 92/101 exact pairs (91.1%), 92/93
  preparation successes (98.9%), and all 92/92 verifier entrants. The proof
  floor is 32/121 (26.4%), 32/92 formula-covered queries (34.8%), and 32/32
  curated obligations.

  The fresh TPCH formula dashboard is 20 / 0 / 2 after 2,996/107,275 ms
  (report SHA-256
  `35187d30af02a953f75e94d80922589987432ac970e4930b2f717296141c679a`).
  The fresh complete TPC-DS formula dashboard reports 72 `FORMULA_EMITTED`,
  9 `UNSUPPORTED`, and 18 `OPTIMIZER_FAILURE`; preparation is 73 succeeded /
  26 failed. It spends 71,782/895,806 ms in preparation/verifier work, with
  q28 at 769/1,245 ms. Its embedded policy is valid with zero violations
  (report SHA-256
  `65dfe8400b01a9b4fa66b1907ac9e8569ba47d8d7dcfe459750251450d7d88b4`).
  Fresh proof-floor reports verify 13/13 TPCH after 1,591/67,583 ms (SHA-256
  `d8555efcaa715565a44a89f3fa94a1c3d904c170153f4e0182e571b36f093dc5`)
  and 19/19 TPC-DS after 14,667/126,545 ms (SHA-256
  `475ebb9751f19d6a3d71fb8dee93a6c2dda082e6c7c72f993a14eac1190454cb`),
  all `VERIFIED_BOUNDED`; q28's complete-floor row spent 746/13,129 ms.

  A focused version-five audit of TPC-DS q12, q20, q49, q51, q53, q63, q89,
  and q98 preserves exact pairs despite failed preparation. All eight are
  semantically unsupported: window callables dominate, q49 first exposes a
  Decimal scale-changing cast, and q51 exposes a secondary range-read boundary.
  It spent 3,186/0 ms and produced report SHA-256
  `37b983f3247c653f5bf4a52c79375cdbc7df588ac79bd893d3a5a89ae25e16e0`.

  The fresh q84 blocker audit repaired MiniKQL String-capacity overflow in
  `82cfcd837f4` and synchronized the exporter's exact `UINT32_MAX` result bound
  in `daab603c2f1`. The widened totality gate admits two maximum Olap cells
  alone, but q84 remains one byte beyond it because of its exact `", "`
  literal. A separate two-row real-YDB demand probe then confirmed production
  optimizer defect ten: legacy returned the selected non-NULL `UNWRAP(S)` row
  for `ORDER BY Id LIMIT 1`, while new RBO evaluated a discarded NULL row and
  failed. Commit `c2c66fb1d7b` fixes that immediate TopSort shape and retains
  the regression. The follow-up trace and independent-key runtime probe found
  optimizer defect eleven: map normalization pushed q84's computed Concat,
  and a checked `UNWRAP`, below row-discarding Filter and Join boundaries even
  though expression pushdown was disabled. Commit `564010e2e4e` removes that
  unsafe mode and moves only direct column accesses and semantic renames.
  At that pre-M75 checkpoint, q84 retained its computed projection after row
  selection but still stopped at the exact checked-Concat result-bound gate.
  The regenerated final trace is
  `Map[Concat] -> Limit[100, Final] -> Map -> TopSort[100, Intermediate] ->`
  joins: Concat is on the stage-11 consumer side and runs on at most 100 rows.
  Before Milestone 75, q84 therefore still had no formula or proof and the
  post-M74 numerical coverage remained the current baseline.

  Milestone 75 implementation commit `cda99a952cb` separates this partial
  expression from the total opaque branch instead of rejecting it. The
  successful result and possible error are shared by canonical fingerprint and
  ordered stored-String arguments, and admission is restricted to the
  independently checked demand corridor and row-bound proof above. The focused
  production formula-only row
  is `FORMULA_EMITTED` after 186/4,461 ms of preparation/verifier work (report
  SHA-256
  `e9e59667815b676420d05b7e70decc3d106b22dbc28e7202c88d3979915341ce`).
  Its Initial and Final snapshots have SHA-256 values
  `9f5d05ad7d373a9160df5d4d37220795dd615e42321d9c6dec56f79c33b5740a`
  and
  `39371b1e7a6b6c2cc97fa215721ae8cc3cb137437b5713fcae95dcf8076186e5`.
  They preserve the exact same complete checked fingerprint, whose prefix is
  `format:13:yql-opaque-v1;node:8:callable;content:6:Concat;`. Its ordered
  arguments are `/Root/test/ds/customer.c_last_name`, then
  `/Root/test/ds/customer.c_first_name`. The canonical 9,339,706-byte,
  977-line SMT formula has SHA-256
  `4ba91650e4486b5e7578a47708c9aeea8750edd44cb5cb4d596ef79bc0a86d97`.

  The separate normal 60-second solver row returns `UNKNOWN` after 64,577 ms:
  `counterexample decomposition remains unresolved; first: global solver
  deadline expired before branch 2/4 (right_language_empty)` (report SHA-256
  `5713bd9065c40c07e31d4f9a1a20cc0fa77e1eaaf62a2b0ef78441f79ab1e9f8`).
  This is formula-construction evidence, not a bounded proof, counterexample,
  replay result, or optimizer finding. Policy commit `c6fbadcc9a8` pins q84
  only at preparation and formula construction. The complete post-M75
  formula-only TPCH dashboard is 20 / 0 / 2 after 3,198/112,378 ms of
  preparation/verifier work (report SHA-256
  `dc0ec2ac610b767e33fbb6e30ab9f1d60ec09beca8ac0a610a68ed89ecc88b2d`).
  TPC-DS is 73 / 8 / 18 after 113,191/1,239,636 ms, with q84 at 177/4,490 ms
  (report SHA-256
  `bc7f0576091888f493971a21228902fd57c75d06c6bc3772d5ad636c531663ce`).
  Both embedded policies are valid with no violations. The authoritative
  partition is therefore 93 / 8 / 20 overall: 93/121 workload queries (76.9%),
  93/101 exact pairs (92.1%), and all 93/93 preparation successes and verifier
  entrants construct formulas. The checked-in proof floor remains 32/121
  (26.4%), 32/93 formula-covered queries (34.4%), and 32 obligations: 13 TPCH
  plus 19 TPC-DS. Fresh post-M75 proof-floor reports are policy-valid with no
  violations and verify every obligation as `VERIFIED_BOUNDED`: TPCH passes
  13/13 after 1,744/81,538 ms of preparation/verifier work (report SHA-256
  `8fe212d2536b7561e630dbd3e1b3bac9b8dfa7510c55b1f2a91114e4b791c5f8`),
  and TPC-DS passes 19/19 after 15,499/124,840 ms (report SHA-256
  `6fe57d9e56cd4ed23755494831a2cd1450ad5a103652583fa234c5292cc4b023`).
  This is 32/32 curated obligations.

  Milestone 76 implementation commit `dbdae0a107f` adds one closed
  relation-dependent expression instead of general window semantics. The
  source must contain exactly one accepted window in one main-plan Project.
  Its scalar root is exactly
  `DecimalDiv(DecimalMul(member(input), Int32("100")),
  YqlAggWin(sum, ..., member(input)))`: the numerator and window consume the
  same direct `Optional<Decimal(35,2)>` member, the result is the same exact
  nullable Decimal type, and the factory is exact option-free
  `YqlWinFactory("sum")`. The matching raw definition is one five-child,
  non-inherited `YqlWindow` with one named `Optional<String>` partition
  `YqlGroup`, its unary lambda and direct four-child `YqlGroupRef` at canonical
  index 3, no ordering, and the ordered frame settings
  `("type","rows"), ("from","up"), ("to","uf")`. That is ROWS from
  unbounded preceding through unbounded following. Source metadata and every
  scalar node pass the existing safety checks and a 64-node/16-depth audit
  bound. Renames are replayed in order before the resolved partition column is
  exported.

  The Project must directly consume a nonempty grouped Aggregate in logical
  phase `Undefined`, or one phase-`Final` Aggregate directly over one matching
  phase-`Intermediate` Aggregate. The partition is a direct Aggregate key; the
  window input is the one plain nullable-Decimal SUM output; final and
  intermediate keys, SUM state, type, producer, and use match exactly. The
  Aggregate-to-Project path has one consumer, the Project cannot fan out, and
  subplans and additional window expressions are rejected. Python checks that
  topology independently. It evaluates `window_sum` over the Project's current
  input relation, and therefore independently for each StageGraph consumer
  task: rows join the current row's partition by SQL `IS NOT DISTINCT FROM`,
  including NULL with NULL; NULL Decimal inputs do not contribute; no non-NULL
  input yields NULL; exact Decimal SUM and bag multiplicity are preserved.

  That task-local meaning exposed bounded pre-physical StageGraph-routing
  finding 12 before the repair. TPC-DS q12's final split Aggregate shuffled on
  all five group keys
  (`i_item_id`, `i_item_desc`, `i_category`, `i_class`, and
  `i_current_price`) and the window Project was fused into the same final
  stage. Rows equal on window partition `i_class` could therefore be separated
  by another group key. The bounded obligation returned `COUNTEREXAMPLE` after
  27,588 ms. Its coverage report, raw verdict, and canonical SMT SHA-256 values
  are respectively
  `cc6576c46b6603a355b81651fde20056aa0fb3842a74b8133351e3aa307adf28`,
  `923e3e56d4a09fd04dc4fb552a764676b55a4e98247720aeeb05c42c9dfccaff`,
  and
  `c849d007958deac4851d8b91bfbf1616cbc78a4ac9f38091c1af78bf51f95709`.
  Initial snapshot SHA-256 is
  `751ac06fb3658a4e6c45cde2e9f3e3b50402353a411b1ffbad9e12e68b01249c`;
  the pre-fix Final snapshot is
  `ce0261fad4adafb790145b022c086e20bacf0b37c7465e69c5b7abed66a69462`.

  Routing commit `70ab3d3631c` always gives a window-bearing Map its own
  stage. For fully tracked, structurally matched windows it hashes on the
  ordered nonempty intersection of their resolved partition-column sets. Any
  such common subset is sound because rows equal on a complete partition agree
  on every selected key. A global, untracked, malformed, inherited, stale,
  unavailable, or disjoint window instead receives a nonparallel `UnionAll`
  gather. In particular nullable equal keys, including NULL, follow the same
  deterministic runtime HashV2 route. q12, q20, and q98 retain the Aggregate's
  original full-key HashV2 boundary and gain a second HashV2 boundary on
  `i_class` immediately before the window Project. The output index is
  allocated once for the new connection. Window metadata is immutable apart
  from ordered rename history, participates in input-IU discovery, and
  prevents Filter, correlated-Filter, Limit/TopSort, stage-limit, predicate
  factoring, and scalar-pruning rules from moving the relation-dependent
  expression back across that boundary.

  The first complete post-routing dashboard exposed a metadata-transport
  regression: q51's broader source `YqlWindow` was attached to the final
  `KqpOpMapElementLambda` even though its partition and ordering lambdas still
  referenced source-row members `x.item_sk` and `x.d_date`. Final-plan type
  annotation failed before Final capture. Commit `a7095c6a797` transports only
  the exact self-contained M76 definition above. Other raw `YqlAggWin`
  expressions remain window-bearing, receive the conservative serial gather,
  and fail closed at export, but their context-dependent metadata no longer
  leaks into the final KQP AST. The focused q51 real-host regression passes
  1/1 in 3.99 seconds, sees neither missing-member diagnostic, and captures
  exactly Initial then Final with both unsupported. Its focused benchmark has
  `capture_count=2`, fails later preparation at the unrelated range seam after
  414 ms, and has report SHA-256
  `b328b80f54a90a0ae3e01e38dc559944fe807e2a7c13e9643efed4d634af5c23`.
  Test-lifetime commit `78e255b5e1f` gives the module resolver an owned
  context before the runner is created. After that cleanup the full real-host
  integration gate is GOOD: one suite / 44 tests in 19.713023 seconds, q51 in
  3.916390 seconds, with approximately 32.190 seconds of `ya` wall time.

  After the fix q12 returns `UNKNOWN` after 61,153 ms because the global
  deadline expires before branch 3/5 (`left_outcome_0_unmatched`), not because
  equivalence was proved. Its coverage report, raw verdict, and canonical SMT
  SHA-256 values are respectively
  `26d7b4b1fe539508720a93b636f409c93c922830e0a200d0c8df4ebaa4067263`,
  `846a4688309d7595a0bc648627e7c54b9d87e770c3f515a8aabaa7a43958b7d8`,
  and
  `2226888e0242bd0bd2f5163bc3d6e7c2d88668f4e3f96f5141dcacbbc3eab743`;
  its post-fix Final snapshot SHA-256 is
  `1cba9b0f71c48c11e084aec24056781d91b942d3df64de55647bf5045ee49271`.
  Physical replay is not yet available: preparation fails closed after capture
  with `Missed callable: YqlAggWin`, so this finding is established from the
  exact source grammar, StageGraph/task semantics, symbolic candidate, and
  focused routing regressions rather than a compiled runtime divergence.

  Focused q12/q20/q98 formula construction succeeds for all three exact pairs
  after 209/934, 137/926, and 167/933 ms of preparation/verifier work; the
  three-row report SHA-256 is
  `9c79494b48140df09addf88af4e76150ffd87807e80f83ed2a35c75a5f3cb3d9`.
  Policy commit `1da14eb637b` advances the strict input schema to version five,
  adds those three rows to the formula floor, and adds the supplemental
  exact-pair list q49/q51/q53/q63/q89. The effective pair floor is 20 TPCH plus
  81 TPC-DS, 101 total; the formula floor is 96. Its q51 mutation locks the
  exact Initial-then-Final requirement and rejects a zero-capture regression.

  The complete post-policy TPCH formula dashboard is 20 / 0 / 2 after
  3,403/108,336 ms of row-summed preparation/verifier work, satisfies all 20
  pair and formula requirements with no violation, and has report SHA-256
  `c5ad2d811bfdb4933d3b538a932f01d621726afc468aa2aa38c57ea303eae9a7`.
  The complete TPC-DS dashboard is 76 / 5 / 18 after 76,740/896,728 ms,
  observes 81/81 effective exact pairs, 76/76 formulas, 73 preparation
  successes, and 76 verifier entrants with no violation, and has report
  SHA-256
  `5015b3fe8e47e1aad88295cc0f4b59088ef5f9087b99c4792ab7b0c999f796c1`.
  The authoritative totals are 96/121 formulas (79.3%), 96/101 exact-pair
  formulas (95.0%), 96/96 verifier entrants, and 93/93 preparation successes.
  The proof floor remains 32 obligations and no M76 window row joins it. Fresh
  reports verify TPCH 13/13 after 1,843/81,211 ms (SHA-256
  `33534f30a78bd392b7abd6206cfdd0ba94e8ef7bfb393966d4e88555fdd040b4`)
  and TPC-DS 19/19 after 15,865/125,293 ms (SHA-256
  `9b7ee8d66acec051d8f20b53e8e100c9d672d718fc07e830b3894477e0839dac`).
  Both proof policies are valid with no violation; proof mode does not enforce
  the dashboard-only exact-pair floor.

  Milestone 77 semantic commit `3f9b9c8b2c6` extends only that closed window
  seam. An admitted AVG leaf is one direct five-child
  `YqlAggWin(YqlWinFactory("avg"), ...)` with a Unit-typed factory, nonempty
  name, no options, and
  exact `Optional<Decimal(35,2)>` input, descriptor, and result. The matching
  source is one non-inherited five-child `YqlWindow` with no ordering and the
  same ordered whole-partition ROWS frame as M76. It contains one through four
  ordered `YqlGroup` entries. Each is a unary lambda whose body is one direct,
  named, four-child `YqlGroupRef`; its canonical unsigned index, source name,
  one-field Struct descriptor, type annotation, and reference agree exactly.
  Names and indices are independently unique. AVG partition fields are exact
  `Optional<String>` or `Optional<Int64>`; ordered rename batches are replayed
  and the resolved names must remain nonempty and unique. Both the source
  definition and AVG expression safety trees are capped at 128 nodes and depth
  16. q53 uses nullable-Int64 `i_manufact_id` and q63 uses nullable-Int64
  `i_manager_id`, each at raw Aggregate ordinal 0. q89 uses nullable-String
  `category`, `brand`, `store_name`, and `company_name` at raw ordinals 0, 2,
  3, and 4 of its six-key Aggregate; `class` and `d_moy` are grouping keys,
  not window partitions.

  Every resolved partition must occur exactly once as a direct grouped
  Aggregate key with the same nullable type. C++ audits the raw source
  index/name pair against the ordered Aggregate-key vector before export; only
  the ordered resolved names cross the wire. The window input selects
  exactly one Aggregate output by result name. That named carrier must be a
  plain, non-distinct, non-unwrap `sum` of exact
  `Optional<Decimal(35,2)>`; other same-source SUM traits do not become
  ambiguous because only the exact named output is selected. The Aggregate is
  phase `Undefined`, or phase `Final` directly over one phase-`Intermediate`
  Aggregate with identical ordered keys and exactly one matching named SUM
  state producer and final use. The Aggregate, optional intermediate, and
  Project retain the private single-consumer/no-fanout/no-subplan topology.

  The production metadata carrier now accepts the same closed one-through-four
  source-definition syntax, including only nullable String/Int64 descriptors
  with canonical unique indices. This is a syntactic transport prefilter, not
  the exporter certificate: annotation agreement and index-to-Aggregate
  ordinal matching are checked only at export. q51 remains fail closed: its
  ordered, context-dependent definition is not transport-safe and therefore
  cannot recreate M76's
  missing-member regression. The existing window barrier gives the
  window-bearing Map its own stage. A fully tracked single window hashes on
  its complete resolved partition vector (the general multi-window rule uses
  a nonempty common subset); unavailable, stale, malformed, or unsafe metadata
  still receives a serial `UnionAll` gather. Movement rules continue to treat
  the window as a relation-dependent barrier.

  Python decodes `window_avg.partition_by` as an ordered unique array of one
  through four columns and independently rechecks the types, Aggregate
  dataflow, and private topology. For each consumer task and each present row,
  it scans that task's current relation, admits rows whose complete partition
  tuple is SQL `IS NOT DISTINCT FROM`, and ignores NULL window inputs. It
  accumulates exact Decimal special-aware `AggrAdd` state plus a `Uint64`
  non-NULL count. Count zero yields NULL. Otherwise division by the positive
  count uses the runtime Decimal nearest/ties-to-even rule and narrows at the
  same scale. Construction fails closed unless the finite sum bound is below
  `10^35`, the count bound is below `2^64`, and the relation-local all-pairs
  work stays within the existing row-pair cap. This is exact bounded,
  task-local AVG, not general ordered/sliding window support.

  The scalar extension is likewise closed: unary `Abs` is accepted only when
  its argument and result are both exact `Optional<Decimal(35,2)>`. The same
  validator guards both the exporter and the opaque-expression positive list,
  so nested `DecimalDiv(Abs(Sub(...)), ...)` is admitted without opening a
  generic `Abs` family. NULL propagates; the signed Decimal value code is
  negated exactly when negative, so finite negatives and `-Inf` become
  positive while `+Inf` and the positive NaN sentinel pass through.

  These semantics are tied to audited runtime sources rather than inferred
  from the workload spelling. `yql/essentials/mount/lib/yql/window.yqls`
  supplies the Optional wrapper, Decimal `WidenIntegral` SUM state, `Uint64`
  count, `AggrAdd`/`Inc` updates, NULL default, and current
  `Cast(Div(sum,count), item type)`. Decimal division and same-scale narrowing
  follow `yql/essentials/public/decimal/yql_decimal.cpp` and `.h`; the positive
  count branch rounds to nearest with even ties. Decimal Abs follows
  `yql/essentials/minikql/invoke_builtins/mkql_builtins_abs.cpp`, whose Decimal
  builtin applies `SafeNeg` only to a negative raw `TInt128`. The routing claim
  additionally assumes runtime HashV2 collocates equal ordered nullable tuples,
  including NULL components.

  Focused formula-only evidence is 3/3 `FORMULA_EMITTED`. q53, q63, and q89
  spend 275/2,383, 238/2,322, and 273/3,233 ms respectively in
  preparation/verifier work. Each exact pair is captured before later physical
  preparation fails with `Missed callable: YqlAggWin`, so replay remains
  unavailable. Their canonical formulas are respectively
  3,489,514, 3,436,841, and 5,757,911 bytes, with SHA-256 values
  `44b183e952114d322ba0f6656364da7e35027049eafbca22e59fc015773291c0`,
  `a78ca722fd57d0a3e9061e6a414f6152a3d391f7d47f3e75a4f18b4cf19b8c4c`,
  and
  `eb385ea6a522499fb76d7c1efc97281107bdf85a38e8a00e94ad949ce3851c12`.
  The three-row report SHA-256 is
  `35bc96ab52ca2dc02ce4a8fea6a762bb3b7d434a27f4a5935a403081a38550d9`.
  Separate real 60-second solver rows are all `UNKNOWN`, after 62,781, 62,842,
  and 63,997 ms. Each reports the same first unresolved decomposition:
  `global solver deadline expired before branch 3/5
  (left_outcome_0_unmatched)`. Their combined report SHA-256 is
  `7b30313b096b9a314b52c56e0048601a267f391d4854e52fd80e7837a4914bfa`.
  These rows add formulas, not bounded proofs, counterexamples, replays, or
  optimizer findings.

  Focused semantic validation passes 11/11 Python tests in two suites across
  the AVG/Abs and routing slices. The routing proof plus wrong-routing
  counterexample takes 4.60 seconds; the four-key q89 mutation counterexample
  takes 1.26 seconds; the `Abs(Sub(...))` self-proof takes 64 ms; and concrete
  NULL/tie/multi-key cases take 4 ms.
  Focused C++ exporter and production-shape validation passes 9/9, including
  the nested-Abs closed-world regression. Policy commit `adfe48088f5` promotes
  q53/q63/q89 from supplemental pair-only rows to the formula floor. The
  checked-in targets are therefore 99 formulas, 101 effective exact pairs,
  and the unchanged 32 proofs; q49 and q51 are the only supplemental pair-only
  rows. The complete post-M77 TPCH dashboard is authoritative at 20 formulas,
  0 unsupported, and 2 no-pair failures after 3,295/108,747 ms of summed
  preparation/verifier work. Preparation is 20/2; all required pair, formula,
  and entry floors pass with zero policy violations. Its report SHA-256 is
  `23dcef98dc8eb5f7be6ca248b89f5d18c6469ad598d4102d764ba33790fd2398`;
  the test takes 114.13 seconds (130.97 seconds wall). The authoritative
  post-M77 TPC-DS dashboard is 79 formulas / 2 unsupported / 18 no-pair
  failures after 76,911/907,768 ms; preparation remains 73/26. It observes all
  81/81 effective exact pairs, including both supplemental rows, all 79/79
  formula-floor rows, and 79 actual verifier entrants against the eight-row
  explicit entry floor. Its policy is valid with zero violations. Report
  SHA-256 is
  `42849489e72f7408aa03c441453c7c48c82e5c24eb0b73f2d74d78fa8d605887`;
  the test takes 990.31 seconds (1,009.59 seconds wall). The authoritative
  combined partition is therefore 99 formulas / 2 unsupported / 20 no-pair
  failures: 99/121 workload formulas (81.8%), 99/101 exact-pair formulas
  (98.0%), all 93/93 preparation successes, all 99/99 verifier entrants, and
  all 101/101 effective pairs. The fresh TPCH proof gate verifies all 13/13
  obligations after 1,802/82,453 ms, with 13 successful preparations, 13 exact
  pairs, a valid policy, and zero violations. Its report SHA-256 is
  `6cd133426b494541647cbd7d618785a7bddab72cd56b01c33f874796171d6219`;
  the test takes 86.39 seconds (103.10 seconds wall). The fresh TPC-DS proof
  gate verifies all 19/19 obligations after 16,306/114,124 ms, with 19
  successful preparations, 19 exact pairs, a valid policy, and zero violations.
  Its report SHA-256 is
  `3a03efa79824900b7a5e4985294de39c290f0696eca253e27bd0d45c08c1e181`;
  the test takes 134.25 seconds (151.42 seconds wall). The proof floor is
  therefore green at 32/32: 32/121 workload queries (26.4%) and 32/99
  formula-covered queries (32.3%), after 18,108/196,577 ms of summed
  preparation/verifier work. All four complete M77 dashboard/proof gates are
  policy-valid with no violations at `adfe48088f5`.
  M77 found no new optimizer defect: the qualified inventory remains eleven
  runtime-confirmed defects plus the one bounded pre-physical q12 routing
  finding.

  Milestone 78 implementation is split across production commits
  `27e3f260017`, `97a03c64ab9`, and `68eb64102c7`, verifier/exporter commit
  `0f12406f6c4`, and policy commit `e926958d96c`. It admits only q49's three
  private Aggregate-to-ratio-Project-to-Rank-Project corridors. Each Rank
  Project contains exactly two direct, option-free `YqlWin(rank)` leaves; the
  complete snapshot contains the six canonical names
  `_yql_anonymous_window0` through `_yql_anonymous_window5`, with local
  execution orders zero and one. Every definition has an empty partition, one
  ascending/nulls-first direct non-null `Decimal(15,4)` order key, and the
  exact ROWS frame from unbounded preceding through current row. Each result is
  non-null `Uint64`. Subplans, mixed aggregate-window leaves, nested Rank
  expressions, additional/fewer corridors, fanout, malformed names, and every
  broader ordered-window shape fail closed.

  The order key is one q49 ratio. The exporter traces it through a separate
  Project to the exact grouped SUM family and requires the observed logical
  `Undefined` Aggregate or matching Final-over-Intermediate split. Across each
  snapshot, six `Int64 -> Decimal(15,4)` and six
  `Decimal(35,2) -> Decimal(15,4)` casts feed those ratios. The latter is the
  sole scale-changing Decimal cast: finite raw codes are multiplied by 100,
  absolute source codes at least `10^13` saturate to signed infinity, and both
  infinities and NaN are preserved. C++ and Python independently enforce the
  source/result types, nullability, cast classification, topology, and the
  two-per-Project/six-per-snapshot limits.

  Each Rank definition receives its own bounded ordinal choice because the
  Stream/Flow lowering uses `UnstableSort`, including consecutive definitions
  rebuilt in one window group. Ordinary equal keys are peers and receive the
  same competition rank with gaps. Decimal order is
  `-Inf < finite < +Inf < NaN`, while peer equality remains ordinary Decimal
  equality; duplicate NaNs are therefore non-peers and their legal unstable
  order affects their ranks. `CalcOverWindow` publishes no sorted constraint,
  so the internal orders preserve row-to-rank association but do not make the
  Project output a sequence. q49's later explicit TopSort remains the observable
  ordering operation.

  Commit `27e3f260017` treats both `YqlAggWin` and `YqlWin` as
  relation-dependent, carries the direct order key through ordered rename
  history, keeps every input live when metadata is unavailable, blocks unsafe
  movement, and gives a global Rank Project a nonparallel `UnionAll` input.
  The real-host q49 integration then exposed two production robustness
  regressions. `NormalizeMemberNames` rewrote a bound Member inside immutable
  source window metadata without rewriting its matching StructType, causing
  type annotation to fail; `97a03c64ab9` excludes attached metadata from that
  row-context rewrite. Preferred-alias rewriting could subsequently repeat for
  an untracked window whose hidden dependencies were conservatively live but
  could not be renamed; `68eb64102c7` makes that shape an alias-rewrite barrier.
  These are fixed preparation/termination regressions, not result-divergence
  findings.

  Focused formula-only q49 is `FORMULA_EMITTED` after 2,303/3,302 ms of
  preparation/verifier work. Preparation fails only after the exact pair is
  captured because physical compilation reports `Missed callable: YqlWin`.
  Its report SHA-256 is
  `e20010534c98ae2589e39274cf59fd396bd87961ae7bf81e2ce4572f04f26838`;
  Initial and Final snapshot SHA-256 values are
  `6d05c8c3503d7457e73617a251d89d42e699ad49372d08c608deca09ab52a7ef`
  and
  `e0441964964197a66185053732180f03f2f5a080c8d66ec7515e249640012d1d`.
  The 3,172,413-byte, 10-second canonical formula has SHA-256
  `797f6ad7e264ce01d063a55a60a2b307f055bedde766619ab863ee899f19707d`.
  A separate normal 60-second run is `UNKNOWN` after 2,267/63,562 ms because
  the global deadline expires before branch 2/4 (`right_language_empty`); its
  report SHA-256 is
  `a45d0f637cc9399f0567e41b3a4ae5d916a9092524e2419a38ab0b67d315dadf`.
  This adds a formula, not a bounded proof or replay result.

  The retained routing evidence isolates the exact captured web ratio Project
  and its two Rank leaves. On a fixed two-row aggregate database the serial
  gather is `VERIFIED_BOUNDED` in 0.32 seconds, while changing only that edge to
  `HashV2(item)` is `COUNTEREXAMPLE` in 0.47 seconds; concrete trace extraction
  reproduces it in 0.67 seconds. The logical result assigns item 1 ranks `(2,2)`
  and item 2 `(1,1)`, whereas the two hash-local singleton tasks incorrectly
  assign `(1,1)` to both. The trace binds semantic snapshots with SHA-256
  `c1d4d545ea43b7c9489fbc367ee72d7c77a80adf0297f5a91b6b19b867bae93f`
  and
  `704125e48987615de9c4005021e8ec324094503878941edd896a27e387e0b657`.
  A larger captured web-branch fixed witness makes the HashV2 variant
  `COUNTEREXAMPLE` in 45.90 seconds; its serial control is `UNKNOWN` after
  60.55 seconds. The full q49 serial plan and its three-edge hash mutation are
  both `UNKNOWN` at 60 seconds. The complete evidence manifest has SHA-256
  `04f58ec27c49638b3fe8c5c9065aa0737b8d7109a4fd1906280814a46cc28b79`.
  Together with the retained pre-fix production trace this establishes a
  bounded pre-physical global-Rank routing defect. It is not runtime-confirmed:
  `YqlWin` still cannot be compiled, and neither full-query result is a proof or
  counterexample.

  Policy commit `e926958d96c` promotes q49 from supplemental pair-only depth to
  formula construction and leaves q51 as the sole supplemental row. Focused
  and full component gates pass 738/738 Python verifier checks, 310/310 C++
  exporter/production checks, 51/51 inspector checks, 46/46 real-host
  integration checks, and 16/16 policy checks; the focused real q49 integration
  is 1/1 and captures exactly Initial then Final. The authoritative post-M78
  TPCH dashboard is 20 formulas / 0 unsupported / 2 no-pair failures after
  3,275/110,159 ms, with preparation 20/2, all 20/20 pair and formula floors,
  and zero policy violations. Its report SHA-256 is
  `796b138b95716c7d7c14c3498701686f69dade11dd2fdcc23bd369e2bed97c23`;
  the test wall time is 132.84 seconds. TPC-DS is 80 formulas / one unsupported
  q51 / 18 no-pair failures after 78,270/919,472 ms, with preparation 73/26,
  all 81 effective pairs, 80 verifier entrants, all 80/80 formulas, and zero
  violations. q49 spends 2,298/3,316 ms in that dashboard. Its report SHA-256 is
  `bd475fedf9e5e8a7c11cdbda7adbb5dc2208a34f99bff6cddfee29fe98a4355f`;
  the test wall time is 1,021.91 seconds.

  The authoritative combined partition is 100 formulas / one unsupported / 20
  no-pair failures: 100/121 workload formulas (82.6%), 100/101 exact-pair
  formulas (99.0%), all 93/93 preparation successes, all 100/100 verifier
  entrants, and all 101/101 effective exact pairs. Fresh proof gates retain the
  32-obligation floor. TPCH verifies 13/13 after 1,745/82,550 ms (report SHA-256
  `8a3ca5e010d927d5f90d06c59a0dec6aeba73338adcfdba4b5d5234267428ffd`;
  103.57 seconds wall), and TPC-DS verifies 19/19 after 16,174/116,051 ms
  (report SHA-256
  `94c68481abf54be64ef912412aa633519f96563dac302e472f955742a59c85ad`;
  153.30 seconds wall). Both proof policies are valid with zero violations.
  This is 32/121 workload queries (26.4%), 32/100 formula-covered queries
  (32.0%), and 32/32 curated obligations after 17,919/198,601 ms of summed
  preparation/verifier work.

  Milestone 79 closes the last exact-pair formula gap with only the ordered
  q51 corridor. Its four leaves and three private Projects are fixed exactly:
  `_yql_anonymous_window0` and `_yql_anonymous_window1` are the web and store
  running SUM definitions at local order zero; `_yql_anonymous_window2` and
  `_yql_anonymous_window3` are the subsequent running MAX definitions at
  independent local orders zero and one. Every leaf partitions on item using
  `IS NOT DISTINCT FROM`, orders `Optional<Date>` ascending with NULLs first,
  and has a `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame. The SUM
  partitions are required Int64; the outer MAX partitions are Optional<Int64>.
  SUM and MAX take and return `Optional<Decimal(35,2)>` and ignore NULL inputs. SUM
  uses exact Decimal addition with a fail-closed finite-headroom check; MAX uses
  the existing raw Decimal order, including specials. Each leaf owns its
  peer-order choices, evaluates task-locally within each current StageGraph
  task, preserves row association, and publishes no result sequence.
  Each of the three Final window-stage boundaries is HashV2 on item alone;
  Date remains live as a window-order input but is not a window shuffle key. The SUM leaves require
  exact Aggregate provenance. The two MAX leaves require distinct typed inputs;
  those inputs are the two running-SUM results in captured q51. Mixed windows,
  extra ROWS-window-bearing Projects, fanout, or subplans remain unsupported.

  The production work is intentionally split. Commits `43af260c8a6` and
  `a749e7800be` centralize conservative untracked-window barriers and strengthen
  transport tests; `82fc8b25bd2` transports only the exact q51 metadata and
  routes it on item. Commit `26f2210d0d7` isolates the private window expression
  and topology audits. Commits `beb329debc8` and `d2965d0a765` add the strict
  Python model and C++ export. Two range changes are robustness work, not a
  semantic finding: `9faa9c19a82` maps presence ranges on a required Data key
  to full for `Exists` and empty for `NotExists`, while `877d65c8f12` keeps the
  exact new-RBO required-Data `Exists(Member(...))` as a residual so a useless
  first component of a composite-key prefix is not emitted. Optional and Pg
  behavior is unchanged, and the verifier's closed q9/q45 range grammar is not
  widened.

  Focused q51 formula-only evidence is `FORMULA_EMITTED` after 521/8,669 ms;
  later physical preparation fails at `Missed callable: YqlAggWin`. The
  13,003,250-byte / 1,335-line formula has SHA-256
  `c586384f88740e8921061f144a1e10ba0a0038bd7f8ca7dd51e3e95addc05858`.
  Initial and Final snapshots have SHA-256 values
  `a30ef1787e372b35e60ddfb494fe92af374be7145cdcabbc9cc5870aadea648c`
  and
  `1191849569247abc1d7984f9b4ed0b69d084bf9252b34e307caba1db61133865`;
  raw verdict and report SHA-256 values are
  `a2188cce10af92e93157698517f54a1d7548ff5f0e5be8ce4835242221caf690`
  and
  `d06d19114300e81124eaf569e724829d34d18e478c3e0740fed1f44e8ebe1ff0`.
  The focused test takes 12.862 seconds. A normal 60-second solver run is
  `UNKNOWN` after 549/68,659 ms, expiring before branch 2/4
  (`right_language_empty`); its formula differs only in the timeout setting
  and has SHA-256
  `4bed335535e6ed921df9b22e639893b1dc9e1b90f2366b9d2c1a0195cac16157`,
  while verdict/report SHA-256 values are
  `23c6015297c07fdad7dc0deca4ed1f089a08094c6f2aeb69f08ab23ebf931d72`
  and
  `842fb654f6e71fc42d89e6afbb5ef187398596006221a8a7311caa82cd231e66`.
  The test takes 72.734 seconds. Neither row is a bounded proof, runtime result
  oracle, or replay result.

  Policy commit `4609f334b0c` promotes q51 and makes both supplemental
  exact-pair lists empty. Focused validation passes the direct required-range
  tests 2/2, the RBO build in 46.37 seconds, q51 plus inspector Python tests
  21/21, a broader direct Python regression 172 passed / 8 skipped, focused C++
  18/18, focused real-host q51 1/1 in 4.094 seconds, and policy 16/16 in 1.043
  seconds. Complete component gates pass three packaged Python suites with
  747/747 checks (one import, 21 flake8, 725 Python; 492.372-second graph),
  C++ 318/318, and real-host integration 46/46 in 18.333 seconds.

  At exact policy HEAD `4609f334b0c`, the authoritative TPCH dashboard has 20
  formulas / zero unsupported / two no-pair optimizer failures after
  3,357/109,633 ms, with preparation 20/2; its report SHA-256 is
  `aedcfb95a6b1d510af703ecbdf0133253d80c242855dd5b3d7297b00080758d9`
  and its test/`ya` wall times are 115.186/142.11 seconds. TPC-DS has 81
  formulas / zero unsupported / 18 no-pair failures after 78,289/952,127 ms,
  with preparation 73/26; q51 spends 498/8,549 ms. Its report SHA-256 is
  `edddfa59297588bfa0e0d73c2cdbbb94551650e782de90b6c6d0583ddb01c983`
  and its test/graph wall times are 1,036.156/1,654.743 seconds. Both report
  empty supplemental pair lists and zero policy violations.

  The authoritative M79 partition is therefore 101 formulas / zero semantic
  unsupported / 20 no-pair optimizer failures: 101/121 workload formulas
  (83.5%), formulas for all 101/101 exact pairs (100%), all 93 preparation
  successes, and 28 failures; 20 have no pair, while eight fail later after an
  exact pair and formula. Fresh proof gates intentionally retain 32 obligations.
  TPCH verifies 13/13 after 1,781/83,003 ms (report
  SHA-256
  `1ed710bf0b97ac232a18a0dd23a32dbeeb1b7a09c5b7a676c559996a275b8b29`;
  86.794/91.855 seconds test/graph), and TPC-DS verifies 19/19 after
  15,717/118,814 ms (report SHA-256
  `2bf878ed597ca712d6831482067ae63d4fc679bae7717896409344cbc74f942f`;
  138.573/278.987 seconds test/graph). This remains 32/121 workload queries
  (26.4%), now 32/101 formula-covered exact pairs (31.7%), and 32/32 curated
  obligations. q51 still fails later physical compilation and has no runtime
  result oracle or proof.

  Milestone 80 promotes the already formula-covered, preparation-successful
  TPC-DS q97 to bounded-proof depth without changing proof-producing code.
  A focused non-gating run captures exactly Initial then Final and returns
  `VERIFIED_BOUNDED` at row/task bound 2/2 after 326/2,255 ms; its test and
  suite take 6.458/7.386 seconds and its 5,754-byte report has SHA-256
  `43174bd7b2d5012b8243fc39fd20bc9bd48f1b9094fbbedab8f02efc1f786753`.
  Successful proof rows intentionally preserve no standalone formula or
  verdict artifact. Policy commit `ebb5c8806fc` adds q97 as TPC-DS proof
  obligation twenty; focused policy validation passes 16/16.

  The first expanded full-floor attempt is retained as failed operational
  evidence, not used as the floor: during observed suite-wide two-to-four-way
  host contention it returned 19 `VERIFIED_BOUNDED` rows plus q9 `UNKNOWN`, whose
  global deadline expired before branch 4/4. Its report SHA-256 is
  `87fac08fa82d81089653ae370fbcf42bc1e24bca125fb46bcdaefa036a6e66c6`.
  The timing correlation does not establish cause. No code changed before the
  clean rerun. That rerun prepares and
  verifies all 20/20 TPC-DS obligations after summed 16,156/120,815 ms;
  q9 spends 9,906/29,055 ms and q97 285/2,240 ms. The subtest/suite/graph/outer
  wall times are 140.921/142.107/185.029/216.26 seconds, and the policy-valid,
  zero-violation report SHA-256 is
  `50f4a7e37793c804265ef94a8ac29ec6a2a86211eb3a88795a8b665a3497cc00`.
  The authoritative floor is therefore 13 unchanged TPCH plus 20 TPC-DS
  proofs: 33/121 workload queries (27.3%), 33/101 formula-covered exact pairs
  (32.7%), and 33/33 curated obligations. Formula, exact-pair, verifier-entry,
  preparation, and defect inventories remain unchanged.

  Milestone 81 is a deliberately non-promoting q33 solver checkpoint at exact
  HEAD `3d1d99a953c`; no code or policy changes accompany it. q33 prepares and
  captures exactly Initial then Final, then returns `UNKNOWN` at row/task bound
  2/2 after 1,543/61,936 ms because the global deadline expires before branch
  4/28 (`left_outcome_1_unmatched`). The 899,980-byte / 1,557-line formula has
  SHA-256
  `388c78990dca6afe2530d113a7a1ee247ebd0eec78f916a080628d8c67c2d9de`;
  the 6,725-byte report has SHA-256
  `2dd30504eaa2e51f61fdd5894cb310360af4716471d40cbfa4aba54d5bdaffb4`.
  Test/suite/graph/outer wall times are
  67.123/68.125/87.583/104.68 seconds. The focused `solver_experiment` policy
  is valid with zero violations and does not enforce the proof floor. q33
  therefore remains formula-covered but unproved, the checked floor remains
  33, and the next step is exact semantic branch reduction rather than timeout
  tuning.

  Milestone 82 promotes the already formula-covered, preparation-successful
  TPC-DS q88 without changing proof-producing code. Two focused non-gating
  runs on that same code capture exactly Initial then Final and return
  `VERIFIED_BOUNDED` at row/task bound 2/2 after 2,141/41,735 and
  2,139/41,728 ms. Their 5,770-byte reports have SHA-256 values
  `303f4af0cc03844a75eab41c8e1eb0b13e4b50dee71d4f60c9cf4f03327bde60`
  and
  `3d6c2b61326f57b3eb33fb108761658b124ef7f5db09eac20709a2be0c2206f1`;
  test/suite times are 47.651/48.609 and 47.436/48.386 seconds. Successful
  proof rows intentionally preserve no standalone formula or verdict artifact.
  Policy commit `42a879e19f5` adds q88 as TPC-DS proof obligation twenty-one;
  focused policy validation passes 16/16. This promotion changes no semantic
  model, bound, or weaker coverage floor.

  Fresh authoritative proof gates are policy-valid with zero violations and
  verify all 34/34 obligations. TPCH prepares and proves 13/13 after summed
  1,779/83,055 ms; its 10,171-byte report has SHA-256
  `d558bc7f56e4d013b539a2023ccd02ad3c696ba9b5b29e54fc80fb92b7d556b0`
  and test/suite/graph/outer wall times of
  86.799/87.601/188.589/209.36 seconds. TPC-DS prepares and proves 21/21 after
  summed 18,227/162,856 ms; q9 spends 9,806/28,951 ms, q88
  2,240/41,824 ms, and q97 304/2,255 ms. Its 17,550-byte report has SHA-256
  `1f45ea729e1bb7f393023545123942c1fa6726c7ac7701b70fa4fb7923c676fd`;
  subtest/suite/chunk/outer wall times are
  184.902/185.910/185.903/210.45 seconds. The current
  floor is therefore 34/121 workload queries (28.1%), 34/101 formula-covered
  exact pairs (33.7%), and 34/34 curated obligations. Within TPC-DS it is
  21/99 workload rows (21.2%) and 21/81 formula-covered rows (25.9%). Formula,
  exact-pair, verifier-entry, preparation, and defect inventories remain
  unchanged.

  Milestone 83 promotes the already formula-covered, preparation-successful
  TPC-DS q99 without changing proof-producing code. Two focused non-gating
  runs on that same code capture exactly Initial then Final and return
  `VERIFIED_BOUNDED` at row/task bound 2/2 after 238/28,834 and 245/29,337 ms.
  Their 5,783-byte reports have SHA-256 values
  `905deaa1c381a24d853b95887c842480666ae5543cf7fbd7295fa2b5c29d7fbf`
  and
  `a44d73544e498f6180aaf9538a7073ca989d52ae95e2591943e181c1c878bdc1`;
  subtest/suite times are 32.740/33.668 and 33.121/34.101 seconds. Successful
  proof rows intentionally preserve no standalone formula or verdict artifact.
  Policy commit `cc85514862d` adds q99 as TPC-DS proof obligation twenty-two;
  focused policy validation passes 16/16. This promotion changes no semantic
  model, bound, or weaker coverage floor.

  Fresh authoritative proof gates are policy-valid with zero violations and
  verify all 35/35 obligations. TPCH prepares and proves 13/13 after summed
  1,841/83,004 ms; its 10,173-byte report has SHA-256
  `7b7904f9d460362253d0b87dc0f3439a67294f8e4a5f9dc35305cdd053c0e726`
  and subtest/suite/outer wall times of 87.011/88.538/360.27 seconds. TPC-DS
  prepares and proves 22/22 after summed 18,379/190,504 ms; q9 spends
  9,803/29,157 ms, q88 2,183/41,625 ms, q97 311/2,273 ms, and q99
  226/27,934 ms. Its 18,154-byte report has SHA-256
  `883a491cc28c06731b47e25d6b7008f17514be0905da9d3eef850235acbd08bd`;
  subtest/suite/outer wall times are 212.859/213.778/251.13 seconds. The
  M83 floor was therefore 35/121 workload queries (28.9%), 35/101
  formula-covered exact pairs (34.7%), and 35/35 curated obligations. Within
  TPC-DS it is 22/99 workload rows (22.2%) and 22/81 formula-covered rows
  (27.2%). Formula, exact-pair, verifier-entry, preparation, and defect
  inventories remain unchanged.

  A fresh non-gating q21/q56/q60 batch on the same proof-producing code
  captures exact pairs and returns `UNKNOWN` after 203/60,905, 1,516/61,876,
  and 1,520/61,835 ms. q21 reaches the deadline before branch 4/5; q56 and q60
  before branch 4/28, all at `left_outcome_1_unmatched`. The 9,816-byte report
  has SHA-256
  `50eabcd0183e49fa928a01309010944890e0a00cbd39adb375146c9f69784271`.
  Each final ordering contains its complete grouped key, so the next exact
  bounded-proof model slice is a derived null-safe unique-key certificate plus
  a cross-task partition certificate, preserved only through audited
  operators. This is a proof-reduction target, not an assumed hidden tie-break;
  q33 remains ineligible because its ordering omits its grouped key.

  Milestone 84 implements that exact proof-reduction slice in commit
  `476f2ea38f4`. The two new `Relation` fields are private semantic
  certificates and never enter the snapshot wire format: `K` is a nonempty
  null-safe unique-key column set, and `P` is a nonempty task-partition column
  set. Both must be subsets of the current schema. Grouped Aggregate mints
  `K` from its complete grouping output; `DistinctAll` uses the corresponding
  aggregate outputs. It retains `P` only when the incoming partition set is a
  subset of the grouping keys, remapping it through `DistinctAll`. Filter,
  exact root/output retention, exact direct aliases, Sort, TopSort, Merge,
  ordered and unordered Limit, and row-preserving window/prune paths preserve
  the applicable certificates. Project maps only direct null-preserving
  aliases; computed, missing, colliding, or `error_on_null` key columns drop
  the affected certificate. Join/Cross, logical `UnionAll`, and other
  uncertified row-combining constructors drop it. HashShuffle records its
  exact key as `P`; Broadcast cannot turn replicated local uniqueness into a
  global key; and multi-task gather promotes a common local `K` only when all
  inputs also carry the same `P` and `P ⊆ K`. Map connections pass the
  certificates unchanged; serial and parallel StageGraph `UnionAll` use that
  same gather rule and infer no disjointness from union alone. HashShuffle
  supplies `P` but never invents a missing `K`.

  Comparator equality on every ordered column then implies equality on `K`.
  Because `K` is null-safe unique, two present rows cannot tie. Ordinary
  complete-key Sort and eligible small Merge therefore use exact predecessor
  counts and preserve the source outcome/decision/choice cardinality. The
  compact-prefix sorting network and eligible large Merge use fixed concrete
  row-index ranks only to order absent slots; they add no symbolic tie choice.
  Incomplete-key Sort/Merge retains the preceding enumerated, ordinal, or
  symbolic-network alternatives and budgets. No exporter, decoder, JSON
  schema, wire artifact, solver theorem, or proof policy changes.

  The fresh M84 q21/q56/q60 `solver_experiment` at exact HEAD
  `476f2ea38f4` prepares all three exact Initial/Final pairs and returns
  `UNKNOWN` after 377/61,341, 3,379/63,374, and 3,952/64,088 ms. Every row's
  verdict says the deadline expired before branch 4/4,
  `right_outcome_0_unmatched`, after earlier solver work; branch 4 itself was
  not attempted. The normalized family has one left and one right outcome.
  Compared with M83, total outcomes fall from 55 to 6
  (-89.09%), bounded ordinal/tie/selection choices from 16 to zero, and SMT
  bytes from 2,344,721 to 1,458,873 (-37.78%). Per query, q21 falls from
  3 to 2 outcomes and 647,187 to 194,997 SMT bytes (-69.87%); q56 and q60
  each fall from 26 to 2 outcomes and from 855,106/842,428 to
  634,486/629,390 bytes (-25.80%/-25.29%). The formula SHA-256 values are
  `e8b528cecfaaeadb7fec86b5519e7e438092b2b1e3d27f75a0479a6c82d6d8f3`,
  `2c52982ec75b3bbd886535332410f362921d2fbd54acea8029aa6726276a9883`,
  and
  `7164f0f1fc7cffb8404782469a3ab437c82322e6ab76edc3150aa905c8924e70`.
  The 9,832-byte report has SHA-256
  `6d69883451d8b71ccb2cc78a8e7e42d59fdfdf563d574f7725cb8e9c157b8714`;
  its policy is valid with zero violations. The 18,375-byte merged trace has
  SHA-256
  `3e3492f504c32486f9a6b3a05b9e2de655a7a7f047e7379a488adce832238991`.
  Preparation/verifier sums are 7,708/188,803 ms, and
  subtest/suite/graph wall times are
  204.231814/206.927671/262.480557 seconds. Independent proof-soundness,
  test-gap, packaging, import, and diff audits found no blocker, and the
  already-registered Python package passes 760/760. No obligation is promoted:
  the authoritative proof floor remains 13/13 TPCH plus 22/22 TPC-DS, 35/35.
  q21's deterministic singleton-family/keyed-comparison residual is the next
  exact target; a longer timeout alone is not evidence.

  Milestone 85 implements that exact comparison target in semantic commit
  `67655eaa786`. It retains the canonical family mismatch and its ordinary
  distributive branches, but may attach a preferred exact cover when both
  sides are ordered singleton outcomes with no decisions or choices. Their
  schemas must agree positionally in type, nullability, and integral-AVG rank;
  their nonempty null-safe unique keys must occupy the same positions; and
  their complete positional order signatures, including direction, NULL
  placement, and comparison tag, must match and cover that key. Any near miss
  uses the prior general comparison.

  The preferred cover retains any syntactically possible language-empty branch
  and both asymmetric error branches. Under two enabled successful outcomes it
  then asks, in both directions, whether each live row lacks a null-safe key
  match. Finally it checks each non-key payload cell from the side with fewer
  live slots against the row with the same key on the other side.
  Bidirectional key inclusion plus null-safe uniqueness gives the same present
  key set with one row per key, so one complete payload direction is
  sufficient; the identical key-covering total order then makes equal rows an
  equal sequence. The prospective cover is limited to 64 branches and 256
  audited row-pair cell comparisons. Crossing either limit declines the
  optimization and keeps the old exact path.

  `query_solver` runs mandatory model-domain exclusions before either semantic
  schedule. For a preferred cover it skips the canonical three-quarter probe
  and checks branches immediately under the same decreasing global deadline.
  A proof still requires every branch to be `UNSAT`; `SAT` wins immediately
  and its exact branch is reused for model extraction; any unknown or untried
  branch prevents a proof. An empty preferred portfolio is rejected. Problems
  without a preferred cover retain canonical-first scheduling. The canonical
  counterexample term, ordinary branch cover, `Problem.formula()`, emitted SMT,
  row/task bound, and theorem are unchanged. The loop also retains its first
  branch `UNKNOWN` when the remaining deadline later reaches zero, correcting
  the diagnostic overwrite visible in the historical M84 verdicts without
  rewriting those artifacts.

  Independent soundness and packaging audits found no blocker. Before the
  final scheduler-diagnostic regression, the Python runner reported
  `754 passed in 273.95s` and the packaged checkpoint passed 776/776: 754 Python, 21
  flake8, and one import check. The earlier `Preferred` keyed-cover slice
  passed 7/7. On the frozen tree, the lowercase `preferred` scheduler slice
  collected 755 Python tests, selected 11, deselected 744, and passed all 11,
  thereby covering the last-added regression. A full package run after that addition
  was not made and is not claimed. Exactness evidence includes exhaustive
  sparse nullable composite keys, an independent Z3 proof
  of preferred/canonical equivalence under uniqueness, a satisfiable branch
  for every observable mismatch class, fail-closed gates and inclusive caps,
  canonical-formula invariance, soundness-exclusion priority, winning-branch
  replay, one shared deadline, untried-branch rejection, first-UNKNOWN
  retention, and unchanged ordinary scheduling.

  The focused q21/q56/q60 run at exact semantic HEAD `67655eaa786` selects and
  prepares all three exact 2x2 pairs under the 60,000-ms deadline. q21 returns
  `VERIFIED_BOUNDED` after 216/47,563 ms of preparation/verification. q56 and
  q60 return `UNKNOWN` after 1,561/61,589 and 1,609/61,586 ms; both now preserve
  the first unresolved result exactly as branch 7/8,
  `preferred_left_row_0_column_1_payload_mismatch`, rather than replacing it
  with a later deadline label. The 8,928-byte, policy-valid zero-violation
  report has SHA-256
  `9f0814d5dbdc732d3dbd7e4eee7d8be398de170dd36492d76bc047901bb611ee`;
  the 18,377-byte trace has SHA-256
  `b5bd8460bd8264899688574d11fea58af8b44e7cc2a6d9073b56ea4a88b374e0`.
  Subtest and outer wall times are 177.857866 and 360.13 seconds. The retained
  q56/q60 canonical formulas remain byte-identical to M84 at
  634,486/629,390 bytes with SHA-256 values
  `2c52982ec75b3bbd886535332410f362921d2fbd54acea8029aa6726276a9883`
  and
  `7164f0f1fc7cffb8404782469a3ab437c82322e6ab76edc3150aa905c8924e70`.
  Successful q21 intentionally retains no standalone solver artifact; its
  unchanged M84 canonical formula is 194,997 bytes with SHA-256
  `e8b528cecfaaeadb7fec86b5519e7e438092b2b1e3d27f75a0479a6c82d6d8f3`.

  An independent q21-only repeat is again `VERIFIED_BOUNDED` after
  199/47,320 ms. Its 5,797-byte report and 18,319-byte trace have SHA-256
  values
  `db25d8caa1a975d6d4264a5521879a7d9a451ccf93e0cd5b76dca2e6086d7a46`
  and
  `1c75bd5b501a079233ad77ad60e17250e4f3cba807aaa91430f5cc05bfa7aafc`;
  subtest and outer wall times are 51.265575 and 68.89 seconds. Policy commit
  `95182b541fb` therefore adds q21 as TPC-DS proof obligation twenty-three;
  focused policy validation passes 16/16 in 0.982651 seconds.

  Fresh proof gates at committed HEAD `95182b541fb` are authoritative and
  policy-valid with zero violations. TPC-DS prepares and proves 23/23 after
  summed 18,808/264,269 ms, with q21 at 169/47,086 ms. Its 18,758-byte report
  and 18,418-byte trace have SHA-256 values
  `f5b103cc73d4d339973610812dfe766560c677a0107f2761fb7ba7cc99ef4888`
  and
  `e44582b7662ce46eb91ba76c6aa12894b1589ec93c4861b340390f95ca59c3b6`;
  subtest and outer wall times are 287.02436 and 306.09 seconds. TPCH prepares
  and proves the unchanged 13/13 after summed 1,805/68,461 ms. Its 10,171-byte
  report and 18,429-byte trace have SHA-256 values
  `5d07e36d8df12909ef1e408d72e2f1bb7a72d033374f8e6e794bd28d67bfdc72`
  and
  `ed3a9cadf535ace43e20dbfd139889783927790b399c3447ae79687da22ce29b`;
  subtest and outer wall times are 72.351277 and 322.93 seconds. Exact `ya`
  graph times were not retained and are not inferred. The current floor is
  therefore 36/36: 36/121 workload queries (29.8%), 36/101 formula-covered
  exact pairs (35.6%), and within TPC-DS 23/99 workload rows (23.2%) and 23/81
  formula-covered rows (28.4%). Formula, exact-pair, verifier-entry,
  preparation, and defect inventories remain unchanged.

  M86 starts with summary-state diagnosis of the shared q56/q60 branch-7
  payload: whether its cost comes from repeated key guards, aggregate summary
  terms, or another exact SMT
  structure must be measured before choosing a reduction. This observation is
  neither a promised proof nor permission to weaken the cover, raise the
  timeout, or promote either query.

  The passive-carrier slice removes q83 from the numeric blocker inventory,
  integral-AVG Slice A removes q7/q13/q26, and exact integral extrema remove
  q35. Narrowly tagged derived-`Double` ordering now removes q22/q85 from the
  generic type inventory. Milestone 64 moves q72 from the Date exporter
  inventory to the join-construction inventory, and Milestone 65 removes q72
  from that inventory with exact direct unique-RHS compaction. Milestone 66
  removes q9's ordered singleton-`Limit` construction blocker. Milestone 67
  removes q24's exact nullable `Unicode.ToUpper` exporter blocker. Milestone 68
  removes TPCH q13/q16's compiled-LIKE, grouped count-distinct, and pushed
  coalesce blockers. Milestone 69 removes q64's delayed Cross-spine
  construction blocker with private unique-RHS factor scheduling. Milestone 71
  removes q31's routed Sort/Merge construction blocker. Milestone 72 removes
  q11/q74's impossible sale-type Cross branches with exact concrete-String
  equality. Milestone 73 removes the last verifier-side construction rejection,
  q4, through the three exact reductions above. Milestone 74 removes q28's
  nullable Decimal count-distinct and staged Decimal AVG-carrier blockers.
  Including exact window semantics for the failed-preparation pairs, the
  historical captured-pair gap was roughly 6--8 feature families or 8--16
  milestones. Those workload-targeted estimates started from 93 formulas and
  changed as later blockers became visible. Milestone 75 removes q84's
  partial-`Concat` boundary from formula construction, and its complete
  formula dashboards and policy gates are closed. M4 remains current. The 20
  no-pair entries
  require frontend/optimizer progress; the
  present captured-pair ceiling is 101/121, and formula construction is not
  solver proof. Milestone 76 reset that planning pass to 96 formulas and
  removed q12/q20/q98 from the exact-pair exporter gap. Milestone 77 moves
  q53/q63/q89 through formula construction with the closed AVG/Abs grammar
  above and raises the checked-in formula floor to 99. Milestone 78 removes
  q49's scale-changing cast/global-Rank combination and raises the checked-in
  floor to 100. q51 alone remains at exact-pair depth: it exposes an ordered
  contextual aggregate window and a secondary range boundary. Both M78 formula
  gates and both proof gates are closed: 100 formulas and all 32/32 curated
  proofs are green.
  Milestone 79 subsequently closes that q51 exporter gap under the fixed
  four-leaf ordered-ROWS contract above. The current floor is 101 formulas for
  all 101 exact pairs; the 20 no-pair workload entries still require
  frontend/optimizer progress, and formula construction is still not proof.

  q54's row spends 50,737 ms in verifier/formula-construction work. Its
  separate 60-second solver experiment is `UNKNOWN`: the global deadline
  expires before branch 3/8 (`left_outcome_0_unmatched`). This adds
  two-row/two-task formula coverage only; the then-current 25/121 (20.7%)
  bounded proof floor remains unchanged. The milestone passes 529/529 Python
  verifier tests, 205/205 C++ exporter tests, 39/39 real-host integration
  tests, and 12/12 coverage-policy tests.

  The Milestone 66 complete proof-floor gate was green and policy-valid. TPCH
  passed 11/11 `VERIFIED_BOUNDED` after 1,390/55,963 ms of
  preparation/verification and produced report SHA-256
  `41dc6386612519a277a896d2a9c6f74318ea5a63af4df0d086f74b0470f0dcaf`;
  TPC-DS passed 17/17 after 12,375/88,511 ms and produced
  `8967fbcdc878772094f5b4acb3aa1b2dfd208a42ef63183204e801693793deef`.
  The focused proof-floor policy target passed 5/5.
  The combined gate confirmed 28/28 obligations, or 28/121 (23.1%), at the
  declared two-row/two-task bound.

  The immediately preceding retained 25-obligation gate passed 11/11 TPCH
  after 1,445/67,977 ms and 14/14 TPC-DS after 3,328/49,834 ms. Its report
  SHA-256 values were
  `c5668fdda1f40493fdcef4729118d634a63c25f44024106198cdd89775d9d4ed` and
  `489f58334593770ff80024bc50a055ed8d60712a34056839d837fbed29c7ff34`.

  Independent focused solver rows return `VERIFIED_BOUNDED` for TPCH q21 after
  168/1,901 ms of preparation/verification, with report SHA-256
  `851b3a040d3aa1126d5b0256da95c8851984b977f8d7671db47a9f2d1a9eccef`.
  TPC-DS q16/q94 return `VERIFIED_BOUNDED` after 260/3,923 and 237/2,887 ms,
  with combined report SHA-256
  `bc2b934bed75e48cb8ebeea112eb07ef282b6ecd1c331163ce99f927b3b0c848`.
  Focused q73 returns `VERIFIED_BOUNDED` after 239/8,940 ms and produced
  `2c9dd4e765f4507bd952189055d67a0db5cf818ecb84abe188bfcdd8a15122e0`.
  These rows retain isolated evidence for the newly added obligations.

  The immediately preceding complete suite measurements were generated on
  2026-07-24 from source `4c2c1359e28` after
  the correlated-COUNT repair, exact
  `DistinctAll` support, and restoration of the production PostgreSQL
  parser/runtime in the benchmark host, then added exact uncorrelated dynamic
  `IN`, the exact nullable Date-year bridge, the side-explicit shared-IU/q95
  aggregate slice, exact String dynamic `IN`, exact proven-total Date `Unwrap`,
  exact positive nullable-integral dynamic `IN`, and exact nullable integral
  and same-scale Decimal widening weak `SafeCast`, then the pushed-predicate
  output-IU resolver. They emit TPCH q1, q3, q4, q5, q6, q7, q8, q9, q10, q11,
  q12, q14, q15, q18, q19, and q22 (16/22) and TPC-DS q2, q3, q5, q6, q10,
  q15, q18, q19, q25, q29, q33, q37, q38, q40, q42, q43, q46, q48, q50, q52,
  q55, q56, q60, q61,
  q62, q65, q68, q69, q71, q76, q77, q79, q80, q82, q87, q88, q90, q91, q93,
  q95, q96, q97, and q99 (43/99), for 59/121 workload queries (48.8%).

  The output-IU slice moves TPC-DS q2 and q97 to formula construction and q59
  through both exporters to a verifier-side 32,640-pair Sort rejection above
  the 16,384-pair audit cap.
  TPCH has four unsupported and two optimizer-failure results; TPC-DS has 30
  unsupported and 26 optimizer-failure results, for 34 unsupported and 28
  optimizer failures across both suites. Sixty-six queries pass both exporters
  and enter the formula verifier, of which 59 construct formulas (89.4%).
  Relative to all 93 optimizer-successful queries, including snapshot-boundary
  failures, formula coverage is 59/93 (63.4%). The 34 unsupported rows split
  into 25 initial-export, two final-export, and seven verifier results. The
  output-IU result was formula/entry coverage only; the proof floor at that
  checkpoint remained twenty-two and no optimizer correctness bug was found.

  That complete TPCH formula dashboard spent 7,401/76,064 ms in
  preparation/verifier work and produced report SHA-256
  `92f8508dcc9eb47e49a4ecbd9ec3577f2ab84aaa254404f555f9f4b60207342a`;
  TPC-DS spent 123,828/490,758 ms and produced
  `e7eef8b14247a35a3c1eb822d15d87eb6c80151064aa89164d58bcaba568f405`.
  TPC-DS q2, q59, and q97 spent 3,874/34,168, 993/1,218, and 595/895 ms,
  respectively. Both complete policy runs are green.

  The immediately preceding 57-formula checkpoint was generated from source
  `5dafcc79a4e`. Its TPCH formula dashboard spent 2,872/28,853 ms and produced
  `c0eadbb10b2b1f394d604bb5cc5097d9fac26646e6d3aa97b90e2ff47b0712d2`;
  TPC-DS spent 63,947/243,682 ms and produced
  `6e895ad5385f95b0528362e228d992065bb44487262cb22e5ddb5ba38ba9b844`.
  Its newly admitted TPC-DS q18 row emitted after 1,002/51,090 ms.

  The immediately preceding positive nullable-integral dynamic-`IN`
  checkpoint on source `dfd6546dfd5` emitted 56/121 formulas and retained the
  same twenty-two-query proof floor. Its TPCH formula dashboard spent
  2,814/29,457 ms and produced
  `b3bc23c618c62f73cbc362ed568a33caf484c98804160b799bf42530f9dc66e4`;
  TPC-DS spent 66,416/198,352 ms and produced
  `e0ab31819ceb0b1764d0e2be5b0af56c20c41e693b51b8b3f33408b389650d3b`.
  Its newly admitted q33 row emitted after 1,551/1,158 ms. The preceding exact
  Date-`Unwrap` checkpoint on source `93a01455afe` emitted
  55/121 formulas and retained the same twenty-two-query proof floor. Its TPCH
  formula dashboard spent 2,810/30,317 ms and produced
  `464b67c4ae5ec2661789e659c349e94f7c45ef958f38214bb733d99b2814ef02`;
  TPC-DS spent 63,563/189,499 ms and produced
  `cdfd41c4ab74b42a884b332c05b006f7a543c23f1f4ab4c924dbd08f2adc16f8`.
  The preceding repaired String checkpoint on source `4f73b38aaaf` emitted
  53/121 formulas. Its TPCH dashboard spent 2,801/30,006 ms and produced
  `deb388eec49e32242cd66bfbe943ef2f73a692d95d96150fbfb68f8281390753`;
  TPC-DS spent 68,622/200,340 ms and produced
  `9d808615985c7c6fce4bc76cfb7b1c92e68e82ecb02294e23921f7dad809af2d`.
  Its q6, q56, q60, and q95 rows emitted after 344/11,938, 1,653/1,082,
  1,464/1,092, and 474/454 ms, respectively.

  `DistinctAll` adds TPC-DS q6. The correlated-COUNT correctness repair
  intentionally moves TPCH q17 and TPC-DS q1, q30, q32, q81, and q92 from
  formula construction to an optimizer-side fail-closed result because their
  computed correlated aggregate shapes require general empty-row
  reconstruction. None was in the proof floor. The preceding 50-formula
  policy-checked TPCH run spent 2,897/29,563 ms and produced report SHA-256
  `6a8cbbeb316d128880ae97295efcc763cdc5ce14d648adec3411e6b0bb8fa214`;
  its TPC-DS counterpart spent 64,077/192,905 ms and produced
  `279318f3d46f585bba33ede252bf723a5ece36c989215015c0046eb6677e8f29`.
  Those digests are retained historical artifacts rather than identifiers for
  the current String-expanded inventory.
  Formula emission confirms end-to-end model coverage at two rows per
  referenced table and two tasks; it is not a proof by itself.

  At the preceding focused post-dashboard checkpoint, the canonical
  String-predicate bridge and Decimal `MIN` left the floor at 46/121. TPCH q2
  passed both exporters and `MIN` before reaching the 32,640-pair Merge cap;
  TPCH q9 cleared both String-predicate spellings but then reached scalar `Map`
  in both snapshots. The small real-host bridge fixture was
  `VERIFIED_BOUNDED`.

  The initial integral dynamic-`IN` slice added TPCH q18. Its focused solver
  run is `VERIFIED_BOUNDED` after 155/3,035 ms at two rows and two tasks, so q18
  is pinned in the formula and proof policy. TPCH q16 and TPC-DS q95 pass this
  gate but reached later blockers at that checkpoint; nullable, `String`, and
  `Date` cases failed closed there. The real-host `IN`-to-`left_semi` proof
  uses production PostgreSQL support because the dummy provider failed
  preparation.

  The subsequent exact nullable Date-year slice adds TPCH q7, q8, and q9 to
  the formula floor. Their complete dashboard rows spend 237/3,318,
  278/2,954, and 187/1,628 ms respectively in preparation/verifier work.
  Focused 60-second solver experiments all return `UNKNOWN`: q7 after
  230/64,641 ms at branch 4/4 `right_outcome_0_unmatched`, q8 after
  280/65,107 ms at branch 4/28 `left_outcome_1_unmatched`, and q9 after
  181/62,461 ms at branch 4/4 `right_outcome_0_unmatched`. These rows extend
  formula construction only; at that checkpoint the proof floor remained
  19/121.

  The preceding side-explicit shared-IU join and exact q95 aggregate slice adds
  TPC-DS q95 to both floors. Its preserved focused formula is 288,499 bytes
  and 1,269 lines. The dedicated two-row/two-task proof-floor row returns
  `VERIFIED_BOUNDED` after 512/3,013 ms of preparation/verification. The other
  former shared-IU candidates then passed that boundary and exposed their
  initial subplan-binding restrictions instead: q16/q94 required exactly one
  outer `EXISTS` dependency, while q33/q56/q60 required a non-null fixed-width
  integral dynamic-`IN` result.

  The exact String extension reuses that relational contract only for
  same-type non-null `String`; `Utf8`, nullable values, and coercions remain
  rejected. It adds TPC-DS q56 and q60 to formula construction, raising the
  floor to 53/121, and moves q45 to the unrelated final Read range/ordering
  boundary. At that checkpoint the remaining initial dynamic-`IN` blockers
  were q33, q58, and q83.
  Pre-fix focused solver runs returned `COUNTEREXAMPLE` candidates for q56
  after 1,260/2,356 ms and q60 after 1,260/2,072 ms. Fixed-witness inspection
  reproduced both symbolic mismatches. A paired embedded real-YDB diagnostic
  with CBO explicitly disabled confirmed one shared RBO root cause: legacy
  execution returned `("same", 10)`, while new RBO returned zero rows. Commit
  `6a2c3acb29b` preserves the finding.

  Commit `98176b0b48c` repairs the ambiguous shared-IU join-key extraction.
  Both old witnesses now return `WITNESS_NOT_REPRODUCED`, and post-fix focused
  q56/q60 runs return `UNKNOWN` at the 60-second limit. On source
  `4f73b38aaaf`, q56 spent 1,286/61,302 ms and q60 spent 1,224/61,274 ms in
  preparation/verification; the focused report SHA-256 is
  `1da4256d6b306933aa54cabc99fce262f12bcac69b1dd64c9dfd599fad7b6caa`.
  That source retains the nonmanual production runtime regression. Formula
  coverage at that repaired String checkpoint was 53/121 overall, 53/93 among
  optimizer-successful queries, and 53/59 among verifier entrants; neither
  query entered the then-current 20/121 proof floor.

  The subsequent exact proven-total Date `Unwrap` slice in
  `93a01455afe` adds TPC-DS q38 and q87 to both floors. It recognizes only the
  reviewed initial Coalesce-plus-complete-cast default and final
  `Just(Date(0))` pair described above; at that Date checkpoint, q8's String
  form remained unsupported. Formula coverage was 55/121 overall, 55/93 among
  optimizer-successful queries, and 55/61 among verifier entrants, while the
  proof floor was 22/121.

  The subsequent positive nullable-integral dynamic-`IN` slice in
  `dfd6546dfd5` adds TPC-DS q33 to formula construction. Lookup and inner
  output may be independently nullable only when both have the same
  fixed-width integral identity and the binding is a direct positive top-level
  Filter conjunct. Filter truth is existential non-NULL equality; `NOT`, `OR`,
  embedded uses, nullable `String`, coercions, and other nullable types fail
  closed. q33 emitted after 1,551/1,158 ms. It is formula-only: it adds no proof
  and revealed no optimizer bug. q58 and q83 remain blocked on broader dynamic
  `IN` semantics. At that checkpoint, formula coverage was 56/121 overall,
  56/93 among optimizer-successful queries, and 56/62 among verifier entrants.

  The subsequent exact Decimal weak-`SafeCast` slice in `5dafcc79a4e` admits
  nullable or non-null fixed-width integral sources and canonical Decimal
  sources widened at the same scale without decreasing precision. It preserves
  source NULL, saturates present integral overflow to signed infinity, and
  preserves every Decimal encoded value, including NaN and both infinities.
  The serialized `source_type` is independently checked against the argument
  type before the Python evaluator selects either meaning. `StrictCast`,
  `Convert` outside constant normalization, nullability mismatch, scale
  change, Decimal narrowing, and other source families fail closed.

  A synthetic production-host query containing both admitted nullable source
  families returns `VERIFIED_BOUNDED` at two rows and two tasks. TPC-DS q18
  exercises the same contract and emits a complete formula after
  1,002/51,090 ms, but it is formula-only and is not in the proof floor. It
  revealed no optimizer correctness bug. At that checkpoint formula coverage
  was 57/121 overall, 57/93 among optimizer-successful queries, and 57/63 among
  verifier entrants.

  The subsequent output-IU resolver maps each OLAP predicate reference through
  the read's physical name, full output-IU name, or short output-IU name to the
  logical scan output. Distinct outputs sharing a referenced spelling fail
  closed; unused ambiguity is accepted. In the complete dashboard q2/q97 emit
  formulas, while q59 reaches the verifier and stops at the audited Sort pair
  cap. At that checkpoint formula coverage was 59/121 overall, 59/93 among
  optimizer-successful queries, and 59/66 among verifier entrants; the proof
  floor was 22/121. The subsequent two-dependency `EXISTS` checkpoint is
  recorded above.
- Construction preflights cap every materialized relation at 4096 candidate
  rows and each unshared quadratic construction or shared symmetric comparison
  triangle at 16384 candidate-row pairs. Milestone 73 removes the last
  verifier-side construction rejection, TPC-DS q4's historical 20,736-pair
  join match, through exact reductions rather than a higher cap. Other shapes
  remain outside the comparator, logical payload, or key-width network gates.
  q1, q5, q11, q25, q29, q31, q46, q59, q64, q65, q68, q74, q77, q78, q80,
  and q91 now construct complete formulas instead of stopping at their
  historical aggregate, join, Sort, or Merge gates. q11/q74 do so through
  exact concrete-String equality, and q31 through the exact routed-copy
  representation above, not by raising a former pair or comparator cap.
- A shared expanded-node/depth budget now caps every complete exact scalar tree
  at 1,024 normalized occurrences and depth 128. Independent C++ and Python
  checks cover exact 1,024/1,025-node and 128/129-depth boundaries, expanded DAG
  occurrences, per-projection resets, assembled OLAP filters, synthesized join
  predicates, decoder recursion, and the unchanged 512-item `IN` and
  64-live-`IfPresent` limits. Opaque fingerprints retain their independent
  256-node/64-depth/64-KiB budget.
- A checked-in hermetic solver floor requires `VERIFIED_BOUNDED` for TPCH q3,
  q4, q6, q11, q12, q13, q14, q15, q16, q18, q19, q21, and q22 plus TPC-DS
  q3, q8, q9, q16, q21, q28, q34, q38, q42, q48, q52, q55, q69, q73, q87, q88, q90,
  q93, q94, q95, q96, q97, and q99 with a fixed 60-second per-query budget. The
  current policy covers 13 TPCH and 23 TPC-DS queries: 36 obligations, 36/121
  (29.8%) of the workload, and 36/101 (35.6%) of formula-covered queries.
  Historically, the post-M78 reports verified all 32/32 as
  `VERIFIED_BOUNDED`: TPCH passed 13/13
  after 1,745/82,550 ms (SHA-256
  `8a3ca5e010d927d5f90d06c59a0dec6aeba73338adcfdba4b5d5234267428ffd`),
  and TPC-DS passed 19/19 after 16,174/116,051 ms (SHA-256
  `94c68481abf54be64ef912412aa633519f96563dac302e472f955742a59c85ad`).
  Both policies were valid with no violations; combined summed
  preparation/verifier work was 17,919/198,601 ms, and proof mode does not
  enforce the dashboard-only exact-pair floor.

  The historical post-M79 reports retained those obligations and verified all
  32/32: TPCH passed 13/13 after 1,781/83,003 ms (SHA-256
  `1ed710bf0b97ac232a18a0dd23a32dbeeb1b7a09c5b7a676c559996a275b8b29`),
  and TPC-DS passed 19/19 after 15,717/118,814 ms (SHA-256
  `2bf878ed597ca712d6831482067ae63d4fc679bae7717896409344cbc74f942f`).
  Both policies were valid with zero violations; q51 did not join the proof
  floor.

  M80 policy commit `ebb5c8806fc` adds only TPC-DS q97. The unchanged M79
  TPCH report above retains 13/13; the authoritative M80 TPC-DS rerun verifies
  20/20 after 16,156/120,815 ms (SHA-256
  `50f4a7e37793c804265ef94a8ac29ec6a2a86211eb3a88795a8b665a3497cc00`).
  At M80 all 33/33 obligations were therefore `VERIFIED_BOUNDED` with zero
  policy violations.

  M82 policy commit `42a879e19f5` adds only TPC-DS q88. Fresh reports verify
  13/13 TPCH after 1,779/83,055 ms (SHA-256
  `d558bc7f56e4d013b539a2023ccd02ad3c696ba9b5b29e54fc80fb92b7d556b0`)
  and 21/21 TPC-DS after 18,227/162,856 ms (SHA-256
  `1f45ea729e1bb7f393023545123942c1fa6726c7ac7701b70fa4fb7923c676fd`).
  All 34/34 M82 obligations are `VERIFIED_BOUNDED` with zero policy
  violations.

  M83 policy commit `cc85514862d` adds only TPC-DS q99. Fresh reports verify
  13/13 TPCH after 1,841/83,004 ms (SHA-256
  `7b7904f9d460362253d0b87dc0f3439a67294f8e4a5f9dc35305cdd053c0e726`)
  and 22/22 TPC-DS after 18,379/190,504 ms (SHA-256
  `883a491cc28c06731b47e25d6b7008f17514be0905da9d3eef850235acbd08bd`).
  All 35/35 M83 obligations are `VERIFIED_BOUNDED` with zero policy
  violations.

  The immediately preceding complete policy gate on source `4c2c1359e28`
  passed 10/10 TPCH and 12/12 TPC-DS. Its TPCH proof-floor report spent
  1,171/70,555 ms and
  produced SHA-256
  `95b250728e656081f7a0469035bef4cd3df289a7ec1f0ce08c17d9cf76698554`;
  TPC-DS spent 7,482/113,885 ms and produced
  `a4b72350384d051958576505f5daf8e09106c59ec87104aa9ebebe1485ca4384`.
  TPCH q14 spent 85/37,202 ms in the isolated green run.
  TPC-DS q18 was not in that twenty-two-query proof policy and remains outside
  the then-current thirty-query policy.

  At the immediately preceding q18 checkpoint, TPCH spent 1,212/75,124 ms and
  produced
  `f90794bec99f5d739648c6f7fca81574ed52b8070257204d14f373edc0d38361`;
  TPC-DS spent 2,937/50,488 ms and produced
  `96e07f8139df89f7b2a0f216dd82ee0044afb592c10a7d43f3183275a796caa9`,
  and the complete verification subtree passed 34/34 suites and 934/934 tests.
  At the preceding q33 checkpoint, TPCH spent 1,185/62,768 ms and produced
  `1b68432f4e269bd19ca6064338fd008439391a1b1ffc9fa3f511d96418c6a8c6`;
  TPC-DS spent 2,800/43,618 ms and produced
  `2b32e78f680ca78e59ca158ceaf35e46cc61623f9f5bfe33c0aa938a525ac5e0`,
  and the complete verification subtree passed 34/34 suites and 925/925 tests.
  q33 was not in that unchanged twenty-two-query proof policy.
  At the preceding Date-`Unwrap` checkpoint, TPCH spent 1,234/58,883 ms and
  produced
  `db65dfe267b0b343f3cded64a32a028fab5561f4ad7b48a5803e0d3629c77f37`,
  TPC-DS spent 2,522/41,013 ms and produced
  `ea0aaa45b9cc8e7de40ad97ce23420bec926838acc8e17c4925edfad9e481751`,
  q38/q87 spent 333/1,115 and 324/1,052 ms, and the complete subtree passed
  34/34 suites and 919/919 tests. The preceding twenty-query proof-floor
  reports spent 1,164/61,112 ms for TPCH and 2,063/40,374 ms for TPC-DS and
  produced SHA-256
  `1971377b7fa14ab2b6823cdacb99a4d79a76ac4ceea9de46117d30df94a154f9`
  and
  `9a0d87075982d9ef4138b1d55b2265bd9ef461c237fec239c817e601da02bf7f`.
  That TPC-DS report's q95 row spent 589/3,429 ms; a preceding dedicated run
  measured 512/3,013 ms. The preceding nineteen-query report SHA-256 values were
  `20540ba5eb16c0d239cd6ed5c9369d4372b774820c4b9033550b0343d577a5d1`
  and
  `62d7539a519ae370278b313d50e83b30a7d50d279cd12f6347b3b1e011163a95`.
  The preceding retained eighteen-query canonical-first exact-branch TPCH run
  spent 1,145/56,389 ms in preparation/verification and produced report SHA-256
  `6d7329166c0cff497adcd86fd2d061bb409ca170c473b51529ed76ca8d80280c`;
  the TPC-DS run spent 1,446/36,036 ms and produced
  `136deef295abfe9c1fa8b4c7d8b01fe8e5131a76886ec998c0a90cbd8b778846`.
  Those historical reports contain the previous eighteen curated proofs. The
  post-M69 policy contained thirty confirmed proofs, 30/121 (24.8% of the
  workload): the first relational `EXISTS` slice contributed TPCH q4/q22 and
  TPC-DS q69, while the two-dependency slice contributes TPCH q21 and TPC-DS
  q16/q94. Dynamic `IN` contributes TPCH q18; the shared-IU/q95 aggregate slice
  contributes TPC-DS q95; and exact proven-total Date `Unwrap` contributes
  TPC-DS q38/q87. Exact same-type integral division contributes TPC-DS q73,
  the restricted floating-predicate bridge contributes TPC-DS q34, and the
  fixed-sequence ordered singleton-`Limit` slice contributes TPC-DS q9.
  Reviewed compiled LIKE, grouped integer count-distinct, and exact pushed
  Boolean coalesce contribute TPCH q13/q16. Milestone 70 subsequently adds
  TPC-DS q8 as the 31st curated bounded proof.
  The solver first checks the stable grouped mismatch with a three-quarter SMT
  timeout, then, only after `UNKNOWN`, replaces that assertion with the exact
  two language-absence predicates and one guarded unmatched predicate per
  normalized source outcome in either direction. Canonical `UNSAT`, or timely
  `UNSAT` for every branch, proves the same theorem. One monotonic deadline
  covers both phases and model extraction. Branch-only solving initially lost
  the existing TPCH q15 proof; the canonical-first portfolio restored all
  eighteen then-policy obligations before this milestone was accepted.
  Independent focused sweeps returned `VERIFIED_BOUNDED` for TPCH q4 after
  85/924 ms and again after 98/949 ms, TPCH q22 after 200/5,645 ms and again
  after 158/5,636 ms, and TPC-DS q69 after 374/3,781 ms and again after
  359/3,758 ms. q50 emits a formula but its
  solver experiment ended `SOLVER_ERROR` after the external process exceeded its
  65.0-second deadline; it is not part of the proof floor. At that historical
  checkpoint TPC-DS q15, q61, q62, q76, q79, and q88 returned `UNKNOWN` at the
  60-second solver budget. M82's fresh proof evidence supersedes only
  q88's current proof status; no particular intervening change is credited for
  the different solver result. q43 likewise returned `UNKNOWN` after
  147/69,391 ms. q61's
  1,572,871-byte formula recorded 955 ms of preparation and 63,897 ms of
  verification. The fresh q76 dashboard row records 424/1,723 ms; its preserved
  focused formula run recorded 391/14,169 ms, and its solver experiment recorded
  419/88,305 ms before `UNKNOWN`. At the earlier scaling milestone, q71's
  118,276,852-byte formula recorded 83,339 ms in the verifier/formula-emission
  phase before a focused solver attempt reached the external process deadline.
  The fresh q71 dashboard row records 380/1,526 ms; no new solver result is
  inferred. q76 is formula-covered but is not part of the proof floor. The
  Date additions q37 and q82 return `UNKNOWN`
  at the 60-second solver budget after 63,782 and 63,078 ms of verifier work; their
  retained formulas are 4,201,832 and 2,841,844 bytes. A separate non-gating
  q40 scaling experiment used a 10-second solver budget, prepared in 178 ms,
  retained a 97,319,076-byte formula, and spent 104,804 ms in verifier
  processing before reporting `SOLVER_ERROR` because the external solver
  exceeded its 15.0-second process deadline. That focused `ya` experiment fails
  on `SOLVER_ERROR` as designed; q40 is formula-covered but neither proved nor
  a counterexample. At that historical checkpoint, focused TPC-DS experiments
  also returned `UNKNOWN` for
  q10 after 524/81,517 ms, q19 after 219/61,811 ms, q65 after 283/80,633 ms,
  and q99 after 218/63,299 ms. Those complete formulas were the immediate input
  to proof scaling and decomposition work; no optimizer correctness bug was
  confirmed by those runs.
  The completed portfolio repeat keeps q19, q65, and q99 `UNKNOWN` after
  207/61,602, 259/73,190, and 206/62,883 ms. It identifies the first unresolved
  exact obligations as q19 left outcome 0 unmatched (branch 3/28), q65 right
  language absent (branch 2/4), and q99 left outcome 0 unmatched (branch 3/4).
  Report SHA-256 is
  `58cc491e30e2b866f36916f2b01db36e385f005ffe3b38685f250d95ccd10164`.
  This milestone improves proof isolation but does not promote a new workload
  proof or confirm an optimizer bug. M83's repeated focused q99 proofs and
  complete proof-floor gate supersede only q99's current proof status. The old
  report remains a historical observation, and no particular intervening
  change is credited for the different solver result.
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
ordered discriminated descriptor for each used binding: stable binding name,
kind, root node, exact type/nullability, dependency list, explicit consumer
nodes, and either the selected scalar output or complete `EXISTS` predicate.
Export fails closed on an unregistered, duplicate, or colliding binding, bad
topology, consumer mismatch, structural root nesting, staging, unsupported
kind, or a physical placement the snapshot cannot represent. The sole
expression-level exception is a closed scalar binding referenced from an
uncorrelated dynamic-`IN` root. Descriptors remain flat; C++ and Python
independently assign every consumer operator to exactly one main or subplan
root.

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
multi-task-producer, and single-task-producer serialization.
At that checkpoint TPC-DS q24 still reached the independent blocker,
`Unsupported scalar callable Map`; Milestone 67 later removes that exact
nullable `Unicode.ToUpper` boundary. TPC-DS q54 copies one Map source IU to two
distinct output IUs; exact
repeated-source projection now exports it and cached structural SMT hashes keep
construction practical. The complete dashboard emits its two-row/two-task
formula after 50,737 ms of verifier work. A separate 60-second solver attempt
is `UNKNOWN` after the global deadline expires before branch 3/8
(`left_outcome_0_unmatched`), so q54 is formula-covered rather than proved.

Equality-correlated scalar aggregation now admits exactly one outer dependency
and exactly one Project or Filter consumer. The subplan root is a no-fanout
unary path matching
`Project* -> Aggregate -> Project* -> Filter -> outer_bind`: it contains
exactly one ungrouped, phase-`undefined`, non-`DistinctAll` Aggregate.
`outer_bind` is an explicit typed relational node that preserves the closed
inner schema and appends the one outer value for a single invocation. Exactly
one Filter conjunct may mention that dependency, and it must be strict,
non-null-safe equality between the dependency and one direct inner column. All
residual conjuncts are inner-only.

For every present outer row, the evaluator injects its dependency value and
evaluates the complete scalar root. Zero rows scalarize to typed NULL, one to
the selected value, and more than one to a cardinality error. Inherited and
cardinality errors are both gated by that row's presence; repeated references
inside the sole consumer share the same binding value. Limit, Sort, scan
`pushed_limit`, ordered `UnionAll`, `EnsureAtMostOne`, nested or staged
bindings, and any per-invocation choice family fail closed. Evaluation
uses one validated plan context and one cumulative 16,384-pair
outer/closed-inner construction budget. The final comparison side is still the
ordinary StageGraph.

The focused real-host Decimal-AVG left-join case returns `VERIFIED_BOUNDED`.
At that equality-correlated milestone, the complete formula dashboards added
TPCH q17 and TPC-DS q1, q30, q32, q81, and q92. q30 and q81 required about
174,386 ms and 218,726 ms to construct their formulas; neither result was a
proof. TPC-DS q6 then passed the correlation gate and failed closed on
`DistinctAll`. The later correlated-COUNT correctness repair intentionally
rejects those six computed empty-row shapes before verification, while exact
`DistinctAll` now moves q6 through formula construction.

Relational `EXISTS` is exact for uncorrelated bindings and two deliberately
narrow correlated forms. An uncorrelated descriptor has no dependency or
predicate and returns non-null Boolean root presence. A correlated source may
have only plain column-projection Maps above one Filter directly over
`AddDependencies`. The original form has exactly one outer dependency and one
dependency-bearing conjunct: strict, non-null-safe equality between that
dependency and one direct inner column. The newer form has exactly two ordered,
distinct outer dependencies, each in a separate conjunct: exactly one strict
direct equality and one strict direct inequality against distinct direct inner
columns. Each comparison pair has the same base type while its nullability may
differ. All remaining conjuncts are inner-only. The descriptor retains the
complete predicate and exports the underlying inner root without synthetic
residual plan nodes.

The C++ exporter normalizes source `!=` to JSON `not(eq)` and validates the
exact `AddDependencies` output schema, order, and types. Python independently
validates the serialized ordered dependencies and exact normalized predicate,
then binds all dependency values from the same outer row.

For each outer row the evaluator ORs
`inner.present AND is_true(predicate(outer, inner))` across inner rows. This
preserves SQL NULL behavior and collapses duplicate matches; `NOT EXISTS` is
ordinary consumer negation. Every `EXISTS` binding is non-null `Bool`, has one
Filter consumer, remains virtual, and cannot be nested or staged. C++ and
Python independently validate their respective registry, topology, consumer,
type, dependency, and predicate contracts.

Observable `EnsureAtMostOne` errors fail closed. Correlated Limit, TopSort, and
scan `pushed_limit` also fail closed because their row choices would need a
fresh decision per outer invocation; plain Sort and exact uncorrelated row
selection remain admissible. A same-name `Void` may be dropped from the
unselected input of a one-sided witness join only when the retained input has
that same `Void`. Unmatched dropped `Void` and `Void` join keys fail closed.
The evaluator preflights at most 16,384 outer/inner pairs. The final side
remains the normal StageGraph, with no `EXISTS`-specific equivalence shortcut.

At the two-dependency milestone, focused gates passed 17/17 for Python
`EXISTS`, 6/6 for C++ `EXISTS`, and 1/1 for the new real-host case. The
complete verifier, exporter, and inspector suites passed 527/527, 203/203,
and 46/46. Exact `left_semi` and
negated `left_anti` solver differentials are `VERIFIED_BOUNDED`, while removing
the second correlation produces a counterexample. Focused TPCH q21 and TPC-DS
q16/q94 all return `VERIFIED_BOUNDED` at the declared two-row/two-task bound.

At the original one-dependency milestone, focused `EXISTS` gates passed 11/11
in Python, 4/4 in the exporter, and 4/4 through the real host. Full validation
then passed 472/472 verifier, 177/177 C++, 45/45 inspector, 37/37 replay, and
29/29 real-host integration tests. That milestone moved TPCH q4/q22 and TPC-DS
q10/q69 to formula construction;
q35 instead exposed `Unsupported scalar type Double`. TPCH q4/q22 and TPC-DS
q69 entered the eighteen-query proof floor; TPC-DS q10 remains formula-covered
and `UNKNOWN`.

Exact uncorrelated dynamic `IN` is a separate typed subplan kind. Its descriptor
records one lookup column from its sole Filter consumer and one output column
from the inner root. They have the same underlying fixed-width integral or
exact `String` or `Date` identity. String lookup/output must both be non-null.
Integral and Date lookup/output may be independently nullable, but if either is
nullable the binding may occur only in direct positive top-level conjuncts in
its sole Filter consumer. The binding is non-null `Bool`, has no dependencies,
and remains virtual. Its root may reference closed uncorrelated scalar
bindings and closed leaf `IN` bindings. Export and decoding independently
require every consumer operator to belong to exactly one main or subplan root.
Each nested `IN` consumes no subplan binding, which excludes cycles and depth
greater than one. They reject
`OuterBind`, `AddDependencies`, observable `EnsureAtMostOne`, multiple
consumers, structural root nesting, a correlated scalar, any other nested
owner/kind, staging, tuple mappings, coercions, nullable `String`, `Utf8`,
Bool, Decimal, other nullable identities, or mismatched identities.

For each present consumer row, the evaluator ORs equality with every present
inner row, requiring the lookup and inner value to be non-NULL. For non-null
columns this gives ordinary existential membership, collapses duplicates,
makes empty inner input false, and leaves `NOT` as ordinary consumer negation.
For nullable columns it is exactly the truth condition of positive SQL `IN` in
a Filter: NULL or unmatched-with-NULL evaluates to UNKNOWN rather than TRUE,
and therefore does not pass. `NOT` fails closed because replacing UNKNOWN with
false changes truth under negation; `OR` and other embedded nullable uses
remain unreviewed and fail closed.
Date values use the existing exact bounded `[0, NUdf::MAX_DATE)` domain; the
membership condition remains true exactly when one present inner row carries
the same non-NULL Date as the present outer lookup.
Repeated uses share a cached subplan family; errors inherited from the root
remain eager even with an empty outer input. A nested scalar uses the same
cached zero/one/many-row semantics as a main-root consumer. Its new cardinality
error is demanded by the immediate consumer inside the `IN` root, while an
inherited root error remains eager. A nested `IN` recursively computes the
same membership relation before its enclosing `IN`; both levels share the
cache and cumulative 16,384-pair preflight. Focused Python tests cover
duplicates, empty input, nullable lookup/output combinations, NULL inner rows,
positive-conjunct validation, `NOT`, cache reuse, nested scalar and nested
`IN` results and errors, left-semi/left-anti references, inherited errors,
malformed descriptors, cycles/depth, and the cap; C++ independently covers
the complete accepted topology and near-miss matrix.

Real-host integer and String fixtures capture initial dynamic `IN` and final
`left_semi`, then return `VERIFIED_BOUNDED` at two rows and two tasks. The
integration target uses production PostgreSQL support because the dummy
provider failed preparation. TPCH q18 emits a formula and returns
`VERIFIED_BOUNDED` after 155/3,035 ms in its focused solver run. The later
shared-IU/q95 slice proves q95. The String extension adds q56/q60 formulas and
moves q45 to final range semantics. Its two pre-fix symbolic counterexamples
led to the confirmed and repaired shared-IU defect documented below; post-fix
both old witnesses are invalid and the corrected obligations are `UNKNOWN`, so
the then-current twenty-query proof floor was unchanged. The nullable positive
integral extension adds q33 formula construction after 1,551/1,158 ms, but no
proof or optimizer finding. The later closed scalar-inside-`IN` slice admits
q58. The subsequent one-level closed `IN`-inside-`IN` slice admits q83's
initial subplan topology.

The subsequent exact Date extension reuses that descriptor and evaluator only
for uncorrelated, same-type Date lookup/output columns. Their nullability may
vary independently; any nullable case must remain a direct positive top-level
Filter conjunct. Focused C++ gates and a real-host nullable-Date
`IN`-to-`left_semi` obligation are green. The later closed-nesting slice admits
an uncorrelated scalar binding only when its expression-level owner is an `IN`
root and keeps all descriptors flat. Together with exact symbolic
producer-order Merge networks, it moves TPC-DS q58 through formula construction.
The focused dashboard takes 3,291/103,862 ms of preparation/verifier work and
has SHA-256
`87c0a2e7d51b077c19c7b261fd899f00dc590106385764574eaa7e46aac50b94`.
Direct retained emission takes 101.43 seconds, peaks at 2,294,048 KiB RSS, and
produces a 324,938,538-byte formula with SHA-256
`22f51f5d1a82091a35d29b6ac120344725f1272b8093ae9a0f1c3fa6fc6eaa70`.
This is formula coverage only: q58 adds no bounded proof or optimizer finding.
At that checkpoint validation passed 568/568 verifier tests, 208/208 C++
exporter tests, and 14/14 coverage-policy tests.

The next closed-nesting extension permits closed leaf dynamic-`IN` bindings
inside a dynamic-`IN` root. Every binding retains the existing exact
descriptor, type, NULL, and positive-Filter gates; each nested binding consumes
no subplan binding, so cycles and deeper chains fail closed. An exhaustive
finite reference covers two membership levels, and a solver differential
proves the accepted shape equivalent to two sequential `left_semi` joins while
omitting the inner membership exposes a counterexample.

q83's final raw static tuple contains three valid direct String-literal
`SafeCast` expressions annotated `Optional<Date>`. The separate narrow
exporter gate parses each literal with MiniKQL, admits it only when the result
is present, and serializes the existing non-null Date literal. Invalid text,
dynamic input, `Nothing`, `StrictCast`, other optional types, and nullable
`AsList` items remain unsupported. After both exact slices, q83's then-current
complete-dashboard row prepares successfully in 1,351 ms, then both snapshots
first fail on `Unsupported scalar type Double`; verifier work is 0 ms. The
preceding focused run spent 1,310/0 ms and produced report SHA-256
`7f1bae257dfcede11aa2f6a37f8e1bc45e079be4f13f8b836887a1768b6d7113`.
It adds no formula, verifier entry, bounded proof, policy change, or optimizer
finding. Validation at that checkpoint passed 577/577 verifier tests, 214/214
C++ exporter tests, and 14/14 coverage-policy tests.

The subsequent restricted whole-predicate and exact literal-wrapper gates add
TPC-DS q21/q34/q75 to formula construction and q34 to the proof floor. Current
validation at that checkpoint passed 577/577 verifier tests, 221/221 C++
exporter tests, and 14/14 coverage-policy tests.

The completed passive-carrier slice admits exactly q83's four reviewed
`Optional<Double>` output expressions as conservative `opaque_double`
identities. Focused formula construction returns `FORMULA_EMITTED` after
1,301/6,081 ms, with report SHA-256
`04e5df3a8f55044002fdf9b231d75b707bf58fd51c8b60a4a8879d4d623b9a5b`.
The 10,953,698-byte canonical formula has SHA-256
`5228c142eef65eb7707ff039c58e6cfc85f599286a9ec2ccf480b5fd94903db6`.
A separate solver run is `UNKNOWN` after 1,313/66,340 ms because the global
deadline expires before branch 2/4 (`right_language_empty`); its report SHA-256
is
`5571045865cbd30d7b2a35e61c379bdb7e3e24b63bfd1df2be8f514454487572`.
The complete TPC-DS dashboard independently records q83 as `FORMULA_EMITTED`
after 1,338/6,175 ms. It confirms a 72/121 formula policy; q83 adds no bounded
proof or optimizer finding, so the proof floor remains 27/121.
Validation passes 588/588 Python verifier tests, 225/225 C++ exporter tests,
46/46 inspector tests, and 14/14 coverage-policy tests. Broader `Double`
dataflow, coercions, nullable String, and nullable non-positive Boolean
contexts remain separate work.

The exact nullable Date-year projection bridge accepts only the reviewed
`Map(SafeCast(Optional<Date> -> Optional<Timestamp>), lambda Timestamp:
GetYear(Split(argument)))` shape. It validates the direct visible source,
complete cast, exact unary binder, complete normalized UDF envelopes, and
`Optional<Uint16>` result before lowering to an explicit `if_present` NULL lift
around `yql-datetime-year-v1`. C++ near-miss mutations and Python semantic
mutations fail as intended, while the real-host initial/final pair is
`VERIFIED_BOUNDED`. TPCH q7, q8, and q9 now emit complete formulas; all three
focused 60-second solver runs remain `UNKNOWN`, so that checkpoint's
nineteen-query proof floor was unchanged.

The auditability consolidation is complete in commits `7a3639d1c16`,
`ebcfdbb1263`, and `4b7f27d492e`: the proof-producing boundary has a maintained
trusted-core map, subplan export is separated into explicit phases, and the C++
descriptor is a typed variant with explicit kind states. The completed
proof-depth sweep promotes only repeatable `VERIFIED_BOUNDED` obligations. The
exact solver portfolio now preserves the stable canonical formula while
isolating language absence and directional membership failures after
`UNKNOWN`. Equality-correlated scalar aggregation and exact row-level
`DistinctAll` are now implemented. `DistinctAll` requires nonempty ordered keys
and one positional plain `distinct` alias per key, preserves exact key
type/nullability, and evaluates as null-safe tuple deduplication. Independent
nullable one- and two-key references cover empty, duplicate, and multirow
results; split intermediate/HashShuffle/final execution is checked against the
logical form, and a non-shuffled mutation exposes a duplicate witness. A
real-host test captures that exact transformation and proves it at two rows and
two tasks.

TPC-DS q6 now constructs its complete formula. Retaining explicit Sort
permutations after an upstream alternative initially produced a
627,951,195-byte formula and roughly 5.3 GiB peak process-tree memory. The exact
bounded-ordinal representation for an already-alternative Sort reduces that
obligation to 32,055,251 bytes and roughly 375 MiB for direct rendering,
without changing the sequence language. A 60-second solver experiment remains
`UNKNOWN`, so q6 enters only the formula floor. The production PostgreSQL
parser/runtime now backs both the coverage host and benchmark-mode prefix
capture, exposing dynamic `IN` as a verifier boundary instead of a dummy-host
preparation failure. Exact uncorrelated same-type non-null integral and String
slices are now implemented, as are the exact nullable Date dynamic-`IN`
positive-Filter slice, the exact nullable Date-year projection bridge, and
exact proven-total Date `Unwrap`.
One-level closed `IN` nesting and proven-present literal Date casts in raw
static-`SqlIn` tuples are also implemented. The exact passive-carrier slice now
moves q83 through formula construction without interpreting floating
arithmetic. Cardinality-certified integral `AVG` now moves q7/q13/q26 through
formula construction without claiming general binary64 semantics. Exact
fixed-width signed/unsigned integral `MIN`/`MAX` now moves q35 through formula
construction. Narrowly tagged derived-`Double` ordering now moves q22/q85
through formula construction. Exact dynamic `Optional<Date>` plus/minus
literal `IntervalFromDays` normalization now moves q72 through both exporters
to the former 4,608-row join-output guard. Exact direct unique-key-aware
right-side join compaction now moves q72 through formula construction without
raising a global cap. Exact fixed-sequence ordered singleton-`Limit`
compaction now moves TPC-DS q9 through formula construction and into the
bounded proof floor. The exact q24 nullable String-to-Utf8
Map/`Unicode.ToUpper` normalization now moves q24 through formula
construction. Reviewed generic and pushed compiled LIKE now share one audited
opaque identity; pushed Boolean coalesce is preserved exactly; and one scalar
or grouped fixed-width integer count-distinct trait is exact. Together those
slices move TPCH q13/q16 through formula construction and bounded proof.
The exact checked nullable-String `Unwrap` Project outcome moves TPC-DS q8
through both exporters and bounded proof. Exact nullable Decimal
count-distinct plus the closed staged Decimal AVG carrier now moves q28
through both exporters and bounded proof; generic physical tuple padding
remains unsupported.
Broader floating-point semantics and dataflow, coercing
dynamic `IN`, nullable String and non-positive nullable uses, more than two
`EXISTS` dependencies, broader correlated predicates, broader range reads, and other
OLAP pushdowns remain future work beyond the admitted q9/q45 point grammars.
The proof policy adds TPCH q18 to the previous eighteen obligations; the
expanded gate confirmed all nineteen at that checkpoint. The later q95 slice adds the
twentieth obligation. The Date `Unwrap` slice adds q38 and q87 as the
twenty-first and twenty-second obligations. The exact two-dependency `EXISTS`
slice adds TPCH q21 and TPC-DS q16/q94 as obligations twenty-three through
twenty-five. Exact same-type integral division adds TPC-DS q73 as obligation
twenty-six, and the later whole-floating-predicate slice adds TPC-DS q34 as
obligation twenty-seven. Integral-AVG Slice A adds formula coverage only, so
that checkpoint's proof floor remained twenty-seven. The fixed-sequence
ordered singleton-`Limit` slice adds TPC-DS q9 as obligation twenty-eight at
the bounded two-row/two-task contract. The compiled-LIKE, grouped integer
count-distinct, and pushed-coalesce slice adds TPCH q13/q16 as obligations
twenty-nine and thirty. The checked nullable-String `Unwrap` slice adds q8 as
obligation thirty-one, and the exact Decimal count-distinct/staged-AVG slice
adds q28 as obligation thirty-two.

The audit has found eleven runtime-confirmed production optimizer defects,
two bounded pre-physical StageGraph-routing findings, and two production
robustness regressions found and fixed during preparation.
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
join rule tests passed 2/2 at that checkpoint,
`KqpRboYql::ExpressionSubquery` passed 1/1 including the CBO2 nested regression,
the full `cpp_ut` passed 165/165, and the affected Python gates passed 507/507.
Defect seven is fixed.

Eighth, the equality-correlated scalar slice found a live empty-input defect in
direct `COUNT(*)`. Its initial keyless Aggregate has COUNT's non-NULL zero
identity. Correlation pull-up adds the outer equality key, converting that
Aggregate into a grouped one that has no row for an unmatched outer key.
Scalar inlining left-joins the grouped result but leaves the missing count as
NULL. The real-host
finding is retained separately in commit `605dca7e9f0`: the integration
regression expected `COUNTEREXAMPLE` at row and task bound two and required an
unmatched outer row without fixing arbitrary model values.

The production fix carries explicit originally-keyless provenance across
correlation pull-up, accepts only a unique exact Member alias path to the
selected direct COUNT trait, and restores the missing value after the left
join with `Just(Coalesce(joined_count, Uint64(0)))`. A narrow exporter
normalization maps only that generated shape to the existing exact
`if`/`if_present` IR. The finding now proves `VERIFIED_BOUNDED`; runtime
regressions cover Project and Filter consumers and prove that originally
grouped COUNT remains NULL on a missing group. Arbitrary computed
post-aggregate empty-row expressions still need general reconstruction and
fail closed in new RBO; legacy fallback is not claimed to repair that broader
class. The later complete dashboards correctly reclassify TPCH q17 and TPC-DS
q1, q30, q32, q81, and q92 at that optimizer gate. This reduces formula
coverage but leaves the proof floor unchanged.

Ninth, the pre-fix q56 and q60 String-`IN` candidates exposed one production
defect in `TPushFilterIntoJoinRule`. The rule classified equality endpoints by
IU membership alone. When an IU name existed on both `LeftSemi` inputs, it
could consume a predicate belonging to the selected left input as an
additional semi-join key. With CBO explicitly disabled, the paired embedded
real-YDB finding returned `("same", 10)` under legacy optimization and zero
rows under new RBO. Commit `6a2c3acb29b` preserves that diagnostic.

Commit `98176b0b48c` requires exclusive endpoint ownership before extracting a
join key, so an ambiguous shared-IU equality continues through the existing
side-routing path and stays on the selected left input. Its direct rule
regression is committed with the repair; commit `4f73b38aaaf` retains the
nonmanual production runtime regression. After the fix, both old witnesses
return `WITNESS_NOT_REPRODUCED`, while focused q56 and q60 solver runs return
`UNKNOWN` at 60 seconds; their SHA-bound timings and report digest are recorded
above. At that repaired String checkpoint, formula coverage was 53/121, 53/93,
and 53/59 under the three documented denominators; the proof floor was 20/121.

Tenth, the q84 allocation/demand audit exposed an independent eager-projection
defect in distributed TopSort. For a two-row Olap table with `(Id=1,
S="present")` and `(Id=2, S=NULL)`, legacy execution of
`SELECT UNWRAP(S) ... ORDER BY Id LIMIT 1` returned `"present"`; pre-fix new
RBO failed with `Failed to unwrap empty optional`. `TPushLimitIntoSortRule`
formed `TopSort(Map(Unwrap, Read))`; physical stage splitting then serialized
that Map below each partition's intermediate TopSort, forcing a row that the
global final Limit discarded.

Commit `c2c66fb1d7b` delays the whole Map until after TopSort only when the Map
and Sort are single-consumer, every sort key is pass-through or a direct
column access, every expression dependency remains available, and a complete
expression-DAG scan finds no Result, position-aware, side-effecting, or
CSE-unsafe node or subplan dependency. Computed keys and shared or unsafe Maps
retain the old topology. The direct rule suite passes 8/8, the complete
verifier C++ target passes 274/274, the real-host regression passes 1/1 under
both optimizer modes, and the broader TopSort stage test passes 1/1. At that
checkpoint the isolated fix did not cover q84 because its computed Map had
already moved into a join input before Limit-to-TopSort fusion.

Eleventh, the follow-up q84 trace identified the earlier movement as a separate
map-normalization defect. `TPushMapElementsThroughInputRule` was registered
with expression pushdown disabled, but computed expressions still crossed
Filter unconditionally and crossed selected Join inputs. A two-row Olap probe
joined `(Id=1, MatchKey=1, S="present")` and `(Id=2, MatchKey=999, S=NULL)`
against the same table while filtering the right side to `S="present"`.
Legacy returned the one matching non-NULL projection; pre-fix new RBO
materialized `UNWRAP(NULL)` for the unmatched left row and failed.

Commit `564010e2e4e` removes the expression-push option from that rule and
permits only direct column accesses and semantic renames to cross Filter,
Limit, Sort, or Join. Mixed-map tests prove that aliases still move while
computed fields remain above Filter and Join; producer-dependent renames stay
with their computed producer. All 18 focused map-element tests and all 10
append-push tests pass, the three real-runtime String demand/error tests pass,
and the complete verifier C++ target remains 274/274. A focused no-solver q84
dashboard still prepares successfully and reports the same exact
checked-Concat result-bound rejection at both snapshots; formula and proof
coverage therefore remain unchanged at this checkpoint. The regenerated final
trace confirms `Map[Concat] -> Limit[100, Final] -> Map -> TopSort[100,
Intermediate] -> joins`, with Concat in stage 11 after the row-selection work.

The qualified twelfth finding came from the first exact whole-partition window
obligation: stage
assignment fused q12's window Project into a final Aggregate stage whose
HashV2 connection used the full five-column grouping key. Task-local window
evaluation then saw only fragments of an `i_class` partition. The pre-fix q12
pair returns `COUNTEREXAMPLE` after 27,588 ms; this is a bounded mathematical
StageGraph-routing defect, although runtime confirmation is blocked because
the later compiler still rejects `YqlAggWin`. Commit `70ab3d3631c` gives every
window-bearing Map a new stage, hashes audited windows on a nonempty common
subset of their resolved partition keys, and serially gathers every unsafe or
unavailable case. The post-fix q12 obligation is `UNKNOWN` at 61,153 ms, not a
proof; exact report, verdict, formula, and snapshot digests are recorded in the
Milestone 76 closeout above.

Milestone 77's q53/q63/q89 AVG/Abs formulas expose no additional candidate or
schema mismatch. All three normal 60-second rows are `UNKNOWN`; the qualified
finding inventory therefore remains eleven runtime-confirmed defects plus this
one bounded pre-physical routing finding.

Milestone 78 adds a second bounded pre-physical routing finding. The retained
pre-fix q49 trace hashes each branch on `item` before evaluating two global
Rank leaves. An exact extracted ratio-and-Rank slice verifies the serial gather
on a fixed database and returns `COUNTEREXAMPLE` when only that edge becomes
`HashV2(item)`: two logical rows with ranks `(2,2)` and `(1,1)` become two
task-local singleton rows both ranked `(1,1)`. Commit `27e3f260017` retains the
hidden order dependencies and serially gathers global windows. Physical
compilation still rejects `YqlWin`, the full-query correct and mutated plans
are both `UNKNOWN`, and this is therefore not a runtime-confirmed defect.

The same real-host integration found two production robustness regressions.
Commit `97a03c64ab9` prevents row-context member normalization from corrupting
immutable window metadata and its self-contained Struct descriptor. Commit
`68eb64102c7` blocks preferred-alias rewriting for untracked windows whose
hidden dependencies cannot be renamed, closing a repeat rewrite loop. These
were preparation/termination failures, not semantic counterexamples. The
qualified inventory is now eleven runtime-confirmed defects, two bounded
pre-physical routing findings, and these two separately classified robustness
regressions.

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
- Trace-v1 replay requires and range-validates every inspector `{value,bound}`
  plan choice and checks exact outcome/mismatch agreement. Choices remain
  diagnostic plan valuations rather than observable result identity; a direct
  inspector/Z3/replay round trip with nonempty choices locks that distinction.
- Version-four and version-five benchmark reports preserve the exact assembled
  query, both snapshots, and byte-exact raw verifier verdict with SHA-256
  bindings. Version five additionally records preparation and semantic
  outcomes as independent axes without discarding an exact pair after a later
  preparation failure. The raw
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
- The M76 96 formula-construction and 32 curated proof obligations have
  separate checked-in regression floors. Schema-v5 policy commit
  `1da14eb637b` raises the formula floor to 96 and adds a supplemental exact-pair
  floor for q49/q51/q53/q63/q89, producing effective pair floors 20/81=101.
  Both complete formula gates satisfy their pair and formula floors. Fresh
  proof reports confirm all 32 as `VERIFIED_BOUNDED`: 13/13 TPCH after
  1,843/81,211 ms and 19/19 TPC-DS after 15,865/125,293 ms, with valid policies
  and no violations. Focused rows retain independent evidence for the newly
  added formula obligations.
  M77 policy commit `adfe48088f5` raises the formula floor to 99 by promoting
  q53/q63/q89 and reduces the supplemental pair-only list to q49/q51, without
  changing the 101-pair or 32-proof floors. Focused formula construction is
  complete; both dashboards and both fresh proof gates are closed in the M77
  closeout above.
  M78 policy commit `e926958d96c` raises the formula floor to 100 by promoting
  q49 and leaves q51 as the sole supplemental pair-only row. The 101-pair and
  32-proof floors remain unchanged. Focused construction and the 60-second
  `UNKNOWN` result, both complete dashboards, both fresh proof gates, and the
  bounded routing evidence are recorded in the M78 closeout above.
  M79 policy commit `4609f334b0c` raises the formula floor to 101 by promoting
  q51 and empties both supplemental pair-only lists. Exact-pair and proof floors
  remain 101 and 32. Focused construction, the separate 60-second `UNKNOWN`,
  complete dashboards, and fresh unchanged proof gates are recorded in the M79
  closeout above.
  M80 policy commit `ebb5c8806fc` promotes already-supported TPC-DS q97 to
  proof depth without changing any semantic model or weaker coverage floor.
  Focused q97 and the clean 20/20 TPC-DS proof gate are recorded above; the
  M80 checked floor became 33 obligations.
  M81 records the focused q33 `UNKNOWN` at unchanged HEAD `3d1d99a953c`.
  It changes no policy or implementation, so the M81 checked floor remained 33;
  exact semantic reduction is the next q33 step.
  M82 policy commit `42a879e19f5` promotes already-supported TPC-DS q88 to
  proof depth without changing proof-producing code or a weaker coverage
  floor. Two focused runs and fresh 13/13 TPCH plus 21/21 TPC-DS gates are
  `VERIFIED_BOUNDED`; the M82 checked floor became 34 obligations.
  M83 policy commit `cc85514862d` promotes already-supported TPC-DS q99 to
  proof depth without changing proof-producing code or a weaker coverage
  floor. Two focused runs and fresh 13/13 TPCH plus 22/22 TPC-DS gates are
  `VERIFIED_BOUNDED`; the M83 checked floor became 35 obligations. Fresh
  q21/q56/q60 `UNKNOWN` evidence directs the next proof-reduction work toward
  an exact derived unique-key ordering certificate.
  M84 semantic commit `476f2ea38f4` implements that private certificate and
  collapses the q21/q56/q60 normalized outcomes from 55 to 6 and bounded
  order-choice variables from 16 to zero. All three still exhaust the budget
  during singleton-family comparison, before the reported fourth branch is
  attempted, so the checked floor remains 35/35 and q21 is the next exact
  keyed-comparison reduction target.
  M85 semantic commit `67655eaa786` adds the certificate-gated exact keyed
  cover and branch-first schedule without changing the canonical SMT theorem.
  q21 proves twice; q56/q60 retain their common preferred payload branch as
  the first `UNKNOWN`. Policy commit `95182b541fb` promotes only q21, and fresh
  13/13 TPCH plus 23/23 TPC-DS gates raise the checked floor to 36/36. M86
  starts with summary-state diagnosis of the shared q56/q60 payload branch, not an assumed
  reduction or promotion.
  Every future solver witness has a
  mandatory, automatic all-candidates confirmation command; the external
  target mutation remains outside recursive tests and the verifier kernel.
- A separate manual real-YDB Decimal `SUM` diagnostic checks one- versus
  two-partition execution of identical rows in both optimizer modes. It
  currently confirms the shared `M` versus `inf` mismatch and is intentionally
  excluded from normal recursive tests until the runtime aggregate state is
  fixed.
- The manual runtime target also retains the paired shared-IU String-`IN`
  diagnostic that confirmed the q56/q60 result loss with CBO disabled. It now
  passes after `98176b0b48c`; the normal real-host suite retains the production
  regression from `4f73b38aaaf`.

## Non-goals

- Proving CBO optimality.
- Checking after every rule in normal runs.
- Treating bounded verification as a general SQL-equivalence theorem.
- Proving `ConvertToPhysical`, task construction, or execution-engine correctness;
  those require a later boundary check and real-YDB replay.
- Growing the verifier into a second optimizer or expression simplifier.
