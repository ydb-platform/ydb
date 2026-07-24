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
rbo_verifier/smt.py         typed SMT terms and deterministic SMT-LIB output
rbo_verifier/string_order.py exact finite String/Utf8 byte-order quotient
rbo_verifier/decimal.py     exact Decimal values, comparison, arithmetic, and ordering
rbo_verifier/scalar.py      nullable values, SQL Bool3, scalar UFs
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
- exact nullable
  `Date -> Timestamp -> DateTime2.Split -> DateTime2.GetYear` projection:
  one direct visible `Optional<Date>` member, a complete cast, and the reviewed
  unary UDF chain normalize to an explicit NULL lift around the shared
  `yql-datetime-year-v1` typed opaque function;
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
The unreachable typed-NULL branch preserves the Optional schema while the
constant-true condition preserves `Just` runtime presence. Incomplete,
mismatched, dynamic, nonzero, or broader safe near-matches remain opaque.
All three gates retain
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
An explicit `cast_decimal` node separately models weak `SafeCast` for two exact
source families. The first is a signed or unsigned 8/16/32/64-bit integer
expression. A source NULL produces target NULL; otherwise the integer
coefficient is scaled by `10^s`, with strict `Decimal(p,s)` bounds and
signed-infinity saturation. In particular, present integral overflow produces
`-Inf` or `+Inf`, not NULL. The second family is canonical Decimal with the
same scale and no greater precision than the result. That widening preserves
the encoded value exactly, including finite values, both infinities, and NaN,
and propagates NULL.

For both families, source and result nullability must match, the canonical
target descriptor and every target annotation must agree with the result, and
the target must retain at least one integral digit. The exporter serializes the
actual `source_type`; the Python decoder independently requires it to equal the
argument type before selecting integral-cast or Decimal-widening semantics.
That field is a required cross-language audit seam rather than redundant
metadata. Complete integer literals remain normalized literals; other admitted
expressions remain explicit casts. Missing or mismatched `source_type`,
`Convert`, `StrictCast`, nullability mismatch, Decimal narrowing or scale
change, non-integral/non-Decimal sources, and zero-integral-digit targets fail
closed outside the existing complete-literal normalization.

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

Direct `count(distinct x)` is admitted only on a keyless, phase-`undefined`,
non-`DistinctAll` Aggregate when `x` is exact non-null `Int64`, the result is
exact non-null `Uint64`, `unwrap` is false, and the Aggregate contains at most
one direct distinct trait. For each present input row, the evaluator counts it
exactly when no earlier present row has an equal input value. Before building
those equalities it charges the `N*(N-1)/2` comparison triangle against the
16,384-pair ceiling. Other direct-distinct types, phases, grouped forms,
multiple distinct traits, and distinct/unwrap combinations fail closed.

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
therefore diagnostic data, not an observable value. A cardinality-checked Limit
may consequently quotient alternative payloads in its greater-than-one error
region while retaining the exact zero- and one-row result language. Inspector
traces render the status explicitly. Version one distinguishes error from
success but does not compare error categories, codes, or text. Error-aware
real-YDB replay remains a separate extension and currently fails closed on such
a trace.

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
44. next: multiple dependencies, broader correlations, coercing or other
    nullable dynamic `IN`, range reads, and other OLAP pushdowns.

The C++ exporter lowers an RBO map mechanically to an exact projection:
all expressions read the input row, rename sources are removed, untouched input
IUs pass through, and map targets are appended in operator order. Exporter tests
cover that normalization before it enters the trusted path. The projection also
records `TOpMap::Ordered`. Both values currently have the same sequence-preserving
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
cap. Larger cases assign a bounded integer ordinal only to syntactically live
slots—those whose guard is not the literal `false`. A fixed-false padding slot
uses constant ordinal zero and consumes no choice; a symbolically guarded slot
still counts as live and is forced to zero only when its guard evaluates false.
Present-row ordinals are in range and pairwise distinct; key comparisons
constrain their relative ordinals, while ties remain unconstrained. Absent rows
do not occupy a compressed position. This is the same finite sequence language
with quadratic constraints rather than factorial outcomes. Small explicit
permutation selection deliberately continues to use the full shaped row vector.
A non-null Sort limit is TopSort and applies an exact prefix by compressed
ordinal rank. The same bounded-ordinal representation is used when an unordered
bag needs a latent sequence. Sort and Limit phases are preserved but do not
independently change the modeled runtime semantics. If the initial root is
ordered, results are compared as compressed sequences; otherwise they are
compared as bags.

Every materialized relation fails closed above 4096 candidate rows. Join
matching/output, UnionAll, and grouped-aggregate sizes are checked before their
large intermediates are allocated. Sort, Merge, and latent-sequence pair
preflights charge only syntactically live slots and fail closed above 16384
pairs before allocating permutations or ordinals. Representation selection and
small explicit permutations/interleavings remain based on the full shaped row
vector. Explicit outcome families separately fail closed above 256
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
- Occurrence- and routing-aware gather compaction for mutually exclusive source
  and shuffle task copies, while broadcast multiplicity and distinct occurrences
  remain explicit.
- Independent exhaustive concrete routing references for every admitted
  non-Merge connection and representative local-join combinations.

### M4: benchmark coverage — in progress

- Grouped/scalar count, integer sum, headroom-bounded Decimal sum, and
  row-level `DistinctAll`, including
  split intermediate/final execution, NULLs, exact 64-bit integer behavior,
  Decimal specials, partial-state bound provenance, same-type Decimal MIN/MAX,
  and phase-aware Decimal AVG with explicit `(sum,count)` state. `DistinctAll`
  accepts exact positional aliases of a nonempty ordered key tuple, deduplicates
  null-safely, and remains task-local across intermediate/final phases;
  direct per-trait distinct remains fail closed except for the exact scalar
  `COUNT(DISTINCT non-null Int64)` contract.
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
  explicit-family threshold, and order/limit/phase mutation tests.
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
  independent lossless-common-type audit. A focused real-host q8 run now passes
  the former mixed-width boundary and fails closed on unsupported scalar
  callable `Unwrap` in both snapshots after 480 ms of preparation and 0 ms of
  verifier work; formula and proof counts are unchanged.
- Exact proven-total Date `Unwrap` recognizes only the reviewed shape occurring
  in q38/q87. Inside
  `Unwrap(Coalesce(Optional<Date> member, fallback))`, the observed initial
  fallback is `SafeCast(Int32(0), Optional<Date>)` and the observed final
  fallback is `Just(Date(0))`; either exact spelling normalizes to the existing
  non-null `if_present` IR. Callable
  metadata, types, nullability, arity, child order, direct-member visibility,
  cast category, literal value, and expression budget must all match; every
  near miss fails closed. In particular, q8's String `Unwrap` remains
  unsupported. This slice changes no production Python semantic module.
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
  prove the normal obligation with the hermetic solver.
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
  may have the same fixed-width integral identity or exact `String` type.
  Fixed-width integral lookup/output columns may instead be independently
  nullable while retaining the same underlying identity, but only when the
  binding appears only in direct positive top-level conjuncts of that Filter.
  At this positive truth boundary, SQL membership is exactly existential
  equality over a non-NULL lookup and a present, non-NULL inner value:
  duplicates collapse,
  empty input is false, and an unmatched NULL never passes the Filter.
  `NOT`, `OR`, embedded nullable uses, nullable `String`, coercions, and other
  nullable identities fail closed because false and SQL UNKNOWN are
  distinguishable outside the positive Filter-truth position. `OuterBind`,
  `AddDependencies`, observable `EnsureAtMostOne`, nesting, staging, fanout,
  tuples, `Utf8`, Bool, Date, Decimal, and mismatched identities also fail
  closed. Non-null consumer `NOT` continues to supply anti-membership, repeated
  references share one cached subplan family, and inherited root errors remain
  eager. The membership product is cumulatively capped at 16,384 outer/inner
  pairs across alternatives and nested evaluation. Real-host integer and
  String cases prove initial dynamic `IN` equivalent to final `left_semi` at
  two rows and two tasks; TPC-DS q33 exercises the nullable positive contract.
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
  rather than becoming NULL. Canonical Decimal sources are also admitted only
  for same-scale, non-decreasing-precision widening, which preserves every
  finite and special encoded value. Result nullability must equal source
  nullability, and the independently checked serialized `source_type` selects
  those two semantics across the C++/Python boundary. Canonical same-type
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
  `Convert` outside constant normalization, other casts, generic division,
  non-core `IN`, and aggregate functions outside this subset remain
  unsupported. Exhaustive cast, rational, ordering, and aggregate references,
  adversarial arithmetic and accumulator-overflow cases, signature and
  mutation tests, and green real-host Decimal filter, integral-cast,
  arithmetic, ordered, and aggregate obligations cover this boundary. TPC-DS
  q90 exercises two
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
- Exact Decimal-only `min`/`max` have independent raw-code, NULL,
  grouped/global, split-state, wrong-shuffle, type, and phase-nullability tests.
  A staged final `min` changed to `max` has a two-row solver witness. At the
  earlier MAX milestone, focused TPC-DS q74 passed its aggregate blocker and
  reached the 65,536-pair join-matching preflight after 463 ms of preparation
  and 375 ms of verifier work; current q74 reaches a deeper Sort construction
  cap. The MIN extension lets TPCH q2 pass verification setup after its
  canonical `EndsWith` lowering, but q2 then fails closed at a 32,640-pair
  Merge construction above the 16,384-pair cap. Neither query joins the formula
  or proof floor.
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
  non-Atom tags, malformed tuples, and unavailable read columns fail closed.
  Physical read names, full output-IU names, and short output-IU names resolve
  to the corresponding logical scan output. A referenced spelling that denotes
  distinct outputs fails closed as ambiguous; unused ambiguity is accepted
  because it cannot affect the predicate. The separate Date gate admits `just`
  only around a direct valid
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
  enforce three monotonic depths: TPCH q1 and TPC-DS q5, q59, q65, and q80
  must reach the verifier, the 59-query formula floor must keep constructing
  SMT, and the
  twenty-two-query hermetic proof floor must remain `VERIFIED_BOUNDED`. A
  verifier-side `UNSUPPORTED` result satisfies only the first tier; later
  formulas and proofs satisfy every weaker tier without pinning brittle blocker
  text.
- Occurrence/routing compaction, scoped shared-term rendering, nonrecursive
  structural IDs, exact grouped-key classes, and the at-most-three-row
  enumeration/symbolic-ordinal selector remove the former factorial and
  repeated-structure construction gates. The latest complete suite
  measurements were generated on 2026-07-24 from source `4c2c1359e28` after
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
  output-IU result is formula/entry coverage only; the proof floor remains
  twenty-two and no optimizer correctness bug was found.

  The latest complete TPCH formula dashboard spent 7,401/76,064 ms in
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
  `Just(Date(0))` pair described above; q8's String form remains unsupported.
  At that checkpoint, formula coverage was 55/121 overall, 55/93 among
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
  cap. Current
  formula coverage is 59/121 overall, 59/93 among optimizer-successful queries,
  and 59/66 among verifier entrants. The proof floor remains 22/121.
- Construction preflights cap every materialized relation at 4096 candidate
  rows and each unshared quadratic construction or shared symmetric comparison
  triangle at 16384 candidate-row pairs. The remaining verifier-side
  construction blockers include TPCH q2's 32,640-pair Merge, TPC-DS q4's
  20,736-pair join match, q64's
  8,192-row join output, q11/q74's 8,126,496-pair Sort constructions, and q31's
  8,386,560-pair Sort construction. q1, q5, q25, q29, q46, q65, q68, q77,
  q80, and q91 now construct complete formulas instead of stopping at their
  historical aggregate, Sort, or Merge gates.
- A shared expanded-node/depth budget now caps every complete exact scalar tree
  at 1,024 normalized occurrences and depth 128. Independent C++ and Python
  checks cover exact 1,024/1,025-node and 128/129-depth boundaries, expanded DAG
  occurrences, per-projection resets, assembled OLAP filters, synthesized join
  predicates, decoder recursion, and the unchanged 512-item `IN` and
  64-live-`IfPresent` limits. Opaque fingerprints retain their independent
  256-node/64-depth/64-KiB budget.
- A checked-in hermetic solver floor requires `VERIFIED_BOUNDED` for TPCH q3,
  q4, q6, q11, q12, q14, q15, q18, q19, and q22 plus TPC-DS q3, q38, q42, q48,
  q52, q55, q69, q87, q90, q93, q95, and q96 with a fixed 60-second per-query
  budget. The current policy gate on source `4c2c1359e28` passes 10/10 TPCH
  and 12/12 TPC-DS, all `VERIFIED_BOUNDED`, so the proof floor remains 22/121.
  Its TPCH proof-floor report spent 1,171/70,555 ms and
  produced SHA-256
  `95b250728e656081f7a0469035bef4cd3df289a7ec1f0ce08c17d9cf76698554`;
  TPC-DS spent 7,482/113,885 ms and produced
  `a4b72350384d051958576505f5daf8e09106c59ec87104aa9ebebe1485ca4384`.
  TPCH q14 spent 85/37,202 ms in the isolated green run.
  TPC-DS q18 is not in this unchanged twenty-two-query proof policy.

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
  current green policy contains twenty-two proofs, 22/121 (18.2% of the
  workload): relational `EXISTS` contributed TPCH q4/q22 and TPC-DS q69, and
  dynamic `IN` contributed TPCH q18; the shared-IU/q95 aggregate slice now
  contributes TPC-DS q95, and exact proven-total Date `Unwrap` contributes
  TPC-DS q38/q87.
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
  65.0-second deadline; it is not part of the proof floor. TPC-DS q15, q61, q62,
  q76, q79, and q88 return `UNKNOWN` at the 60-second solver budget. q43 likewise
  returns `UNKNOWN` after 147/69,391 ms. q61's
  1,572,871-byte formula recorded 955 ms of preparation and 63,897 ms of
  verification. The fresh q76 dashboard row records 395/3,400 ms; its preserved
  focused formula run recorded 391/14,169 ms, and its solver experiment recorded
  419/88,305 ms before `UNKNOWN`. At the earlier scaling milestone, q71's
  118,276,852-byte formula recorded 83,339 ms in the verifier/formula-emission
  phase before a focused solver attempt reached the external process deadline.
  The fresh q71 dashboard row records 329/1,437 ms; no new solver result is
  inferred. q76 is formula-covered but is not part of the proof floor. The
  Date additions q37 and q82 return `UNKNOWN`
  at the 60-second solver budget after 63,782 and 63,078 ms of verifier work; their
  retained formulas are 4,201,832 and 2,841,844 bytes. A separate non-gating
  q40 scaling experiment used a 10-second solver budget, prepared in 178 ms,
  retained a 97,319,076-byte formula, and spent 104,804 ms in verifier
  processing before reporting `SOLVER_ERROR` because the external solver
  exceeded its 15.0-second process deadline. That focused `ya` experiment fails
  on `SOLVER_ERROR` as designed; q40 is formula-covered but neither proved nor
  a counterexample. Fresh focused TPC-DS experiments also return `UNKNOWN` for
  q10 after 524/81,517 ms, q19 after 219/61,811 ms, q65 after 283/80,633 ms,
  and q99 after 218/63,299 ms. These complete formulas are the immediate input
  to proof scaling and decomposition work; no optimizer correctness bug is
  confirmed by these runs.
  The completed portfolio repeat keeps q19, q65, and q99 `UNKNOWN` after
  207/61,602, 259/73,190, and 206/62,883 ms. It identifies the first unresolved
  exact obligations as q19 left outcome 0 unmatched (branch 3/28), q65 right
  language absent (branch 2/4), and q99 left outcome 0 unmatched (branch 3/4).
  Report SHA-256 is
  `58cc491e30e2b866f36916f2b01db36e385f005ffe3b38685f250d95ccd10164`.
  This milestone improves proof isolation but does not promote a new workload
  proof or confirm an optimizer bug.
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
topology, consumer mismatch, nesting, staging, unsupported kind, or a physical
placement the snapshot cannot represent.

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
TPC-DS q24 still reaches the independent blocker, `Unsupported scalar callable
Map`. In the fresh complete dashboard, q54 fails initial export with
`Invalid Map rename source _yql_source_5.segment`; it does not emit a formula.

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

Relational `EXISTS` is exact for uncorrelated bindings and one deliberately
narrow correlated form. An uncorrelated descriptor has no dependency or
predicate and returns non-null Boolean root presence. A correlated source may
have only plain column-projection Maps above one Filter directly over
`AddDependencies`. It has exactly one outer dependency and exactly one
dependency-bearing conjunct: strict, non-null-safe equality between that
dependency and one direct inner column. The descriptor retains the complete
predicate, including supported inner-only residual conjuncts, and exports the
underlying inner root without synthetic residual plan nodes.

For each outer row the evaluator ORs
`inner.present AND is_true(predicate(outer, inner))` across inner rows. This
preserves SQL NULL behavior and collapses duplicate matches; `NOT EXISTS` is
ordinary consumer negation. Every `EXISTS` binding is non-null `Bool`, has one
Filter consumer, remains virtual, and cannot be nested or staged. C++ and
Python independently validate registry, topology, consumer, type, nullability,
dependency, and predicate shape.

Observable `EnsureAtMostOne` errors fail closed. Correlated Limit and TopSort
also fail closed because their row choices would need a fresh decision per
outer invocation; plain Sort and exact uncorrelated row selection remain
admissible. The evaluator preflights at most 16,384 outer/inner pairs. The final
side remains the normal StageGraph, with no `EXISTS`-specific equivalence
shortcut.

Focused `EXISTS` gates passed 11/11 in Python, 4/4 in the exporter, and 4/4
through the real host. Full validation at that milestone passed 472/472 verifier, 177/177
C++, 45/45 inspector, 37/37 replay, and 29/29 real-host integration tests.
That milestone moved TPCH q4/q22 and TPC-DS q10/q69 to formula construction;
q35 instead exposed `Unsupported scalar type Double`. TPCH q4/q22 and TPC-DS
q69 entered the eighteen-query proof floor; TPC-DS q10 remains formula-covered
and `UNKNOWN`.

Exact uncorrelated dynamic `IN` is a separate typed subplan kind. Its descriptor
records one lookup column from its sole Filter consumer and one output column
from the inner root. They have the same underlying fixed-width integral or
exact `String` identity. String lookup/output must both be non-null. Integral
lookup/output may be independently nullable, but if either is nullable the
binding may occur only in direct positive top-level conjuncts in its sole
Filter consumer. The binding is non-null `Bool`, has no dependencies, and
remains virtual. Export and decoding reject `OuterBind`, `AddDependencies`,
observable `EnsureAtMostOne`, multiple consumers, nesting, staging, tuple mappings,
coercions, nullable `String`, and `Utf8`, Bool, Date, Decimal, other nullable
identities, or mismatched identities.

For each present consumer row, the evaluator ORs equality with every present
inner row, requiring the lookup and inner value to be non-NULL. For non-null
columns this gives ordinary existential membership, collapses duplicates,
makes empty inner input false, and leaves `NOT` as ordinary consumer negation.
For nullable columns it is exactly the truth condition of positive SQL `IN` in
a Filter: NULL or unmatched-with-NULL evaluates to UNKNOWN rather than TRUE,
and therefore does not pass. `NOT` fails closed because replacing UNKNOWN with
false changes truth under negation; `OR` and other embedded nullable uses
remain unreviewed and fail closed.
Repeated uses share a cached subplan family; errors inherited from the root
remain eager even with an empty outer input. The membership product is
preflighted at 16,384 pairs cumulatively across alternatives. Focused Python
tests cover duplicates, empty input, nullable lookup/output combinations,
NULL inner rows, positive-conjunct validation, `NOT`, cache reuse,
left-semi/left-anti references, inherited errors, malformed descriptors, and
the cap; C++ independently covers the complete accepted topology and near-miss
matrix.

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
proof or optimizer finding; q58 and q83 remain outside the admitted contract.

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
slices are now implemented, as is the exact nullable Date-year projection
bridge and exact proven-total Date `Unwrap`.
Coercing dynamic `IN`, nullable String/Date and non-positive nullable uses,
multiple dependencies, broader correlations, range reads, and other OLAP
pushdowns remain future work. The
proof policy adds TPCH q18 to the previous eighteen obligations; the expanded
gate confirmed all nineteen at that checkpoint. The later q95 slice adds the
twentieth obligation. The Date `Unwrap` slice adds q38 and q87 as the
twenty-first and twenty-second obligations, and the current expanded gate
confirms all twenty-two.

The audit and solver/real-YDB confirmation workflow have found nine production
optimizer defects.
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
- The 57 formula-construction and twenty-two curated proof obligations have
  separate checked-in regression floors. The expanded gate confirms all
  twenty-two proofs as `VERIFIED_BOUNDED`. Every future solver witness has a
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
