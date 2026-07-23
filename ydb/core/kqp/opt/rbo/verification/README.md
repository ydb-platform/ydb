# New RBO equivalence verifier

This directory contains the standalone bounded-equivalence checker described in
[PLAN.md](PLAN.md). It compares two versioned semantic snapshots and asks Z3 for
a bounded input database on which their result bags or ordered sequences differ.
The reproducible 121-query dashboard contract and current unsupported inventory
are recorded in [BENCHMARK_COVERAGE.md](BENCHMARK_COVERAGE.md).

The current implementation contains the M1 logical kernel, the M2 C++ boundary
hooks, the supported M3 StageGraph routing slice, and the aggregate, Limit,
ordered Sort/TopSort/Merge, pushed OLAP-filter including exact presence tests,
restricted static `IN`, exact `Exists`/`If`/unary `IfPresent`, exact all-pairs
ordinary integral comparison, exact String/Utf8 comparison and ordering, exact
partial integral `SafeCast`, exact direct String/Utf8-literal `SafeCast` to
optional Decimal, exact reviewed `Coalesce(..., false)` forms over either a
direct comparison or a binary same-member String membership/complement
predicate, exact direct Decimal `Coalesce(member, zero)`, exact reviewed
Decimal `Just` forms, exact Decimal semantics for
comparison, integral casts,
arithmetic, ordering, `SUM`, and Decimal `MAX`, plus the benchmark-dashboard
parts of M4. The reviewed exact wrapper forms retain their Optional schema through
existing `IfPresent`/`If` IR instead of being erased. The exporter also exactly
folds the reviewed constant
String/Utf8-to-Date plus-or-minus `DateTime2.IntervalFromDays` shape, erases
the corresponding direct Date-literal OLAP `just` wrapper, exactly folds direct
numeric Date/Interval literal arithmetic, exactly folds the reviewed constant
`DateTime2.Split`/calendar-shift/`MakeDate` shape, and admits the reviewed
catalog-bounded stored-String `Concat` shape.
Separate normalized-plan, concrete-counterexample inspection, and isolated
real-YDB replay tools are also implemented. A real-host transformation-prefix capture
command and sequential localizer are implemented outside the verifier kernel.
Version-four benchmark artifacts SHA-bind the exact initial and final
snapshots, assembled query, and byte-exact raw verifier verdict. A separate
all-candidates confirmation driver pins each saved solver database from that
raw verdict before inspection and replay.
Committed rule applications and mutating non-rule stages share one explicit
transformation-event stream. Solver-backed tests use the pinned, standalone Z3
target under `contrib/tools/z3`; it is not linked into `ydbd`.
The latest complete formula-only measurements reran both suites on current code
on 2026-07-23. They establish
formula construction for TPCH q3, q5, q6,
q10, q12, q14, and q19 plus TPC-DS q3,
q15, q19, q37, q40,
q42, q43, q48, q50, q52, q55, q61, q62, q71, q76, q79, q82, q88, q90, q93,
q96, and q99: 29/121 workload queries (24.0%). Formula emission means that both
snapshots were modeled and SMT was constructed; it is not a solver proof. The
checked-in solver proof floor returns
`VERIFIED_BOUNDED` for TPCH q3, q6, q12, q14, and q19 plus TPC-DS q3, q42,
q48, q52, q55, q90, q93, and q96: thirteen obligations (10.7% of the
workload). The latest TPCH proof-floor run prepared/verified q3 in
107/11,691 ms, q6 in 57/709 ms, q12 in 78/34,956 ms, q14 in 95/30,288 ms,
and q19 in 100/830 ms. Focused q12 took 108/38,880 ms. The latest TPC-DS
proof-floor run retained all eight proofs: q3, q42, q48, q52, q55, q90, q93,
and q96 prepared/verified in 103/14,687, 114/15,310, 199/3,911, 121/15,208,
94/10,689, 231/8,209, 104/14,782, and 128/445 ms, respectively.
Focused q42 returned `VERIFIED_BOUNDED` after 106 ms of preparation and 15,904
ms of verification. q50 emits a formula but its solver experiment reached the
65.0-second external process deadline; q71 did likewise, and q15, q61, q62,
q76, q79, and q88 are `UNKNOWN` at the 60-second solver budget. The new q37 and
q82 obligations are likewise `UNKNOWN` at 60 seconds. q43 is formula-covered
but returned `UNKNOWN` after 147/69,391 ms at that same budget. A separate non-gating
q40 experiment with a 10-second solver budget reported `SOLVER_ERROR` after the
external solver exceeded its 15.0-second process deadline; the focused `ya`
experiment failed on that status as designed. These obligations are
formula-covered, not proved, and not part of the proof floor. None is evidence of an optimizer
correctness bug.
The complete formula dashboard also enforces a monotonic verifier-entry floor
for TPCH q1 and TPC-DS q5, q65, and q80: both snapshots must continue to export
and reach the verifier. Their current verifier-side results stop at aggregate
`avg`, the 32,896-pair Merge construction cap, aggregate `avg`, and the
41,616-pair grouped-aggregate comparison cap, respectively; none is counted
as a formula. Later formula or proof results satisfy the same depth floor
automatically.
TPC-DS q77 also passes both snapshot exporters, finite Decimal `SUM` headroom,
and both 160-row grouped aggregates, then fails closed on a 51,360-pair Sort
above the 16,384-pair construction bound. It is not counted as a formula or
proof.
Separately, the isolated manual
[Decimal `SUM` runtime diagnostic](runtime_ut/README.md) confirms that execution
depends on partitioning in both the new-RBO and legacy optimizer modes. It is a
shared aggregation/runtime defect, not a new-RBO-only counterexample. These
results retain the two-row-per-table, two-task, pre-physical-boundary
qualifications described below.
`CaptureSemanticSnapshotCatalogV1` records the initial query-level catalog once,
and `ExportSemanticSnapshotV1`
deterministically lowers supported RBO operators without doing file I/O. An
optional sink on `TKqlTransformContext` receives the initial snapshot before the
first RBO stage and the final snapshot immediately before physical generation.
`CreateKqpHost` accepts the same sink as immutable instrumentation configuration
and copies it into every per-query transform context created by the host.
Supported final plans include exact stage membership and Map, HashShuffle,
Broadcast, serial or parallel UnionAll, and ordered Merge connections.

Stage execution uses at most two bounded source/shuffle tasks. Source-row task
placement and hash routing are shared symbolic functions, so a two-task source
covers both partitions instead of fixing rows to one convenient task. Map
preserves the producer count, serial UnionAll requires one consumer task,
parallel UnionAll uses the runtime's cross-input round-robin offset, and a
broadcast-only stage uses one task just as KQP task construction does. Invalid
connection combinations are rejected. Merge has one consumer task, validates
that every producer stream has an order compatible with the edge order, and
represents every sorted interleaving that preserves each producer sequence. It
uses explicit interleavings for small inputs and bounded symbolic ordinals once
enumeration would cross the ordinary outcome cap. The final stage is gathered
before root column projection.

Rows carry structural occurrence provenance and facts about symbolic source or
hash routing. When a non-Merge gather sees task copies of the same occurrence
with contradictory routing facts, those guards are mutually exclusive and the copies
can be coalesced exactly, including conditional task-local values. Broadcast
copies have no such proof and retain their bag multiplicity; distinct or unknown
occurrences also remain separate. This keeps routed StageGraphs compact without
silently deduplicating SQL rows.

The exporter and decoder both validate the StageGraph independently: plan nodes
partition into stages, each stage has one logical sink, every cross-stage child
occurrence has one edge, and producer output indices are a bijection with
outgoing edge occurrences. Shuffle-elimination/co-partitioning assumptions are
rejected until the snapshot can substantiate them.

Version one preserves exact supported YQL scalar identities (`Bool`, signed and
unsigned integer widths, `String`, `Utf8`, `Date`, and canonical parameterized
`Decimal(p,s)`) even when several identities use the same SMT domain. Decimal
identity requires `1 <= p <= 35` and `0 <= s <= p`, with no alternate spelling.
Integer identities use SMT integer carriers with exact signed or unsigned
8/16/32/64-bit domains on literals, source cells, and non-null opaque results.
Date is likewise exact: numeric literals, source cells, and non-null opaque
results are constrained to unsigned day values in `[0, 49673)`.

`String` and `Utf8` use one exact bounded integer-rank quotient of YDB's
unsigned UTF-8/raw-byte lexicographic order; there is no collation or Unicode
normalization. Z3 sees integer ranks rather than its string theory. Ordinary
and null-safe equality and ordinary ordering accept either identity on either
side. HashShuffle likewise shares one symbolic hash family for `String` and
`Utf8`, matching the runtime's type-independent raw-byte hash, while snapshot
type identity remains distinct. The deliberately narrower static-`IN` gate
still requires identical string types.

The quotient fixes every strict-UTF-8 snapshot literal and registers every
distinct observable nonliteral string term from both plans. If there are `M`
such terms, it keeps `M` concrete representatives in every infinite open
literal interval and all available representatives up to `M` in the finite
prefix/NUL gaps. This is enough to preserve every equality and ordering among
the literals and at most `M` assigned terms in both directions. Representatives
are constructed from complete UTF-8 literals by NUL extension, so every decoded
witness is valid UTF-8 and replayable even when it stands for an arbitrary-byte
`String` value. Construction preflights limits of 65,536 representatives,
64 MiB of total encoded representative bytes, and 1,000,000 bytes per value;
exceeding any limit returns `UNSUPPORTED`. The per-value limit is shared with
witness inspection and replay.

Literal ranks and term bounds are deferred until SMT rendering, after both
plans and any fixed witness strings have registered their values. Rendering
seals the universe; later registration fails closed, and the model decoder
accepts only ranks in that sealed universe. A string-valued term that depends
on a sequence ordinal which may be rebound under family-comparison quantifiers
also fails closed, because a top-level rank bound would not constrain every
rebound valuation.

The same choice-independence audit applies to every top-level source, catalog,
and opaque-result domain invariant: any such invariant that depends on a
rebound sequence ordinal fails closed, not only String rank bounds.
Global invariants render before the ordinary counterexample obligation even
when deferred String sealing registers them later.

Decimal uses the exact YDB scaled-integer representation. A legal non-null
`Decimal(p,s)` value is a finite code `c` with `-10^p < c < 10^p`, negative or
positive infinity, or NaN. The snapshot literal is explicitly tagged; finite
codes use a canonical signed-integer string, while specials have no `scaled`
field:

```json
{"kind":"literal","type":"Decimal(7,2)","value":{"kind":"finite","scaled":"1234"}}
{"kind":"literal","type":"Decimal(7,2)","value":{"kind":"neg_inf"}}
```

The other special tags are `pos_inf` and `nan`. Source cells and non-null opaque
Decimal results receive the same typed-domain constraint.

Same-type integer `+`, `-`, and `*` are structural scalar nodes. Both operands
and the result must have exactly the same signed or unsigned width, and result
nullability must be the OR of operand nullability; otherwise the expression
remains opaque. Evaluation is strict on NULL and wraps modulo the result width,
using the signed two's-complement representative for `Int8` through `Int64`.
This narrow rule was added when TPC-DS q88 exposed a spurious witness between
source arithmetic constants and optimizer-folded literals.
Source and opaque integer range constraints also prevent out-of-width witnesses
from observing a difference between a direct integer use and its wrapped
arithmetic identity.

Static `SqlIn` is exact for a deliberately narrow shape: a direct raw tuple or
`AsList` with 1..512 recursively supported, non-null items of one scalar type.
That type is identical to the lookup type, or both are integers for which one
lossless common type represents both domains: equal signedness, or a signed
width greater than the unsigned width. This gate is deliberately narrower than
ordinary integral comparison. Membership evaluates as the SQL three-valued OR
of those equalities, so only a nullable lookup can make the Boolean result
nullable. `ansi`, `warnNoAnsi`, `isCompact`, and `nullsProcessed` normalize to
the same node under this gate. Dynamic, empty, oversized, nullable,
heterogeneous-item, lossy or non-integer mixed-type, `tableSource`,
malformed-option, and unknown-option forms fail closed.

`Exists` is the exact non-null presence test for one scalar value. `If` models
MiniKQL's lazy branch selection, including NULL propagation from an optional
Boolean condition. Unary `IfPresent` binds the non-null payload of exactly one
`Optional<Data>` input; the snapshot represents nested handlers with lexical
de Bruijn depths, so argument names and allocations cannot affect identity.
At most 64 handler bindings may be live at once.
The new RBO's lowered static-membership form is normalized to the same `in`
node only for `Contains(ToDict(List(...), identity, Void, (One, Auto)), bound)`
inside that handler. Every item must be non-null, have the bound value's exact
type, and pass the ordinary recursive scalar audit. Other lambdas, dictionary
settings, payloads, lookups, and generic `Contains` remain unsupported.

The exact Decimal zero wrapper admits only
`Coalesce(direct_optional_member, zero) -> Decimal(p,s)`. The member must be a
visible `Optional<Decimal(p,s)>` input and the fallback must be either a
canonical non-null Decimal zero of that exact type or a complete
`SafeCast(Int32("0"), Decimal(p,s))`. It lowers to
`if_present(member, bound(0), zero)`, preserving NULL-to-zero behavior and the
non-null result schema. A matching `Just` may wrap that exact result and reuses
the existing `if(true, value, typed-null)` Optional-preserving form. Nonzero or
special fallbacks, dynamic or incomplete casts, `Convert`, computed first
children, mismatched types, and broader `Coalesce` shapes remain opaque when
the closed-world safety audit admits them; malformed or unsafe trees fail
closed.

Scalar syntax outside the explicit Boolean, equality, ordering, integer
arithmetic, and restricted static-membership core is represented by a shared
typed uninterpreted function when
the C++ exporter can positively audit it as deterministic and total. The
current reviewed opaque families are scalar comparisons; `Just` and
`Coalesce`; `SafeCast`; non-failing `Convert`; `Substring`; and stored-String
`Concat` in the exact workload shapes described below. The same audit
treats the explicit `DecimalDiv` core node as total, so a supported opaque
parent may contain it. YQL has no complete generic totality or determinism flag,
so all other callables fail closed rather than relying on a denylist. This
includes UDF and PG calls, generic division, strict casts, `Unwrap`, runtime-
dependent generators, free variables, and unsafe AST metadata.

The reviewed `Substring` shape is exactly
`Substring(Optional<String>, start, count) -> Optional<String>`. Both bounds
must be non-null `Uint32` literals, either directly or as an in-range integer
literal converted to `Uint32`; that conversion exception is confined to those
two direct bound positions. The canonical fingerprint retains both bounds and
the String column is the only external function argument. Other arities,
`Utf8`, nullable or dynamic bounds, and out-of-range conversions fail closed.

The reviewed `Concat` shape is confined to the body root of a Map expression
and returns exactly non-null `String`. It is a binary tree whose leaves are
canonical String literals, non-null stored String members, or exactly
`Coalesce(nullable stored String member, String(""))`. At least one and at most
two stored-member occurrences are required; repeated occurrences count
separately. Generic or nested-parent `Concat`, `Utf8`, computed strings,
nonempty nullable fallbacks, and every other leaf fail closed. The entire tree
is encoded as one opaque function: its canonical fingerprint retains tree
shape, literal bytes, argument order, and repeated uses, while IU names are
alpha-normalized. Consequently this rule can prove only syntax-preserving
uses of the same total function; reassociation or another semantic rewrite may
cause a false counterexample, never a false proof.

Stored-member provenance begins only at catalog-confirmed Datashard or Olap
tables and excludes system views, generated values, and external sources. It
survives Map pass-through/rename, Filter, Limit, Sort, aggregate group keys,
and the value-preserving sides of joins and UnionAll. Outer and exclusion joins
widen the affected side to nullable; semi/anti joins drop the absent side; and
UnionAll ORs catalog-derived nullability from both inputs. The final Member
annotation must equal that carried nullability, so stale intermediate type
annotations cannot manufacture provenance.

Totality is justified by representation, storage, and allocation bounds, not
by assuming Concat cannot fail. Datashard caps a stored value at 16 MiB through
`NDataShard::NLimits::MaxWriteValueSize`. Column-store String values reach
MiniKQL as Arrow `BinaryType`; its validated signed 32-bit offsets bound one
logical cell by `INT32_MAX` bytes independently of compression. The auditor
charges the bound carried by each stored occurrence plus exact literal bytes.
It also requires the complete result to stay below the largest `ui32` size for
which MiniKQL's `newSize + newSize / 2` allocation capacity cannot wrap. Thus
one Olap occurrence plus the audited literals is safe, while two generic Olap
occurrences fail closed. The authoritative implementations are
`ydb/core/tx/datashard/const.h`,
`ydb/core/tx/datashard/datashard_write_operation.cpp`,
`ydb/core/tx/datashard/datashard_common_upload.cpp`,
`ydb/core/formats/arrow/switch/switch_type.h`,
`yql/essentials/public/udf/arrow/dispatch_traits.h`,
`contrib/libs/apache/arrow/cpp/src/arrow/type.h`,
`contrib/libs/apache/arrow/cpp/src/arrow/array/validate.cc`, and
`yql/essentials/minikql/mkql_string_util.cpp`.

This gate moves TPC-DS q5 and q80 past snapshot export to the deeper Decimal-SUM
headroom and 82,944-pair grouped-aggregate construction checks. q84 has two
Olap String occurrences, so it fails the allocation-totality gate. The formula
slice at that milestone remained 23/121 (19.0%) and the proof floor remained
ten.

An explicit `cast_integral` node models only partial integer `SafeCast`: the
source is any exact signed or unsigned 8/16/32/64-bit integer expression, and
YQL cast analysis must classify the conversion to the optional integer target
as `MayFail`. The target descriptor, its outer and item annotations, and the
optional result must agree exactly. Evaluation propagates a source NULL,
preserves an in-range integer value, and returns NULL for a value outside the
target's exact integer domain; the verifier gives every NULL result the
canonical zero payload. The entire callable still passes the closed-world
opaque-expression safety audit before entering this exact gate. Complete
integer conversions remain opaque, while `Convert`, `StrictCast`, non-integer
pairs, and non-optional partial results are outside this exact node.

TPC-DS q79 exposed the need for this node. Its first symbolic candidate used
`d_year = 1998`: the initial plan tested the nullable `Int64` directly against
the `Int32` membership constants, while the final lowering first performed an
opaque `SafeCast(Int64 -> Int32)`. Treating that cast as an independent function
allowed it to return NULL for 1998, which the runtime cannot do. Regenerating
the obligation with the exact partial-cast semantics removes that witness. A
focused direct-membership versus `Exists`/cast/`IfPresent` lowering returns
`VERIFIED_BOUNDED`; the full q79 solver run returns `UNKNOWN` at the 60-second
budget. The candidate was therefore a verifier-modeling false positive, not a
confirmed optimizer bug.

A complete cast of a non-null integer constant to a non-null Decimal is handled
before opaque fallback: the exporter evaluates the YDB cast and emits the
resulting tagged Decimal literal. A separate explicit `cast_decimal` node models
`SafeCast` of any non-null exact signed or unsigned 8/16/32/64-bit integer
expression to a non-null canonical `Decimal(p,s)`. The target descriptor and
its annotation must agree exactly with the result, and `p - s >= 1`. Evaluation
multiplies the integer by `10^s`; a coefficient whose absolute value reaches
`10^p` becomes the corresponding signed infinity, matching MiniKQL. Complete
integer literals retain the normalized-literal representation, while
value-specific incomplete literals use `cast_decimal`. `Convert`, `StrictCast`,
nullable source/target/result shapes, zero-integral-digit targets, and
non-integer Decimal cast sources remain outside this explicit gate.

A second constant normalization covers only a direct non-null `String` or
`Utf8` literal passed to `SafeCast` with an `Optional<Decimal(p,s)>` result and
target. The result, outer target annotation, nested non-null Decimal item
annotation, and descriptor must agree exactly, and YQL cast analysis must
classify the source-to-item conversion as `MayFail | MayLoseData`. The literal
must be non-empty 7-bit ASCII; dynamic, nullable, malformed, or differently
annotated sources fail closed.

The exporter evaluates that fixed conversion with the runtime's
`NDecimal::FromStringEx`. A parser error becomes an existing typed `null` node.
Successful finite values use the parser's round-half-to-even behavior and must
be normal for the requested precision; a successful nonnormal value is rejected
rather than normalized. NaN and signed infinities remain tagged Decimal
literals, numeric overflow saturates to signed infinity, and underflow can round
to zero. Successful casts become existing Decimal literal nodes, so this
milestone changes neither the snapshot IR nor the Python decoder/evaluator.

A third constant normalization covers only Optional-Date arithmetic of the
form `SafeCast(text, Optional<Date>) +/- Apply(udf, days)`. The text must be a
direct non-null `String` or `Utf8` literal. The cast result and descriptor must
be exactly `Optional<Date>`, and YQL cast analysis must classify it as the
reviewed `MayFail` conversion. The right side must be the strict normalized
eight-child `DateTime2.IntervalFromDays` UDF applied to a direct non-null
`Int32` literal in `[-49672, 49672]`, with its callable annotations, cached
descriptor, Void fields, empty configuration/file alias, and `blocks, strict`
settings all matching exactly. Other Date arithmetic remains unsupported.

The exporter parses the Date with MiniKQL `ValueFromString`, applies the signed
day offset, and reuses an existing typed Date literal or NULL node. An invalid
Date or a result outside `[0, 49673)` becomes typed Date NULL, matching runtime
optional arithmetic. No Interval snapshot node or Python evaluator extension
is introduced. At the pushed-OLAP boundary, `just` is erased only around a
direct, valid, non-null Date literal; nullable, malformed, dynamic, or non-Date
arguments fail closed.

An independent constant normalization admits only a direct non-null numeric
`Date` left operand and `Interval` right operand under an exactly
`Optional<Date>` `+` or `-`.
The Interval atom must satisfy the runtime type's strict open range. The
exporter follows MiniKQL exactly: it scales Date midnight to microseconds,
performs signed arithmetic, rejects a scaled result outside
`[0, MAX_TIMESTAMP)` as typed NULL, and only then truncates back to whole days.
This matters for fractional-day intervals such as one microsecond. Valid input
ranges prove that the arithmetic cannot overflow `i64`; every other shape
fails closed. Synthetic open-boundary, fractional-day, malformed-shape, and
typed-NULL tests cover the gate, while a real-host pushed-filter fixture folds
the exact TPCH q1 constant `Date('1998-12-01') - Interval('P90D')` to day
10,471 and returns `VERIFIED_BOUNDED`.

This exact fold moved TPC-DS q37, q40, and q82 through formula construction.
At that milestone the dashboard covered 23/121 queries (19.0%). q37 and q82
are `UNKNOWN` at a 60-second solver budget. A separate non-gating q40 scaling
experiment retained a
97,319,076-byte formula but reported `SOLVER_ERROR` after the external solver
exceeded its 15.0-second process deadline; it is neither a proof nor a
counterexample. Before restricted stored-String `Concat` was added, TPC-DS q5, q80,
and q84 stopped at the generic callable. q5 and q80 now have the deeper outcomes
described above; subsequent finite Decimal-bound propagation moves q5 again to
the 32,896-pair Merge construction cap. q84 remains unsupported on its
two-cell allocation bound. Before constant DateTime2 calendar-shift folding,
the dashboard covered 23/121 queries and the proof floor remained ten. TPCH q1
passes both snapshot
exporters and reaches unmodeled aggregate
`avg`; q21 exposes `Double`; q72 still has a dynamic Date-fold
`SafeCast(Optional<Date>)` mismatch; and, at that earlier milestone, q77 passed
export but failed the verifier's Decimal-SUM headroom gate.

A further constant normalization accepts only the exact optimizer-generated
`Map(Shift(Split(Date), Int32), MakeDate)` tree for `DateTime2.ShiftYears` or
`DateTime2.ShiftMonths`. The root and shift must be optional; every Date,
Int32, TM-resource, callable, cached descriptor, Void field, setting, AutoMap
flag, and UDF user type must match the reviewed normalized form; and the unary
lambda may pass its bound TM value only to `DateTime2.MakeDate`. All other
`Map`, DateTime2, dynamic, or differently annotated shapes fail closed.
The exact signatures for `IntervalFromDays`, `Split`, both shifts, and
`MakeDate` live in one five-row reviewed table shared by uniform envelope,
cached-type, and `Apply` validators.

The fold uses MiniKQL's Date split/make tables and the runtime calendar rules:
year shifts clamp February 29, while month shifts use the runtime signed
quotient/remainder sequence and clamp the day to the target month. It rejects
shifts that would wrap the unsigned 12-bit `DateTime2.TM` year field; a valid
shift outside the Date domain becomes typed NULL. Synthetic exact-shape,
leap-day, month-end, Date-boundary, descriptor, flag, setting, lambda-binding,
and year-wrap tests cover the gate. A real-host pushed-filter obligation folds
q5/q6-style `1994-01-01 + 1 year` and q10-style `1993-10-01 + 3 months`
constants to days 9,131 and 8,766 and is `VERIFIED_BOUNDED`.

The complete formula dashboard now emits TPCH q5, q6, q10, and q14 in addition
to q3 and q19. Their preparation/verifier times were respectively 153/109,903,
55/213, 109/8,144, and 59/247 ms. At that milestone q12 passed the DateTime2
fold and exposed unordered scalar children at both snapshot boundaries. This
raised the formula slice at that point to 27/121 (22.3%). In the initial focused
solver experiments q5 reported `SOLVER_ERROR` after
180/230,982 ms of preparation/verifier work and the 65-second external-process
watchdog, while q10 returned `UNKNOWN` after 142/74,871 ms. q6 and q14 produced
symbolic counterexamples after 54/788 and 87/1,046 ms. Inspection indicates
verifier false positives: equivalent final Decimal predicates or constants are
opaque or fingerprinted differently from their initial forms. Neither was
replay-confirmed, and neither established an optimizer bug.

The subsequent narrow scalar gates lower only a nullable direct comparison
under exact `Coalesce(..., false)` and only an exact constant Decimal `Just`.
They retain wrapper semantics and Optional schema with existing `IfPresent` and
`If` nodes; broader shapes still fail closed to opaque modeling. The old q6 and
q14 witnesses disappear, and the current policy-backed run returns
`VERIFIED_BOUNDED` after 72/749 and 97/33,152 ms, respectively. Both obligations
now enter the proof floor. A bounded UNSAT result has no witness to replay; q5
and q10 remain unproved, and no optimizer correctness bug is confirmed.

The subsequent q12 gate admits only exact
`Coalesce(Or(member == literal, member == literal), false)` and
`Coalesce(And(member != literal, member != literal), false)` forms. Both leaves
must compare the same direct `Optional<String>` member with a non-null `String`
literal; broader Boolean trees remain opaque. The wrapper again
lowers through schema-preserving `if_present`. The fresh complete dashboard
records q12 as `FORMULA_EMITTED` after 109/5,343 ms of preparation/verifier
work; an earlier focused formula run recorded 108/5,816 ms. Focused and
policy-backed proofs returned `VERIFIED_BOUNDED`: the focused run recorded
108/38,880 ms and the then-current policy-floor run 106/40,602 ms. This raises
TPCH formula coverage to 7/22, total formula coverage to 28/121 (23.1%), TPCH
proofs to five, and the total proof floor to 13/121 (10.7%). No proof produced
a candidate, so replay was not invoked and no optimizer correctness bug was
found.

The next exact gate normalizes only a direct `Optional<Decimal>` member under
`Coalesce(member, zero)`, including its matching `Just` wrapper, to the
schema-preserving forms described above. At that milestone the complete TPC-DS
dashboard moves q43 through formula construction after 145/4,760 ms and moves
q77 past Decimal `SUM` headroom to the 25,600-pair grouped-aggregate cap after
2,063/442 ms. TPC-DS now emits 22/99 formulas and the combined workload emits
29/121 (24.0%); q77 remains unsupported. A focused q43 solver run returned
`UNKNOWN` after 147/69,391 ms at the 60-second budget, so the proof floor stays
at thirteen. The first complete run also caught incomplete Decimal-zero casts
in q40 and q80 being rejected while classifying a near-match. Classification
now leaves those forms opaque before invoking the strict exact-cast exporter;
targeted regressions and the repeated complete dashboard restore q40's formula
and q80's verifier entry. No candidate or optimizer bug arose.

Exact scalable grouped-aggregate sharing preserves the established directional
formula while its square fits the ordinary pair cap. Above that threshold it
caches only the symmetric composite null-safe group-key comparisons as an upper
triangle; row membership and first-representative suppression remain directional. The
complete current-code TPC-DS dashboard remains policy-valid at 22/99 formulas,
48 unsupported queries, and 29 optimizer failures. q25 and q29 now reject
32,896 unique composite group-key row comparisons instead of 65,536, q80
rejects 41,616 instead of 82,944, and q77 clears both 160-row aggregates before
its 321-row Sort rejects 51,360 unordered pairs after 2,161/19,363 ms of
preparation/verifier work. Formula and proof coverage remain 29/121 and 13/121,
respectively.

The explicit ordinary comparison core accepts every pair drawn from signed and
unsigned 8-, 16-, 32-, and 64-bit integers for equality, null-safe equality,
and ordering. MiniKQL `DataCompare` uses sign-aware mathematical-integer
semantics for these pairs: a negative signed value remains below every unsigned
value, rather than being converted or wrapped to the unsigned width. The SMT
carrier is already a mathematical integer, and the exact declared-width domain
constraints on literals, source cells, and non-null opaque results make that
encoding exact: width `w` is constrained to `[-2^(w-1), 2^(w-1)-1]` for signed
types and `[0, 2^w-1]` for unsigned types. Ordinary equality and ordering return
SQL NULL if either operand is NULL. Null-safe equality is always non-null: two
NULLs are equal, one NULL is unequal, and two values use the same mathematical
comparison.
Static `IN` deliberately retains its separate lossless-common-type gate above.
`String` and `Utf8` are mutually compatible under the raw-byte comparison
above. Date comparison requires Date on both sides.

This expansion removes TPC-DS q8's former `Uint64 > Int32` snapshot blocker.
The focused real-host run prepared q8 in 480 ms, then failed closed before
verifier construction (`verify_ms = 0`) on unsupported scalar callable
`Unwrap` at both the initial and final boundaries. It therefore changes neither
the then-current formula-construction slice nor the proof floor.

Ordinary Decimal `=`, `<`, `<=`, `>`, and `>=` are strict on NULL; NaN makes
every ordinary comparison false, and infinities participate in the YDB order.
Decimal/Decimal and Decimal/integer operands use the same scale alignment as
YDB `DataCompare`: integers first receive their declared decimal width, scales
are raised, precision is capped at 35, and narrowing or scale-up saturates to an
infinity exactly where the YDB Decimal conversion does. Alignments that would
require an invalid zero-precision Decimal fail closed. Null-safe Decimal
equality is admitted only for exactly identical Decimal types and compares the
encoded non-null values, including NaN; its usual both-NULL/one-NULL behavior is
unchanged.

Decimal Sort, TopSort, and Merge deliberately use a different comparison.
MiniKQL orders the raw signed 128-bit codes, giving the total non-null order
`-Inf < finite values < +Inf < NaN`; descending reverses it. NaN therefore ties
with NaN instead of making the ordering predicate false. Every order item keeps
one exact canonical `Decimal(p,s)` identity. Separate tuple keys may have
different Decimal identities, but one key is never scale-aligned as if it were
a scalar `DataCompare` expression. Explicit `nulls_first` remains orthogonal to
that non-null order at the selected pre-physical boundary.

Decimal arithmetic is deliberately canonical and narrow. The exporter accepts
only binary `+` and `-` with both operands and the result having one exact
canonical `Decimal(p,s)` type, or binary `DecimalMul`/`DecimalDiv` whose left
operand and result have that exact Decimal type and whose right operand is
either the same Decimal type or a signed/unsigned 8/16/32/64-bit integer.
Result nullability must be exactly the OR of operand nullability, and the
subtree must still pass the reviewed closed-world scalar checks. These
callables normalize to explicit `add`, `sub`, `mul`, and `div` snapshot nodes.
An integer is accepted only on the right of `DecimalMul` or `DecimalDiv`; YQL
canonicalizes the supported reversed multiplication spelling before this
boundary.

Evaluation matches `NDecimal` scaled-integer behavior and is strict on NULL.
Addition and subtraction preserve the common scale. Same-type Decimal
multiplication rescales the coefficient product by `10^s` with round-to-nearest,
ties-to-even for both signs; an integer right operand multiplies the coefficient
without rescaling and therefore preserves the Decimal scale. NaN propagation,
indeterminate opposite-infinity addition/same-infinity subtraction,
infinity-times-zero, signed infinities, and finite precision overflow are
explicit. Finite overflow saturates to the appropriate infinity before an
in-band NaN code can be mistaken for a calculated NaN.

Same-type Decimal division multiplies the left coefficient by `10^s` before
division; an integer right operand divides the coefficient directly. The model
matches `NDecimal::Div`'s current signed-remainder behavior rather than assuming
algebraic sign symmetry: positive divisors round to nearest with ties to even,
negative-divisor non-ties truncate toward zero, and exact ties still round to
even. Zero divisors, NaN, signed infinities, global 35-digit normalization,
result-precision saturation, and finite collisions with the reserved NaN code
are explicit.

The arithmetic kernel is checked against independent rational and literal
NDecimal-control-flow references on every legal finite code and all specials
for precisions up to two. Adversarial cases cover both signs of ties-to-even,
negative-divisor non-ties, every integer width, infinity-times-zero, precision
overflow, and finite products or quotients numerically colliding with the NaN
code. C++ exporter tests audit the admitted and rejected signatures. Separate
solver tests send unchanged `add`/`sub`/`mul`/`div` through the normal
logical-to-StageGraph obligation and require `VERIFIED_BOUNDED`; mutations
between those operations must produce concrete counterexamples.

Opaque identity is an inspectable `yql-opaque-v1` canonical string, not a hash.
It preserves exact callable/atom bytes, normalized atom flags, formatted types,
child order, constants, settings, and repeated arguments. Input IU names are
replaced by first-use ordinals and emitted separately as the ordered `args`, so
the same expression remains one UF across optimizer renames while swapped or
repeated values retain their meaning. Positions, allocations, and DAG sharing
do not affect identity. The representation fails closed above 256 expanded
nodes, depth 64, or 64 KiB.

Every complete normalized scalar tree has a separate 1,024-expanded-node and
128-level structural-depth budget, with its root at level one. Repeated source
DAG uses count as separate emitted occurrences. A scan or filter predicate,
each projection, and each literal count or offset reset the budget; generated
join-key equalities and residuals share one synthesized join-predicate budget,
while chained pushed OLAP filters share one assembled scan-predicate budget.
The C++ exporter charges before expansion, guards source recursion, and then
audits the completed normalized JSON iteratively. The Python parser
independently enforces the same contract and turns excessive JSON decoder
nesting into a normal fail-closed error; replay's strict loader does likewise.
The opaque fingerprint limits above, the 512-item static-`IN` limit, and the
64-live-`IfPresent` binding limit remain independent.

The chosen final boundary is immediately before `ConvertToPhysical`. Therefore
the verifier does not prove physical lowering, task construction, or execution.
In particular, the current lowering does not visibly preserve
`TSortElement::NullsFirst`; explicit NULL ordering remains a replay case until
that contract is clarified.

Sort order is an exact, non-empty sequence of `(column, ascending,
nulls_first)` entries. Small inputs are represented by explicit permutations.
For larger inputs, the bounded evaluator gives each candidate row an integer
ordinal: present-row ordinals are in range and pairwise distinct, and strict key
comparisons imply the corresponding ordinal order. Tied keys remain free in
either order, so the symbolic encoding denotes the same complete sequence set
without factorial expansion. A non-null Sort `limit` is TopSort and applies an
exact prefix by compressed ordinal rank after sorting. Sort and Limit phases
(`undefined`, `intermediate`, and `final`) are preserved but are not otherwise
semantic.
Project nodes carry the exact `TOpMap::Ordered` Boolean. Both `ordered: true`
and `ordered: false` currently preserve an input sequence and compatible order
metadata. This matches RBO's streaming WideMap lowering; retaining the exact
flag makes a future semantic distinction an explicit verifier change.

Limit on an ordered input takes the exact `offset:offset+count` slice of the
compressed present-row sequence. If the initial root is ordered, equivalence is
sequence equality; an unordered initial root retains bag equality. The Merge
encoding is exact within the declared row/task/family bounds: it rejects an
unordered or differently ordered producer and represents all sorted,
producer-order-preserving interleavings. Symbolic Merge ordinals preserve the
relative input ordinals within each producer as well as the output sort order.

Ordinal variables are bounded by the fixed candidate-row vector. When result
languages are compared, one side's choices describe a candidate sequence and
the other side's choices are existentially quantified inside the membership
test; the reverse direction is checked as well. The SMT renderer shares repeated
DAG terms through hygienic, dependency-ordered `let` bindings separately inside
each quantifier scope, never hoisting an expression past a binder. These are
exact finite encodings and rendering transformations, not unbounded ordering
proofs or semantic approximations.

`String` and `Utf8` comparison, Sort, TopSort, and Merge use the exact bounded
byte-order quotient above. Date is an exact bounded integer-day type with
literals, equality, ordinary ordering, Sort, and Merge. Decimal has exact
literals, its legal typed domain, and the comparison and arithmetic semantics
above, plus exact raw-code Sort/TopSort/Merge ordering. Generic division, casts
outside the exact integral-`SafeCast` and constant normalization gates, and
aggregate functions outside the modeled subset below remain unsupported.

The aggregate subset covers grouped and scalar `count`, integer `sum`, Decimal
`sum`, and Decimal-only `max`, including NULL grouping and inputs and
optimizer-generated intermediate/final phases. Signed inputs widen to `Int64`,
unsigned inputs widen to `Uint64`, and both integer sums use the runtime's exact
64-bit modular overflow. `sum(Decimal(p,s))` widens every input, partial state,
and result to `Decimal(35,s)` and preserves YDB's NaN/infinity algebra.
Decimal `AggrMax` keeps its input type and uses the runtime's raw signed
128-bit-code order, `-Inf < finite < +Inf < NaN`; this is intentionally
different from ordinary Decimal comparison. NULL inputs are ignored, and the
same scalar state combines exactly across partial and final phases.

Decimal `AggrAdd` saturates each intermediate result and is not associative when
finite overflow is possible. The verifier therefore carries a conservative
absolute finite-code bound through partial states and admits a Decimal sum only
when the total bound is strictly below `10^35`. Within that headroom every row
order and distributed parenthesization has the same exact result; otherwise the
query fails closed. A known bound covers every non-NULL finite coefficient; it
does not approximate NaN or infinities, whose exact value terms remain separate.
Finite literals seed their absolute coefficient, while typed NULL and special
literals seed the vacuous zero bound. Exact same-type `+` and `-` use the capped
sum of operand bounds, and an exact integral-to-Decimal cast derives its bound
from the complete signed or unsigned source-type domain, target scale, and
finite saturation point. `If`/`IfPresent` branches and stage alternatives take
conservative maxima, aliases preserve bounds, and Decimal `SUM` states
accumulate them. Any missing operand proof remains unknown and can only make a
later sum fail closed.

A column-storage source is split into symbolic source tasks before a pushed
intermediate aggregate executes. The separate manual runtime
diagnostic deliberately crosses this rejected boundary with the same three
valid rows in one- and two-partition column tables. Both optimizer modes return
`M` in the former case and `inf` in the latter, confirming why the verifier must
not assume associativity outside the proved headroom.
The optimizer's canonical `COUNT(*)` extractor is represented by the exact
zero-child, typed `Void` expression and evaluated as one non-null unit value;
it may pass through routing and relational operators, but every path must
terminate in a non-distinct, non-unwrapped `count`. Inspecting, dropping, or
exposing that unit fails closed. `Void` is not a catalog, literal, NULL, or
opaque-result type.
`distinct`, `DistinctAll`, `unwrap`, min, non-Decimal max, average, and variance
currently return `UNSUPPORTED` rather than use an approximation.
Intermediate aggregation models the pre-physical logical state per task and
key; memory-pressure batching performed later by a physical hash combiner is
outside the snapshot boundary.

An unordered `Limit` is not modeled as an arbitrary fixed vector prefix. For
each input outcome, the kernel enumerates every guarded-row mask whose size is
`min(count, max(input_size - offset, 0))`. Plans are equivalent only when their
sets of enabled output bags mutually include one another. Choice identities are
carried through the DAG, so two uses of one Limit node share a choice, while
stage-task instances choose independently. Count and offset are restricted to
non-null `Uint64` literals in v1; parameterized or otherwise computed limits
fail closed. Phase is preserved as `undefined`, `intermediate`, or `final`, but
does not itself change runtime semantics. Distinct Limit observers downstream
of one shared unordered plan stream fail closed until their latent-order
correlation is modeled. Ordered Limit is deterministic; Aggregate, Join, and
UnionAll start new unordered streams.

```json
{
  "id": "n2",
  "op": "limit",
  "input": "n1",
  "count": {"kind": "literal", "type": "Uint64", "value": 10},
  "offset": null,
  "phase": "final"
}
```

The current exporter emits `pushed_limit` on every scan, either `null` or the
same exact literal shape. Its absence in a legacy-v1 snapshot means `null`,
because the earlier v1 exporter rejected every pushed read limit. A non-null
pushed limit is valid only on a column-storage source and executes independently
on each source task after symbolic row partitioning.

The exporter also emits `predicate` on every scan, with legacy absence meaning
`null`. A non-null predicate is decoded from the executed `OlapFilterLambda`
rather than `OriginalPredicate` statistics metadata. Version one accepts only a
one-argument chain of `KqpOlapFilter` operations ending at that exact argument;
its scalar subset is physical columns, supported literals, Boolean
AND/OR/NOT, the equality and ordering families described above (including
all-pairs ordinary integral `DataCompare`), and exact presence tests. A
`TKqpOlapFilterUnaryOp` is admitted only as an exact two-child tuple whose
operator is the Atom `exists` or `empty`; its recursively decoded argument is
lowered respectively to `exists(x)` or `not(exists(x))`. A non-Atom or unknown
operator (including `just`) and an unavailable physical column fail closed.
`Coalesce(predicate, false)` is erased only in a positive filter context: at
the filter boundary or beneath AND/OR. The same form beneath NOT, comparison,
or a unary presence operation fails closed because the erasure is not
value-preserving there. Projection wrappers, range reads, malformed type
descriptors, and unknown operations also fail closed. The predicate filters raw
scan rows before symbolic source partitioning and any per-task pushed limit.

Every relation is capped at 4096 candidate rows. Join matrices and outputs,
UnionAll, and grouped aggregation are checked before construction; Sort, Merge,
and latent sequences may construct at most 16384 candidate-row pairs before
factorials or symbolic ordinals are allocated. Every explicit outcome family is
separately capped at 256 alternatives, including unordered-Limit choices, small
enumerated sequence choices, Cartesian products, and gathers. Large ordered
families switch to bounded symbolic ordinals, and cross-plan bag or sequence
equality is capped at 4096 explicit outcome pairs. Exceeding any audit bound
returns `UNSUPPORTED` rather than allocating an unbounded intermediate or
approximating semantics.
The pair ceiling admits q71's 9072-term Merge ordinal construction while q31
deterministically fails before allocating its 32768-pair join matrix.
When a grouped aggregate's directional square fits the pair ceiling, it retains
the established directional formula. Otherwise, null-safe group-key equality
is shared as one symmetric upper-triangular term per unordered row pair and the
same ceiling is applied to that triangle. Row-presence membership and
first-representative guards remain directional; the accepted triangular bound
keeps their `N^2` count strictly below twice the ordinary pair ceiling. No
directional predicate is treated as symmetric.

Aggregate nodes preserve ordered keys and traits, output type/nullability, and
phase explicitly:

```json
{
  "id": "n1",
  "op": "aggregate",
  "input": "n0",
  "keys": ["t.k"],
  "aggregates": [{
    "input": "t.v",
    "function": "count",
    "output": "cnt",
    "type": "Uint64",
    "nullable": false,
    "distinct": false,
    "unwrap": false
  }],
  "phase": "intermediate",
  "distinct_all": false
}
```

## StageGraph v1 shape

The strict decoder rejects unknown fields and missing required fields (with the
documented legacy-v1 scan exception above). The abbreviated shape is:

```json
{
  "root_stage": "s2",
  "stages": [{
    "id": "s0",
    "nodes": ["n0"],
    "inputs": [],
    "outputs": [{"index": 0, "node": "n0"}],
    "source_storage": "row"
  }],
  "edges": [{
    "id": "e0",
    "producer": "s0",
    "consumer": "s2",
    "occurrence": 0,
    "producer_output": 0,
    "consumer_input": 0,
    "kind": "hash_shuffle",
    "keys": ["a.k"],
    "hash_function": "HashV1",
    "use_spilling": true
  }],
  "assumptions": []
}
```

Map and Broadcast add no variant fields. UnionAll adds `parallel`. Merge adds
`order`, whose elements contain `column`, `ascending`, and `nulls_first`.
`source_storage` is `row`, `column`, or `null`. Version one requires an empty
`assumptions` array.

## Development setup

The Python kernel has no package dependencies. Its CLI accepts an explicit
Z3-compatible executable when solving a formula. Tests run through `ya` instead
resolve the pinned Z3 4.16.0 target under `contrib/tools/z3`; no ambient solver
installation or `RBO_Z3` setting can affect those hermetic test runs.

```bash
python3 -m unittest discover -s ydb/core/kqp/opt/rbo/verification/ut
```

Run its tests with:

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/ut 2>&1 | tail
```

The `ya` test target always builds and runs the solver integration tests. The
raw `unittest` command above still supports lightweight development outside the
`ya` environment: it skips solver cases unless `RBO_Z3` names an external
compatible executable.

The real-host integration tests construct a new-RBO `IKqpHost` and send each
captured initial/final pair through the normal CLI. One test covers an explicit
`LIMIT 1` and its split intermediate/final form. The ordered test covers
`ORDER BY A DESC, B ASC LIMIT 1`: it checks the initial Sort+Limit, final
per-task intermediate TopSort, exact Merge metadata (including NULL placement),
and final Limit. Column-store tests compare initial logical Filters with final
pushed OLAP predicates, cover `IS NULL` combined with comparisons and
`IS NULL OR IS NOT NULL`, and require the normal bounded proof. The benchmark
test loads the exact `TPCDS_YQL` schema and q96 source used by the new-RBO suite,
checks its split `COUNT(*)`, pushed predicates, four-table join, and StageGraph,
and proves the two-row/two-task obligation with a query-specific 60-second
solver budget. Nullable `String IN ('first', 'second')` and
`Int64 IN (Int32...)` expressions exercise both static-membership type gates
through the real host and prove the normal obligation. A separate
`COUNT(*) > 1` query captures ordinary `Uint64 > Int32` comparison at both
real-host boundaries and returns `VERIFIED_BOUNDED` at two rows and two tasks.
A native Decimal column filter likewise checks exact tagged literals and
comparison predicates at both real-host boundaries and proves its normal
two-row/two-task obligation.
A Decimal cast query checks `COUNT(*)` and `1 + COUNT(*)` as non-null `Uint64`
expressions cast to `Decimal(15,4)` at both boundaries, then proves the normal
two-row/two-task obligation.
A Decimal arithmetic query checks `+`, `-`, same-type multiplication and
division, integer-right multiplication and division, and YQL's normalization
of the reversed multiplication spelling at both boundaries, then proves the
two-row/two-task obligation. All equivalent real-host fixtures require
`VERIFIED_BOUNDED` from the hermetic solver. A
Decimal aggregate query checks the `Decimal(7,2)` to `Decimal(35,2)` widening,
undefined/intermediate/final `sum` phases, and serial partial-state UnionAll,
then proves its two-row/two-task obligation. A
separate Decimal ordered query covers logical Sort+Limit and the transformed
per-task TopSort+Merge+final-Limit path. That proof ends before physical
lowering, consistent with the boundary caveat above.

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/integration_ut 2>&1 | tail
```

Build the Ya-owned CLI with:

```bash
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/bin
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/confirm_bin
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/inspect_bin
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/replay_bin
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/prefix_capture/bin
./ya make --build relwithdebinfo contrib/tools/z3
```

## CLI

```bash
PYTHONPATH=ydb/core/kqp/opt/rbo/verification \
python3 -m rbo_verifier before.json after.json \
  --rows 2 --timeout-ms 10000 --solver /path/to/z3
```

The command prints a JSON verdict. A `COUNTEREXAMPLE` verdict contains only the
present base-table rows; opaque-function and symbolic-routing interpretations
and the unmatched unordered-Limit choice are deliberately not treated as a
stable witness format, so concrete replay is the confirmation boundary. Every
bounded verdict reports both `row_bound` and the fixed `task_bound` of two. A
`SCHEMA_MISMATCH` verdict is a direct correctness failure and does not depend on
either bound. Use `--emit-smt formula.smt2` without `--solver` to inspect the
exact proof obligation. If satisfiability succeeds but model extraction returns
unknown, the result remains `COUNTEREXAMPLE` with a reason and no witness.

Normal verification requires a logical initial snapshot and a final snapshot
with a complete StageGraph. The separate localization path may compare that
same logical initial snapshot with a captured logical or complete staged
transformation prefix:

```bash
PYTHONPATH=ydb/core/kqp/opt/rbo/verification \
python3 -m rbo_verifier initial.json prefix.json \
  --diagnostic-transformation-prefix --rows 2 --solver /path/to/z3
```

This mode uses the same formula kernel but an explicitly different boundary
contract. Every result, including errors, carries
`"comparison_scope":"OPTIMIZER_TRANSFORMATION_PREFIX"`; it is evidence about one
optimizer prefix, not a whole start-to-finish verdict. A standalone sequential
localizer drives this mode because equivalence is not monotonic across rule
applications or atomic stage commits and therefore cannot be bisected soundly
with binary search.
`kqp_rbo_prefix_capture` supplies the real-host capture side of that protocol;
see [tools/README.md](tools/README.md) for its strict artifact contract and the
complete localizer invocation.

## Plan and counterexample inspection

The separate `kqp_rbo_inspect` executable renders every modeled snapshot field
as deterministic line-oriented text:

```bash
ydb/core/kqp/opt/rbo/verification/inspect_bin/kqp_rbo_inspect \
  plan final.snapshot.json
```

Its `witness` command rebuilds and solves the normal start-to-finish obligation,
then prints the candidate database and concrete result family at every logical
operator, stage task, connection input, and compared root boundary:

```bash
ydb/core/kqp/opt/rbo/verification/inspect_bin/kqp_rbo_inspect \
  witness initial.snapshot.json final.snapshot.json \
  --query exact-query.yql \
  --solver /path/to/z3 --rows 2 --timeout-ms 10000
```

The inspector observes immutable SMT terms during normal evaluation. Only after
the obligation is complete does it add definitional aliases for model
extraction. Base rows, routing-dependent rows, opaque scalar results,
nondeterministic outcomes, and the exact root mismatch are requested together
from one SAT model. All enabled outcomes are printed; absent rows omit their
meaningless payloads. Trace extraction fails closed above 100,000 unique terms.
Enabling the read-only observers without aliases is regression-tested to leave
the normal SMT-LIB obligation byte-for-byte unchanged. Every trace carries
SHA-256 digests of the complete normalized before/after snapshots; supplying
`--query` also binds the exact query bytes and is mandatory for real replay.
When tracing a saved verifier candidate, `--verifier-verdict verdict.json`
constrains the rebuilt obligation to that verdict's decoded base-table rows.
The inspector may resolve routing, opaque-function, and ordering choices, but
cannot silently select a different database. If the saved database no longer
makes the obligation satisfiable, the diagnostic status is
`WITNESS_NOT_REPRODUCED`, not a global equivalence proof.

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/inspect_ut 2>&1 | tail
```

## Real-YDB counterexample replay

`kqp_rbo_replay` consumes the inspector's version-one concrete trace and runs
the exact witness against two isolated YDB targets. The baseline target must
have the legacy optimizer enabled; the candidate must have the new RBO enabled
with fallback disabled. For parity with the benchmark host, use identical
settings except for `enable_new_rbo`:

```yaml
table_service_config:
  enable_new_rbo: true # false on the baseline target
  enable_fallback_to_yql_optimizer: false
  allow_olap_data_query: true
  default_lang_ver: 202602
  default_cost_based_optimization_level: 2
  backport_mode: All
query_service_config:
  script_result_rows_limit: 0
  script_result_size_limit: 0
```

The query file must contain the exact text used for snapshot capture. In
particular, TPC-DS replay includes the benchmark's `$to_decimal`,
`$to_decimal_max_precision`, and `$round` compatibility definitions.

```bash
ydb/core/kqp/opt/rbo/verification/replay_bin/kqp_rbo_replay \
  initial.snapshot.json final.snapshot.json concrete-trace.json query.yql \
  --ydb /path/to/ydb \
  --baseline-endpoint grpc://baseline-host:2136 \
  --baseline-database /Root/baseline \
  --candidate-endpoint grpc://candidate-host:2136 \
  --candidate-database /Root/candidate
```

Before connecting, the tool strictly validates both snapshots, the trace and
witness shape, primary keys, source integer/Date/Decimal ranges, table identity
encoding, storage inference, exact backtick-quoted source paths, result schema,
and observable determinism. Legal Decimal special codes are accepted and
rendered for BulkUpsert as `-inf`, `inf`, and `nan`; invalid or out-of-precision
finite codes still fail closed. The tool accepts exactly one top-level result
query; the current TPC-DS sources q14, q23, and q39 contain two result queries
and fail closed until replay gains an explicit multi-result contract. It returns
`INCONCLUSIVE_NONDETERMINISM` when the bounded model admits more than one
distinct result for either side.

Each target receives a fresh `_rbo_replay_<128-bit-id>` namespace containing
reduced two-partition tables. Rows are loaded with the CLI BulkUpsert import
path, not an SQL write that would exercise the optimizer under test. Explain is
then run through QueryService. The current new-RBO statistics shape is required
on the candidate, the legacy/absent shape on the baseline, and every candidate
CBO tree must be optimized; this also detects transparent fallback to the old
optimizer. Results use `json-base64-array`, preserve binary strings and
duplicates, and are compared as a sequence or bag according to the initial
snapshot contract.

`REAL_RESULT_DIVERGENCE` means the two real executions differed, while
`NOT_REPRODUCED` means this concrete realization did not differ; neither result
claims unbounded equivalence. The first is strong evidence of an optimizer
correctness problem, but it is not attributed to the supplied symbolic trace
without reproducing that exact final StageGraph. The reduced catalog does not
reproduce indexes, statistics, or every physical table setting, and the
external CLI cannot recapture the final semantic snapshot, so output explicitly
records `trace_plan_reproduced: false`. Column-store sources use two hash
partitions. Row-store synthesis currently fails closed unless the leading
primary-key column is `Uint32` or `Uint64`, the types for which YDB supports
auditable two-way `UNIFORM_PARTITIONS` creation.

Replay namespaces are deliberately retained for diagnosis and are printed in
the JSON result. The tool never deletes an existing or generated YDB object.
Its audit boundary is split by responsibility: `case.py` validates the
certificate and witness, `materialize.py` renders the isolated catalog and
read-only query, `observation.py` decodes real results, and `runner.py` contains
the external mutation boundary.

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/replay_ut 2>&1 | tail
```

## Automatic counterexample confirmation

A version-four benchmark coverage report preserves the exact assembled query,
both snapshots, and byte-exact raw verifier verdict, with SHA-256 bindings, for
every symbolic counterexample. The raw verdict artifact is the authoritative
witness source; the parsed verdict in the report contains metadata only and
deliberately omits the witness so wide Decimal integers cannot be rounded by a
JSON object round trip. `kqp_rbo_confirm` consumes one such report and processes
every counterexample in query-ID order. For each candidate it validates and
copies all four bound inputs, gives the raw verdict directly to the inspector
to fix that exact database, and then invokes `kqp_rbo_replay` against explicit
isolated targets:

```bash
ydb/core/kqp/opt/rbo/verification/confirm_bin/kqp_rbo_confirm \
  /path/to/tpcds_coverage.json \
  --inspector /path/to/kqp_rbo_inspect \
  --solver /path/to/z3 \
  --replay /path/to/kqp_rbo_replay \
  --ydb /path/to/ydb \
  --artifacts /new/path/tpcds_confirmation \
  --baseline-endpoint grpc://baseline-host:2136 \
  --baseline-database /Root/baseline \
  --candidate-endpoint grpc://candidate-host:2136 \
  --candidate-database /Root/candidate
```

The artifact directory must not already exist. It retains the source report,
byte-exact copies of the four SHA-bound inputs and their digests, exact child
commands/stdout/stderr, per-query results, and one versioned summary. The driver
never stops after the first candidate. `NO_COUNTEREXAMPLES` and
`ALL_NOT_REPRODUCED` exit zero; a fully processed
`REAL_RESULT_DIVERGENCE` exits one; any missing witness, artifact violation,
inspector inconsistency, nondeterminism, setup failure, or child protocol error
makes the whole run `UNRESOLVED` and exits two. A confirmed divergence is the
input to the separate transformation-prefix localizer; confirmation itself
does not add rule-bisection machinery. Automatic replay currently accepts one
top-level result query; a counterexample for multi-result TPC-DS q14, q23, or
q39 fails closed as `UNRESOLVED` until replay gains an explicit multi-result
contract.

This command is the mandatory follow-up for every coverage
`COUNTEREXAMPLE`. It is separate from recursive tests because it writes retained
namespaces to user-supplied external YDB targets.

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/confirmation_ut 2>&1 | tail
```
