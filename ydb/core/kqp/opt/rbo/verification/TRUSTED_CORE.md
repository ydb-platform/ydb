# Current trusted core and audit map

This is the current review map. The full
[bounded theorem and solver protocol](contracts/THEOREM.md) and
[external runtime assumptions](contracts/EXTERNAL_ASSUMPTIONS.md) are maintained
separately so their exact restrictions remain visible without milestone history.
The [archive](history/README.md) is evidence history, not the current contract.
The [findings ledger](FINDINGS.md) separates current runtime reproductions,
fixed regressions, bounded routing findings, and model false positives.

## What is trusted

| Review unit | Responsibility |
|---|---|
| [semantic_snapshot.cpp](semantic_snapshot.cpp), [semantic_snapshot.h](semantic_snapshot.h), private exporter helpers | Faithful boundary/catalog/scalar/operator/StageGraph export, exact admission, and fail-closed diagnostics. |
| [ir.py](rbo_verifier/ir.py), [analysis.py](rbo_verifier/analysis.py), [window_admission.py](rbo_verifier/window_admission.py) | Strict versioned decoding, immutable validated plan facts shared across evaluations, and mandatory normalized-window capability checks. |
| [types.py](rbo_verifier/types.py), [scalar.py](rbo_verifier/scalar.py), [decimal.py](rbo_verifier/decimal.py), [string_order.py](rbo_verifier/string_order.py) | Domains, NULLs, scalar semantics, closed opaque identities, and certified aggregate/value metadata. |
| [stable_pickle.py](rbo_verifier/stable_pickle.py) | Reviewed scalar serialization equality; linear left-inverse axioms preserve NULL and value identity under every bounded choice. Byte contents and cross-type identity remain abstract. |
| [floating.py](rbo_verifier/floating.py) | Explicit binary64 primitive identities, bit comparisons, and literal Welford state transitions; requires the sufficient-proof mode, not default language equality. |
| [join.py](rbo_verifier/join.py), [relation.py](rbo_verifier/relation.py) | Operator semantics, baseline and optimized encodings, family composition, and equality of bags/sequences/result languages. |
| [aggregate.py](rbo_verifier/aggregate.py), [window.py](rbo_verifier/window.py) | Typed group reductions and task-local window values over admitted presences, values, and ordinals; grouping, routing, choices, and provenance stay with their callers. |
| [value_transport.py](rbo_verifier/value_transport.py), [stages.py](rbo_verifier/stages.py) | Explicit scalar/physical-state/proof-hint transport, task execution, routing, locality, and occurrence/certificate propagation. |
| [sort_strategy.py](rbo_verifier/sort_strategy.py) | Pure cost/certificate-based encoding selection; choosing an encoding still affects the trusted proof path. |
| [sort_network.py](rbo_verifier/sort_network.py), [smt.py](rbo_verifier/smt.py) | Exact network topology, typed owned terms, binder hygiene, structural sharing, and SMT serialization. |
| [verify.py](rbo_verifier/verify.py), [bundle.py](rbo_verifier/bundle.py) | Shared database, joint buffered result slots, explicit comparison mode, model-domain observation, exact solver portfolio, one-deadline status interpretation, and witness extraction. |

Large-script quantifier rendering may reuse an immutable body's independently
hygienic text and complete source-name set. Enclosing lets still reserve those
names, and cached roots stay alive to prevent object-identity reuse. The cache
is per Script, capped at 128 MiB including name storage and 4,096 entries, and
activates at 65,536 source DAG nodes. Small scripts retain the original renderer.
Declaration/assertion changes still invalidate the complete-body cache.

Private exporter helpers include [read ranges](read_range_predicate_impl.h),
[opaque-expression audit and identity](opaque_expression_audit_impl.h),
[window leaves](window_expression_export_impl.h), [window dataflow](window_projection_audit_impl.h),
and the separately audited [ordered-ROWS corridor](q51_window_projection_audit_impl.h). They remain part of the
same trusted exporter, not independent serialization APIs.
Logical `GroupingSets` is exported independently as a bag union of aggregates
and projections over the template's input. Repeated sets retain multiplicity;
omitted keys become NULL, and indicators distinguish those keys from NULL groups.
The ordinary aggregate auditor checks each trait and branch nullability. A direct
`Just(Member)` of an admitted required Data value is an exact present optional,
with matching source/result types and the normal scalar safety audit. Treating
this wrapper as opaque would permit spurious NULLs and changed grouping values.
Binary integral `Coalesce(Member, fallback)` lowers to an exact optional branch
only when all annotations agree on the same integer type, the fallback is
required, and the closed scalar audit accepts the complete expression.
Boolean `Coalesce(Member, false)` uses the same exact optional branch with
strict Bool/Optional<Bool> annotations. Static membership may peel only complete
required Utf8-to-String byte copies. Whole-partition Decimal SUM/AVG accepts
nullable Utf8 keys under the existing grouped-key, headroom and private-path
certificates. Date-plus-whole-day arithmetic may consume a reviewed direct
String/Utf8 member cast; the parser's NULL wrapper and existing opaque Date
operation identity remain explicit. Point read ranges may use catalog Int32 or
Int64 keys, with every boundary/descriptor checked against that same type and
only the existing Int32 literal point grammar admitted.
The pushed OLAP Apply extension admits only
`Coalesce(SafeCast(column, Optional<Date>) comparison Date-literal, false)`,
including a typed `Just` around the literal. Its single String/Utf8 column,
binding, descriptors, annotations, comparison and false fallback are checked.
It shares the logical parser abstraction and preserves NULL behavior; an
abstract parser counterexample still requires concrete replay.
The SafeCast membership spelling, pushed Date comparison and native Int32
fixture corrections pass the R4 native and focused SQL gates.
Required `SafeCast(Int32(0), Decimal(p,s))` may feed the reviewed Decimal `Just`
and zero-Coalesce forms when `p > s`, even if the complete Int32 domain would
lose precision. The full target descriptor/annotation audit and exact weak-cast
kernel remain in force; other weak-cast constants are not newly admitted.

Pushed LIKE bodies may compose audited leaves with unannotated And/Or nodes
of at least two operands and unary Not. Every leaf must still use the sole
bound String/Utf8 column and the exact reviewed Re2 configuration. This does
not admit arbitrary Apply bodies.
Scan `predicate_columns` map captured physical catalog columns to bindings
visible only while evaluating the pushed predicate. They never create output
IUs or downstream values. Source/output collisions and unknown catalog columns
reject; types and nullability come from the same captured catalog as the
ordinary scan columns. The new native/fresh-SQL gates for these two extensions
are pending.
Decimal AVG transport may normalize its internal optional `(sum, count)` tuple
only after certifying the entire single-consumer path, the exact widened sum and
Uint64 count types, and the sole live AVG producer among UnionAll padding leaves.
This normalization does not admit tuples as general scalar values.
Decimal AVG enumerates constant divisors only when its independently derived
count bound is at most eight. Counts are sums of guarded ones or certified
partial counts; enumerating every integer from zero through that bound preserves
rounding, infinities and NaN. Larger bounds retain general division. Twenty
symbolic equivalence checks and 3,268 concrete signed/special-value cases compare
this encoding with the general kernel and an independent runtime transcription.
Checked-concat results may cross pure column projections inside the final stage.
An appended copy can retain the original identity column; every retained alias
remains audited on later consumers. At most one fresh copy per projection is
allowed. Computation, stage changes, selectors, dropped results and duplicate
new copies reject.
Both checked values and failure conditions remain observable. Up to two direct
COUNT DISTINCT traits use independent per-trait membership sets; the pair budget
accounts for both sets.
The shared [RelationError boundary](rbo_verifier/errors.py) classifies unsupported
relational semantics consistently across the extracted kernels and their callers.

Certificates and optimized encodings are trusted even if private, separated into
modules, or covered by differential tests. A fast path that removes rows,
equality branches, errors, choices, or metadata must establish the corresponding
semantic-preservation premises. A compact top-level obligation builder is not a
proxy for the total trusted-code size.

## Review one operator vertically

Use the [five-item operator audit card](PLAN.md#operator-review-unit):
denotation, admission, composition, encoding equivalence, and evidence.
Trace a field through C++ export, JSON decoding/validated facts, evaluator
semantics, transported state, result equality, and the final assertion.
Review every nearby rejected shape and exact resource boundary.

Shared analysis is structural evidence, not a license to accept extra semantics.
`ValidatedPlan` performs strict schema/admission checks; cross-plan root schema
comparison precedes `analyze_validated`'s relational capability checks. Thus an
unsupported fanout cannot hide a definite root type/nullability mismatch.
Its immutable `ProjectEffects` records window leaves and error-producing
expressions once. These are syntactic facts, not proofs of totality: evaluation
still observes actual routed inputs and discharges the existing obligations.
A strategy choice must not change catalog constraints or the admitted snapshot.
Bag comparison omits only slots whose presence term is the literal false: their
multiplicity contribution is zero and their candidate obligation is true.
Symbolic guards, duplicate counts, NULL equality and schema checks remain in the
comparison. Differential tests retain the original construction by replacing
literal absence with a symbolic guard constrained to false.
For a tiny accepted input, compare the baseline, forced eligible optimization,
and an independent concrete reference; then mutate a semantic field and an
eligibility premise separately. Retain error and nondeterministic-outcome tests,
not only successful-row comparisons.

## Independent confidence boundaries

| Slice | Primary evidence |
|---|---|
| Capture/catalog/typed export | [native fixtures](native_cpp_ut), [legacy fixtures awaiting migration](cpp_ut), [real-host integration](integration_ut) |
| Logical operators and joins | [concrete bag reference](ut/test_logical_reference.py), [operator and mutation tests](ut/test_verify.py) |
| StageGraph and routing | [concrete two-task reference](ut/test_stagegraph_reference.py), [passive ports and shared choices](ut/test_stage_ports.py), [compaction tests](ut/test_stage_compaction.py) |
| NULL/scalars/Decimals/subplans/windows | [semantic unit tests](ut) and cross-language exporter mutations |
| Sort/Limit/result languages | [sort tests](ut/test_sort.py), [limit tests](ut/test_limit.py), [native Sort/Merge NULL-order comparator and mutation checks](native_cpp_ut/native_snapshot_ut.cpp) |
| SMT ownership/rendering/solver protocol | [SMT tests](ut/test_smt.py), [verdict tests](ut/test_verify.py) |
| Reach and operational regressions | [coverage policy](benchmark_ut/coverage_policy.json), [benchmark runbook](BENCHMARK_COVERAGE.md) |

Small exhaustive and solver-backed differential tests provide strong evidence
within their tested domains. They do not establish universal correctness of a
semantic rule. Workload reach is the last gate, not the semantic oracle.

Inspector, replay, confirmation, prefix capture/localization, benchmark policy,
reports, all tests, documentation, and retained artifacts are outside the
normal proof-producing encoder. Their correctness is still essential to
operational use, replay safety, and attribution of findings. Passing them cannot
repair an unsound exporter or semantic encoder.

The optional [nonempty-output diagnostic](rbo_verifier/diagnostics.py) is also
outside verdict classification. It reuses bounded family composition, errors,
choice quantification and the solver protocol, replacing only the marked query
predicate under a separate deadline after the equivalence verdict is complete.

## Preserve the theorem when refactoring

Normal verification ends before physical lowering and runtime execution.
It depends on faithful host-hook placement, the captured catalog/plans, reviewed
runtime semantics, and a correct solver/protocol; see
[external assumptions](contracts/EXTERNAL_ASSUMPTIONS.md).

The emitted canonical SMT formula is not a transcript of the internal branch
portfolio. A proof requires the mandatory model-domain exclusions and either
canonical UNSAT or every required exact branch UNSAT under one deadline.
Unknown/untried branches never establish a proof. Checked-String totality,
native scalar cardinality-guard totality,
integral-AVG domain/abstract-value restrictions and explicit binary64
SAT-to-UNKNOWN rules apply to standalone SMT as well.

Do not broaden an admission gate, replace a certificate with an unchecked flag,
erase an opaque fingerprint detail, or change a runtime/hash/solver assumption
as an incidental file split. Such changes require a new semantic review.

### Null-equal join keys

A side-explicit join key may carry `equal_nulls: true`; absence retains ordinary
SQL equality. The exporter preserves `TJoinKey::EqualNulls` from the new RBO
representation. Matching is exact non-NULL equality or two NULLs, and still
requires both rows and the residual predicate. This follows the per-key
`EqualNulls` setting in `kqp_rbo_physical_join_builder.cpp` and the decorrelator's
`MakeNullSafeJoinKeys` choice between native null matching and StablePickle.

Admission requires identical fixed-width integer, Bool, String, Utf8, Date,
Datetime or Timestamp types, with at most one Optional layer. Decimal, floating
point and cross-type null-equal comparisons remain unsupported. Existing compact
join admission still requires a nonnullable complete catalog key on the RHS.
The concrete oracle covers all ten join kinds over 256 two-slot bag pairs each;
native mutation checks remove the flag and must expose lost NULL matches.

### Date fallback and complete integer constants

Optional Date `Coalesce` with the audited Date-zero fallback uses the same exact
present/missing semantics as its existing Unwrap form. The source must be a
visible optional Date member or a reviewed String/Utf8-to-Date conversion. The
fallback is exactly `Just(Date(0))` or the reviewed Int32-zero SafeCast spelling;
an unreachable typed NULL branch retains the declared Optional annotation.

A SafeCast of a direct required integer literal folds to the target-typed literal
only when the runtime cast classifier says Complete. Exact source annotation,
target descriptor, literal range and closed scalar-source audits still apply.
Native positives compare pre/post constant-folding forms, and altered fallbacks
must produce counterexamples. General integer casts do not gain new admission.

### StablePickle text equality

String and Utf8 use the same typed injectivity abstraction as other admitted
StablePickle scalars. `ScanTypeProperties` unwraps one Optional and sets
`UseTopLength` for either text type. `PackImpl` copies the complete byte payload;
`TValuePackerGeneric::Pack` uses an unambiguous length header and a distinct empty
Optional flag. Required text and present Optional text therefore share bytes,
including the empty string, while NULL has its own representation. The native
packer checks byte equality and round trips at short/long length boundaries,
embedded zero bytes, arbitrary String bytes, and multibyte UTF-8 values.

This admission asserts neither actual encoded bytes nor an ordering between
encoded values. Cross-type identity remains abstract, including String versus
Utf8; counterexamples involving unconstrained bytes require runtime replay.

### Required and nullable integer AVG

The `integral_double_v1` state records the exact fixed-width integer source and
its nullability. Intermediate logical carriers retain source nullability;
completed scalar AVG becomes Optional even for required input, while a grouped
AVG preserves source nullability. Exporter admission mirrors `GetAvgResultType`
and RBO aggregate type annotation, and still requires one direct matching
intermediate/final state edge with identical ordered grouping keys.

The kernel stores count and source-range minimum/maximum. Final NULL depends
on count zero, including a present required intermediate carrier for an empty
task. Per-source extrema bounds include the whole Uint64 domain. The existing
mandatory proof that every AVG sees at most two non-NULL values is retained:
within that domain the rounded finite binary64 sum is independent of ordering,
and count/minimum/maximum identify its complete input multiset. Larger groups
remain unproved (UNKNOWN when the model-domain exclusion is reachable). Float/Double/Bool AVG, distinct AVG and indirect integral
state transport remain unsupported. A counterexample involving the abstract
non-binary64 result carrier still requires runtime replay.

### Native ANSI Rank

Native `TOpWindow` Rank now exports the existing typed `window_rank` kernel.
Only option-free native `rank` functions with no arguments are admitted, with
at most two functions/order keys and four partition keys. Ordered ranks retain
the existing private-project and six-leaf snapshot bounds. Native frames do
not affect Rank: `TPhysicalWindowBuilder::BuildAccumulator` starts at one,
increments the row position, and changes rank only when `AggrNotEquals` detects
a different complete sort key. Partitions use the same aggregate equality.
`BuildSortKeys` passes direction only, so `SnapshotSortElements` records NULL
first for ascending and last for descending. With no order every row is a peer
and the exported result is literal Uint64 one. This admission does not cover
dense rank, row number, mixed native/aggregate functions, or Double keys.

### Private scan predicate inputs and logical pushdown

A scan may bind catalog columns privately for its reviewed row-local predicate
when pruning removes them from the visible output. Bindings must have exact
catalog types and distinct source/output identities; private values are scoped
to predicate evaluation and cannot become downstream columns. Empty visible
rows still retain their presence and bag multiplicity. Native and Python
mutations change a private key predicate and must produce a counterexample.

Closed And/Or/Not compositions of compiled LIKE leaves preserve the existing
leaf audit, including matching member types, constant patterns and reviewed
Re2 descriptors. String and Utf8 remain distinct typed inputs.

R6 allows the same reviewed predicate before stage assignment, where it has
ordinary row-local filter semantics. A physical stage graph must still place
the predicate in a column-storage source stage. Pushed limits continue to
require that stage boundary because their selection is task-local. Logical
intermediate comparisons use the diagnostic transformation-prefix entrypoint;
normal initial/final admission roles are unchanged.

### GroupingSets COUNT(*) carriers

R6 retains the exact Void unit carrier for COUNT(*) when auditing a
GroupingSets aggregate template. Ordinary aggregate admission and the private
Void lineage audit run first; only COUNT on exact Void bypasses the scalar
input-nullability lookup. Passive Replicate ports may copy exact Void to exact
Void; the parser still requires every such path to terminate in canonical COUNT.
Grouping keys, result types, grouping indicators
and duplicate grouping-set multiplicity retain their existing checks.
This admission does not make Void a general scalar or result type.

### Zero-column UnionAll

UnionAll may project away every value column while retaining the presence and
occurrence identity of every input row. It still requires at least two inputs
and matching input/output arities, including zero. Input graphs remain fully
validated. The relational kernel concatenates the input bags with empty value
maps; a later canonical COUNT therefore observes the complete multiplicity.
The final result must still have output columns, and private Void carriers
remain confined to the existing COUNT lineage.

### Grouped exact outcome obligations

For one logical outcome and at least sixteen physical outcomes, the default
exact comparator may solve consecutive groups of four original mismatch
branches. An existing keyed portfolio takes precedence. Each group is precisely
the disjunction of its original predicates; every branch appears in one group,
including both language-nonemptiness checks and the final short group.
The canonical mismatch formula, row/task bounds and model-domain exclusions
are unchanged. UNSAT requires every group to be UNSAT. A satisfiable group
uses that same predicate for model extraction, and an unresolved group keeps
the result UNKNOWN. Multiple logical outcomes and the binary64 universal
comparison retain their previous strategies.

One global solver deadline covers rendering, all solver checks and model
extraction. Subprocess waits cannot extend beyond it. Late UNSAT and late
models remain inconclusive; no timeout is treated as proof.
