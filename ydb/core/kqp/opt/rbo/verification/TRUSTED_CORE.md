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
| [floating.py](rbo_verifier/floating.py) | Explicit binary64 primitive identities, bit comparisons, and literal Welford state transitions; requires the sufficient-proof mode, not default language equality. |
| [join.py](rbo_verifier/join.py), [relation.py](rbo_verifier/relation.py) | Operator semantics, baseline and optimized encodings, family composition, and equality of bags/sequences/result languages. |
| [aggregate.py](rbo_verifier/aggregate.py), [window.py](rbo_verifier/window.py) | Typed group reductions and task-local window values over admitted presences, values, and ordinals; grouping, routing, choices, and provenance stay with their callers. |
| [value_transport.py](rbo_verifier/value_transport.py), [stages.py](rbo_verifier/stages.py) | Explicit scalar/physical-state/proof-hint transport, task execution, routing, locality, and occurrence/certificate propagation. |
| [sort_strategy.py](rbo_verifier/sort_strategy.py) | Pure cost/certificate-based encoding selection; choosing an encoding still affects the trusted proof path. |
| [sort_network.py](rbo_verifier/sort_network.py), [smt.py](rbo_verifier/smt.py) | Exact network topology, typed owned terms, binder hygiene, structural sharing, and SMT serialization. |
| [verify.py](rbo_verifier/verify.py), [bundle.py](rbo_verifier/bundle.py) | Shared database, joint buffered result slots, explicit comparison mode, model-domain observation, exact solver portfolio, one-deadline status interpretation, and witness extraction. |

Private exporter helpers include [read ranges](read_range_predicate_impl.h),
[opaque-expression audit and identity](opaque_expression_audit_impl.h),
[window leaves](window_expression_export_impl.h), [window dataflow](window_projection_audit_impl.h),
and the separately audited [ordered-ROWS corridor](q51_window_projection_audit_impl.h). They remain part of the
same trusted exporter, not independent serialization APIs.
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
A strategy choice must not change catalog constraints or the admitted snapshot.
For a tiny accepted input, compare the baseline, forced eligible optimization,
and an independent concrete reference; then mutate a semantic field and an
eligibility premise separately. Retain error and nondeterministic-outcome tests,
not only successful-row comparisons.

## Independent confidence boundaries

| Slice | Primary evidence |
|---|---|
| Capture/catalog/typed export | [cpp_ut](cpp_ut), [real-host integration](integration_ut) |
| Logical operators and joins | [concrete bag reference](ut/test_logical_reference.py), [operator and mutation tests](ut/test_verify.py) |
| StageGraph and routing | [concrete two-task reference](ut/test_stagegraph_reference.py), [compaction tests](ut/test_stage_compaction.py) |
| NULL/scalars/Decimals/subplans/windows | [semantic unit tests](ut) and cross-language exporter mutations |
| Sort/Limit/result languages | [sort tests](ut/test_sort.py), [limit tests](ut/test_limit.py) |
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

## Preserve the theorem when refactoring

Normal verification ends before physical lowering and runtime execution.
It depends on faithful host-hook placement, the captured catalog/plans, reviewed
runtime semantics, and a correct solver/protocol; see
[external assumptions](contracts/EXTERNAL_ASSUMPTIONS.md).

The emitted canonical SMT formula is not a transcript of the internal branch
portfolio. A proof requires the mandatory model-domain exclusions and either
canonical UNSAT or every required exact branch UNSAT under one deadline.
Unknown/untried branches never establish a proof. Checked-String totality,
integral-AVG domain/abstract-value restrictions and explicit binary64
SAT-to-UNKNOWN rules apply to standalone SMT as well.

Do not broaden an admission gate, replace a certificate with an unchecked flag,
erase an opaque fingerprint detail, or change a runtime/hash/solver assumption
as an incidental file split. Such changes require a new semantic review.
