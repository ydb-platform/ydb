# Findings and focused regressions

This is the maintained status ledger, not a count of failing tests. A historical
counterexample, a deliberately mutated plan, and a current runtime divergence
are different evidence. The [archived finding inventory](history/pre-audit-refactor/BENCHMARK_COVERAGE.md#optimizer-correctness-findings)
retains the original observations and fix commits; do not read its milestone
statuses as fresh results.

## Current runtime check

On **2026-09-05**, the manual paired diagnostics at source `b000dfaba5f` observed:

- **Open: Decimal SUM partition dependence.** Identical `Decimal(35,0)` rows
  `(M, -M, M)`, where `M = 10^35 - 1`, produced `M` with one partition and
  `inf` with two, under both new RBO and legacy optimization. The
  [partition-invariance diagnostic](runtime_ut/decimal_sum_runtime_ut.cpp),
  `DecimalSumRuntimeDiagnostic::PartitionInvariantAcrossOptimizerModes`, first
  validated physical partition counts, exact input rows, and hash routing;
  it then failed with `CONFIRMED_MISMATCH`, not `HARNESS_ASSUMPTION_FAILED`.
  This is a shared aggregation/runtime issue, not an RBO-only finding.
- **Fixed: shared-IU String IN result loss.** The paired
  [String-IN diagnostic](runtime_ut/string_in_runtime_ut.cpp),
  `StringInRuntimeDiagnostic::SharedIUSemiJoinPreservesFactJoinPredicate`, passed:
  both optimizers returned `("same", 10)` with CBO disabled.

The raw run log is local `/tmp/rbo-auditability-known-bugs-runtime.log`, not a
checked-in evidence bundle. These are dated observations; rerun the focused
diagnostic before claiming that a later revision still reproduces a failure.
See [runtime assumptions and setup](runtime_ut/README.md).

The passing `DecimalSumAccumulatorOverflowRequiresHeadroom` test in
[the C++ scalar-contract tests](cpp_ut/semantic_snapshot_exporter_ut.cpp) also
demonstrates the underlying nonassociative Decimal addition. It is not a
substitute for the distributed runtime check. The verifier rejects this unsafe
overflow domain; its bounded proofs do not certify this witness.

## Fixed production optimizer defects

The eleven historically runtime-confirmed defects remain grouped by root cause
below. “Fixed” records the repair and retained regression, not a universal proof
or a claim that every listed runtime test ran in the latest verifier gate.
The production tests below live in [KqpRboYql](../../../ut/rbo/kqp_rbo_yql_ut.cpp).

| Historical cases | Retained focused regression / detector |
|---|---|
| 1: unrelated `NOT flag` leaked negation into a later positive `EXISTS` (`95a2afad1d3`). | `KqpRboYql::ExpressionSubquery`, the `NOT bar.flag AND EXISTS` case, checks the expected row. |
| 2–6: scalar multirow cardinality; lost subplan binding discovery; unlowered scalar projection; invalid empty optional branch; eager local cardinality error with an empty consumer (`e1e3419012c`, `52a1d7c4084`, `9e50d234264`). | `KqpRboYql::ExpressionSubquery` checks aggregate/singleton/computed projections, typed NULL, same-IU bindings, multirow errors, and empty-consumer success. [ScalarSubplanEvaluationTest](ut/test_subplans.py) separately checks model cardinality and demand. |
| 7: CBO/input order suppressed an inherited nested-scalar error when the top-level consumer was empty (`cab0dd1e89c`; model repairs `125962c87df`, `1aaf281c07a`). | `KqpRboYql::ExpressionSubquery`, `nestedMultirowScalarWithEmptyConsumer`; [limit rules](cpp_ut/limit_pushdown_rules_ut.cpp) `KeepsMarkedJoinOutsideCboWhileOptimizingBothSides` / `DoesNotConvertMarkedCrossJoinThroughFilterPushdown`; model `test_inherited_scalar_error_is_observed_without_a_consumer_input_row`. |
| 8: correlation pull-up changed empty keyless `COUNT(*)` from zero to NULL. | `KqpRboYql::CorrelatedSubquery` checks projection, Filter consumption, and the originally grouped negative case. [Correlated rules](cpp_ut/correlated_scalar_rules_ut.cpp) `InlineDirectCountAddsExactOptionalZeroRepair` and [integration](integration_ut/optimizer_snapshot_pair_ut.cpp) `RealHostVerifiesCorrelatedScalarCountEmptyInput` guard the exact repair. |
| 9: shared-IU equality was consumed as an invalid semi-join key, losing q56/q60 rows (`98176b0b48c`, `4f73b38aaaf`). | Paired String-IN diagnostic above; `KqpRboYql::SharedIUStringInPreservesFactJoinPredicate`; [limit rules](cpp_ut/limit_pushdown_rules_ut.cpp) `PushesSharedIUPredicateToLeftOfLeftSemiJoin`. The two workload candidates shared this one root cause. |
| 10–11: computed Map/`UNWRAP` crossed TopSort, Filter, or Join and evaluated discarded rows (`c2c66fb1d7b`, `564010e2e4e`). | [Runtime integration](integration_ut/optimizer_snapshot_pair_ut.cpp) `RealRuntimeStringUnwrapOrderLimitDemand` / `RealRuntimeStringUnwrapJoinDemand`; `KqpRboYql::PushAppendKeepsComputedExpressionAboveFilter` / `PushAppendKeepsComputedExpressionAboveJoin`; [limit rules](cpp_ut/limit_pushdown_rules_ut.cpp) `DelaysPureProjectionUntilAfterTopSort`. |

## Bounded routing findings and robustness repairs

These are separate from the eleven runtime-confirmed defects. Their archived
full-query `UNKNOWN` results are neither proofs nor evidence of current bugs.

| Finding and scope | Existing focused checks |
|---|---|
| q12: hashing the full Aggregate key split a window partition. Routing repaired in `70ab3d3631c`; historical post-fix full query was `UNKNOWN`. Physical compilation rejected `YqlAggWin`, so no runtime confirmation. | [Window SUM](ut/test_window_sum.py) `test_hashing_all_aggregate_keys_can_split_a_window_partition` detects the bad routing; `test_hashing_the_window_partition_preserves_the_logical_result` checks the repair. [Stage rules](cpp_ut/stage_assignment_rules_ut.cpp) `AggregateThenWindowHasTwoShuffleBoundaries` checks production topology. |
| q49: global ranks became task-local under `HashV2(item)`. Repair `27e3f260017` preserves dependencies and gathers serially. The historical extracted fixed-database slice distinguished serial from hash; full query/mutation remained `UNKNOWN`, and `YqlWin` prevented runtime replay. | [Window RANK](ut/test_window_rank.py) `test_global_rank_serial_gather_proves_and_hash_split_is_wrong`; [stage rules](cpp_ut/stage_assignment_rules_ut.cpp) `ExactGlobalRankGathersSerially` / `GlobalRankDependenciesPreventRatioAndTraitPruning`. These deliberately bad-plan detectors do not assert that current production still chooses bad routing. |
| Robustness: source window metadata names became inconsistent during normalization (`97a03c64ab9`), causing type-annotation failure. | [Integration](integration_ut/optimizer_snapshot_pair_ut.cpp) `RealHostCapturesExactTpcdsGlobalRanks` checks both supported semantic boundaries despite later physical rejection. This is a shared capture guard, not a dedicated replay of the original annotation failure. |
| Robustness: an untracked window entered a repeated preferred-alias rewrite (`68eb64102c7`). | [Stage rules](cpp_ut/stage_assignment_rules_ut.cpp) `UntrackedRawRankBlocksPreferredAliasRewrite` checks rejection without replacing the expression. This guards preparation/termination, not wrong-result behavior. |

## Model corrections are not production findings

The [historical model-candidate classification](history/pre-audit-refactor/BENCHMARK_COVERAGE.md#curated-proof-floor-and-focused-results)
must remain separate from runtime attribution:

- TPCH q6/q14 were model false positives from equivalent opaque Decimal/Boolean
  wrappers. [Exporter regressions](cpp_ut/semantic_snapshot_exporter_ut.cpp)
  `LowersExactBoolCoalesceFalseThroughIfPresent` / `FoldsExactConstantDecimalJust`
  guard the correction. The same file's `FoldsDirectTextLiteralDateSafeCastsExactly`
  and `FoldsTextLiteralDateSafeCastInActualOlapFilterDialect` guard TPC-DS q5's
  false positive: opaque Date casts admitted dates outside the real predicate.
- TPC-DS q79's opaque integral cast could return NULL for an in-range value;
  q88's opaque arithmetic disagreed with constant folding. [Scalar regressions](ut/test_scalar.py)
  `IntegralSafeCastTest` / `IntegerArithmeticTest` guard those model corrections.
  Corrected q6/q14/q88 are in the [current proof floor](benchmark_ut/coverage_policy.json).
- q77's old candidate was not rediscovered after exact Date-cast modeling, but
  the corrected full and fixed-witness checks were `UNKNOWN`: neither a current
  candidate nor a confirmed false positive. No fresh q77 replay is claimed.
- The q56/q60 witnesses led to defect 9 and were invalidated after its repair;
  subsequent archived `UNKNOWN` solver runs do not reopen that bug.

## Run only the evidence you need

From the repository root, reproduce the open runtime issue without external YDB
targets (the test owns its embedded instances):

```bash
set -o pipefail
./ya make --build relwithdebinfo -tA --test-tag ya:manual \
  ydb/core/kqp/opt/rbo/verification/runtime_ut \
  -F '*DecimalSumRuntimeDiagnostic*' 2>&1 | tail -n 100
```

Use `-F '*StringInRuntimeDiagnostic*'` for the passing paired fix regression.
For the fixed scalar group, use target `ydb/core/kqp/ut/rbo` and
`-F '*KqpRboYql::ExpressionSubquery'`; use `'*KqpRboYql::CorrelatedSubquery'`
for the COUNT cases. Other exact test names above work with their existing
`cpp_ut`, `integration_ut`, or `ut` targets; no extra runner or fixture copy is
needed. Manual runtime tests and `KqpRboYql` are outside the recursive verifier
gate. Preserve a dated result before changing a finding's status; a green
formula dashboard, finite-domain differential test, or `UNKNOWN` cannot replace
runtime confirmation. Tests at these different evidence layers are not duplicates.
