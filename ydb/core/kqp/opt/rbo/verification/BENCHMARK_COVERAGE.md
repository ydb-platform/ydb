# Current TPCH/TPC-DS coverage runbook

Formula construction measures reach, not equivalence. Preparation success,
exact capture, verifier entry, formula construction, and bounded proof are
different facts. [coverage_policy.json](benchmark_ut/coverage_policy.json) is
the authoritative checked-in floor; the table below is generated from it, not
a newly measured corpus run.

<!-- coverage-policy:start -->
| Suite | Corpus | Prepare | Exact pair | Explicit entry | Formula | Proof |
|---|---:|---:|---:|---:|---:|---:|
| TPCH_YQL | 22 | 20 | 20 | 3 | 20 | 14 |
| TPCDS_YQL | 99 | 74 | 82 | 8 | 82 | 28 |
| Total | 121 | 94 | 102 | 11 | 102 | 42 |
<!-- coverage-policy:end -->

The exact-pair floor is the union of supplemental pair, verifier-entry, and
formula requirements. Proof requirements are a subset of formula requirements.
Preparation is orthogonal: an exact pre-physical pair can remain auditable after
later physical preparation fails. Supplemental pair lists are currently empty.
The explicit entry column is not the total number of formula/proof queries
that necessarily pass through the verifier.

Regenerate the table with `python3 benchmark_ut/render_policy.py` from this
directory; use `--check` to check this document without writing it.
The [archived measurements and findings](history/pre-audit-refactor/BENCHMARK_COVERAGE.md)
retain all earlier corpus inventories, focused solver results, artifacts,
commit references, and corrected/superseded checkpoints. They are historical
evidence, not results of the current working tree.

## Fixed workload and host

Sources are `ydb/core/kqp/ut/rbo/data/schema/tpch.sql`,
`schema/tpcds.sql`, `yql-tpch/q1.yql..q22.yql`, and
`yql-tpcds/q1.yql..q99.yql`. The exact assembled query includes:

```yql
$to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
$to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
$round = ($x,$y) -> { return $x; };
```

The real KQP host creates column-store tables with minimum partition count 16,
enables new RBO, disables YQL fallback, permits OLAP data queries, enables the
maximum language version and all backports, and clears the result-row limit.
It links the production PostgreSQL translator/runtime, not the dummy provider.
Every obligation uses two row slots per referenced table and two modeled tasks.
Capture ends before `ConvertToPhysical`; successful proof does not establish
executability or correctness of later lowering/runtime behavior.

## Run from the repository root

```bash
set -o pipefail
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_ut \
  -F '*::TPCH' --test-env=RBO_COVERAGE_USE_SOLVER=0 \
  2>&1 | tail -n 100

./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_ut \
  -F '*::TPCDS' --test-env=RBO_COVERAGE_USE_SOLVER=0 \
  2>&1 | tail -n 100

./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_ut \
  -F '*ProofFloor*' 2>&1 | tail -n 100
```

The proof floor selects exactly the policy's proof queries, uses the pinned
`contrib/tools/z3/z3` executable with 60 seconds per query, and ignores ambient
`RBO_COVERAGE_*` settings. Only `VERIFIED_BOUNDED` satisfies its proof floor.

For a non-gating experiment, use `-F '*::TPCDS'` and explicitly pass:

```text
--test-env=RBO_COVERAGE_USE_SOLVER=1
--test-env=RBO_COVERAGE_TIMEOUT_MS=60000
--test-env=RBO_COVERAGE_QUERIES=1,4-7,96
```

Absent/empty/zero solver selection means formula-only; `1` selects hermetic Z3;
other values fail closed. The positive timeout defaults to 10000 ms.
Empty query selection means the full suite. Focused runs and solver experiments
do not enforce the full formula-dashboard floors. Bare ambient variables are
not inherited by the `ya` sandbox: pass `--test-env` explicitly.

## Reports and retained evidence

Tests write `tpch_coverage.json` / `tpcds_coverage.json` or
`tpch_proof_floor.json` / `tpcds_proof_floor.json` to their test output
directories. Report format version 5 has separate `summary` and
`prepare_summary` partitions. Each row retains `prepare_status` and
`prepare_reason` independently of semantic status.

`optimizer_failure_inventory` groups all failed preparations, including
formula/proof/unsupported rows captured before the failure.
`unsupported_inventory` groups semantic admission failures.
These inventories overlap; do not sum them. Terminal `OPTIMIZER_FAILURE`
means no exact normal pair was available. `HARNESS_ERROR` means capture,
execution, or subprocess protocol failed; it is not a semantic verdict.

Counterexamples, unknowns, schema mismatches, solver/protocol failures, failed-
preparation pairs, and proof-floor results retain relevant evidence.
Routine successful formula dashboards do not retain every large formula.
The subprocess result is captured before decoding. Diagnostic/proof records
retain raw stdout/stderr, command arguments, exit code, timing, and any emitted
formula in `process_artifacts`, with SHA-256 digests. Malformed JSON or an
exit/status mismatch must not discard those bytes.

The normal `artifacts` map retains the exact query, both supported snapshots
(or unsupported boundary diagnostics), and the byte-exact verdict when valid.
Its existing counterexample shape is unchanged for confirmation compatibility.
The parsed report verdict deliberately omits the witness: wide Decimal
integers must not be rounded by the C++ JSON object's numeric representation.
Only the SHA-bound raw verdict is authoritative for the candidate database.

The saved SMT is the canonical formula, not a transcript of the exact internal
solver portfolio. The command record identifies the verifier invocation, not
every Z3 branch. Preserve that distinction when reproducing timing or auditing
a proof; see [the theorem](contracts/THEOREM.md).
No source/tool content hashes or full solver transcript are implied by the
current report.

Every `COUNTEREXAMPLE` requires the separate
[confirmation workflow](README.md#confirm-a-candidate) on explicit isolated
targets. A missing witness or inconsistent artifact remains unresolved.

## Policy maintenance

[benchmark_policy](benchmark_policy) is a JSON-only library for strict decoding,
floor evaluation, and policy-report rendering. Its
[small unit target](benchmark_policy/ut) runs without KQP, Z3, or per-test forks:

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/benchmark_policy/ut 2>&1 | tail -n 100
```

The benchmark harness owns the host, environment, subprocesses, artifact
retention, and corpus iteration. It retains the single fixed-contract query
membership lock and host/environment/process tests. Pure tiny-policy, malformed
decoder, monotonic-depth, and floor-regression tests run in the small target;
their assertions are not deleted to reduce heavyweight test overhead.
Do not weaken the lock as an incidental refactor or infer a new floor from a
focused run. Raise measured-coverage claims only with retained complete reruns.
