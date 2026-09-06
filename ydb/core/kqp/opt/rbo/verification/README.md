# New RBO equivalence verifier

This standalone checker compares the logical Initial RBO snapshot with the
Final pre-physical StageGraph over one shared bounded database. A bounded
proof is about the accepted model, not unbounded SQL equivalence or runtime
execution. The exporter and every encoding strategy are part of the trusted path.

The default checks exact equality of modeled outcome languages. The explicit
`binary64_uf_universal_v1` mode instead proves the stronger sufficient condition
that every enabled initial/final schedule pair agrees under shared primitive
floating-point abstractions. Its `SAT` result is `UNKNOWN`, never a concrete
counterexample. See the [theorem](contracts/THEOREM.md) and the single
[assumptions card](contracts/EXTERNAL_ASSUMPTIONS.md) for the precise boundary.

Start with [the semantic contract](PLAN.md), [the audit map](TRUSTED_CORE.md),
[the findings and regression ledger](FINDINGS.md), or
[the benchmark runbook](BENCHMARK_COVERAGE.md). The
[historical archive](history/README.md) preserves the previous milestone journals;
historical test counts and workload outcomes are not current guarantees.

## Build and test

From the repository root:

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/ut 2>&1 | tail -n 100

./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/cpp_ut 2>&1 | tail -n 100

./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/integration_ut 2>&1 | tail -n 100
```

Tests include the build; add `-F '*test-filter*'` for a focused run.
The recursive verification target also includes inspector, replay, confirmation,
localization, prefix-capture, and benchmark tests. The intentionally failing
[Decimal runtime diagnostic](runtime_ut/README.md) is manual and not recursed.
Pure coverage-policy checks also have a lightweight
[benchmark_policy/ut](benchmark_policy/ut) target without the real KQP host or Z3.
Use `set -o pipefail` when the shell must retain the build/test exit status.

The Python kernel has no package dependencies. Hermetic `ya` tests use the
pinned `contrib/tools/z3` executable, not an ambient solver. Lightweight
`python3 -m unittest discover -s ydb/core/kqp/opt/rbo/verification/ut`
skips solver cases unless `RBO_Z3` names a compatible executable.

## Verify and inspect

```bash
PYTHONPATH=ydb/core/kqp/opt/rbo/verification \
python3 -m rbo_verifier initial.json final.json \
  --rows 2 --timeout-ms 10000 --solver /path/to/z3 \
  --emit-smt obligation.smt2
```

The verdict includes the row bound and fixed task bound of two. Without
`--solver`, successful construction returns `FORMULA_EMITTED`, not a proof.
`--emit-smt` writes the canonical obligation, not the actual internal solver
portfolio. See the [theorem and solver protocol](contracts/THEOREM.md), especially
the mandatory model-domain exclusions for checked-String totality and integral
AVG: standalone `SAT` of the raw disjunction is not a counterexample.

For multiple buffered results, pass `--bundle bundle.json` instead of the two
snapshot paths. Its strict manifest lists result slots in client-visible order:

```json
{"format":"ydb-rbo-result-bundle","version":1,
 "observation":"buffered_tuple_or_error",
 "results":[{"before":"r0.initial.json","after":"r0.final.json"},
            {"before":"r1.initial.json","after":"r1.final.json"}]}
```

Paths are relative to the manifest. The checker compares the joint result tuple
over one shared database, not independent per-result proofs; any slot error is
one buffered query error. Streaming prefixes and effects are excluded. If any
root requests binary64 mode, it applies to every slot and both sides and is
recorded in the verdict. Bundle proofs do not enable multi-result runtime replay.

### Check whether a bound exercises nonempty results

Add `--diagnose-nonempty-output --diagnostic-timeout-ms 10000` to a solver
invocation. After verification, a separate diagnostic asks whether each side can
produce a successful nonempty result at the same bound. Its own shared deadline,
errors and `SAT`/`UNSAT`/`UNKNOWN` answers never change the equivalence verdict.

`SAT` means modeled reachability, not a runtime witness: opaque or floating
abstractions may admit extra behaviors. `UNSAT` means no successful nonempty
execution; it does not distinguish empty results from unavoidable errors.
Model-domain exclusions must first be ruled out. For bundles, all result slots
must succeed and at least one must contain a row; before and after may use
different databases. This checks neither every branch nor mutation sensitivity.

A passing proof can therefore have two `UNSAT` diagnostics. For example, TPCDS
q8's `HAVING COUNT(*) > 10` cannot produce a group at two rows per table.
Use targeted larger bounds or small semantic mutation checks to exercise such
behavior; do not weaken the original query or count reachability as a proof.

### Inspect a candidate

```bash
ydb/core/kqp/opt/rbo/verification/inspect_bin/kqp_rbo_inspect plan final.json

ydb/core/kqp/opt/rbo/verification/inspect_bin/kqp_rbo_inspect \
  witness initial.json final.json --query exact-query.yql \
  --verifier-verdict saved-verdict.json --rows 2 --solver /path/to/z3
```

The saved verdict pins the exact candidate database; the inspector may solve
routing and bounded choices but cannot silently substitute another database.
Trace digests bind normalized snapshots and the exact query. They are
producer-revision-specific, not a promise of cross-revision renderer stability.

## Confirm a candidate

Every coverage `COUNTEREXAMPLE` requires explicit confirmation on isolated YDB
targets; this is separate from recursive tests because replay creates and
retains namespaces on those targets.

```bash
ydb/core/kqp/opt/rbo/verification/confirm_bin/kqp_rbo_confirm \
  /path/to/tpcds_coverage.json \
  --inspector /path/to/kqp_rbo_inspect --solver /path/to/z3 \
  --replay /path/to/kqp_rbo_replay --ydb /path/to/ydb \
  --artifacts /new/path/confirmation \
  --baseline-endpoint grpc://baseline-host:2136 --baseline-database /Root/baseline \
  --candidate-endpoint grpc://candidate-host:2136 --candidate-database /Root/candidate
```

Use matching configurations except for legacy versus new RBO, with new-RBO
fallback disabled. Supply the exact captured query, including benchmark
compatibility definitions. Confirmation validates SHA-bound inputs and processes
every candidate. Missing evidence, nondeterminism, multi-result queries, or child
protocol failures produce `UNRESOLVED`, never a proof.

`REAL_RESULT_DIVERGENCE` means real executions differed; `NOT_REPRODUCED`
does not prove equivalence. Replay records `trace_plan_reproduced:false`:
the reduced catalog and external CLI do not establish that the exact supplied
StageGraph ran. Replay never deletes existing or generated YDB objects.
Detailed replay mechanics and the retained original invocation are in the
[archived replay section](history/pre-audit-refactor/README.md#real-ydb-counterexample-replay).

## Localize and preserve findings

The [sequential prefix localizer](tools/README.md) uses the same kernel with an
explicit `OPTIMIZER_TRANSFORMATION_PREFIX` comparison scope. Prefix results
are diagnostic, not normal Initial/Final proofs. Equivalence is not monotonic
across transformations; binary search is not a sound substitute.

Preserve the exact query, snapshots, raw verdict, formula, inspector trace,
confirmation streams, and relevant prefix captures. A model error and a runtime
divergence can coexist; retain a regression for each established cause.
Keep verifier/model changes separate from optimizer fixes; land an optimizer
fix with its focused regression. Change measured coverage claims only after
the corresponding complete rerun. No intentionally failing commit is needed
solely to preserve a finding.
