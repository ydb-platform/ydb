# New RBO equivalence verifier

This directory contains the standalone bounded-equivalence checker described in
[PLAN.md](PLAN.md). It compares two versioned semantic snapshots and asks Z3 for
a bounded input database on which their result bags differ.

The current implementation is the M1 logical kernel. StageGraph execution and
the C++ snapshot exporter are intentionally subsequent milestones; snapshots
that contain unsupported physical semantics fail closed.

The chosen final boundary is immediately before `ConvertToPhysical`. Therefore
the verifier does not prove physical lowering, task construction, or execution.
In particular, the current lowering does not visibly preserve
`TSortElement::NullsFirst`; explicit NULL ordering must remain a replay case
until that contract is clarified.

## Development setup

The Python code has no package dependencies. Pass an explicit Z3-compatible
solver executable when asking it to solve a formula. During local development,
that can be a system or isolated-development Z3 installation; hermetic `ya`
integration will use a separately vendored binary.

```bash
python3 -m unittest discover -s ydb/core/kqp/opt/rbo/verification/ut
```

Run its tests with:

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/ut 2>&1 | tail
```

Solver integration tests are enabled when an explicit solver binary is
available. Formula construction and parsing tests do not depend on Z3.

Build the Ya-owned CLI with:

```bash
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/bin
```

## CLI

```bash
PYTHONPATH=ydb/core/kqp/opt/rbo/verification \
python3 -m rbo_verifier before.json after.json \
  --rows 2 --timeout-ms 10000 --solver /path/to/z3
```

The command prints a JSON verdict. A `COUNTEREXAMPLE` verdict contains only the
present base-table rows; opaque-function interpretations are deliberately not
treated as a stable witness format. A `SCHEMA_MISMATCH` verdict is a direct
correctness failure and does not depend on the row bound. Use `--emit-smt
formula.smt2` without `--solver` to inspect the exact proof obligation.
