# New RBO equivalence verifier

This directory contains the standalone bounded-equivalence checker described in
[PLAN.md](PLAN.md). It compares two versioned semantic snapshots and asks Z3 for
a bounded input database on which their result bags differ.

The current implementation contains the M1 logical kernel and the first M2 C++
export path. `CaptureSemanticSnapshotCatalogV1` records the initial query-level
catalog once, and `ExportSemanticSnapshotV1` deterministically lowers supported
logical RBO operators without doing file I/O. An optional sink on
`TKqlTransformContext` receives the initial snapshot before the first RBO stage
and the final snapshot immediately before physical generation. StageGraph
execution remains subsequent work, so the real final snapshot currently arrives
as an explicit unsupported diagnostic rather than silently dropping its graph.

Version one preserves exact supported YQL scalar identities (`Bool`, signed and
unsigned integer widths, `String`, and `Utf8`) even when several identities use
the same SMT domain.

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
