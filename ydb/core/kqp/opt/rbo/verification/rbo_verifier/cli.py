"""Command-line entry point kept separate from the verification kernel."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .ir import SnapshotError, load_snapshot
from .bundle import build_bundle_problem, load_bundle
from .stages import TASKS
from .verify import (
    SchemaMismatch,
    SolverError,
    VerificationError,
    build_problem,
    build_transformation_prefix_problem,
    solve,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Check bounded equivalence of two new-RBO snapshots")
    result.add_argument("before", type=Path, nargs="?")
    result.add_argument("after", type=Path, nargs="?")
    result.add_argument("--bundle", type=Path, help="strict v1 buffered result-bundle manifest (instead of two snapshots)")
    result.add_argument("--rows", type=int, default=2, help="symbolic row slots per table")
    result.add_argument("--timeout-ms", type=int, default=10_000)
    result.add_argument("--solver", type=Path, help="explicit Z3 executable")
    result.add_argument(
        "--emit-smt",
        type=Path,
        help="write the exact canonical SMT-LIB obligation, not the solver portfolio transcript",
    )
    result.add_argument(
        "--diagnostic-transformation-prefix",
        action="store_true",
        help="compare the logical initial snapshot with one transformation prefix",
    )
    return result


def main(arguments: Sequence[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    comparison_scope = (
        "OPTIMIZER_TRANSFORMATION_PREFIX"
        if options.diagnostic_transformation_prefix
        else "BUFFERED_RESULT_BUNDLE" if options.bundle is not None else None
    )
    if options.bundle is not None:
        if options.before is not None or options.after is not None or options.diagnostic_transformation_prefix:
            return _error("INVALID_ARGUMENT", "--bundle excludes positional snapshots and transformation-prefix mode", comparison_scope)
    elif options.before is None or options.after is None:
        return _error("INVALID_ARGUMENT", "provide before and after snapshots, or --bundle", comparison_scope)
    if options.rows < 0:
        return _error(
            "INVALID_ARGUMENT", "--rows must not be negative", comparison_scope
        )
    if options.timeout_ms <= 0:
        return _error(
            "INVALID_ARGUMENT", "--timeout-ms must be positive", comparison_scope
        )
    if options.solver is None and options.emit_smt is None:
        return _error(
            "INVALID_ARGUMENT",
            "provide --solver, --emit-smt, or both",
            comparison_scope,
        )

    try:
        if options.bundle is not None:
            problem = build_bundle_problem(load_bundle(options.bundle), options.rows, options.timeout_ms)
        else:
            before = load_snapshot(options.before)
            after = load_snapshot(options.after)
            builder = build_transformation_prefix_problem if options.diagnostic_transformation_prefix else build_problem
            problem = builder(before, after, options.rows, options.timeout_ms)
        if options.emit_smt is not None:
            options.emit_smt.write_text(problem.formula(), encoding="utf-8")
        if options.solver is None:
            verdict = _scoped(
                {
                    "status": "FORMULA_EMITTED",
                    "row_bound": options.rows,
                    "task_bound": TASKS,
                    **({"semantic_mode": problem.semantic_mode} if problem.semantic_mode is not None else {}),
                },
                comparison_scope,
            )
            print(json.dumps(verdict, sort_keys=True))
            return 0
        result = solve(problem, options.solver, options.rows, options.timeout_ms)
    except SchemaMismatch as error:
        verdict = _scoped(
            {
                "status": "SCHEMA_MISMATCH",
                "row_bound": options.rows,
                "task_bound": TASKS,
                "reason": str(error),
            },
            comparison_scope,
        )
        print(json.dumps(verdict, sort_keys=True))
        return 1
    except (SnapshotError, VerificationError) as error:
        return _error("UNSUPPORTED", str(error), comparison_scope)
    except SolverError as error:
        return _error("SOLVER_ERROR", str(error), comparison_scope)

    verdict = _scoped(result.to_json(), comparison_scope)
    print(json.dumps(verdict, sort_keys=True))
    return {
        "VERIFIED_BOUNDED": 0,
        "COUNTEREXAMPLE": 1,
        "UNKNOWN": 2,
    }[result.status]


def _error(status: str, reason: str, comparison_scope: str | None = None) -> int:
    verdict = _scoped({"status": status, "reason": reason}, comparison_scope)
    print(json.dumps(verdict, sort_keys=True), file=sys.stderr)
    return 2


def _scoped(verdict: dict[str, object], comparison_scope: str | None) -> dict[str, object]:
    if comparison_scope is not None:
        return {**verdict, "comparison_scope": comparison_scope}
    return verdict
