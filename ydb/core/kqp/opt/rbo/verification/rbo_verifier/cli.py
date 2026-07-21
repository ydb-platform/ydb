"""Command-line entry point kept separate from the verification kernel."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .ir import SnapshotError, load_snapshot
from .stages import TASKS
from .verify import SchemaMismatch, SolverError, VerificationError, build_problem, solve


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Check bounded equivalence of two new-RBO snapshots")
    result.add_argument("before", type=Path)
    result.add_argument("after", type=Path)
    result.add_argument("--rows", type=int, default=2, help="symbolic row slots per table")
    result.add_argument("--timeout-ms", type=int, default=10_000)
    result.add_argument("--solver", type=Path, help="explicit Z3 executable")
    result.add_argument("--emit-smt", type=Path, help="write the exact SMT-LIB obligation")
    return result


def main(arguments: Sequence[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    if options.rows < 0:
        return _error("INVALID_ARGUMENT", "--rows must not be negative")
    if options.timeout_ms <= 0:
        return _error("INVALID_ARGUMENT", "--timeout-ms must be positive")
    if options.solver is None and options.emit_smt is None:
        return _error("INVALID_ARGUMENT", "provide --solver, --emit-smt, or both")

    try:
        before = load_snapshot(options.before)
        after = load_snapshot(options.after)
        problem = build_problem(before, after, options.rows, options.timeout_ms)
        if options.emit_smt is not None:
            options.emit_smt.write_text(problem.formula(), encoding="utf-8")
        if options.solver is None:
            print(
                json.dumps(
                    {
                        "status": "FORMULA_EMITTED",
                        "row_bound": options.rows,
                        "task_bound": TASKS,
                    },
                    sort_keys=True,
                )
            )
            return 0
        result = solve(problem, options.solver, options.rows, options.timeout_ms)
    except SchemaMismatch as error:
        print(
            json.dumps(
                {
                    "status": "SCHEMA_MISMATCH",
                    "row_bound": options.rows,
                    "task_bound": TASKS,
                    "reason": str(error),
                },
                sort_keys=True,
            )
        )
        return 1
    except (SnapshotError, VerificationError) as error:
        return _error("UNSUPPORTED", str(error))
    except SolverError as error:
        return _error("SOLVER_ERROR", str(error))

    print(json.dumps(result.to_json(), sort_keys=True))
    return {
        "VERIFIED_BOUNDED": 0,
        "COUNTEREXAMPLE": 1,
        "UNKNOWN": 2,
    }[result.status]


def _error(status: str, reason: str) -> int:
    print(json.dumps({"status": status, "reason": reason}, sort_keys=True), file=sys.stderr)
    return 2
