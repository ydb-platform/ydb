"""Thin command line for normalized plans and concrete counterexample traces."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from ..rbo_verifier.ir import SnapshotError, load_snapshot
from ..rbo_verifier.stages import TASKS
from ..rbo_verifier.verify import SchemaMismatch, SolverError, VerificationError
from .plan import InspectionError, render_snapshot
from .trace import TRACE_FORMAT, TRACE_VERSION, prepare


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Inspect new-RBO equivalence inputs and failures")
    commands = result.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan", help="render one normalized semantic snapshot")
    plan.add_argument("snapshot", type=Path)

    witness = commands.add_parser("witness", help="solve and render one concrete execution trace")
    witness.add_argument("before", type=Path)
    witness.add_argument("after", type=Path)
    witness.add_argument("--rows", type=int, default=2)
    witness.add_argument("--timeout-ms", type=int, default=10_000)
    witness.add_argument("--solver", type=Path)
    witness.add_argument("--emit-smt", type=Path)
    return result


def main(arguments: Sequence[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    try:
        if options.command == "plan":
            print(render_snapshot(load_snapshot(options.snapshot)), end="")
            return 0
        if options.rows < 0:
            return _error("INVALID_ARGUMENT", "--rows must not be negative")
        if options.timeout_ms <= 0:
            return _error("INVALID_ARGUMENT", "--timeout-ms must be positive")
        if options.solver is None and options.emit_smt is None:
            return _error("INVALID_ARGUMENT", "provide --solver, --emit-smt, or both")

        prepared = prepare(
            load_snapshot(options.before),
            load_snapshot(options.after),
            options.rows,
            options.timeout_ms,
        )
        if options.emit_smt is not None:
            options.emit_smt.write_text(prepared.formula(), encoding="utf-8")
        if options.solver is None:
            result = {
                "format": TRACE_FORMAT,
                "version": TRACE_VERSION,
                "status": "FORMULA_EMITTED",
                "row_bound": options.rows,
                "task_bound": TASKS,
            }
        else:
            result = prepared.solve(options.solver, options.timeout_ms)
    except SchemaMismatch as error:
        return _error("SCHEMA_MISMATCH", str(error), exit_code=1)
    except (SnapshotError, VerificationError, InspectionError) as error:
        return _error("UNSUPPORTED", str(error))
    except SolverError as error:
        return _error("SOLVER_ERROR", str(error))
    except OSError as error:
        return _error("IO_ERROR", str(error))

    print(json.dumps(result, indent=2, sort_keys=True))
    return {
        "FORMULA_EMITTED": 0,
        "VERIFIED_BOUNDED": 0,
        "COUNTEREXAMPLE": 1,
        "UNKNOWN": 2,
    }[result["status"]]


def _error(status: str, reason: str, exit_code: int = 2) -> int:
    print(json.dumps({"status": status, "reason": reason}, sort_keys=True), file=sys.stderr)
    return exit_code
