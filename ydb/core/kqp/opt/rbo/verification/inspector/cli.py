"""Thin command line for normalized plans and concrete counterexample traces."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Sequence

from ..rbo_verifier.ir import SnapshotError, load_snapshot
from ..rbo_verifier.stages import TASKS
from ..rbo_verifier.verify import SchemaMismatch, SolverError, VerificationError
from .plan import InspectionError, render_snapshot
from .trace import TRACE_FORMAT, TRACE_VERSION, prepare
from .witness import InvalidWitness


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
    witness.add_argument(
        "--query",
        type=Path,
        help="bind the trace to the exact query file required by real-YDB replay",
    )
    witness.add_argument(
        "--verifier-verdict",
        type=Path,
        help="pin tracing to the decoded database in one saved verifier counterexample",
    )
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
        if options.verifier_verdict is not None and options.solver is None:
            return _error(
                "INVALID_ARGUMENT",
                "--verifier-verdict requires --solver",
            )

        before = load_snapshot(options.before)
        after = load_snapshot(options.after)
        fixed_witness = (
            None
            if options.verifier_verdict is None
            else _load_verifier_witness(options.verifier_verdict, options.rows)
        )
        prepared = prepare(
            before,
            after,
            options.rows,
            options.timeout_ms,
            fixed_witness=fixed_witness,
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
        if options.query is not None:
            result.setdefault("inputs", {})["query_sha256"] = hashlib.sha256(
                options.query.read_bytes()
            ).hexdigest()
    except SchemaMismatch as error:
        return _error("SCHEMA_MISMATCH", str(error), exit_code=1)
    except InvalidWitness as error:
        return _error("INVALID_WITNESS", str(error))
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
        "WITNESS_NOT_REPRODUCED": 2,
    }[result["status"]]


def _error(status: str, reason: str, exit_code: int = 2) -> int:
    print(json.dumps({"status": status, "reason": reason}, sort_keys=True), file=sys.stderr)
    return exit_code


def _load_verifier_witness(path: Path, row_bound: int) -> object:
    def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise InvalidWitness(f"{path}: duplicate JSON key {key!r}")
            result[key] = value
        return result

    def invalid_constant(value: str) -> None:
        raise InvalidWitness(f"{path}: non-standard JSON constant {value!r}")

    try:
        with path.open("r", encoding="utf-8") as stream:
            verdict = json.load(
                stream,
                object_pairs_hook=object_pairs,
                parse_constant=invalid_constant,
            )
    except (OSError, UnicodeError, ValueError, RecursionError) as error:
        raise InvalidWitness(f"{path}: invalid verifier verdict: {error}") from error
    if not isinstance(verdict, dict):
        raise InvalidWitness("verifier verdict must be a JSON object")
    if verdict.get("status") != "COUNTEREXAMPLE":
        raise InvalidWitness("verifier verdict is not a counterexample")
    if verdict.get("row_bound") != row_bound or type(verdict.get("row_bound")) is not int:
        raise InvalidWitness("verifier verdict row bound does not match --rows")
    if verdict.get("task_bound") != TASKS or type(verdict.get("task_bound")) is not int:
        raise InvalidWitness("verifier verdict task bound does not match the inspector")
    if "witness" not in verdict:
        raise InvalidWitness("verifier counterexample has no extracted witness")
    if "comparison_scope" in verdict:
        raise InvalidWitness(
            "transformation-prefix verdict cannot be traced as a whole-optimizer witness"
        )
    expected_fields = {"status", "row_bound", "task_bound", "witness"}
    if set(verdict) != expected_fields:
        raise InvalidWitness("verifier counterexample has unknown or missing fields")
    return verdict["witness"]
