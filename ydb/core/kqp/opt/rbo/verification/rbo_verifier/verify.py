"""Construct and solve one bounded counterexample obligation."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from . import smt
from .ir import Snapshot
from .relation import Database, Evaluator, RelationError, WitnessRow, bag_equal
from .scalar import Encoder as ScalarEncoder
from .stages import TASKS, Evaluator as StageEvaluator, Router, StageError
from .types import family


class VerificationError(ValueError):
    pass


class SchemaMismatch(VerificationError):
    """The two plans have observably different root result schemas."""


class SolverError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Problem:
    script: smt.Script
    witness: Mapping[str, tuple[WitnessRow, ...]]

    def formula(self, include_witness_values: bool = False) -> str:
        values: list[smt.Term] = []
        if include_witness_values:
            for rows in self.witness.values():
                for row in rows:
                    values.append(row.present)
                    for cell in row.cells.values():
                        if cell.is_null.operation == "symbol":
                            values.append(cell.is_null)
                        values.append(cell.value)
        return self.script.render(values)


@dataclass(frozen=True, slots=True)
class Result:
    status: str
    row_bound: int
    task_bound: int = TASKS
    witness: Mapping[str, list[dict[str, Any]]] | None = None
    reason: str | None = None

    def to_json(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "status": self.status,
            "row_bound": self.row_bound,
            "task_bound": self.task_bound,
        }
        if self.witness is not None:
            result["witness"] = self.witness
        if self.reason is not None:
            result["reason"] = self.reason
        return result


def build_problem(
    before: Snapshot,
    after: Snapshot,
    row_bound: int,
    timeout_ms: int | None = None,
) -> Problem:
    _check_boundary_roles(before, after)
    return _build_problem(before, after, row_bound, timeout_ms)


def build_logical_kernel_problem_for_tests(
    before: Snapshot,
    after: Snapshot,
    row_bound: int,
    timeout_ms: int | None = None,
) -> Problem:
    """Bypass boundary roles only for direct tests of the logical kernel."""
    if before.stage_graph is not None or after.stage_graph is not None:
        raise VerificationError(
            "logical-kernel test comparisons require stage_graph:null on both snapshots"
        )
    return _build_problem(before, after, row_bound, timeout_ms)


def _build_problem(
    before: Snapshot,
    after: Snapshot,
    row_bound: int,
    timeout_ms: int | None,
) -> Problem:
    _check_catalogs(before, after)
    if len(before.plan.output) != len(after.plan.output):
        raise SchemaMismatch("root output arity differs")
    if before.plan.output != after.plan.output:
        raise SchemaMismatch("root output names or order differ")
    before_output = before.output_schema()
    after_output = after.output_schema()
    for index, (left, right) in enumerate(zip(before_output, after_output)):
        if left.type != right.type:
            raise SchemaMismatch(
                f"root output type differs at position {index}: {left.type!r} and {right.type!r}"
            )
        if left.nullable != right.nullable:
            raise SchemaMismatch(
                f"root output nullability differs at position {index}: "
                f"{left.nullable!r} and {right.nullable!r}"
            )

    script = smt.Script(timeout_ms)
    database = Database(before, row_bound, script)
    scalar = ScalarEncoder(script)
    router = Router(script)
    try:
        before_relation = (
            Evaluator(before, database, scalar).root()
            if before.stage_graph is None
            else StageEvaluator(before, database, scalar, router).root()
        )
        after_relation = (
            Evaluator(after, database, scalar).root()
            if after.stage_graph is None
            else StageEvaluator(after, database, scalar, router).root()
        )
    except (RelationError, StageError) as error:
        raise VerificationError(str(error)) from error
    script.assert_(smt.not_(bag_equal(before_relation, after_relation, scalar)))
    return Problem(script, database.witness)


def _check_boundary_roles(before: Snapshot, after: Snapshot) -> None:
    if before.stage_graph is not None:
        raise VerificationError(
            "initial snapshot must be captured before stage assignment with stage_graph:null"
        )
    if after.stage_graph is None:
        raise VerificationError(
            "final snapshot must be captured after stage assignment with a non-null stage_graph"
        )


def solve(problem: Problem, solver: str | Path, row_bound: int, timeout_ms: int | None = None) -> Result:
    first = _run_solver(solver, problem.formula(), timeout_ms)
    _ensure_clean(first)
    status = _single_status(first)
    if status == "unsat":
        return Result("VERIFIED_BOUNDED", row_bound)
    if status == "unknown":
        reason = first.stderr.strip() or "solver returned unknown"
        return Result("UNKNOWN", row_bound, reason=reason)
    if status != "sat":
        raise SolverError(_diagnostic(first))

    if not any(problem.witness.values()):
        return Result("COUNTEREXAMPLE", row_bound, witness={})
    second = _run_solver(solver, problem.formula(include_witness_values=True), timeout_ms)
    _ensure_clean(second)
    values = _get_values(second.stdout)
    return Result(
        "COUNTEREXAMPLE",
        row_bound,
        witness=_decode_witness(problem.witness, values, problem.script.string_literals),
    )


def _check_catalogs(before: Snapshot, after: Snapshot) -> None:
    if before.tables != after.tables:
        raise VerificationError("before and after snapshots do not have the same ordered table schema")


def _run_solver(solver: str | Path, formula: str, timeout_ms: int | None) -> subprocess.CompletedProcess[str]:
    process_timeout = None if timeout_ms is None else timeout_ms / 1000.0 + 5.0
    try:
        return subprocess.run(
            [str(solver), "-in", "-smt2"],
            input=formula,
            text=True,
            capture_output=True,
            timeout=process_timeout,
            check=False,
        )
    except FileNotFoundError as error:
        raise SolverError(f"solver executable not found: {solver}") from error
    except subprocess.TimeoutExpired as error:
        raise SolverError(f"solver process exceeded {process_timeout} seconds") from error


def _ensure_clean(process: subprocess.CompletedProcess[str]) -> None:
    if process.returncode != 0 or process.stderr.strip():
        raise SolverError(_diagnostic(process))


def _responses(output: str) -> list[smt.SExpr]:
    try:
        return smt.parse_sexpressions(output)
    except smt.SmtError as error:
        raise SolverError(f"malformed solver output: {error}; stdout={output.strip()!r}") from error


def _single_status(process: subprocess.CompletedProcess[str]) -> str:
    responses = _responses(process.stdout)
    if len(responses) != 1 or responses[0] not in {"sat", "unsat", "unknown"}:
        raise SolverError(f"expected exactly one solver status: {_diagnostic(process)}")
    assert isinstance(responses[0], str)
    return responses[0]


def _diagnostic(process: subprocess.CompletedProcess[str]) -> str:
    return (
        f"solver exited with code {process.returncode}; "
        f"stdout={process.stdout.strip()!r}; stderr={process.stderr.strip()!r}"
    )


def _get_values(output: str) -> dict[str, bool | int | str]:
    expressions = _responses(output)
    if len(expressions) != 2 or expressions[0] != "sat" or not isinstance(expressions[1], list):
        raise SolverError(f"expected SAT and one get-value response, got {expressions!r}")
    bindings = expressions[1]
    result: dict[str, bool | int | str] = {}
    for binding in bindings:
        if not isinstance(binding, list) or len(binding) != 2 or not isinstance(binding[0], str):
            raise SolverError(f"malformed get-value binding {binding!r}")
        result[binding[0]] = smt.atom_value(binding[1])
    return result


def _term_value(term: smt.Term, values: Mapping[str, bool | int | str]) -> bool | int | str:
    if term.operation == "symbol":
        assert isinstance(term.atom, str)
        try:
            return values[term.atom]
        except KeyError as error:
            raise SolverError(f"solver omitted requested value {term.atom!r}") from error
    if term.operation in {"bool", "int"}:
        assert isinstance(term.atom, (bool, int, str))
        return term.atom
    raise SolverError(f"cannot decode non-constant witness term {term.render()}")


def _decode_witness(
    witness: Mapping[str, tuple[WitnessRow, ...]],
    values: Mapping[str, bool | int | str],
    string_literals: Mapping[int, str],
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for table, rows in witness.items():
        present_rows: list[dict[str, Any]] = []
        for row in rows:
            if _term_value(row.present, values) is not True:
                continue
            decoded: dict[str, Any] = {}
            for name, cell in row.cells.items():
                if _term_value(cell.is_null, values) is True:
                    decoded[name] = None
                else:
                    raw_value = _term_value(cell.value, values)
                    decoded[name] = (
                        _string_witness(raw_value, string_literals)
                        if family(cell.type) == "string"
                        else raw_value
                    )
            present_rows.append(decoded)
        result[table] = present_rows
    return result


def _string_witness(value: bool | int | str, literals: Mapping[int, str]) -> str:
    if not isinstance(value, int) or isinstance(value, bool):
        raise SolverError(f"string atom is not an integer: {value!r}")
    if value in literals:
        return literals[value]
    candidate = f"__ydb_rbo_string_atom_{value}"
    literal_values = set(literals.values())
    while candidate in literal_values:
        candidate = "_" + candidate
    return candidate
