"""Audited subprocess contracts for inspector and real-YDB replay."""

from __future__ import annotations

import subprocess
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .input import strict_json, write_json
from .model import (
    REPLAY_FORMAT,
    REPLAY_VERSION,
    SHA256,
    TRACE_FORMAT,
    TRACE_VERSION,
    ConfirmationError,
)


CommandRunner = Callable[[Sequence[str], int], subprocess.CompletedProcess[str]]


def run_command(
    arguments: Sequence[str],
    timeout_seconds: int,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        arguments,
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout_seconds,
    )


def invoke(
    arguments: Sequence[str],
    timeout_seconds: int,
    prefix: Path,
    run: CommandRunner,
) -> subprocess.CompletedProcess[str]:
    write_json(
        prefix.with_suffix(".command.json"),
        {"arguments": list(arguments), "timeout_seconds": timeout_seconds},
    )
    try:
        process = run(arguments, timeout_seconds)
    except Exception as error:
        _write_text(
            prefix.with_suffix(".error"),
            f"{type(error).__name__}: {error}\n",
        )
        raise ConfirmationError(f"cannot run {arguments[0]!r}: {error}") from error
    if not isinstance(process.stdout, str) or not isinstance(process.stderr, str):
        raise ConfirmationError(f"{arguments[0]!r} returned non-text output")
    _write_text(prefix.with_suffix(".stdout"), process.stdout)
    _write_text(prefix.with_suffix(".stderr"), process.stderr)
    return process


def decode_process(
    process: subprocess.CompletedProcess[str],
    phase: str,
) -> Mapping[str, Any]:
    stdout = process.stdout.strip()
    stderr = process.stderr.strip()
    if bool(stdout) == bool(stderr):
        raise ConfirmationError(f"{phase} must emit exactly one JSON document")
    value = strict_json((stdout or stderr).encode("utf-8"), f"{phase} output")
    if not isinstance(value, Mapping) or not isinstance(value.get("status"), str):
        raise ConfirmationError(f"{phase} output is not an object with a status")
    return value


def validate_trace(
    process: subprocess.CompletedProcess[str],
    trace: Mapping[str, Any],
    witness: Any,
    row_bound: int,
    task_bound: int,
    query_sha256: str,
) -> tuple[str, tuple[str, ...]]:
    if process.returncode != 1 or not process.stdout.strip() or process.stderr.strip():
        raise ConfirmationError("inspector counterexample status disagrees with its process result")
    expected_fields = {
        "format",
        "version",
        "status",
        "row_bound",
        "task_bound",
        "inputs",
        "witness",
        "mismatches",
        "trace",
    }
    if (
        trace.get("format") != TRACE_FORMAT
        or type(trace.get("version")) is not int
        or trace.get("version") != TRACE_VERSION
        or trace.get("status") != "COUNTEREXAMPLE"
        or set(trace) != expected_fields
    ):
        raise ConfirmationError("inspector did not emit a version-one concrete counterexample")
    if (
        type(trace.get("row_bound")) is not int
        or trace.get("row_bound") != row_bound
        or type(trace.get("task_bound")) is not int
        or trace.get("task_bound") != task_bound
    ):
        raise ConfirmationError("inspector trace bounds differ from the coverage report")
    if _canonical_json(trace.get("witness")) != _canonical_json(witness):
        raise ConfirmationError("inspector trace does not use the saved verifier witness")
    inputs = trace.get("inputs")
    input_fields = {
        "before_semantic_sha256",
        "after_semantic_sha256",
        "query_sha256",
    }
    if not isinstance(inputs, Mapping) or set(inputs) != input_fields:
        raise ConfirmationError("inspector trace input bindings are incomplete or unknown")
    if inputs.get("query_sha256") != query_sha256:
        raise ConfirmationError("inspector trace query digest differs from the saved query")
    if any(
        not isinstance(inputs.get(key), str) or not SHA256.fullmatch(inputs[key])
        for key in ("before_semantic_sha256", "after_semantic_sha256")
    ):
        raise ConfirmationError("inspector snapshot digests are invalid")

    execution = trace.get("trace")
    if (
        not isinstance(execution, Mapping)
        or set(execution) != {"before", "after", "comparison"}
        or not isinstance(trace.get("mismatches"), list)
        or not trace["mismatches"]
        or any(not isinstance(item, Mapping) for item in trace["mismatches"])
    ):
        raise ConfirmationError("inspector counterexample has no concrete execution trace")
    comparison = execution.get("comparison")
    if (
        not isinstance(comparison, Mapping)
        or set(comparison) != {"semantics", "before", "after"}
        or comparison.get("semantics") not in {"bag", "sequence"}
    ):
        raise ConfirmationError("inspector trace has no valid root comparison semantics")
    before_columns = _trace_columns(comparison.get("before"), "before")
    after_columns = _trace_columns(comparison.get("after"), "after")
    if before_columns != after_columns:
        raise ConfirmationError("inspector root comparison schemas differ")
    return comparison["semantics"], tuple(column["name"] for column in before_columns)


def validate_replay(
    process: subprocess.CompletedProcess[str],
    replay: Mapping[str, Any],
    row_bound: int,
    comparison: str,
    columns: tuple[str, ...],
    baseline_database: str,
    candidate_database: str,
) -> str:
    status = replay.get("status")
    expected_exit = {
        "NOT_REPRODUCED": 0,
        "REAL_RESULT_DIVERGENCE": 1,
    }.get(status)
    if expected_exit is None:
        if process.returncode == 2 and status in {
            "INCONCLUSIVE_NONDETERMINISM",
            "SETUP_ERROR",
        }:
            expected_stdout = status == "INCONCLUSIVE_NONDETERMINISM"
            if bool(process.stdout.strip()) is not expected_stdout:
                raise ConfirmationError(f"replay {status} used the wrong output stream")
            raise ConfirmationError(f"replay returned {status}: {replay.get('reason', '')}")
        raise ConfirmationError(
            f"replay status {status!r} disagrees with exit code {process.returncode}"
        )

    expected_fields = {
        "format",
        "version",
        "status",
        "comparison",
        "row_bound",
        "symbolic_string_cells",
        "trace_plan_reproduced",
        "namespaces_retained",
        "baseline",
        "candidate",
        "difference",
    }
    if (
        process.returncode != expected_exit
        or not process.stdout.strip()
        or process.stderr.strip()
        or replay.get("format") != REPLAY_FORMAT
        or type(replay.get("version")) is not int
        or replay.get("version") != REPLAY_VERSION
        or set(replay) != expected_fields
        or replay.get("comparison") != comparison
        or type(replay.get("row_bound")) is not int
        or replay.get("row_bound") != row_bound
        or type(replay.get("symbolic_string_cells")) is not int
        or replay["symbolic_string_cells"] < 0
        or replay.get("trace_plan_reproduced") is not False
        or replay.get("namespaces_retained") is not True
    ):
        raise ConfirmationError("replay result violates the resolved-result protocol")

    baseline_rows, baseline_namespace = _validate_replay_side(
        replay.get("baseline"),
        "baseline",
        baseline_database,
        "LEGACY_RBO",
        columns,
    )
    candidate_rows, candidate_namespace = _validate_replay_side(
        replay.get("candidate"),
        "candidate",
        candidate_database,
        "NEW_RBO",
        columns,
    )
    if baseline_namespace != candidate_namespace:
        raise ConfirmationError("replay targets used different retained namespaces")
    difference = replay.get("difference")
    if not isinstance(difference, Mapping):
        raise ConfirmationError("replay difference must be an object")
    expected_difference = _result_difference(
        baseline_rows,
        candidate_rows,
        comparison,
    )
    if (status == "NOT_REPRODUCED") != (not expected_difference):
        raise ConfirmationError("replay status disagrees with the retained result rows")
    if difference != expected_difference:
        raise ConfirmationError("replay difference disagrees with the retained result rows")
    return status


def phase_result(
    root: Path,
    prefix: Path,
    process: subprocess.CompletedProcess[str],
    value: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "status": value["status"],
        "exit_code": process.returncode,
        "stdout": str(prefix.with_suffix(".stdout").relative_to(root)),
        "stderr": str(prefix.with_suffix(".stderr").relative_to(root)),
        "command": str(prefix.with_suffix(".command.json").relative_to(root)),
    }


def _validate_replay_side(
    value: Any,
    role: str,
    database: str,
    optimizer: str,
    columns: tuple[str, ...],
) -> tuple[list[Mapping[str, Any]], str]:
    if not isinstance(value, Mapping):
        raise ConfirmationError(f"replay {role} result must be an object")
    expected_fields = {"database", "namespace", "optimizer", "optimizer_stats", "rows"}
    if set(value) != expected_fields:
        raise ConfirmationError(f"replay {role} fields are incomplete or unknown")
    namespace = value.get("namespace")
    if value.get("database") != database:
        raise ConfirmationError(f"replay {role} database differs from the requested target")
    prefix = database.rstrip("/") + "/_rbo_replay_"
    if not isinstance(namespace, str) or not namespace.startswith(prefix):
        raise ConfirmationError(f"replay {role} namespace is invalid")
    suffix = namespace[len(prefix) :]
    if len(suffix) != 32 or any(character not in "0123456789abcdef" for character in suffix):
        raise ConfirmationError(f"replay {role} namespace is invalid")
    if value.get("optimizer") != optimizer:
        raise ConfirmationError(f"replay {role} optimizer mode is invalid")
    _validate_optimizer_stats(value.get("optimizer_stats"), role)
    rows = value.get("rows")
    expected_columns = set(columns)
    if (
        not isinstance(rows, list)
        or any(
            not isinstance(row, Mapping) or set(row) != expected_columns
            for row in rows
        )
    ):
        raise ConfirmationError(f"replay {role} rows are not a JSON row array")
    return rows, suffix


def _validate_optimizer_stats(value: Any, role: str) -> None:
    if value is None:
        if role != "baseline":
            raise ConfirmationError("replay candidate has no optimizer statistics")
        return
    if not isinstance(value, Mapping):
        raise ConfirmationError(f"replay {role} optimizer statistics are invalid")
    for key in ("CBOTreesTotal", "CBOTreesOptimized"):
        if type(value.get(key)) is not int or value[key] < 0:
            raise ConfirmationError(f"replay {role} optimizer statistic {key} is invalid")
    legacy = {"JoinsCount", "EquiJoinsCount"}
    if role == "baseline":
        if not legacy <= set(value):
            raise ConfirmationError("replay baseline optimizer statistics lack legacy markers")
    elif legacy & set(value):
        raise ConfirmationError("replay candidate optimizer statistics contain legacy markers")
    if role == "candidate" and value["CBOTreesOptimized"] != value["CBOTreesTotal"]:
        raise ConfirmationError("replay candidate did not optimize every CBO tree")


def _result_difference(
    baseline: list[Mapping[str, Any]],
    candidate: list[Mapping[str, Any]],
    comparison: str,
) -> dict[str, Any]:
    left = tuple(_canonical_json(row) for row in baseline)
    right = tuple(_canonical_json(row) for row in candidate)
    if comparison == "sequence":
        if left == right:
            return {}
        mismatch = next(
            (index for index, pair in enumerate(zip(left, right)) if pair[0] != pair[1]),
            min(len(left), len(right)),
        )
        return {"first_mismatch": mismatch}
    left_count = Counter(left)
    right_count = Counter(right)
    if left_count == right_count:
        return {}
    return {
        "baseline_only": _counter_json(left_count - right_count),
        "candidate_only": _counter_json(right_count - left_count),
    }


def _counter_json(counter: Counter[Any]) -> list[dict[str, Any]]:
    return [
        {"row": repr(row), "multiplicity": count}
        for row, count in sorted(counter.items(), key=lambda item: repr(item[0]))
    ]


def _canonical_json(value: Any) -> Any:
    if value is None:
        return ("null",)
    if type(value) is bool:
        return ("bool", value)
    if type(value) is int:
        return ("int", value)
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, list):
        return ("array", tuple(_canonical_json(item) for item in value))
    if isinstance(value, Mapping):
        return (
            "object",
            tuple(sorted((key, _canonical_json(item)) for key, item in value.items())),
        )
    raise ConfirmationError(f"replay result contains unsupported JSON value {value!r}")


def _trace_columns(value: Any, side: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Mapping) or set(value) != {
        "columns",
        "disabled_outcome_count",
        "outcomes",
    }:
        raise ConfirmationError(f"inspector {side} root result is incomplete or unknown")
    raw_columns = value.get("columns")
    if not isinstance(raw_columns, list):
        raise ConfirmationError(f"inspector {side} root columns are invalid")
    columns: list[Mapping[str, Any]] = []
    names: set[str] = set()
    for column in raw_columns:
        if (
            not isinstance(column, Mapping)
            or set(column) != {"name", "type", "nullable"}
            or not isinstance(column.get("name"), str)
            or not column["name"]
            or column["name"] in names
            or not isinstance(column.get("type"), str)
            or not column["type"]
            or type(column.get("nullable")) is not bool
        ):
            raise ConfirmationError(f"inspector {side} root columns are invalid")
        names.add(column["name"])
        columns.append(column)
    return tuple(columns)


def _write_text(path: Path, value: str) -> None:
    try:
        path.write_text(value, encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise ConfirmationError(f"cannot write child artifact {path}: {error}") from error
