"""Strict coverage-report and artifact input boundary for confirmation."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, Mapping

from .model import (
    COVERAGE_FORMAT,
    COVERAGE_VERSIONS,
    SHA256,
    Candidate,
    CandidateInputs,
    Config,
    ConfirmationError,
)


def validate_config(config: Config) -> None:
    if config.artifacts.exists():
        raise ConfirmationError(f"artifact directory already exists: {config.artifacts}")
    if not config.artifacts.parent.is_dir():
        raise ConfirmationError(
            f"artifact directory parent does not exist: {config.artifacts.parent}"
        )
    if config.solver_timeout_ms <= 0:
        raise ConfirmationError("solver timeout must be positive")
    if config.replay_timeout_seconds <= 0:
        raise ConfirmationError("replay timeout must be positive")
    if config.replay_process_timeout_seconds <= 0:
        raise ConfirmationError("replay process timeout must be positive")
    if not config.baseline_endpoint or not config.candidate_endpoint:
        raise ConfirmationError("both replay endpoints must be non-empty")
    if not config.baseline_database or not config.candidate_database:
        raise ConfirmationError("both replay databases must be non-empty")
    if (
        config.baseline_endpoint,
        config.baseline_database,
    ) == (
        config.candidate_endpoint,
        config.candidate_database,
    ):
        raise ConfirmationError("baseline and candidate must be different YDB targets")


def load_report(
    path: Path,
) -> tuple[Path, bytes, str, int, int, tuple[Candidate, ...]]:
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise ConfirmationError(f"cannot resolve coverage report {path}: {error}") from error
    if not resolved.is_file():
        raise ConfirmationError(f"coverage report is not a file: {resolved}")
    content = read_bytes(resolved, "coverage report")
    suite, row_bound, task_bound, candidates = _decode_report(
        strict_json(content, str(resolved))
    )
    return resolved, content, suite, row_bound, task_bound, candidates


def prepare_inputs(
    root: Path,
    source_root: Path,
    candidate: Candidate,
    directory: Path,
) -> CandidateInputs:
    artifacts = candidate.artifacts
    if not isinstance(artifacts, Mapping):
        raise ConfirmationError(f"q{candidate.query_id} has no artifact map")
    required = {
        "initial_snapshot",
        "initial_snapshot_sha256",
        "final_snapshot",
        "final_snapshot_sha256",
        "query",
        "query_sha256",
        "verifier_verdict",
        "verifier_verdict_sha256",
    }
    optional = {"formula"}
    if not required <= set(artifacts) or not set(artifacts) <= required | optional:
        raise ConfirmationError(f"q{candidate.query_id} artifact fields are incomplete or unknown")

    rendered: dict[str, Any] = {}
    initial, initial_sha = _copy_bound_artifact(
        source_root, artifacts, "initial_snapshot", directory / "initial.json"
    )
    final, final_sha = _copy_bound_artifact(
        source_root, artifacts, "final_snapshot", directory / "final.json"
    )
    query, query_sha = _copy_bound_artifact(
        source_root, artifacts, "query", directory / "query.yql"
    )
    try:
        if not query.read_text(encoding="utf-8").strip():
            raise ConfirmationError(f"q{candidate.query_id} query is empty")
    except (OSError, UnicodeError) as error:
        raise ConfirmationError(f"q{candidate.query_id} query is not valid UTF-8: {error}") from error

    rendered["initial_snapshot"] = _artifact_result(root, initial, initial_sha)
    rendered["final_snapshot"] = _artifact_result(root, final, final_sha)
    rendered["query"] = _artifact_result(root, query, query_sha)
    if "formula" in artifacts:
        source = _resolve_artifact(source_root, artifacts["formula"], "formula")
        formula = directory / "obligation.smt2"
        shutil.copyfile(source, formula)
        rendered["formula"] = _artifact_result(root, formula, sha256(formula.read_bytes()))

    verifier, verifier_sha = _copy_bound_artifact(
        source_root,
        artifacts,
        "verifier_verdict",
        directory / "verifier.json",
    )
    verdict = strict_json(
        verifier.read_bytes(),
        f"q{candidate.query_id} verifier verdict",
    )
    _validate_verifier_verdict(candidate, verdict)
    rendered["verifier_verdict"] = _artifact_result(root, verifier, verifier_sha)
    return CandidateInputs(
        initial,
        final,
        query,
        verifier,
        verdict,
        query_sha,
        rendered,
    )


def strict_json(content: bytes, source: str) -> Any:
    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ConfirmationError(f"{source}: duplicate JSON key {key!r}")
            result[key] = value
        return result

    def invalid_constant(value: str) -> None:
        raise ConfirmationError(f"{source}: non-standard JSON constant {value!r}")

    def invalid_float(value: str) -> None:
        raise ConfirmationError(f"{source}: floating JSON number {value!r} is unsupported")

    try:
        return json.loads(
            content.decode("utf-8", errors="strict"),
            object_pairs_hook=object_pairs,
            parse_float=invalid_float,
            parse_constant=invalid_constant,
        )
    except (UnicodeError, ValueError, RecursionError) as error:
        raise ConfirmationError(f"{source}: invalid JSON: {error}") from error


def read_bytes(path: Path, description: str) -> bytes:
    try:
        return path.read_bytes()
    except OSError as error:
        raise ConfirmationError(f"cannot read {description} {path}: {error}") from error


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def write_json(path: Path, value: Any) -> None:
    try:
        path.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except (OSError, UnicodeError) as error:
        raise ConfirmationError(f"cannot write JSON artifact {path}: {error}") from error


def _decode_report(value: Any) -> tuple[str, int, int, tuple[Candidate, ...]]:
    if not isinstance(value, Mapping):
        raise ConfirmationError("coverage report must be a JSON object")
    version = value.get("version")
    if (
        value.get("format") != COVERAGE_FORMAT
        or type(version) is not int
        or version not in COVERAGE_VERSIONS
    ):
        raise ConfirmationError(
            "confirmation requires a version-four or version-five benchmark coverage report"
        )
    suite = value.get("suite")
    row_bound = value.get("row_bound")
    task_bound = value.get("task_bound")
    if not isinstance(suite, str) or not suite:
        raise ConfirmationError("coverage report suite must be a non-empty string")
    if type(row_bound) is not int or row_bound < 0:
        raise ConfirmationError("coverage report row_bound must be non-negative")
    if type(task_bound) is not int or task_bound != 2:
        raise ConfirmationError("coverage report task_bound must be two")
    rows = value.get("queries")
    if not isinstance(rows, list):
        raise ConfirmationError("coverage report queries must be an array")

    observed: dict[str, int] = {}
    observed_prepare: dict[str, int] = {}
    seen_ids: set[int] = set()
    candidates: list[Candidate] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ConfirmationError(f"coverage query row {index} must be an object")
        query_id = row.get("query_id")
        status = row.get("status")
        if type(query_id) is not int or query_id < 0:
            raise ConfirmationError(f"coverage query row {index} has an invalid query_id")
        if query_id and query_id in seen_ids:
            raise ConfirmationError(f"coverage report repeats query_id {query_id}")
        if query_id:
            seen_ids.add(query_id)
        if not isinstance(status, str) or not status:
            raise ConfirmationError(f"coverage query row {index} has an invalid status")
        if row.get("suite") != suite:
            raise ConfirmationError(f"coverage query row {index} has the wrong suite")
        observed[status] = observed.get(status, 0) + 1
        if version == 5:
            prepare_status = row.get("prepare_status")
            allowed_prepare_statuses = {"SUCCEEDED", "FAILED", "UNKNOWN"}
            if query_id == 0:
                allowed_prepare_statuses.add("NOT_RUN")
            if (
                not isinstance(prepare_status, str)
                or prepare_status not in allowed_prepare_statuses
            ):
                raise ConfirmationError(
                    f"coverage query row {index} has an invalid prepare_status"
                )
            prepare_reason = row.get("prepare_reason")
            if (
                not isinstance(prepare_reason, str)
                or (prepare_status == "SUCCEEDED" and prepare_reason)
                or (
                    prepare_status in {"FAILED", "UNKNOWN"}
                    and not prepare_reason
                )
            ):
                raise ConfirmationError(
                    f"coverage query row {index} has an invalid prepare_reason"
                )
            observed_prepare[prepare_status] = observed_prepare.get(prepare_status, 0) + 1
        if status != "COUNTEREXAMPLE":
            continue
        if query_id <= 0:
            raise ConfirmationError("counterexample query_id must be positive")
        verdict = row.get("verdict")
        if not isinstance(verdict, Mapping) or verdict.get("status") != status:
            raise ConfirmationError(f"q{query_id} has an inconsistent verifier verdict")
        metadata = {"status", "row_bound", "task_bound"}
        if not metadata <= set(verdict) or not set(verdict) <= metadata | {"reason"}:
            raise ConfirmationError(f"q{query_id} report verdict must contain metadata only")
        if "reason" in verdict and not isinstance(verdict["reason"], str):
            raise ConfirmationError(f"q{query_id} report verdict reason is invalid")
        if (
            type(verdict.get("row_bound")) is not int
            or verdict.get("row_bound") != row_bound
            or type(verdict.get("task_bound")) is not int
            or verdict.get("task_bound") != task_bound
        ):
            raise ConfirmationError(f"q{query_id} verifier bounds differ from the report")
        source = row.get("source")
        if not isinstance(source, str) or not source:
            raise ConfirmationError(f"q{query_id} source must be a non-empty string")
        candidates.append(Candidate(query_id, source, verdict, row.get("artifacts")))

    summary = value.get("summary")
    if not isinstance(summary, Mapping) or any(
        not isinstance(key, str) or type(count) is not int or count < 0
        for key, count in summary.items()
    ):
        raise ConfirmationError("coverage report summary is invalid")
    if dict(summary) != observed:
        raise ConfirmationError("coverage report summary does not match its query rows")
    if version == 5:
        prepare_summary = value.get("prepare_summary")
        if not isinstance(prepare_summary, Mapping) or any(
            status not in {"SUCCEEDED", "FAILED", "UNKNOWN", "NOT_RUN"}
            or type(count) is not int
            or count < 0
            for status, count in prepare_summary.items()
        ):
            raise ConfirmationError("coverage report prepare_summary is invalid")
        if dict(prepare_summary) != observed_prepare:
            raise ConfirmationError(
                "coverage report prepare_summary does not match its query rows"
            )
    if candidates and value.get("solver_present") is not True:
        raise ConfirmationError("coverage counterexamples require solver_present:true")
    return suite, row_bound, task_bound, tuple(sorted(candidates, key=lambda item: item.query_id))


def _validate_verifier_verdict(candidate: Candidate, value: Any) -> None:
    if not isinstance(value, Mapping):
        raise ConfirmationError(
            f"q{candidate.query_id} raw verifier verdict must be an object"
        )
    report = candidate.report_verdict
    for field in ("status", "row_bound", "task_bound"):
        if value.get(field) != report.get(field) or type(value.get(field)) is not type(
            report.get(field)
        ):
            raise ConfirmationError(
                f"q{candidate.query_id} raw verifier {field} differs from the report"
            )
    if value.get("status") != "COUNTEREXAMPLE":
        raise ConfirmationError(f"q{candidate.query_id} raw verdict is not a counterexample")
    if "comparison_scope" in value:
        raise ConfirmationError(
            f"q{candidate.query_id} transformation-prefix verdict is not a normal candidate"
        )
    if "witness" in value:
        expected = {"status", "row_bound", "task_bound", "witness"}
    else:
        expected = {"status", "row_bound", "task_bound", "reason"}
        if not isinstance(value.get("reason"), str) or not value["reason"]:
            raise ConfirmationError(
                f"q{candidate.query_id} counterexample without a witness has no reason"
            )
    if set(value) != expected:
        raise ConfirmationError(
            f"q{candidate.query_id} raw verifier verdict has unknown or missing fields"
        )
    if "reason" in report and value.get("reason") != report["reason"]:
        raise ConfirmationError(
            f"q{candidate.query_id} raw verifier reason differs from the report"
        )


def _copy_bound_artifact(
    source_root: Path,
    artifacts: Mapping[str, Any],
    key: str,
    destination: Path,
) -> tuple[Path, str]:
    source = _resolve_artifact(source_root, artifacts[key], key)
    expected = artifacts[f"{key}_sha256"]
    if not isinstance(expected, str) or not SHA256.fullmatch(expected):
        raise ConfirmationError(f"{key}_sha256 is not a lowercase SHA-256 digest")
    content = read_bytes(source, key)
    actual = sha256(content)
    if actual != expected:
        raise ConfirmationError(f"{key} SHA-256 does not match the coverage report")
    destination.write_bytes(content)
    return destination, actual


def _resolve_artifact(root: Path, raw: Any, field: str) -> Path:
    if not isinstance(raw, str) or not raw:
        raise ConfirmationError(f"{field} must be a non-empty relative path")
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        raise ConfirmationError(f"{field} must stay inside the coverage artifact directory")
    try:
        resolved_root = root.resolve(strict=True)
        path = (resolved_root / relative).resolve(strict=True)
        path.relative_to(resolved_root)
    except (OSError, ValueError) as error:
        raise ConfirmationError(
            f"{field} must stay inside the coverage artifact directory"
        ) from error
    if not path.is_file():
        raise ConfirmationError(f"{field} does not name an artifact file")
    return path


def _artifact_result(root: Path, path: Path, digest: str) -> dict[str, str]:
    return {"path": str(path.relative_to(root)), "sha256": digest}
