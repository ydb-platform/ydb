"""Strict capture and verifier subprocess contracts for transformation localization."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


PROTOCOL = "ydb-rbo-transformation-prefix-capture-v2"
CAPTURE_MANIFEST = "capture.json"
EVENT_KINDS = frozenset({"RULE_APPLICATION", "ATOMIC_STAGE_COMMIT"})


class LocalizationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Event:
    ordinal: int
    kind: str
    stage: str
    name: str

    def to_json(self) -> dict[str, Any]:
        return {
            "ordinal": self.ordinal,
            "kind": self.kind,
            "stage": self.stage,
            "name": self.name,
        }


@dataclass(frozen=True, slots=True)
class Capture:
    status: str
    initial: Path
    events: tuple[Event, ...]
    prefix: Path | None = None
    final: Path | None = None
    unsupported_reason: str | None = None


@dataclass(frozen=True, slots=True)
class Config:
    capture_command: tuple[str, ...]
    verifier: Path
    solver: Path
    artifacts: Path
    rows: int = 2
    timeout_ms: int = 10_000
    capture_timeout_seconds: int = 300
    max_events: int = 10_000


CommandRunner = Callable[[Sequence[str], int], subprocess.CompletedProcess[str]]


def capture(
    config: Config,
    ordinal: int,
    directory: Path,
    run: CommandRunner,
) -> Capture:
    arguments = [
        *config.capture_command,
        "--rbo-transformation-prefix-ordinal",
        str(ordinal),
        "--rbo-transformation-prefix-output",
        str(directory),
    ]
    process = run(arguments, config.capture_timeout_seconds)
    _save_process(directory / "capture", process)
    if process.returncode != 0:
        raise LocalizationError(
            f"capture command failed for ordinal {ordinal} with exit code {process.returncode}"
        )
    manifest = directory / CAPTURE_MANIFEST
    try:
        raw = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise LocalizationError(f"cannot read {manifest}: {error}") from error
    return _decode_capture(raw, ordinal, directory)


def verify(
    config: Config,
    initial: Path,
    candidate: Path,
    directory: Path,
    diagnostic: bool,
    run: CommandRunner,
) -> Mapping[str, Any]:
    arguments = [
        str(config.verifier),
        str(initial),
        str(candidate),
        "--rows",
        str(config.rows),
        "--timeout-ms",
        str(config.timeout_ms),
        "--solver",
        str(config.solver),
        "--emit-smt",
        str(directory / "obligation.smt2"),
    ]
    if diagnostic:
        arguments.append("--diagnostic-transformation-prefix")
    process = run(arguments, max(30, config.timeout_ms // 1000 + 30))
    _save_process(directory / "verifier", process)
    verdict = _decode_verdict(process)
    if diagnostic and verdict.get("comparison_scope") != "OPTIMIZER_TRANSFORMATION_PREFIX":
        raise LocalizationError(
            "verifier did not confirm transformation-prefix comparison scope"
        )
    if not diagnostic and "comparison_scope" in verdict:
        raise LocalizationError("normal final verifier unexpectedly reported a diagnostic scope")
    expected_exit = {
        "VERIFIED_BOUNDED": 0,
        "COUNTEREXAMPLE": 1,
        "SCHEMA_MISMATCH": 1,
        "UNKNOWN": 2,
        "UNSUPPORTED": 2,
        "SOLVER_ERROR": 2,
    }.get(verdict.get("status"))
    if expected_exit is None:
        raise LocalizationError(f"unexpected verifier status: {verdict.get('status')!r}")
    if process.returncode != expected_exit:
        raise LocalizationError(
            f"verifier status {verdict['status']} disagrees with exit code {process.returncode}"
        )
    return verdict


def digest(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as error:
        raise LocalizationError(f"cannot read captured initial snapshot {path}: {error}") from error


def required(path: Path | None) -> Path:
    if path is None:
        raise AssertionError("validated capture is missing an artifact")
    return path


def validate_config(config: Config) -> None:
    if not config.capture_command:
        raise LocalizationError("capture command must not be empty")
    if config.rows < 0:
        raise LocalizationError("row bound must not be negative")
    if config.timeout_ms <= 0:
        raise LocalizationError("solver timeout must be positive")
    if config.capture_timeout_seconds <= 0:
        raise LocalizationError("capture timeout must be positive")
    if config.max_events <= 0:
        raise LocalizationError("maximum event count must be positive")
    if config.artifacts.exists():
        raise LocalizationError(f"artifact directory already exists: {config.artifacts}")


def run_command(arguments: Sequence[str], timeout: int) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            arguments,
            capture_output=True,
            check=False,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise LocalizationError(f"cannot run {arguments[0]!r}: {error}") from error


def _decode_capture(raw: Any, ordinal: int, directory: Path) -> Capture:
    if not isinstance(raw, dict):
        raise LocalizationError("capture manifest must be a JSON object")
    status = raw.get("status")
    common = {
        "protocol",
        "requested_ordinal",
        "status",
        "initial_snapshot",
        "events",
    }
    specific = {
        "PREFIX_CAPTURED": "prefix_snapshot",
        "PREFIX_UNSUPPORTED": "unsupported_reason",
        "OPTIMIZER_COMPLETE": "final_snapshot",
        "FINAL_UNSUPPORTED": "unsupported_reason",
    }.get(status)
    if specific is None:
        raise LocalizationError(f"unknown capture status: {status!r}")
    expected_fields = common | {specific}
    if set(raw) != expected_fields:
        raise LocalizationError(
            f"capture manifest fields differ: expected {sorted(expected_fields)!r}"
        )
    if raw["protocol"] != PROTOCOL:
        raise LocalizationError(f"unsupported capture protocol: {raw['protocol']!r}")
    if type(raw["requested_ordinal"]) is not int or raw["requested_ordinal"] != ordinal:
        raise LocalizationError("capture manifest does not match the requested ordinal")

    events = _decode_events(raw["events"])
    initial = _artifact(directory, raw["initial_snapshot"], "initial_snapshot")
    is_prefix = status in {"PREFIX_CAPTURED", "PREFIX_UNSUPPORTED"}
    if is_prefix and len(events) != ordinal:
        raise LocalizationError(f"{status} must contain the requested event prefix")
    if not is_prefix and len(events) >= ordinal:
        raise LocalizationError(f"{status} must contain fewer events than requested")
    if status == "PREFIX_CAPTURED":
        return Capture(
            status,
            initial,
            events,
            prefix=_artifact(directory, raw[specific], specific),
        )
    if status == "OPTIMIZER_COMPLETE":
        return Capture(
            status,
            initial,
            events,
            final=_artifact(directory, raw[specific], specific),
        )
    reason = raw[specific]
    if not isinstance(reason, str) or not reason:
        raise LocalizationError("unsupported_reason must be a non-empty string")
    return Capture(status, initial, events, unsupported_reason=reason)


def _decode_events(raw: Any) -> tuple[Event, ...]:
    if not isinstance(raw, list):
        raise LocalizationError("events must be a JSON array")
    result: list[Event] = []
    for index, item in enumerate(raw, 1):
        expected_fields = {"ordinal", "kind", "stage", "name"}
        if not isinstance(item, dict) or set(item) != expected_fields:
            raise LocalizationError(f"event {index} has invalid fields")
        if type(item["ordinal"]) is not int or item["ordinal"] != index:
            raise LocalizationError(f"event {index} has a non-contiguous ordinal")
        if not isinstance(item["kind"], str) or item["kind"] not in EVENT_KINDS:
            raise LocalizationError(f"event {index} has an invalid kind")
        if not isinstance(item["stage"], str) or not item["stage"]:
            raise LocalizationError(f"event {index} has an invalid stage")
        if not isinstance(item["name"], str) or not item["name"]:
            raise LocalizationError(f"event {index} has an invalid name")
        result.append(Event(index, item["kind"], item["stage"], item["name"]))
    return tuple(result)


def _artifact(directory: Path, raw: Any, field: str) -> Path:
    if not isinstance(raw, str) or not raw:
        raise LocalizationError(f"{field} must be a non-empty relative path")
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        raise LocalizationError(f"{field} must stay inside the capture directory")
    path = directory / relative
    try:
        path.resolve(strict=True).relative_to(directory.resolve(strict=True))
    except (OSError, ValueError) as error:
        raise LocalizationError(f"{field} must stay inside the capture directory") from error
    if not path.is_file():
        raise LocalizationError(f"{field} does not name a captured file: {raw!r}")
    return path


def _decode_verdict(process: subprocess.CompletedProcess[str]) -> Mapping[str, Any]:
    text = process.stdout.strip() or process.stderr.strip()
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as error:
        raise LocalizationError("verifier did not emit one JSON verdict") from error
    if not isinstance(raw, dict) or not isinstance(raw.get("status"), str):
        raise LocalizationError("verifier verdict must be an object with a status")
    return raw


def _save_process(prefix: Path, process: subprocess.CompletedProcess[str]) -> None:
    prefix.with_suffix(".stdout").write_text(process.stdout, encoding="utf-8")
    prefix.with_suffix(".stderr").write_text(process.stderr, encoding="utf-8")
