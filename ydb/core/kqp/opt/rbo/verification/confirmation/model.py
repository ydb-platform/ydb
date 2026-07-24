"""Small shared types for counterexample confirmation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


FORMAT = "ydb-rbo-counterexample-confirmation"
VERSION = 1
COVERAGE_FORMAT = "ydb-rbo-benchmark-coverage"
COVERAGE_VERSION = 5
COVERAGE_VERSIONS = frozenset({4, COVERAGE_VERSION})
TRACE_FORMAT = "ydb-rbo-concrete-trace"
TRACE_VERSION = 1
REPLAY_FORMAT = "ydb-rbo-real-replay"
REPLAY_VERSION = 1
SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class ConfirmationError(RuntimeError):
    """The confirmation input, child protocol, or environment is invalid."""


@dataclass(frozen=True, slots=True)
class Config:
    report: Path
    inspector: Path
    solver: Path
    replay: Path
    ydb: Path
    artifacts: Path
    baseline_endpoint: str
    baseline_database: str
    candidate_endpoint: str
    candidate_database: str
    solver_timeout_ms: int = 60_000
    replay_timeout_seconds: int = 300
    replay_process_timeout_seconds: int = 3_600


@dataclass(frozen=True, slots=True)
class Candidate:
    query_id: int
    source: str
    report_verdict: Mapping[str, Any]
    artifacts: Any


@dataclass(frozen=True, slots=True)
class CandidateInputs:
    initial: Path
    final: Path
    query: Path
    verifier: Path
    verdict: Mapping[str, Any]
    query_sha256: str
    result: Mapping[str, Any]
