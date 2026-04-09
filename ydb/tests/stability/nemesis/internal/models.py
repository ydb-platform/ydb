"""
UI-oriented dataclasses and request validation helpers for the Nemesis app.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, List, Optional


# --- API / UI payloads ---------------------------------------------------------


@dataclass
class ProcessInfo:
    """Single nemesis process row as returned by GET /api/processes."""

    id: int
    type: str
    command: str
    logs: str
    ret_code: Optional[int]
    status: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ProcessTypeRow:
    """Entry for GET /api/process_types."""

    name: str
    description: str
    schedule: int = 60  # default interval (sec) from catalog.NEMESIS_TYPES

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ApiMessageResponse:
    """Generic {status, message?} JSON."""

    status: str
    message: Optional[str] = None

    def to_json(self) -> dict[str, Any]:
        d = {"status": self.status}
        if self.message is not None:
            d["message"] = self.message
        return d


@dataclass
class WardenCheckResult:
    """Result of a single warden check (agent or orchestrator)."""

    name: str
    category: str  # 'liveness' or 'safety'
    violations: List[str]
    status: str  # 'ok', 'violation', 'error'
    error_message: Optional[str] = None
    affected_hosts: List[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class WardenCheckReport:
    """Full warden report for API consumers."""

    status: str  # 'idle', 'running', 'completed', 'error'
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    liveness_checks: List[WardenCheckResult] = field(default_factory=list)
    safety_checks: List[WardenCheckResult] = field(default_factory=list)
    error_message: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "liveness_checks": [c.to_json() for c in self.liveness_checks],
            "safety_checks": [c.to_json() for c in self.safety_checks],
            "error_message": self.error_message,
        }


# --- Validation helpers (unchanged contract) ---------------------------------


def validate_create_process_request(data: dict) -> tuple:
    if not data:
        return False, "No data provided", None

    process_type = data.get("type")
    if not process_type:
        return False, "Missing type field", None

    action = data.get("action", "inject")

    validated_data = {
        "type": process_type,
        "action": action,
    }

    return True, None, validated_data


def validate_set_schedule_request(data: dict) -> tuple:
    if not data:
        return False, "No data provided", None

    process_type = data.get("type")
    if not process_type:
        return False, "Missing type field", None

    enabled = data.get("enabled")
    if enabled is None:
        return False, "Missing enabled field", None

    if not isinstance(enabled, bool):
        return False, "enabled must be a boolean", None

    interval = data.get("interval")
    if interval is not None and not isinstance(interval, int):
        return False, "interval must be an integer", None

    validated_data = {
        "type": process_type,
        "enabled": enabled,
        "interval": interval,
    }

    return True, None, validated_data


def validate_create_host_process_request(data: dict) -> tuple:
    if not data:
        return False, "No data provided", None

    host = data.get("host")
    if not host:
        return False, "Missing host field", None

    process_type = data.get("type")
    if not process_type:
        return False, "Missing type field", None

    action = data.get("action", "inject")

    validated_data = {
        "host": host,
        "type": process_type,
        "action": action,
    }

    return True, None, validated_data
