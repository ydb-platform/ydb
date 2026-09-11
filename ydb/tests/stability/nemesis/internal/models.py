"""
UI-oriented dataclasses and request validation helpers for the Nemesis app.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, List, Optional, Tuple


DEFAULT_WARDEN_HOURS_BACK = 24.0


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


@dataclass(frozen=True)
class WardenTimeWindow:
    """Inclusive log-search window for safety wardens (unix seconds)."""

    start_ts: float
    end_ts: float

    def as_local_naive(self) -> Tuple[datetime, datetime]:
        return datetime.fromtimestamp(self.start_ts), datetime.fromtimestamp(self.end_ts)

    def to_json(self) -> dict[str, Any]:
        return {"start_time": self.start_ts, "end_time": self.end_ts}

    @classmethod
    def from_hours_back(cls, hours_back: float, *, end_ts: Optional[float] = None) -> WardenTimeWindow:
        end = time.time() if end_ts is None else float(end_ts)
        return cls(start_ts=end - float(hours_back) * 3600.0, end_ts=end)


def _parse_warden_timestamp(value: Any, field_name: str) -> Tuple[Optional[float], Optional[str]]:
    if value is None:
        return None, None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None, f"{field_name} must be a unix timestamp or ISO-8601 string"
    if isinstance(value, (int, float)):
        return float(value), None
    text = value.strip()
    if not text:
        return None, f"{field_name} must not be empty"
    if text.endswith("Z") or text.endswith("z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None, f"{field_name} is not a valid timestamp"
    return parsed.timestamp(), None


def parse_warden_time_window(data: Optional[dict]) -> Tuple[bool, Optional[str], Optional[WardenTimeWindow]]:
    """Parse POST /api/warden/start body into a search window.

    Accepted fields (all optional):
    - ``start_time`` / ``end_time`` — unix timestamps (stability tests pass these)
    - ``since`` / ``until`` — ISO-8601 datetimes
    - ``hours_back`` — relative window ending at ``until``/``end_time``/now

    Empty body keeps the historical default of the last 24 hours.
    """
    if data is None:
        data = {}
    if not isinstance(data, dict):
        return False, "Body must be a JSON object", None

    if data.get("start_time") is not None and data.get("since") is not None:
        return False, "Pass either start_time or since, not both", None
    if data.get("end_time") is not None and data.get("until") is not None:
        return False, "Pass either end_time or until, not both", None

    start_ts, err = _parse_warden_timestamp(data.get("start_time"), "start_time")
    if err:
        return False, err, None
    since_ts, err = _parse_warden_timestamp(data.get("since"), "since")
    if err:
        return False, err, None
    end_ts, err = _parse_warden_timestamp(data.get("end_time"), "end_time")
    if err:
        return False, err, None
    until_ts, err = _parse_warden_timestamp(data.get("until"), "until")
    if err:
        return False, err, None

    start_ts = start_ts if start_ts is not None else since_ts
    end_ts = end_ts if end_ts is not None else until_ts

    hours_back = data.get("hours_back")
    if hours_back is not None:
        if isinstance(hours_back, bool) or not isinstance(hours_back, (int, float)):
            return False, "hours_back must be a number", None
        if hours_back <= 0:
            return False, "hours_back must be positive", None

    if end_ts is None:
        end_ts = time.time()
    if start_ts is None:
        span = float(hours_back) if hours_back is not None else DEFAULT_WARDEN_HOURS_BACK
        start_ts = end_ts - span * 3600.0

    if start_ts >= end_ts:
        return False, "start_time/since must be earlier than end_time/until", None

    return True, None, WardenTimeWindow(start_ts=start_ts, end_ts=end_ts)


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
