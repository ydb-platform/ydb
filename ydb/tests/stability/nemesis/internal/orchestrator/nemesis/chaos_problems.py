"""Problem log for chaos-side anomalies, served by ``GET /api/problems``.

Things the cluster checks cannot see: a fault that never recovered (its budget is still held, so the
scheduler is quietly doing less chaos) and a degraded inventory (synthesized node ids, no slot
chaos). ``ydb/tests/stability/tests`` pulls this when disabling nemesis and attaches it to Allure.

Entries are latched (the report is read once, after the run) and deduplicated by kind + target.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Kinds are stable: the test report groups by them.
KIND_STUCK_FAULT = "stuck_fault"
KIND_INVENTORY_DEGRADED = "inventory_degraded"
KIND_PROBE_BLIND = "recovery_probe_blind"

DEFAULT_LIMIT = 200


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class ChaosProblem:
    kind: str
    summary: str
    nemesis_type: str = ""
    target: str = ""
    host: str = ""
    details: dict = field(default_factory=dict)
    first_seen: str = field(default_factory=_now_iso)
    last_seen: str = field(default_factory=_now_iso)
    count: int = 1

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.kind, self.nemesis_type, self.target)

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "summary": self.summary,
            "nemesis_type": self.nemesis_type,
            "target": self.target,
            "host": self.host,
            "details": dict(self.details),
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "count": self.count,
        }


class ChaosProblemStore:
    """Thread-safe, deduplicating, bounded list of :class:`ChaosProblem`."""

    def __init__(self, limit: int = DEFAULT_LIMIT) -> None:
        self._lock = threading.Lock()
        self._problems: dict[tuple[str, str, str], ChaosProblem] = {}
        self._limit = max(1, int(limit))
        self._dropped = 0

    def record(
        self,
        kind: str,
        summary: str,
        *,
        nemesis_type: str = "",
        target: str = "",
        host: str = "",
        details: dict | None = None,
    ) -> None:
        problem = ChaosProblem(
            kind=kind,
            summary=summary,
            nemesis_type=nemesis_type,
            target=target,
            host=host,
            details=dict(details or {}),
        )
        with self._lock:
            existing = self._problems.get(problem.key)
            if existing is not None:
                existing.count += 1
                existing.last_seen = problem.last_seen
                existing.summary = summary
                existing.details = problem.details
                return
            if len(self._problems) >= self._limit:
                self._dropped += 1
                return
            self._problems[problem.key] = problem
        logger.warning("chaos problem recorded [%s]: %s", kind, summary)

    def record_stuck_fault(self, fault) -> None:
        """``on_stuck`` callback for the recovery probe."""
        target = fault.target
        self.record(
            KIND_STUCK_FAULT,
            (
                f"{fault.nemesis_type} on {target.identity_key()} did not recover within "
                f"{fault.timeout_sec:.0f}s (held {fault.held_sec:.0f}s); failure budget still held"
            ),
            nemesis_type=fault.nemesis_type,
            target=target.identity_key(),
            host=target.host,
            details={
                "held_sec": round(float(fault.held_sec), 1),
                "timeout_sec": round(float(fault.timeout_sec), 1),
                "lease_id": fault.lease_id,
                "phase": getattr(fault, "phase", ""),
            },
        )

    def record_probe_blind(self, details: dict | None = None) -> None:
        """``on_blind``: no fresh healthcheck data."""
        self.record(
            KIND_PROBE_BLIND,
            (
                "recovery probe has no fresh healthcheck data: budget releases and stuck "
                "detection are paused, scheduled extracts still fire"
            ),
            details=details,
        )

    def resolve_kind(self, kind: str) -> int:
        """Drop every entry of ``kind`` (e.g. clear probe-blind when sight returns)."""
        with self._lock:
            doomed = [key for key in self._problems if key[0] == kind]
            for key in doomed:
                del self._problems[key]
        if doomed:
            logger.info("chaos problem resolved [%s]: %d entr(ies)", kind, len(doomed))
        return len(doomed)

    def record_inventory_degraded(self, reason: str, details: dict | None = None) -> None:
        self.record(
            KIND_INVENTORY_DEGRADED,
            (
                f"chaos inventory is degraded ({reason}): node/slot ids and ports are synthesized, "
                f"slot chaos does not run"
            ),
            details=details,
        )

    def snapshot(self) -> list[dict]:
        with self._lock:
            problems = sorted(self._problems.values(), key=lambda p: p.first_seen)
            return [p.to_dict() for p in problems]

    def counts_by_kind(self) -> dict[str, int]:
        with self._lock:
            out: dict[str, int] = {}
            for p in self._problems.values():
                out[p.kind] = out.get(p.kind, 0) + 1
            return out

    @property
    def dropped(self) -> int:
        with self._lock:
            return self._dropped


__all__ = [
    "ChaosProblem",
    "ChaosProblemStore",
    "KIND_STUCK_FAULT",
    "KIND_INVENTORY_DEGRADED",
    "KIND_PROBE_BLIND",
]
