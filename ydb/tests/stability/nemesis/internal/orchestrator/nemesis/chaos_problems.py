"""Problem log for nemesis-side anomalies, exposed via ``GET /api/problems``.

Some things go wrong on the chaos side rather than on the cluster side, and nothing in the
stability test would notice them otherwise:

* a fault that never recovered (``RecoveryProbe`` reports it stuck and keeps holding its budget,
  so the cluster is running degraded and the scheduler is quietly doing less chaos);
* a degraded inventory (the cluster harness was unavailable, so node/slot ids and ports are
  synthesized guesses and slot chaos does not run at all).

An unusable failure model is *not* in this list: the orchestrator refuses to start without one
(see ``app._require_failure_model``), so there is no "chaos ran unguarded" case to report.

Both are recorded here so ``ydb/tests/stability/tests`` can pull them at the end of a phase and
attach them to its Allure report (see ``StressUtilDeployer._report_nemesis_problems``).

Problems are latched: an entry stays in the list after the fault eventually recovers, because the
report is read once, after the run. Repeats of the same problem update ``last_seen`` / ``count``
instead of piling up.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Problem kinds (kept stable — the stability test report groups by them).
KIND_STUCK_FAULT = "stuck_fault"
KIND_INVENTORY_DEGRADED = "inventory_degraded"

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
        """``on_stuck`` callback for :class:`RecoveryProbe`."""
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
            },
        )

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

    def clear(self) -> None:
        with self._lock:
            self._problems.clear()
            self._dropped = 0


__all__ = [
    "ChaosProblem",
    "ChaosProblemStore",
    "KIND_STUCK_FAULT",
    "KIND_INVENTORY_DEGRADED",
]
