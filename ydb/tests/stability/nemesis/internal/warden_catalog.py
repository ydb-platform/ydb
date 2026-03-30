"""
Объединённый индекс для API: строки safety и get_all_warden_definitions.

Определения чеков:
- internal/agent/agent_warden_catalog.py — агент
- internal/orchestrator/orchestrator_warden_catalog.py — оркестратор
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Tuple

from ydb.tests.stability.nemesis.internal.agent.agent_warden_catalog import AGENT_SAFETY_CHECKS
from ydb.tests.stability.nemesis.internal.orchestrator.orchestrator_warden_catalog import (
    ORCHESTRATOR_LIVENESS_CHECKS,
    ORCHESTRATOR_SAFETY_CHECKS,
)


@dataclass(frozen=True)
class SafetyCheckRow:
    """One documented safety check."""

    name: str
    description: str
    location: Literal["agent", "orchestrator"]


SAFETY_CHECK_ROWS: Tuple[SafetyCheckRow, ...] = (
    *(
        SafetyCheckRow(s.name, s.description, "agent")
        for s in AGENT_SAFETY_CHECKS
    ),
    *(
        SafetyCheckRow(s.name, s.description, "orchestrator")
        for s in ORCHESTRATOR_SAFETY_CHECKS
    ),
)


def get_all_warden_definitions() -> List[Dict[str, Any]]:
    """Flat list for API: liveness (orchestrator) + safety (per location row)."""
    out: List[Dict[str, Any]] = []
    for c in ORCHESTRATOR_LIVENESS_CHECKS:
        out.append(
            {
                "name": c.name,
                "category": "liveness",
                "description": c.description,
                "location": "orchestrator",
            }
        )
    for r in SAFETY_CHECK_ROWS:
        out.append(
            {
                "name": r.name,
                "category": "safety",
                "description": r.description,
                "location": r.location,
            }
        )
    return out
