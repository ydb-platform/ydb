"""
Orchestrator chaos planning: ChaosOrchestratorStore holds planners (no process-wide singleton).
Create one instance per app and pass it to OrchestratorNemesisSchedule / wire from orchestrator_router.
"""

from __future__ import annotations

import threading
from ydb.tests.stability.nemesis.internal.nemesis.catalog import build_all_planners, build_planner
from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase


class ChaosOrchestratorStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._planners: dict[str, NemesisPlannerBase] = build_all_planners()

    def rebuild_planner(self, nemesis_type: str, params: dict | None = None) -> bool:
        """Re-create a planner for ``nemesis_type`` with the supplied ``params``.

        Used when the UI starts a scheduled run with custom parameters. Returns
        ``True`` if the planner was rebuilt, ``False`` if the type is unknown
        or planner construction failed.
        """
        with self._lock:
            try:
                self._planners[nemesis_type] = build_planner(nemesis_type, params)
                return True
            except Exception:
                return False

    def plan_scheduled_tick(self, nemesis_type: str, hosts: list[str]) -> list[DispatchCommand]:
        planner = self._planners.get(nemesis_type)
        if planner is None:
            return []
        return planner.scheduled_tick(hosts)

    def plan_disable_schedule(self, nemesis_type: str) -> list[DispatchCommand]:
        planner = self._planners.get(nemesis_type)
        if planner is None:
            return []
        return planner.extract_all_on_disable()

    def plan_manual(
        self, nemesis_type: str, host: str, action: str
    ) -> list[DispatchCommand] | None:
        action = (action or "inject").lower()
        if action not in ("inject", "extract"):
            return None
        planner = self._planners.get(nemesis_type)
        if planner is None:
            return None
        return planner.manual(host, action)


__all__ = [
    "ChaosOrchestratorStore",
    "DispatchCommand",
]
