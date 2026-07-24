"""
Orchestrator chaos planning: ChaosOrchestratorStore holds planners (no process-wide singleton).
Create one instance per app and pass it to OrchestratorNemesisSchedule / wire from orchestrator_router.
"""

from __future__ import annotations

import logging
import threading

from ydb.tests.stability.nemesis.internal.nemesis.catalog import (
    build_all_planners,
    build_planner,
    guard_mode_for,
    impact_scope_for,
    target_kind_for,
)
from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.cluster_inventory import ClusterInventory
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import FailureModelGuard, GuardMode
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase

logger = logging.getLogger(__name__)


class ChaosOrchestratorStore:
    def __init__(
        self,
        failure_guard: FailureModelGuard | None = None,
        inventory: ClusterInventory | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._planners: dict[str, NemesisPlannerBase] = build_all_planners()
        self._failure_guard = failure_guard
        self._inventory = inventory

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

    def plan_scheduled_tick(
        self,
        nemesis_type: str,
        hosts: list[str] | None = None,
    ) -> list[DispatchCommand]:
        planner = self._planners.get(nemesis_type)
        if planner is None:
            return []

        kind = target_kind_for(nemesis_type)
        if self._inventory is not None:
            candidates = self._inventory.entities(kind)
        else:
            # Fallback: host-only candidates from the agent host list.
            candidates = [ChaosTarget.for_host(h) for h in (hosts or [])]

        mode = guard_mode_for(nemesis_type)
        if (
            self._failure_guard is not None
            and self._failure_guard.enabled
            and mode in (GuardMode.FULL, GuardMode.PREFILTER_ONLY)
        ):
            scope = impact_scope_for(nemesis_type)
            filtered = self._failure_guard.filter_safe(candidates, scope)
            if len(filtered) != len(candidates):
                logger.info(
                    "Failure model pre-filter: %s %d -> %d safe candidate(s) (kind=%s)",
                    nemesis_type,
                    len(candidates),
                    len(filtered),
                    kind.value,
                )
            candidates = filtered

        if not candidates:
            logger.info("No safe candidates for %s (kind=%s)", nemesis_type, kind.value)
            return []
        return planner.scheduled_tick(candidates)

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
