"""
Orchestrator chaos planning: ChaosOrchestratorStore holds planners (no process-wide singleton).
Create one instance per app and pass it to OrchestratorNemesisSchedule / wire from orchestrator_router.
"""

from __future__ import annotations

import logging
import random
import threading

from ydb.tests.stability.nemesis.internal.nemesis.catalog import (
    build_all_planners,
    build_planner,
    guard_mode_for,
    impact_scope_for,
    recovery_mode_for,
    target_kind_for,
)
from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch, fanout
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.cluster_inventory import ClusterInventory
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import FailureModelGuard, GuardMode
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase

logger = logging.getLogger(__name__)


def datacenter_inject_fanout(
    nemesis_type: str, target: ChaosTarget, inventory
) -> list[DispatchCommand]:
    """Inject on every host of ``target``'s DC (one scenario) — the DC is already reserved, so the
    round-robin ``DataCenterFanoutPlanner`` must not pick a different one."""
    dc = target.group_id
    hosts = [
        t.host for t in inventory.entities(TargetKind.DATACENTER) if t.group_id == dc
    ] or [target.host]
    return fanout(
        nemesis_type,
        [ChaosTarget.for_datacenter(h, dc) for h in hosts],
        "inject",
        {"datacenter": dc},
    )


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
        """Re-create a planner with UI-supplied ``params``; False if that failed."""
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
        candidates = list(candidates)
        extract_mode = recovery_mode_for(nemesis_type) == "extract"
        # Shuffle before joint packing so multi-inject planners do not always take inventory order.
        if not extract_mode:
            random.shuffle(candidates)

        if self._failure_guard is not None and guard_mode_for(nemesis_type) is GuardMode.FULL:
            scope = impact_scope_for(nemesis_type)
            # jointly=False only for single-pick extract ticks; multi-inject planners need packing.
            filtered = self._failure_guard.filter_safe(
                candidates, scope, jointly=not extract_mode
            )
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
        # Toggle faults: probe owns extract. Planner sets like NetworkNemesis.isolated_hosts
        # would stay populated after a probe extract and permanently starve later injects.
        if extract_mode:
            target = random.choice(candidates)
            return self._direct_commands(nemesis_type, target, "inject")
        return planner.scheduled_tick(candidates)

    def plan_inject_target(
        self, nemesis_type: str, target: ChaosTarget
    ) -> list[DispatchCommand]:
        """Inject on one already-reserved ``target`` (boundary scheduler path), so no pre-filter.

        A DATACENTER target is fanned out over its DC; a toggle fault is dispatched directly, so a
        planner's cross-tick state cannot fight the scheduler; anything else goes to its planner.
        """
        if target.kind is TargetKind.DATACENTER and self._inventory is not None:
            return datacenter_inject_fanout(nemesis_type, target, self._inventory)
        if recovery_mode_for(nemesis_type) == "extract":
            return self._direct_commands(nemesis_type, target, "inject")
        planner = self._planners.get(nemesis_type)
        if planner is None:
            return []
        return planner.scheduled_tick([target])

    def plan_extract_target(
        self, nemesis_type: str, target: ChaosTarget
    ) -> list[DispatchCommand]:
        """Extract one chosen ``target``: the scheduler owns what is broken, and the agent-side
        actor restores from its own stored state."""
        return self._direct_commands(nemesis_type, target, "extract")

    def _direct_commands(
        self, nemesis_type: str, target: ChaosTarget, action: str
    ) -> list[DispatchCommand]:
        planner = self._planners.get(nemesis_type)
        attr = "PAYLOAD_INJECT" if action == "inject" else "PAYLOAD_EXTRACT"
        payload = dict(getattr(planner, attr, {}) or {}) if planner is not None else {}
        return [dispatch(nemesis_type, target, action, payload)]

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
    "datacenter_inject_fanout",
    "dispatch",
]
