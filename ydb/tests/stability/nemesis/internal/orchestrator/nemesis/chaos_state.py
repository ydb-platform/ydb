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
    """Inject the DC fault on every host of ``target``'s datacenter (one shared scenario).

    The scheduler already chose and reserved this DC, so we fan out to exactly its hosts
    rather than let the round-robin ``DataCenterFanoutPlanner`` pick a different one."""
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

    def plan_inject_target(
        self, nemesis_type: str, target: ChaosTarget
    ) -> list[DispatchCommand]:
        """Inject command(s) for one already-chosen ``target`` (weighted scheduler path).

        The scheduler has already reserved the failure budget, so this skips the legacy
        ``filter_safe`` pre-filter. A DATACENTER target fans out to every host in the chosen
        DC (the round-robin fanout planner would pick its own DC, not this one); toggle faults
        (``recovery: extract``) are dispatched directly so a stateful planner's cross-tick
        bookkeeping can't fight the scheduler; anything else goes to its planner's single-target
        tick.
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
        """Extract command for one chosen ``target`` (weighted-scheduler toggle recovery).

        Bypasses the planner's cross-tick state — the scheduler owns which target is broken;
        the agent-side actor restores from its own stored state on extract."""
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
