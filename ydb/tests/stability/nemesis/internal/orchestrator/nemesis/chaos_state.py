"""
Orchestrator chaos planning: ChaosOrchestratorStore holds planners (no process-wide singleton).
One app-owned planner store supplies immutable command plans to FaultEngine.
"""

from __future__ import annotations


from ydb.tests.stability.nemesis.internal.nemesis.catalog import (
    NEMESIS_TYPES,
    supports_boundary_scheduler,
    build_planner,
    recovery_mode_for,
)
from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch, fanout
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.cluster_inventory import ClusterInventory
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import FailureModelGuard
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase


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


def pile_inject_fanout(
    nemesis_type: str, target: ChaosTarget, inventory
) -> list[DispatchCommand]:
    """Inject on every host of ``target``'s pile (one scenario).

    Same contract as :func:`datacenter_inject_fanout`: the boundary scheduler already chose the
    pile, so ``BridgePileFanoutPlanner.scheduled_tick`` must not ``random.choice`` another one.
    """
    pile_id = target.group_id
    hosts = [
        t.host for t in inventory.entities(TargetKind.PILE) if t.group_id == pile_id
    ] or [target.host]
    payload_id = int(pile_id) if str(pile_id).isdigit() else pile_id
    return fanout(
        nemesis_type,
        [ChaosTarget.for_pile(h, pile_id) for h in hosts],
        "inject",
        {"bridge_pile_id": payload_id},
    )


class ChaosOrchestratorStore:
    def __init__(
        self,
        failure_guard: FailureModelGuard | None = None,
        inventory: ClusterInventory | None = None,
        enabled_types=None,
    ) -> None:
        names = enabled_types if enabled_types is not None else (
            name for name in NEMESIS_TYPES if supports_boundary_scheduler(name)
        )
        self._allowed_types = frozenset(names)
        self._planners: dict[str, NemesisPlannerBase] = {
            name: build_planner(name) for name in self._allowed_types
        }
        self._inventory = inventory

    def plan_inject_target(
        self, nemesis_type: str, target: ChaosTarget
    ) -> list[DispatchCommand]:
        """Inject on one already-reserved ``target`` (boundary scheduler path), so no pre-filter.

        A DATACENTER or PILE target is fanned out over that group; a toggle fault is dispatched
        directly, so a planner's cross-tick state cannot fight the scheduler; anything else goes
        to its planner.
        """
        if nemesis_type not in self._allowed_types:
            raise ValueError("Fault has not been migrated to FaultEngine")
        if target.kind is TargetKind.DATACENTER and self._inventory is not None:
            return datacenter_inject_fanout(nemesis_type, target, self._inventory)
        if target.kind is TargetKind.PILE and self._inventory is not None:
            return pile_inject_fanout(nemesis_type, target, self._inventory)
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


__all__ = [
    "ChaosOrchestratorStore",
    "DispatchCommand",
    "datacenter_inject_fanout",
    "dispatch",
]
