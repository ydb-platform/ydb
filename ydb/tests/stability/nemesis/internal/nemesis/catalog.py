"""
Nemesis registration for the stability nemesis app.

Single registry:
- NEMESIS_TYPES — each type: runner, schedule, ui_group, optional planner_cls or planner_factory(key).
  If both are omitted, build_all_planners() uses DefaultRandomHostPlanner.

- NEMESIS_UI_GROUPS — group id -> description for /api/process_types/grouped.

ChaosOrchestratorStore receives a planner map from build_all_planners().

All nemesis entries (core + cluster + topology-conditional) are built in
``cluster_entries.all_nemesis_type_entries()``.
Runner classes are re-exported from ``runners/__init__.py``.
"""

from __future__ import annotations

from typing import Any, Type

from ydb.tests.stability.nemesis.internal.nemesis.cluster_entries import all_nemesis_type_entries
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.default_planner import DefaultRandomHostPlanner
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    DEFAULT_RECOVERY_SEC,
    GuardMode,
    ImpactScope,
)


# ---------------------------------------------------------------------------
# UI groups
# ---------------------------------------------------------------------------

NEMESIS_UI_GROUPS: dict[str, dict[str, str]] = {
    "NetworkNemesis": {
        "description": "Network fault injection",
    },
    "NodeNemesis": {
        "description": "Node process failures",
    },
    "ClusterChaos": {
        "description": "External cluster harness chaos (cluster.yaml on the agent host)",
    },
    "ClusterTablets": {
        "description": "Tablet kill / Hive tablet moves (cluster.yaml on the agent host)",
    },
    "DatacenterChaos": {
        "description": "Multi-datacenter scenarios (enabled when cluster.yaml lists 2+ data_center)",
    },
    "BridgePileChaos": {
        "description": "Bridge pile scenarios (enabled when cluster.yaml has config.bridge_config.piles)",
    },
}


# ---------------------------------------------------------------------------
# Main registry
# ---------------------------------------------------------------------------

NEMESIS_TYPES: dict[str, dict[str, Any]] = all_nemesis_type_entries()


# ---------------------------------------------------------------------------
# Planner construction
# ---------------------------------------------------------------------------


def build_planner(key: str, params: dict[str, Any] | None = None) -> NemesisPlannerBase:
    """Build a single planner for ``key``, optionally with user-supplied ``params``.

    Resolution order:
        1. ``planner_factory(key, params=...)`` if it accepts ``params``;
           else ``planner_factory(key)`` (params silently ignored).
        2. ``planner_cls(**params)`` if ``params`` non-empty, else ``planner_cls()``.
        3. ``DefaultRandomHostPlanner(nemesis_type=key)``.
    """
    import inspect

    spec = NEMESIS_TYPES.get(key)
    if spec is None:
        return DefaultRandomHostPlanner(nemesis_type=key)

    params = params or {}
    planner_factory = spec.get("planner_factory")
    if planner_factory is not None:
        try:
            sig = inspect.signature(planner_factory)
            if "params" in sig.parameters or any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
            ):
                return planner_factory(key, params=params)
        except (TypeError, ValueError):
            pass
        return planner_factory(key)

    cls: Type[NemesisPlannerBase] | None = spec.get("planner_cls")
    if cls is not None:
        return cls(**params) if params else cls()
    return DefaultRandomHostPlanner(nemesis_type=key)


def build_all_planners() -> dict[str, NemesisPlannerBase]:
    """Planner per registered type using default parameters."""
    return {key: build_planner(key) for key in NEMESIS_TYPES.keys()}


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


def get_all_nemesis_types() -> list[str]:
    return list(NEMESIS_TYPES.keys())


def impact_scope_for(nemesis_type: str) -> ImpactScope:
    """ImpactScope for ``nemesis_type`` (defaults to NODE when unannotated)."""
    spec = NEMESIS_TYPES.get(nemesis_type)
    if spec is None:
        return ImpactScope.UNKNOWN
    scope = spec.get("impact_scope")
    return scope if isinstance(scope, ImpactScope) else ImpactScope.NODE


def target_kind_for(nemesis_type: str) -> TargetKind:
    """TargetKind for ``nemesis_type`` (defaults to HOST when unannotated)."""
    spec = NEMESIS_TYPES.get(nemesis_type)
    if spec is None:
        return TargetKind.HOST
    kind = spec.get("target_kind")
    return kind if isinstance(kind, TargetKind) else TargetKind.HOST


def guard_mode_for(nemesis_type: str) -> GuardMode:
    """GuardMode for ``nemesis_type``.

    Default when unannotated: FULL for stateless types (no ``planner_cls`` /
    ``planner_factory`` -> DefaultRandomHostPlanner), else BYPASS.
    """
    spec = NEMESIS_TYPES.get(nemesis_type)
    if spec is None:
        return GuardMode.BYPASS
    mode = spec.get("guard_mode")
    if isinstance(mode, GuardMode):
        return mode
    has_custom_planner = spec.get("planner_cls") is not None or spec.get("planner_factory") is not None
    return GuardMode.BYPASS if has_custom_planner else GuardMode.FULL


def recovery_sec_for(nemesis_type: str) -> float | None:
    """Auto-recovery window (seconds) passed to ``record_inject`` as ``recovery_sec``.

    Number -> impairment auto-expires after that many seconds; ``None`` -> held until an
    explicit extract (toggle faults); unannotated -> :data:`DEFAULT_RECOVERY_SEC`.
    """
    spec = NEMESIS_TYPES.get(nemesis_type)
    if spec is None:
        return DEFAULT_RECOVERY_SEC
    if "auto_recovery_sec" not in spec:
        return DEFAULT_RECOVERY_SEC
    val = spec.get("auto_recovery_sec")
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return DEFAULT_RECOVERY_SEC


def supports_manual_for(nemesis_type: str) -> bool:
    """Whether UI/API manual inject on a host/target is supported for this type.

    Planners that keep cross-tick state (network isolation, rolling restart, DC/pile
    fanout) return None from ``manual()`` → ``False`` here.
    """
    spec = NEMESIS_TYPES.get(nemesis_type)
    if spec is None:
        return False
    if "supports_manual" in spec:
        return bool(spec["supports_manual"])
    return True


def nemesis_types_flat_for_api() -> list[dict[str, Any]]:
    """Rows for GET /api/process_types."""
    result: list[dict[str, Any]] = []
    for name, definition in NEMESIS_TYPES.items():
        runner = definition.get("runner")
        description = (
            runner.nemesis_description if runner and hasattr(runner, "nemesis_description") else ""
        )
        kind = definition.get("target_kind")
        result.append(
            {
                "name": name,
                "description": description,
                "schedule": int(definition.get("schedule") or 60),
                "params": list(definition.get("params") or []),
                "target_kind": kind.value if hasattr(kind, "value") else "host",
                "supports_manual": supports_manual_for(name),
            }
        )
    return result


def nemesis_types_grouped_for_api() -> dict[str, Any]:
    """Payload for GET /api/process_types/grouped."""
    groups: dict[str, Any] = {}
    for gid, meta in NEMESIS_UI_GROUPS.items():
        groups[gid] = {"description": meta["description"], "nemesis": []}
    groups["Other"] = {"description": "Other nemesis types", "nemesis": []}

    for name, definition in NEMESIS_TYPES.items():
        runner = definition.get("runner")
        description = (
            runner.nemesis_description if runner and hasattr(runner, "nemesis_description") else ""
        )
        gid = definition.get("ui_group", "Other")
        if gid not in groups:
            groups[gid] = {"description": "", "nemesis": []}
        kind = definition.get("target_kind")
        groups[gid]["nemesis"].append(
            {
                "name": name,
                "description": description,
                "schedule": int(definition.get("schedule") or 60),
                "params": list(definition.get("params") or []),
                "target_kind": kind.value if hasattr(kind, "value") else "host",
                "supports_manual": supports_manual_for(name),
            }
        )

    return {k: v for k, v in groups.items() if v["nemesis"]}
