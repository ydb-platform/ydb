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


def nemesis_types_flat_for_api() -> list[dict[str, Any]]:
    """Rows for GET /api/process_types."""
    result: list[dict[str, Any]] = []
    for name, definition in NEMESIS_TYPES.items():
        runner = definition.get("runner")
        description = (
            runner.nemesis_description if runner and hasattr(runner, "nemesis_description") else ""
        )
        result.append(
            {
                "name": name,
                "description": description,
                "schedule": int(definition.get("schedule") or 60),
                "params": list(definition.get("params") or []),
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
        groups[gid]["nemesis"].append(
            {
                "name": name,
                "description": description,
                "schedule": int(definition.get("schedule") or 60),
                "params": list(definition.get("params") or []),
            }
        )

    return {k: v for k, v in groups.items() if v["nemesis"]}
