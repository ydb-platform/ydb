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

from ydb.tests.stability.nemesis.internal.config import get_orchestrator_settings
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

# In local mode (single-host harness via ydb/tests/tools/local_cluster) we
# expose only runners that opt in via supports_local_mode=True.  Everything
# else assumes systemd, /Berkanavt paths, multi-host networking, or destructive
# system-wide operations that aren't safe on a developer's machine.
if get_orchestrator_settings().local_mode:
    NEMESIS_TYPES = {
        key: spec
        for key, spec in NEMESIS_TYPES.items()
        if getattr(spec["runner"], "supports_local_mode", False)
    }


# ---------------------------------------------------------------------------
# Planner construction
# ---------------------------------------------------------------------------


def build_all_planners() -> dict[str, NemesisPlannerBase]:
    """Planner per registered type: planner_factory(key), planner_cls(), or DefaultRandomHostPlanner."""
    merged: dict[str, NemesisPlannerBase] = {}
    for key, spec in NEMESIS_TYPES.items():
        planner_factory = spec.get("planner_factory")
        if planner_factory is not None:
            merged[key] = planner_factory(key)
            continue
        cls: Type[NemesisPlannerBase] | None = spec.get("planner_cls")
        if cls is not None:
            merged[key] = cls()
        else:
            merged[key] = DefaultRandomHostPlanner(nemesis_type=key)
    return merged


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
            }
        )

    return {k: v for k, v in groups.items() if v["nemesis"]}
