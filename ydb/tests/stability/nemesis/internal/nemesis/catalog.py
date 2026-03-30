"""
Nemesis registration for the stability nemesis app.

Single registry:
- NEMESIS_TYPES — each type: runner, schedule, ui_group, optional planner_cls.
  If planner_cls is omitted, build_all_planners() uses DefaultRandomHostPlanner.

- NEMESIS_UI_GROUPS — group id -> description for /api/process_types/grouped.

ChaosOrchestratorStore receives a planner map from build_all_planners().
"""

from __future__ import annotations

import logging
import signal
import subprocess
from typing import Any, Type

from ydb.tests.tools.nemesis.library import base
from ydb.tests.library.nemesis.network.client import NetworkClient

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.kill_node_planner import (
    NODE_KILLER,
    KillNodeNemesisPlanner,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.default_planner import DefaultRandomHostPlanner
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.network_planner import (
    NETWORK_NEMESIS,
    NetworkNemesisPlanner,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase

# Dedicated logger for in-process nemesis runs; NemesisManager attaches a thread-local handler here (not on root).
NEMESIS_EXECUTION_LOGGER = "ydb.tests.stability.nemesis.execution"


class MonitoredAgentActor(base.AbstractMonitoredNemesis):
    """
    Agent-side execution only: AbstractMonitoredNemesis metrics.
    Planning is on the orchestrator (ChaosOrchestratorStore).
    """

    def __init__(self, scope="node"):
        base.AbstractMonitoredNemesis.__init__(self, scope=scope)
        self._logger = logging.getLogger(NEMESIS_EXECUTION_LOGGER)
        self._logger.setLevel(logging.DEBUG)

    def prepare_fault(self, hosts):
        raise RuntimeError(
            f"{self.__class__.__name__} is orchestrator-planned only; "
            "prepare_fault must not be called on the agent runner."
        )

    @property
    def nemesis_description(self):
        return self.__class__.__doc__ or ""


class NetworkNemesis(MonitoredAgentActor):
    """Isolates localhost from the network or restores connectivity (payload is unused)."""

    def inject_fault(self, payload=None):
        del payload
        self._logger.info("=== INJECT_FAULT START: NetworkNemesis ===")
        client = NetworkClient("localhost", port=19001, ssh_username=None)
        self._logger.info("Isolating node...")
        client.isolate_node()
        self.on_success_inject_fault()
        self._logger.info("=== INJECT_FAULT SUCCESS: NetworkNemesis ===")

    def extract_fault(self, payload=None):
        del payload
        self._logger.info("Extracting fault (network)")
        client = NetworkClient("localhost", port=19001, ssh_username=None)
        self._logger.info("Restoring node...")
        client.clear_all_drops()
        self.on_success_extract_fault()


class KillNodeNemesis(MonitoredAgentActor):
    """SIGKILL one local YDB ic-port process; extract is a monitoring noop."""

    def inject_fault(self, payload=None):
        payload = payload or {}
        sig_name = payload.get("signal", "SIGKILL")
        sig = getattr(signal, sig_name, signal.SIGKILL)
        self._logger.info("=== INJECT_FAULT START: KillNodeNemesis ===")
        cmd = (
            "ps aux | grep '\\--ic-port' | grep -v grep | awk '{ print $2 }' | shuf -n 1 | xargs -r sudo kill -%d"
            % (int(sig),)
        )
        self._logger.info("Executing: %s", cmd)
        subprocess.check_call(cmd, shell=True)
        self.on_success_inject_fault()
        self._logger.info("=== INJECT_FAULT SUCCESS: KillNodeNemesis ===")

    def extract_fault(self, payload=None):
        del payload
        self._logger.info("Extract noop (kill node has no reversible extract)")
        self.on_success_extract_fault()


NEMESIS_UI_GROUPS: dict[str, dict[str, str]] = {
    "NetworkNemesis": {
        "description": "Network fault injection",
    },
    "NodeNemesis": {
        "description": "Node process failures",
    },
}


NEMESIS_TYPES: dict[str, dict[str, Any]] = {
    NETWORK_NEMESIS: {
        "runner": NetworkNemesis(),
        "schedule": 200,
        "ui_group": "NetworkNemesis",
        "planner_cls": NetworkNemesisPlanner,
    },
    NODE_KILLER: {
        "runner": KillNodeNemesis(),
        "schedule": 300,
        "ui_group": "NodeNemesis",
        "planner_cls": KillNodeNemesisPlanner,
    },
}


def build_all_planners() -> dict[str, NemesisPlannerBase]:
    """Planner per registered type: planner_cls from registry or DefaultRandomHostPlanner."""
    merged: dict[str, NemesisPlannerBase] = {}
    for key, spec in NEMESIS_TYPES.items():
        cls: Type[NemesisPlannerBase] | None = spec.get("planner_cls")
        if cls is not None:
            merged[key] = cls()
        else:
            merged[key] = DefaultRandomHostPlanner(nemesis_type=key)
    return merged


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
