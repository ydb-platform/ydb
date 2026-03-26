"""
Nemesis registration for the stability nemesis app.

Source of truth:
- ALL_NEMESIS_TYPES — every nemesis: agent runner + default schedule interval (keys = process types)
- NEMESIS_PLANNERS — optional custom master planners (subset of ALL_NEMESIS_TYPES keys)
  Types without an entry use DefaultRandomHostPlanner (one random host per scheduled tick).

ChaosMasterStore uses build_all_planners().
"""

from __future__ import annotations

import logging
import signal
import subprocess

from ydb.tests.tools.nemesis.library import base
from ydb.tests.library.nemesis.network.client import NetworkClient

from ydb.tests.stability.nemesis.internal.master.nemesis.kill_node_planner import (
    NODE_KILLER,
    KillNodeNemesisPlanner,
)
from ydb.tests.stability.nemesis.internal.master.nemesis.default_planner import DefaultRandomHostPlanner
from ydb.tests.stability.nemesis.internal.master.nemesis.network_planner import (
    NETWORK_NEMESIS,
    NetworkNemesisPlanner,
)
from ydb.tests.stability.nemesis.internal.master.nemesis.nemesis_planner_base import NemesisPlannerBase

# Dedicated logger for in-process nemesis runs; NemesisManager attaches a thread-local handler here (not on root).
NEMESIS_EXECUTION_LOGGER = "ydb.tests.stability.nemesis.execution"


class MonitoredAgentActor(base.AbstractMonitoredNemesis):
    """
    Agent-side execution only: AbstractMonitoredNemesis metrics.
    Planning is on the orchestrator (ChaosMasterStore).
    """

    def __init__(self, scope="node"):
        base.AbstractMonitoredNemesis.__init__(self, scope=scope)
        self._logger = logging.getLogger(NEMESIS_EXECUTION_LOGGER)

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


# Agent-side: runner instance + default schedule interval (seconds)
ALL_NEMESIS_TYPES = {
    NETWORK_NEMESIS: {
        "runner": NetworkNemesis(),
        "schedule": 200,
    },
    NODE_KILLER: {
        "runner": KillNodeNemesis(),
        "schedule": 300,
    },
}

# Master-side: only custom planners; other ALL_NEMESIS_TYPES keys get DefaultRandomHostPlanner
NEMESIS_PLANNERS = {
    NETWORK_NEMESIS: NetworkNemesisPlanner(),
    NODE_KILLER: KillNodeNemesisPlanner(),
}

if not set(NEMESIS_PLANNERS) <= set(ALL_NEMESIS_TYPES):
    raise RuntimeError("NEMESIS_PLANNERS keys must be a subset of ALL_NEMESIS_TYPES")


def build_all_planners() -> dict[str, NemesisPlannerBase]:
    """Full planner map: explicit entries from NEMESIS_PLANNERS, else default random host per tick."""
    merged: dict[str, NemesisPlannerBase] = {}
    for key in ALL_NEMESIS_TYPES:
        if key in NEMESIS_PLANNERS:
            merged[key] = NEMESIS_PLANNERS[key]
        else:
            merged[key] = DefaultRandomHostPlanner(nemesis_type=key)
    return merged


MASTER_MANAGED_TYPES = frozenset(ALL_NEMESIS_TYPES.keys())

PROCESS_TYPES = {**ALL_NEMESIS_TYPES}


def get_all_nemesis_types():
    return list(PROCESS_TYPES.keys())
