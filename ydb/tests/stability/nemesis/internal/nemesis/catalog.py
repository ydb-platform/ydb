"""
Nemesis registration for the stability nemesis app.

Single registry:
- NEMESIS_TYPES — each type: runner, schedule, ui_group, optional planner_cls or planner_factory(key).
  If both are omitted, build_all_planners() uses DefaultRandomHostPlanner.

- NEMESIS_UI_GROUPS — group id -> description for /api/process_types/grouped.

ChaosOrchestratorStore receives a planner map from build_all_planners().

Optional cluster topology nemeses (datacenter / bridge pile) are merged from
``cluster_chaos.registry`` only when ``cluster.yaml`` (``YAML_CONFIG_LOCATION`` /
``Settings.yaml_config_location``) contains the corresponding sections; see
``yaml_nemesis_gates.py``.
"""

from __future__ import annotations

import signal
import subprocess
from typing import Any, Type

from ydb.tests.stability.nemesis.internal.nemesis.cluster_chaos.registry import cluster_nemesis_type_entries
from ydb.tests.stability.nemesis.internal.nemesis.local_network import LocalNetworkClient
from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import (
    MonitoredAgentActor,
    NEMESIS_EXECUTION_LOGGER,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.kill_node_planner import (
    NODE_KILLER,
    KillNodeNemesisPlanner,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.default_planner import DefaultRandomHostPlanner
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.network_planner import (
    DNS_NEMESIS,
    DnsNemesisPlanner,
    NETWORK_NEMESIS,
    NetworkNemesisPlanner,
    TIME_SKEW_NEMESIS,
    TimeSkewNemesisPlanner,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase


class NetworkNemesis(MonitoredAgentActor):
    """Isolates localhost from the network or restores connectivity (payload is unused)."""

    def inject_fault(self, payload=None):
        del payload
        self._logger.info("=== INJECT_FAULT START: NetworkNemesis ===")
        client = LocalNetworkClient(port=19001)
        self._logger.info("Isolating node...")
        client.isolate_node()
        self.on_success_inject_fault()
        self._logger.info("=== INJECT_FAULT SUCCESS: NetworkNemesis ===")

    def extract_fault(self, payload=None):
        del payload
        self._logger.info("Extracting fault (network)")
        client = LocalNetworkClient(port=19001)
        self._logger.info("Restoring node...")
        client.clear_all_drops()
        self.on_success_extract_fault()


class DnsNemesis(MonitoredAgentActor):
    """iptables DNS isolation on localhost (same idea as ``ydb.tests.library.nemesis.nemesis_network.DnsNemesis``)."""

    def inject_fault(self, payload=None):
        del payload
        self._logger.info("=== INJECT_FAULT START: DnsNemesis ===")
        client = LocalNetworkClient(port=19001)
        client.isolate_dns()
        self.on_success_inject_fault()
        self._logger.info("=== INJECT_FAULT SUCCESS: DnsNemesis ===")

    def extract_fault(self, payload=None):
        del payload
        self._logger.info("Extracting DNS isolation")
        client = LocalNetworkClient(port=19001)
        client.clear_all_drops()
        self.on_success_extract_fault()


class TimeSkewNemesis(MonitoredAgentActor):
    """Step local system time forward; extract re-enables NTP sync (best-effort)."""

    def inject_fault(self, payload=None):
        payload = payload or {}
        delta = int(payload.get("delta_sec", 300))
        self._logger.info("=== INJECT_FAULT START: TimeSkewNemesis delta_sec=%s ===", delta)
        cmd = (
            "sudo timedatectl set-ntp false 2>/dev/null; "
            f"sudo date -s @$(($(date +%s)+{delta}))"
        )
        subprocess.run(cmd, shell=True, check=False)
        self.on_success_inject_fault()

    def extract_fault(self, payload=None):
        del payload
        self._logger.info("Restoring time sync (TimeSkewNemesis)")
        subprocess.run(
            "sudo timedatectl set-ntp true 2>/dev/null; "
            "(command -v chronyc >/dev/null && sudo chronyc -a makestep 2>/dev/null) || true",
            shell=True,
            check=False,
        )
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
    # DNS_NEMESIS: {
    #     "runner": DnsNemesis(),
    #     "schedule": 120,
    #     "ui_group": "NetworkNemesis",
    #     "planner_cls": DnsNemesisPlanner,
    # },
    TIME_SKEW_NEMESIS: {
        "runner": TimeSkewNemesis(),
        "schedule": 400,
        "ui_group": "NetworkNemesis",
        "planner_cls": TimeSkewNemesisPlanner,
    },
    **cluster_nemesis_type_entries(),
}


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
