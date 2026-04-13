"""Fanout planners for datacenter and bridge-pile nemeses.

Each ``scheduled_tick`` reads the cluster topology from ``cluster.yaml``,
picks a datacenter / bridge pile, and fans out inject commands to **every**
host in that group.  The agent runner on each host then executes the fault
locally (no SSH).

Two concrete planners:

* **DataCenterFanoutPlanner** — cycles through datacenters.
* **BridgePileFanoutPlanner** — randomly picks a bridge pile.
"""

from __future__ import annotations

import collections
import random
import uuid
from typing import ClassVar

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch
from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import require_external_cluster
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase


# ---------------------------------------------------------------------------
# DataCenter fanout
# ---------------------------------------------------------------------------


class DataCenterFanoutPlanner(NemesisPlannerBase):
    """Fan out inject to **all hosts in a datacenter** (round-robin DC selection).

    On disable-schedule, extract is sent to the same set of hosts that received
    the last inject.
    """

    PAYLOAD_INJECT: ClassVar[dict] = {}
    PAYLOAD_EXTRACT: ClassVar[dict] = {}

    def __init__(self, nemesis_type_key: str) -> None:
        super().__init__()
        self._nemesis_type_key = nemesis_type_key
        self._last_hosts: list[str] = []
        self._dc_cycle_iter = None
        self._dc_list: list[str] = []

    @property
    def nemesis_type(self) -> str:  # type: ignore[override]
        return self._nemesis_type_key

    # -- topology helpers ---------------------------------------------------

    def _build_dc_map(self, cluster) -> dict[str, list[str]]:
        """datacenter → list of unique hostnames."""
        dc_to_hosts: dict[str, set[str]] = collections.defaultdict(set)
        for node in cluster.nodes.values():
            if node.datacenter is not None:
                dc_to_hosts[node.datacenter].add(node.host)
        for slot in cluster.slots.values():
            if getattr(slot, "datacenter", None) is not None:
                dc_to_hosts[slot.datacenter].add(slot.host)
        return {dc: sorted(hosts) for dc, hosts in dc_to_hosts.items()}

    def _ensure_dc_cycle(self, cluster):
        dc_map = self._build_dc_map(cluster)
        dc_list = sorted(dc_map.keys())
        if dc_list != self._dc_list:
            self._dc_list = dc_list
            self._dc_cycle_iter = None
        if self._dc_cycle_iter is None and self._dc_list:
            import itertools
            self._dc_cycle_iter = itertools.cycle(self._dc_list)
        return dc_map

    # -- planner interface --------------------------------------------------

    def scheduled_tick(self, hosts: list[str]) -> list[DispatchCommand]:
        if not hosts:
            return []
        cluster = require_external_cluster()
        if cluster is None:
            return []
        dc_map = self._ensure_dc_cycle(cluster)
        if not dc_map or self._dc_cycle_iter is None:
            return []
        dc = next(self._dc_cycle_iter)
        dc_hosts = dc_map.get(dc, [])
        # Only dispatch to hosts that are in the known agent host list
        target_hosts = [h for h in dc_hosts if h in hosts]
        if not target_hosts:
            return []
        with self._lock:
            self._last_hosts = list(target_hosts)
        scenario_id = str(uuid.uuid4())
        return [
            dispatch(
                self._nemesis_type_key,
                h,
                "inject",
                {"datacenter": dc},
                scenario_id=scenario_id,
            )
            for h in target_hosts
        ]

    def _drain_tracked_hosts(self) -> list[str]:
        out = list(self._last_hosts)
        self._last_hosts = []
        return out

    def _register_inject(self, host: str) -> None:
        if host not in self._last_hosts:
            self._last_hosts.append(host)

    def _register_extract(self, host: str) -> None:
        self._last_hosts = [h for h in self._last_hosts if h != host]


# ---------------------------------------------------------------------------
# Bridge-pile fanout
# ---------------------------------------------------------------------------


class BridgePileFanoutPlanner(NemesisPlannerBase):
    """Fan out inject to **all hosts in a bridge pile** (random pile selection).

    On disable-schedule, extract is sent to the same set of hosts that received
    the last inject.
    """

    PAYLOAD_INJECT: ClassVar[dict] = {}
    PAYLOAD_EXTRACT: ClassVar[dict] = {}

    def __init__(self, nemesis_type_key: str) -> None:
        super().__init__()
        self._nemesis_type_key = nemesis_type_key
        self._last_hosts: list[str] = []

    @property
    def nemesis_type(self) -> str:  # type: ignore[override]
        return self._nemesis_type_key

    # -- topology helpers ---------------------------------------------------

    def _build_pile_map(self, cluster) -> dict[int, list[str]]:
        """bridge_pile_id → list of unique hostnames."""
        pile_to_hosts: dict[int, set[str]] = collections.defaultdict(set)
        for node in cluster.nodes.values():
            pid = getattr(node, "bridge_pile_id", None)
            if pid is not None:
                pile_to_hosts[pid].add(node.host)
        for slot in cluster.slots.values():
            pid = getattr(slot, "bridge_pile_id", None)
            if pid is not None:
                pile_to_hosts[pid].add(slot.host)
        return {pid: sorted(hosts) for pid, hosts in pile_to_hosts.items()}

    # -- planner interface --------------------------------------------------

    def scheduled_tick(self, hosts: list[str]) -> list[DispatchCommand]:
        if not hosts:
            return []
        cluster = require_external_cluster()
        if cluster is None:
            return []
        pile_map = self._build_pile_map(cluster)
        if not pile_map:
            return []
        pile_id = random.choice(list(pile_map.keys()))
        pile_hosts = pile_map.get(pile_id, [])
        # Only dispatch to hosts that are in the known agent host list
        target_hosts = [h for h in pile_hosts if h in hosts]
        if not target_hosts:
            return []
        with self._lock:
            self._last_hosts = list(target_hosts)
        scenario_id = str(uuid.uuid4())
        return [
            dispatch(
                self._nemesis_type_key,
                h,
                "inject",
                {"bridge_pile_id": pile_id},
                scenario_id=scenario_id,
            )
            for h in target_hosts
        ]

    def _drain_tracked_hosts(self) -> list[str]:
        out = list(self._last_hosts)
        self._last_hosts = []
        return out

    def _register_inject(self, host: str) -> None:
        if host not in self._last_hosts:
            self._last_hosts.append(host)

    def _register_extract(self, host: str) -> None:
        self._last_hosts = [h for h in self._last_hosts if h != host]
