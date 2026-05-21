"""Master-side state and planning for NetworkNemesis (agent execution is stateless)."""

from __future__ import annotations

import logging
import random

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch, fanout
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster

# Payloads — keep in sync with NetworkNemesis actor (catalog)
PAYLOAD_INJECT = {"op": "isolate_node"}
PAYLOAD_EXTRACT = {"op": "clear_network_isolation"}
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class RollingRestartNemesisPlanner(NemesisPlannerBase):
    """Holds isolation set on the orchestrator; produces DispatchCommand lists for agents."""

    PAYLOAD_INJECT = PAYLOAD_INJECT
    PAYLOAD_EXTRACT = PAYLOAD_EXTRACT

    def __init__(self, nodes_per_step: int = 2, use_storage_nodes: bool = False, node_downtime_sec: int = 60) -> None:
        super().__init__()
        YdbCluster.ydb_endpoint = 'grpc://localhost:2135'
        storage_nodes = YdbCluster.get_cluster_nodes(role=YdbCluster.Node.Role.STORAGE)
        compute_nodes = YdbCluster.get_cluster_nodes(role=YdbCluster.Node.Role.COMPUTE)
        logger.info('storage_nodes: %s', storage_nodes)
        logger.info('compute_nodes: %s', compute_nodes)
        self._target_nodes = storage_nodes if use_storage_nodes else compute_nodes
        self._nodes_per_step = nodes_per_step
        self._use_storage_nodes = use_storage_nodes
        self._node_downtime_sec = node_downtime_sec
        self._affected_nodes = set()
        self.nemesis_type = "ClusterRollingRestartNemesis"

    def scheduled_tick(self, hosts: list[str]) -> list[DispatchCommand]:
        if not self._target_nodes:
            return []
        inject_targets: list[YdbCluster.Node] = []
        extract_targets: list[str] = []
        with self._lock:
            if len(self._affected_nodes) >= len(self._target_nodes):
                extract_targets = list({node.host for node in self._affected_nodes})
                self._affected_nodes.clear()
            else:
                avail = [n for n in self._target_nodes if n not in self._affected_nodes]
                if not avail:
                    return []
                k = min(self._nodes_per_step, len(avail))
                inject_targets = random.sample(avail, k)
                self._affected_nodes.update(inject_targets)

        if inject_targets:
            result = [
                dispatch(
                    self.nemesis_type,
                    node.host,
                    "inject",
                    {
                        'duration': self._node_downtime_sec,
                        'node_ic_port': node.ic_port,
                        **self.PAYLOAD_INJECT,
                    },
                )
                for node in inject_targets
            ]
            logger.info('rolling restart inject dispatch: %s', result)
            return result
        if not extract_targets:
            return []
        return fanout(self.nemesis_type, extract_targets, "extract", self.PAYLOAD_EXTRACT)

    def _find_node_by_host(self, host: str) -> "YdbCluster.Node | None":
        for node in self._target_nodes:
            if node.host == host:
                return node
        return None

    def _drain_tracked_hosts(self) -> list[str]:
        targets = list({node.host for node in self._affected_nodes})
        self._affected_nodes.clear()
        return targets

    def _register_inject(self, host: str) -> None:
        node = self._find_node_by_host(host)
        if node is not None:
            self._affected_nodes.add(node)

    def _register_extract(self, host: str) -> None:
        node = self._find_node_by_host(host)
        if node is not None:
            self._affected_nodes.discard(node)
