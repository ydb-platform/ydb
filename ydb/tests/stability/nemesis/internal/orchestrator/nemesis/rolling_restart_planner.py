"""Orchestrator-side planner for ClusterRollingRestartNemesis: picks nodes to restart each tick; agent does the actual restart."""

from __future__ import annotations

import logging
import random

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch, fanout
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class RollingRestartNemesisPlanner(NemesisPlannerBase):
    """Rolling restart planner: each tick restarts up to ``nodes_per_step`` not-yet-restarted nodes;
    when all target nodes have been restarted once, the round resets.

    Params: ``nodes_per_step``, ``use_storage_nodes`` (storage vs compute), ``node_downtime_sec``.

    Agent-side runner ``ClusterRollingRestartNemesis.inject_fault`` requires
    ``node_ic_port`` + ``duration`` in the payload, so this planner builds the
    payload itself and overrides ``manual``/``extract_all_on_disable`` instead
    of relying on the base class' constant ``PAYLOAD_INJECT``/``PAYLOAD_EXTRACT``.
    """

    # Empty placeholders — actual payload is built per node in scheduled_tick / manual.
    PAYLOAD_INJECT: dict = {}
    PAYLOAD_EXTRACT: dict = {}

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
        already_affected_hosts: list[str] = []
        avail_hosts: list[str] = []
        with self._lock:
            already_affected_hosts = sorted({n.host for n in self._affected_nodes})
            if len(self._affected_nodes) >= len(self._target_nodes):
                extract_targets = list({node.host for node in self._affected_nodes})
                self._affected_nodes.clear()
                logger.info(
                    "rolling restart tick: all %d target nodes already affected -> extract: %s",
                    len(self._target_nodes), extract_targets,
                )
            else:
                avail = [n for n in self._target_nodes if n not in self._affected_nodes]
                avail_hosts = sorted({n.host for n in avail})
                if not avail:
                    logger.info(
                        "rolling restart tick: no available nodes (already affected=%s)",
                        already_affected_hosts,
                    )
                    return []
                k = min(self._nodes_per_step, len(avail))
                inject_targets = random.sample(avail, k)
                self._affected_nodes.update(inject_targets)
                logger.info(
                    "rolling restart tick: already_affected=%s; available_for_pick=%s; picked_this_tick=%s",
                    already_affected_hosts,
                    avail_hosts,
                    sorted({n.host for n in inject_targets}),
                )

        if inject_targets:
            result = [
                dispatch(
                    self.nemesis_type,
                    node.host,
                    "inject",
                    self._build_inject_payload(node),
                )
                for node in inject_targets
            ]
            logger.info('rolling restart inject dispatch: %s', result)
            return result
        if not extract_targets:
            return []
        return fanout(self.nemesis_type, extract_targets, "extract", {})

    def _build_inject_payload(self, node: "YdbCluster.Node") -> dict:
        return {
            'duration': self._node_downtime_sec,
            'node_ic_port': node.ic_port,
        }

    def manual(self, host: str, action: str) -> list[DispatchCommand] | None:
        """Disabled: planner relies on cross-tick state."""
        return None

    def _find_node_by_host(self, host: str) -> "YdbCluster.Node | None":
        for node in self._target_nodes:
            if node.host == host:
                return node
        return None

    def _drain_tracked_hosts(self) -> list[str]:
        targets = list({node.host for node in self._affected_nodes})
        self._affected_nodes.clear()
        return targets
