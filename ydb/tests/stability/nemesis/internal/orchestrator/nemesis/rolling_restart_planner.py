"""Master-side state and planning for ClusterRollingRestartNemesis.

The planner runs on the orchestrator and decides which cluster nodes should be
restarted on every scheduled tick. The actual restart is performed by the
agent-side runner ``ClusterRollingRestartNemesis``; this module only emits
``DispatchCommand``s.
"""

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
    """Orchestrator-side planner for rolling restart of cluster nodes.

    Strategy:
        On every scheduled tick the planner picks up to ``nodes_per_step``
        nodes from ``_target_nodes`` (storage or compute, depending on
        ``use_storage_nodes``) that have not been restarted yet in the current
        round, and emits one ``inject`` ``DispatchCommand`` per picked node.

        Each command carries ``node_ic_port`` and ``duration`` (=node_downtime_sec)
        so the agent runner can stop the corresponding systemd unit, sleep for
        ``duration`` seconds and start it back.

        Once every node in ``_target_nodes`` has been restarted once, the next
        tick emits ``extract`` commands and clears ``_affected_nodes``, starting
        a new round.

    State:
        ``_target_nodes``      — full list of candidate nodes resolved at init.
        ``_affected_nodes``    — nodes already picked in the current round.
        ``_nodes_per_step``    — maximum nodes restarted per tick.
        ``_node_downtime_sec`` — how long the agent keeps each node down.

    Tracking helpers (``_register_inject``/``_register_extract``/
    ``_drain_tracked_hosts``) keep the same ``_affected_nodes`` set in sync
    with manual ``inject``/``extract`` actions issued from the UI, so that
    scheduled and manual flows share state.

    On every tick the planner logs three sets: ``already_affected``,
    ``available_for_pick`` and ``picked_this_tick`` — useful for diagnosing
    "why does it skip this node" / "why no dispatch this tick".
    """

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
