# Adapted from ydb/tests/tools/nemesis/library/tablet.py (KickTabletsFromNode, ReBalanceTabletsNemesis).

from __future__ import annotations

from ydb.tests.library.clients.kikimr_http_client import HiveClient

from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import require_external_cluster
from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor
from ydb.tests.stability.nemesis.internal.nemesis.runners.target_payload import target_from_payload


class ClusterKickTabletsFromNodeNemesis(MonitoredAgentActor):
    """block_node → kick_tablets_from_node → unblock_node on ChaosTarget.node_id."""

    def __init__(self) -> None:
        super().__init__(scope="tablets")
        self._hive: HiveClient | None = None

    def _hive_client(self) -> HiveClient:
        if self._hive is None:
            cluster = require_external_cluster()
            n = cluster.nodes[1]
            self._hive = HiveClient(n.host, n.mon_port)
        return self._hive

    def inject_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        cluster = require_external_cluster()
        target = target_from_payload(payload)
        if target is None or target.node_id is None:
            self._logger.error("KickTabletsFromNode: ChaosTarget.node_id is required")
            return
        node = cluster.nodes.get(target.node_id)
        if node is None:
            self._logger.error("KickTabletsFromNode: unknown node_id=%s", target.node_id)
            return
        try:
            h = self._hive_client()
            h.block_node(node.node_id)
            h.kick_tablets_from_node(node.node_id)
            h.unblock_node(node.node_id)
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("KickTabletsFromNode failed: %s", e)
            raise

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterReBalanceTabletsNemesis(MonitoredAgentActor):
    """Hive rebalance_all_tablets."""

    def __init__(self) -> None:
        super().__init__(scope="tablets")
        self._hive: HiveClient | None = None

    def _hive_client(self) -> HiveClient:
        if self._hive is None:
            cluster = require_external_cluster()
            n = cluster.nodes[1]
            self._hive = HiveClient(n.host, n.mon_port)
        return self._hive

    def inject_fault(self, payload=None) -> None:
        del payload
        try:
            self._hive_client().rebalance_all_tablets()
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("rebalance_all_tablets failed: %s", e)
            raise

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()
