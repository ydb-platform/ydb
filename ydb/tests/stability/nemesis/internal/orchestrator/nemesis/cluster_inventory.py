"""ClusterInventory: host/node/slot entities for chaos planning.

Built from ``cluster.yaml`` topology plus optional ``ExternalKiKiMRCluster`` enrichment
(node_id, ic_port, slots). Fail-open: if cluster harness is unavailable, host-only
candidates are still produced.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import ClusterTopologyModel

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NodeInfo:
    node_id: int
    host: str
    ic_port: int | None
    rack: str | None
    datacenter: str | None
    bridge_pile_id: int | None = None


@dataclass(frozen=True)
class SlotInfo:
    slot_idx: int
    host: str
    ic_port: int | None
    node_id: int | None
    rack: str | None
    datacenter: str | None
    bridge_pile_id: int | None = None


class ClusterInventory:
    def __init__(
        self,
        topology: ClusterTopologyModel,
        *,
        agent_hosts: Iterable[str] | None = None,
    ) -> None:
        self._topology = topology
        self._agent_hosts = [h for h in (agent_hosts or []) if h]
        if not self._agent_hosts:
            self._agent_hosts = list(topology.hosts.keys())
        self.nodes: dict[int, NodeInfo] = {}
        self.slots: dict[int, SlotInfo] = {}
        self._load_from_harness()

    # -- loading ------------------------------------------------------------

    def _load_from_harness(self) -> None:
        try:
            from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import require_external_cluster

            cluster = require_external_cluster()
        except Exception as e:  # noqa: BLE001 - inventory must fail open
            logger.warning("ClusterInventory: harness unavailable (%s); host-only candidates", e)
            self._synthesize_nodes_from_hosts()
            return

        try:
            for node_id, node in cluster.nodes.items():
                host = str(node.host)
                self.nodes[int(node_id)] = NodeInfo(
                    node_id=int(node_id),
                    host=host,
                    ic_port=int(node.ic_port) if getattr(node, "ic_port", None) else None,
                    rack=getattr(node, "rack", None) or self._topology.rack_of(host),
                    datacenter=getattr(node, "datacenter", None) or self._topology.dc_of(host),
                    bridge_pile_id=getattr(node, "bridge_pile_id", None),
                )
            for slot_idx, slot in cluster.slots.items():
                host = str(slot.host)
                self.slots[int(slot_idx)] = SlotInfo(
                    slot_idx=int(slot_idx),
                    host=host,
                    ic_port=int(slot.ic_port) if getattr(slot, "ic_port", None) else None,
                    node_id=int(slot.node_id) if getattr(slot, "node_id", None) is not None else None,
                    rack=getattr(slot, "rack", None) or self._topology.rack_of(host),
                    datacenter=getattr(slot, "datacenter", None) or self._topology.dc_of(host),
                    bridge_pile_id=getattr(slot, "bridge_pile_id", None),
                )
        except Exception as e:  # noqa: BLE001
            logger.warning("ClusterInventory: failed to read nodes/slots (%s); host-only", e)
            self.nodes.clear()
            self.slots.clear()
            self._synthesize_nodes_from_hosts()

        if not self.nodes:
            self._synthesize_nodes_from_hosts()

        logger.info(
            "ClusterInventory: %d host(s), %d node(s), %d slot(s)",
            len(self._agent_hosts),
            len(self.nodes),
            len(self.slots),
        )

    def _synthesize_nodes_from_hosts(self) -> None:
        """One synthetic node per agent host when harness has no node map."""
        for i, host in enumerate(self._agent_hosts, start=1):
            if any(n.host == host for n in self.nodes.values()):
                continue
            self.nodes[i] = NodeInfo(
                node_id=i,
                host=host,
                ic_port=19001,
                rack=self._topology.rack_of(host),
                datacenter=self._topology.dc_of(host),
            )

    # -- lookups ------------------------------------------------------------

    @property
    def hosts(self) -> list[str]:
        return list(self._agent_hosts)

    def control_host(self) -> str | None:
        """Pinned first host for cluster-API tablet chaos."""
        return self._agent_hosts[0] if self._agent_hosts else None

    def nodes_on_host(self, host: str) -> list[NodeInfo]:
        return [n for n in self.nodes.values() if n.host == host]

    def slots_on_host(self, host: str) -> list[SlotInfo]:
        return [s for s in self.slots.values() if s.host == host]

    def node_by_id(self, node_id: int) -> NodeInfo | None:
        return self.nodes.get(node_id)

    def slot_by_idx(self, slot_idx: int) -> SlotInfo | None:
        return self.slots.get(slot_idx)

    # -- candidate generation -----------------------------------------------

    def entities(self, kind: TargetKind) -> list[ChaosTarget]:
        if kind is TargetKind.HOST:
            return [ChaosTarget.for_host(h) for h in self._agent_hosts]
        if kind is TargetKind.NODE:
            return [
                ChaosTarget.for_node(n.host, node_id=n.node_id, ic_port=n.ic_port)
                for n in self.nodes.values()
                if n.host in self._agent_hosts or not self._agent_hosts
            ]
        if kind is TargetKind.SLOT:
            return [
                ChaosTarget.for_slot(
                    s.host, slot_idx=s.slot_idx, ic_port=s.ic_port, node_id=s.node_id
                )
                for s in self.slots.values()
                if s.host in self._agent_hosts or not self._agent_hosts
            ]
        if kind is TargetKind.DISK:
            # Disk path chosen on agent; plan at node granularity.
            return [
                ChaosTarget.for_disk(n.host, node_id=n.node_id, ic_port=n.ic_port)
                for n in self.nodes.values()
                if n.host in self._agent_hosts or not self._agent_hosts
            ]
        if kind is TargetKind.TABLET:
            h = self.control_host()
            return [ChaosTarget.for_tablet(h)] if h else []
        if kind is TargetKind.DATACENTER:
            by_dc: dict[str, list[str]] = {}
            for h in self._agent_hosts:
                dc = self._topology.dc_of(h) or "__unknown__"
                by_dc.setdefault(dc, []).append(h)
            out: list[ChaosTarget] = []
            for dc, hosts in by_dc.items():
                for h in hosts:
                    out.append(ChaosTarget.for_datacenter(h, dc))
            return out
        if kind is TargetKind.PILE:
            by_pile: dict[str, list[str]] = {}
            for n in self.nodes.values():
                if n.bridge_pile_id is None:
                    continue
                by_pile.setdefault(str(n.bridge_pile_id), []).append(n.host)
            out = []
            for pile_id, hosts in by_pile.items():
                for h in sorted(set(hosts)):
                    out.append(ChaosTarget.for_pile(h, pile_id))
            return out
        return []


__all__ = [
    "NodeInfo",
    "SlotInfo",
    "ClusterInventory",
]
