"""Immutable identity and endpoint view of a live local ``KiKiMR`` cluster."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LocalNodeView:
    node_id: int
    logical_host: str
    connect_host: str
    grpc_port: int
    mon_port: int
    ic_port: int
    rack: str
    datacenter: str | None


@dataclass(frozen=True)
class LocalSlotView:
    slot_idx: int
    node_id: int
    logical_host: str
    connect_host: str
    grpc_port: int
    mon_port: int
    ic_port: int


@dataclass(frozen=True)
class LocalClusterView:
    nodes: dict[int, LocalNodeView]
    static_erasure: str
    slots: dict[int, LocalSlotView]

    @classmethod
    def from_cluster(cls, cluster: Any) -> "LocalClusterView":
        nameservice = cluster.config.yaml_config.get("nameservice_config", {})
        locations = {
            int(entry["node_id"]): entry.get("walle_location", {})
            for entry in nameservice.get("node", [])
        }

        nodes = {}
        for node_id, node in cluster.nodes.items():
            node_id = int(node_id)
            location = locations.get(node_id, {})
            rack = location.get("rack")
            if rack is None:
                rack = f"node-{node_id}"
            datacenter = location.get("data_center")
            nodes[node_id] = LocalNodeView(
                node_id=node_id,
                logical_host=f"node-{node_id}",
                connect_host=str(node.host),
                grpc_port=int(node.grpc_port),
                mon_port=int(node.mon_port),
                ic_port=int(node.ic_port),
                rack=str(rack),
                datacenter=str(datacenter) if datacenter is not None else None,
            )

        slots = {}
        for slot_idx, slot in getattr(cluster, "slots", {}).items():
            slot_idx = int(slot_idx)
            slots[slot_idx] = LocalSlotView(
                slot_idx=slot_idx,
                node_id=int(slot.node_id),
                logical_host=f"slot-{slot_idx}",
                connect_host=str(slot.host),
                grpc_port=int(slot.grpc_port),
                mon_port=int(slot.mon_port),
                ic_port=int(slot.ic_port),
            )

        return cls(
            nodes=nodes,
            static_erasure=str(cluster.config.static_erasure),
            slots=slots,
        )

    def topology_document(self) -> dict:
        hosts = []
        for node in self.nodes.values():
            location = {"rack": node.rack}
            if node.datacenter is not None:
                location["data_center"] = node.datacenter
            hosts.append({
                "name": node.logical_host,
                "location": location,
            })
        return {
            "static_erasure": self.static_erasure,
            "hosts": hosts,
        }
