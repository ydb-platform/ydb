"""Run one local nemesis action in a process that does not own the cluster."""

from __future__ import annotations

import json
import os
from pathlib import Path

from ydb.tests.stability.nemesis.internal.nemesis.catalog import NEMESIS_TYPES
from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import (
    LOCAL_HARNESS_NEMESIS_TYPES,
    use_cluster,
)
from ydb.tests.stability.nemesis.internal.nemesis.local_control_proxy import (
    HarnessControlClient,
    RemoteHarnessCluster,
    use_local_node_controller,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import (
    ChaosTarget,
)


def run_local_action(
    manifest_path: str | os.PathLike[str],
    nemesis_type: str,
    action: str,
    *,
    node_id: int | None = None,
    slot_idx: int | None = None,
) -> dict:
    if nemesis_type not in LOCAL_HARNESS_NEMESIS_TYPES:
        raise RuntimeError(f"{nemesis_type} is not enabled for local harness execution")
    if action not in ("inject", "extract"):
        raise RuntimeError(f"Unsupported local action: {action}")
    if nemesis_type not in NEMESIS_TYPES:
        raise RuntimeError(f"Unknown nemesis type: {nemesis_type}")

    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    control = HarnessControlClient(manifest["control_socket"])
    cluster = RemoteHarnessCluster(manifest, control)
    payload = _payload(cluster, node_id=node_id, slot_idx=slot_idx, nemesis_type=nemesis_type)
    runner = NEMESIS_TYPES[nemesis_type]["runner"]
    with use_cluster(cluster), use_local_node_controller(control):
        if action == "inject":
            runner.inject_fault(payload)
        else:
            runner.extract_fault(payload)
    return {"ok": True, "pid": os.getpid(), "type": nemesis_type, "action": action}


def _payload(cluster: RemoteHarnessCluster, *, node_id, slot_idx, nemesis_type: str) -> dict:
    if slot_idx is not None:
        slot = cluster.slots.get(int(slot_idx))
        if slot is None:
            raise ValueError(f"Manifest has no slot_idx={slot_idx}")
        target = ChaosTarget.for_slot(
            slot.logical_host,
            slot_idx=slot.slot_idx,
            node_id=slot.node_id,
            ic_port=slot.ic_port,
        )
    elif node_id is not None:
        node = cluster.nodes.get(int(node_id))
        if node is None:
            raise ValueError(f"Manifest has no node_id={node_id}")
        if nemesis_type == "SafelyBreakDiskNemesis":
            target = ChaosTarget.for_disk(
                node.logical_host,
                node_id=node.node_id,
                ic_port=node.ic_port,
            )
        else:
            target = ChaosTarget.for_node(
                node.logical_host,
                node_id=node.node_id,
                ic_port=node.ic_port,
            )
    else:
        raise ValueError("local action requires --node-id or --slot-idx")
    payload = {"chaos_target": target.to_dict()}
    if node_id is not None:
        payload["node_id"] = int(node_id)
    if slot_idx is not None:
        payload["slot_idx"] = int(slot_idx)
    return payload
