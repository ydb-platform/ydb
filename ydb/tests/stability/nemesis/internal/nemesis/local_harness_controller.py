"""Process control for an in-process KiKiMR test harness.

This adapter deliberately exposes only operations that are implemented by the
harness node objects. It must not discover processes or invoke host-level
commands.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NodeRestart:
    node_id: int
    previous_pid: int
    current_pid: int


class HarnessNodeController:
    """Narrow, fail-closed process controller for a local block-4-2 cluster."""

    def __init__(self, cluster: Any) -> None:
        self._cluster = cluster

    def kill_and_restart(self, node_id: int) -> NodeRestart:
        self._require_safe_node_fault_cluster()
        node_id = int(node_id)
        node = self._cluster.nodes.get(node_id)
        if node is None:
            raise ValueError(f"Unknown local harness node_id={node_id}")

        previous_pid = int(node.pid)
        node.kill()
        current_pid = int(node.pid)
        if not node.is_alive():
            raise RuntimeError(
                f"Local harness node_id={node_id} did not restart after kill"
            )
        if current_pid == previous_pid:
            raise RuntimeError(
                f"Local harness node_id={node_id} retained pid={current_pid} after kill"
            )
        return NodeRestart(
            node_id=node_id,
            previous_pid=previous_pid,
            current_pid=current_pid,
        )

    def _require_safe_node_fault_cluster(self) -> None:
        nodes = getattr(self._cluster, "nodes", {})
        erasure = str(
            getattr(getattr(self._cluster, "config", None), "static_erasure", "")
        ).lower()
        if erasure != "block-4-2" or len(nodes) < 8:
            raise RuntimeError(
                "Local harness node faults require at least 8 block-4-2 nodes; "
                f"got erasure={erasure or 'unknown'}, nodes={len(nodes)}"
            )


__all__ = [
    "HarnessNodeController",
    "NodeRestart",
]
