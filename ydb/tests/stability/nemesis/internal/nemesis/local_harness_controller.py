"""Process control for an in-process KiKiMR test harness.

This adapter deliberately exposes only operations that are implemented by the
harness node objects. It must not discover processes or invoke host-level
commands.
"""

from __future__ import annotations

import socket
import threading
from dataclasses import dataclass
from typing import Any

from ydb.tests.library.harness import daemon


@dataclass(frozen=True)
class NodeRestart:
    node_id: int
    previous_pid: int
    current_pid: int


@dataclass(frozen=True)
class NodeStop:
    node_id: int
    previous_pid: int
    held_ports: tuple[int, ...]
    previous_log_file: str | None


@dataclass(frozen=True)
class NodeStart:
    node_id: int
    previous_pid: int
    current_pid: int
    log_file: str | None


@dataclass(frozen=True)
class NodeFreeze:
    node_id: int
    pid: int


@dataclass
class _StoppedNode:
    result: NodeStop
    sockets: dict[int, socket.socket]


class HarnessNodeController:
    """Narrow, fail-closed process controller for a local block-4-2 cluster."""

    def __init__(self, cluster: Any) -> None:
        self._cluster = cluster
        self._lock = threading.RLock()
        self._stopped: dict[int, _StoppedNode] = {}
        self._frozen: dict[int, NodeFreeze] = {}
        self._incarnations: dict[int, int] = {}

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

    def stop(self, node_id: int) -> NodeStop:
        """Kill one harness process without auto-restart and reserve all its ports."""
        self._require_safe_node_fault_cluster()
        node_id = int(node_id)
        with self._lock:
            stopped = self._stopped.get(node_id)
            if stopped is not None:
                return stopped.result
            node = self._node(node_id)
            if not node.is_alive():
                raise RuntimeError(
                    f"Local harness node_id={node_id} is not alive before stop"
                )
            previous_pid = int(node.pid)
            previous_log = getattr(node, "ydbd_log_file_path", None)
            self._kill_without_restart(node)
            if node.is_alive():
                raise RuntimeError(
                    f"Local harness node_id={node_id} remained alive after stop"
                )
            try:
                sockets = self._hold_node_ports(node)
            except Exception:
                node.start()
                raise
            result = NodeStop(
                node_id=node_id,
                previous_pid=previous_pid,
                held_ports=tuple(sorted(sockets)),
                previous_log_file=previous_log,
            )
            self._stopped[node_id] = _StoppedNode(result=result, sockets=sockets)
            return result

    def start(self, node_id: int) -> NodeStart | None:
        """Restart a node stopped by this controller; repeated calls are harmless."""
        self._require_safe_node_fault_cluster()
        node_id = int(node_id)
        with self._lock:
            stopped = self._stopped.get(node_id)
            if stopped is None:
                return None
            node = self._node(node_id)
            self._close_sockets(stopped.sockets)
            stopped.sockets = {}
            incarnation = self._incarnations.get(node_id, 0) + 1
            self._incarnations[node_id] = incarnation
            node.set_log_file_prefix(
                f"local_nemesis_node_{node_id}_{incarnation:03d}_"
            )
            try:
                node.start()
            except Exception:
                if not node.is_alive():
                    stopped.sockets = self._hold_node_ports(node)
                raise
            current_pid = int(node.pid)
            if not node.is_alive():
                raise RuntimeError(
                    f"Local harness node_id={node_id} did not start"
                )
            if current_pid == stopped.result.previous_pid:
                raise RuntimeError(
                    f"Local harness node_id={node_id} retained pid={current_pid} after start"
                )
            self._stopped.pop(node_id, None)
            return NodeStart(
                node_id=node_id,
                previous_pid=stopped.result.previous_pid,
                current_pid=current_pid,
                log_file=getattr(node, "ydbd_log_file_path", None),
            )

    def stopped_node_ids(self) -> tuple[int, ...]:
        with self._lock:
            return tuple(sorted(self._stopped))

    def freeze(self, node_id: int) -> NodeFreeze:
        self._require_safe_node_fault_cluster()
        node_id = int(node_id)
        with self._lock:
            frozen = self._frozen.get(node_id)
            if frozen is not None:
                return frozen
            node = self._node(node_id)
            if not node.is_alive():
                raise RuntimeError(
                    f"Local harness node_id={node_id} is not alive before freeze"
                )
            frozen = NodeFreeze(node_id=node_id, pid=int(node.pid))
            node.freeze()
            self._frozen[node_id] = frozen
            return frozen

    def unfreeze(self, node_id: int) -> NodeFreeze | None:
        self._require_safe_node_fault_cluster()
        node_id = int(node_id)
        with self._lock:
            frozen = self._frozen.get(node_id)
            if frozen is None:
                return None
            node = self._node(node_id)
            if int(node.pid) != frozen.pid:
                raise RuntimeError(
                    f"Frozen local harness node_id={node_id} changed pid "
                    f"from {frozen.pid} to {node.pid}"
                )
            node.unfreeze()
            self._frozen.pop(node_id, None)
            return frozen

    def frozen_node_ids(self) -> tuple[int, ...]:
        with self._lock:
            return tuple(sorted(self._frozen))

    def held_ports(self, node_id: int) -> tuple[int, ...]:
        with self._lock:
            stopped = self._stopped.get(int(node_id))
            return tuple(sorted(stopped.sockets)) if stopped is not None else ()

    def restore_all(self) -> None:
        errors = []
        for node_id in self.frozen_node_ids():
            try:
                self.unfreeze(node_id)
            except Exception as error:
                errors.append((node_id, error))
        for node_id in self.stopped_node_ids():
            try:
                self.start(node_id)
            except Exception as error:
                errors.append((node_id, error))
        if errors:
            details = ", ".join(
                f"node-{node_id}: {error}" for node_id, error in errors
            )
            raise RuntimeError(f"Failed to restore local harness nodes: {details}")

    @staticmethod
    def _kill_without_restart(node: Any) -> None:
        daemon.Daemon.kill(node)

    @staticmethod
    def _hold_node_ports(node: Any) -> dict[int, socket.socket]:
        ports = {
            int(port)
            for port in (
                getattr(node, "ic_port", None),
                getattr(node, "grpc_port", None),
                getattr(node, "mon_port", None),
                getattr(node, "grpc_ssl_port", None),
            )
            if port is not None
        }
        if not ports:
            raise RuntimeError("Local harness node exposes no ports to reserve")
        sockets: dict[int, socket.socket] = {}
        try:
            for port in sorted(ports):
                sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
                sock.bind(("::", port))
                sock.listen(1)
                sockets[port] = sock
        except Exception:
            HarnessNodeController._close_sockets(sockets)
            raise
        return sockets

    @staticmethod
    def _close_sockets(sockets: dict[int, socket.socket]) -> None:
        for sock in sockets.values():
            sock.close()

    def _node(self, node_id: int) -> Any:
        node = self._cluster.nodes.get(node_id)
        if node is None:
            raise ValueError(f"Unknown local harness node_id={node_id}")
        return node

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
    "NodeFreeze",
    "NodeRestart",
    "NodeStart",
    "NodeStop",
]
