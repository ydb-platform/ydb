"""Owner-only Unix-socket transport for narrow local harness capabilities."""

from __future__ import annotations

import json
import os
import socket
import socketserver
import stat
import threading
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator

from ydb.tests.library.common.msgbus_types import EDriveStatus
from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import (
    require_cluster,
)
from ydb.tests.stability.nemesis.internal.nemesis.local_harness_controller import (
    HarnessNodeController,
    NodeFreeze,
    NodeRestart,
    NodeStart,
    NodeStop,
    SlotRestart,
)


_PROTOCOL_VERSION = 1
_MAX_MESSAGE_BYTES = 1024 * 1024
_OPERATIONS = {
    "kill_and_restart": frozenset({"node_id"}),
    "stop": frozenset({"node_id"}),
    "start": frozenset({"node_id"}),
    "freeze": frozenset({"node_id"}),
    "unfreeze": frozenset({"node_id"}),
    "kill_slot_and_restart": frozenset({"slot_idx"}),
    "held_ports": frozenset({"node_id"}),
    "stopped_node_ids": frozenset(),
    "frozen_node_ids": frozenset(),
    "restore_all": frozenset(),
    "read_drive_status": frozenset({"node_id"}),
    "update_drive_status": frozenset({"node_id", "path", "status"}),
    "query_base_config": frozenset(),
}
_controller_override: Any | None = None
_override_lock = threading.RLock()


class _ControlRequestHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        raw = self.rfile.readline(_MAX_MESSAGE_BYTES + 1)
        if not raw or len(raw) > _MAX_MESSAGE_BYTES or not raw.endswith(b"\n"):
            self._reply(error="invalid request framing")
            return
        try:
            request = json.loads(raw)
            self._validate_request(request)
            result = self._dispatch(request)
            self._reply(result=_jsonable(result))
        except Exception as error:
            self._reply(error=str(error))

    def _validate_request(self, request: Any) -> None:
        if not isinstance(request, dict):
            raise ValueError("request must be an object")
        operation = request.get("operation")
        fields = _OPERATIONS.get(operation)
        if fields is None:
            raise ValueError("operation is not allowed")
        if set(request) != {"version", "operation", *fields}:
            raise ValueError("request fields are not allowed")
        if request.get("version") != _PROTOCOL_VERSION:
            raise ValueError("unsupported protocol version")

    def _dispatch(self, request: dict) -> Any:
        operation = request["operation"]
        controller = self.server.controller
        if operation == "kill_and_restart":
            return controller.kill_and_restart(_integer(request["node_id"]))
        if operation == "stop":
            return controller.stop(_integer(request["node_id"]))
        if operation == "start":
            return controller.start(_integer(request["node_id"]))
        if operation == "freeze":
            return controller.freeze(_integer(request["node_id"]))
        if operation == "unfreeze":
            return controller.unfreeze(_integer(request["node_id"]))
        if operation == "kill_slot_and_restart":
            return controller.kill_slot_and_restart(_integer(request["slot_idx"]))
        if operation == "held_ports":
            return list(controller.held_ports(_integer(request["node_id"])))
        if operation == "stopped_node_ids":
            return list(controller.stopped_node_ids())
        if operation == "frozen_node_ids":
            return list(controller.frozen_node_ids())
        if operation == "restore_all":
            controller.restore_all()
            return None
        if operation == "read_drive_status":
            return _read_drive_status(self._cluster(), _integer(request["node_id"]))
        if operation == "update_drive_status":
            return _update_drive_status(
                self._cluster(),
                _integer(request["node_id"]),
                str(request["path"]),
                _integer(request["status"]),
            )
        if operation == "query_base_config":
            return _query_base_config(self._cluster())
        raise ValueError("operation is not allowed")

    def _cluster(self) -> Any:
        cluster = self.server.cluster
        if cluster is None:
            raise RuntimeError("local control proxy has no cluster for storage operations")
        return cluster

    def _reply(self, *, result=None, error: str | None = None) -> None:
        response = {"ok": error is None}
        if error is None:
            response["result"] = result
        else:
            response["error"] = error
        self.wfile.write(
            json.dumps(response, separators=(",", ":")).encode("utf-8") + b"\n"
        )


class _UnixControlServer(socketserver.UnixStreamServer):
    def __init__(self, path: str, controller: Any, cluster: Any | None) -> None:
        self.controller = controller
        self.cluster = cluster
        super().__init__(path, _ControlRequestHandler)
        os.chmod(path, 0o600)


class HarnessControlProxy:
    """Serve one allowlisted controller operation on an owner-only Unix socket."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        controller: Any,
        cluster: Any | None = None,
    ) -> None:
        self.path = Path(path)
        self._server = _UnixControlServer(str(self.path), controller, cluster)
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("local control proxy is already started")
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="local-harness-control-proxy",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        thread = self._thread
        if thread is not None:
            self._server.shutdown()
            thread.join(timeout=5)
            if thread.is_alive():
                raise RuntimeError("local control proxy did not stop")
            self._thread = None
        self._server.server_close()
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass

    def __enter__(self) -> "HarnessControlProxy":
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        del exc_type, exc_value, traceback
        self.stop()


class HarnessControlClient:
    """Socket client for the allowlisted local harness operations."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        timeout: float = 120,
    ) -> None:
        self.path = Path(path)
        self.timeout = float(timeout)

    def kill_and_restart(self, node_id: int) -> NodeRestart:
        result = self._call("kill_and_restart", node_id=int(node_id))
        return NodeRestart(
            node_id=int(result["node_id"]),
            previous_pid=int(result["previous_pid"]),
            current_pid=int(result["current_pid"]),
        )

    def stop(self, node_id: int) -> NodeStop:
        result = self._call("stop", node_id=int(node_id))
        return NodeStop(
            node_id=int(result["node_id"]),
            previous_pid=int(result["previous_pid"]),
            held_ports=tuple(result["held_ports"]),
            previous_log_file=result["previous_log_file"],
        )

    def start(self, node_id: int) -> NodeStart | None:
        result = self._call("start", node_id=int(node_id))
        if result is None:
            return None
        return NodeStart(
            node_id=int(result["node_id"]),
            previous_pid=int(result["previous_pid"]),
            current_pid=int(result["current_pid"]),
            log_file=result["log_file"],
        )

    def freeze(self, node_id: int) -> NodeFreeze:
        result = self._call("freeze", node_id=int(node_id))
        return NodeFreeze(node_id=int(result["node_id"]), pid=int(result["pid"]))

    def unfreeze(self, node_id: int) -> NodeFreeze | None:
        result = self._call("unfreeze", node_id=int(node_id))
        if result is None:
            return None
        return NodeFreeze(node_id=int(result["node_id"]), pid=int(result["pid"]))

    def kill_slot_and_restart(self, slot_idx: int) -> SlotRestart:
        result = self._call("kill_slot_and_restart", slot_idx=int(slot_idx))
        return SlotRestart(
            slot_idx=int(result["slot_idx"]),
            node_id=int(result["node_id"]),
            previous_pid=int(result["previous_pid"]),
            current_pid=int(result["current_pid"]),
        )

    def held_ports(self, node_id: int) -> tuple[int, ...]:
        return tuple(self._call("held_ports", node_id=int(node_id)))

    def stopped_node_ids(self) -> tuple[int, ...]:
        return tuple(self._call("stopped_node_ids"))

    def frozen_node_ids(self) -> tuple[int, ...]:
        return tuple(self._call("frozen_node_ids"))

    def restore_all(self) -> None:
        self._call("restore_all")

    def read_drive_status(self, node_id: int) -> dict:
        return self._call("read_drive_status", node_id=int(node_id))

    def update_drive_status(self, node_id: int, path: str, status: int) -> dict:
        return self._call(
            "update_drive_status",
            node_id=int(node_id),
            path=str(path),
            status=int(status),
        )

    def query_base_config(self) -> dict:
        return self._call("query_base_config")

    def _call(self, operation: str, **fields: Any) -> Any:
        self._validate_socket()
        request = {"version": _PROTOCOL_VERSION, "operation": operation, **fields}
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(self.timeout)
            client.connect(str(self.path))
            client.sendall(json.dumps(request, separators=(",", ":")).encode("utf-8") + b"\n")
            with client.makefile("rb") as response_file:
                raw = response_file.readline(_MAX_MESSAGE_BYTES + 1)
        if not raw or len(raw) > _MAX_MESSAGE_BYTES:
            raise RuntimeError("local control proxy returned an invalid response")
        response = json.loads(raw)
        if not isinstance(response, dict) or response.get("ok") is not True:
            error = response.get("error") if isinstance(response, dict) else None
            raise RuntimeError(f"local control proxy rejected request: {error}")
        return response.get("result")

    def _validate_socket(self) -> None:
        info = self.path.stat()
        if not stat.S_ISSOCK(info.st_mode):
            raise RuntimeError(f"Local control path is not a Unix socket: {self.path}")
        if info.st_uid != os.geteuid():
            raise RuntimeError(f"Local control socket is not owned by this user: {self.path}")
        if stat.S_IMODE(info.st_mode) & 0o077:
            raise RuntimeError(
                f"Local control socket permissions are not owner-only: {self.path}"
            )


def local_node_controller() -> Any:
    controller = installed_local_node_controller()
    if controller is not None:
        return controller
    return HarnessNodeController(require_cluster())


def installed_local_node_controller() -> Any | None:
    with _override_lock:
        return _controller_override


@contextmanager
def use_local_node_controller(controller: Any) -> Iterator[Any]:
    """Install a process-wide controller transport visible to agent threads."""
    global _controller_override
    with _override_lock:
        previous = _controller_override
        _controller_override = controller
    try:
        yield controller
    finally:
        with _override_lock:
            _controller_override = previous


__all__ = [
    "HarnessControlClient",
    "HarnessControlProxy",
    "RemoteHarnessCluster",
    "installed_local_node_controller",
    "local_node_controller",
    "use_local_node_controller",
]


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    return value


def _integer(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("numeric field must be an integer")
    return value


def _cluster_node(cluster: Any, node_id: int) -> Any:
    node = cluster.nodes.get(node_id)
    if node is None:
        raise ValueError(f"Unknown local harness node_id={node_id}")
    return node


def _read_drive_status(cluster: Any, node_id: int) -> dict:
    node = _cluster_node(cluster, node_id)
    response = cluster.client.read_drive_status(node.host, node.ic_port).BlobStorageConfigResponse
    return {
        "drives": [
            {"path": drive.Path, "status": int(drive.Status)}
            for status in response.Status
            for drive in status.DriveStatus
        ]
    }


def _update_drive_status(cluster: Any, node_id: int, path: str, status: int) -> dict:
    node = _cluster_node(cluster, node_id)
    response = cluster.client.update_drive_status(
        node.host,
        node.ic_port,
        path,
        status,
    ).BlobStorageConfigResponse
    fail_reason = None
    if response.Status:
        fail_reason = int(response.Status[0].FailReason)
    return {"success": bool(response.Success), "fail_reason": fail_reason}


def _query_base_config(cluster: Any) -> dict:
    base = cluster.client.query_base_config().BaseConfig
    return {
        "pdisks": [
            {"node_id": int(pdisk.NodeId), "pdisk_id": int(pdisk.PDiskId), "path": pdisk.Path}
            for pdisk in base.PDisk
        ],
        "vslots": [
            {"node_id": int(vslot.VSlotId.NodeId), "pdisk_id": int(vslot.VSlotId.PDiskId)}
            for vslot in base.VSlot
        ],
    }


class _RemoteNode:
    def __init__(self, record: dict) -> None:
        self.node_id = int(record["node_id"])
        self.logical_host = str(record["logical_host"])
        self.host = str(record["connect_host"])
        self.grpc_port = int(record["grpc_port"])
        self.mon_port = int(record["mon_port"])
        self.ic_port = int(record["ic_port"])


class _RemoteDriveClient:
    def __init__(self, cluster: "RemoteHarnessCluster") -> None:
        self._cluster = cluster

    def read_drive_status(self, host: str, ic_port: int, drive_path=None, domain: int = 1):
        del drive_path, domain
        node_id = self._node_id(host, ic_port)
        result = self._cluster.control.read_drive_status(node_id)
        drives = [
            SimpleNamespace(Path=drive["path"], Status=EDriveStatus(int(drive["status"])))
            for drive in result["drives"]
        ]
        return SimpleNamespace(
            BlobStorageConfigResponse=SimpleNamespace(
                Status=[SimpleNamespace(DriveStatus=drives)]
            )
        )

    def update_drive_status(self, host: str, ic_port: int, path: str, status, domain: int = 1):
        del domain
        node_id = self._node_id(host, ic_port)
        result = self._cluster.control.update_drive_status(node_id, path, int(status))
        status_row = SimpleNamespace(FailReason=result["fail_reason"])
        return SimpleNamespace(
            BlobStorageConfigResponse=SimpleNamespace(
                Success=bool(result["success"]),
                Status=[] if result["fail_reason"] is None else [status_row],
            )
        )

    def query_base_config(self, domain: int = 1):
        del domain
        result = self._cluster.control.query_base_config()
        return SimpleNamespace(
            BaseConfig=SimpleNamespace(
                PDisk=[
                    SimpleNamespace(
                        NodeId=int(pdisk["node_id"]),
                        PDiskId=int(pdisk["pdisk_id"]),
                        Path=pdisk["path"],
                    )
                    for pdisk in result["pdisks"]
                ],
                VSlot=[
                    SimpleNamespace(
                        VSlotId=SimpleNamespace(
                            NodeId=int(vslot["node_id"]),
                            PDiskId=int(vslot["pdisk_id"]),
                        )
                    )
                    for vslot in result["vslots"]
                ],
            )
        )

    def _node_id(self, host: str, ic_port: int) -> int:
        for node in self._cluster.nodes.values():
            if node.host == host and node.ic_port == int(ic_port):
                return node.node_id
        raise ValueError(f"Unknown remote harness node {host}:{ic_port}")


class _RemoteSlot:
    def __init__(self, record: dict) -> None:
        self.slot_idx = int(record["slot_idx"])
        self.node_id = int(record["node_id"])
        self.logical_host = str(record["logical_host"])
        self.host = str(record["connect_host"])
        self.grpc_port = int(record["grpc_port"])
        self.mon_port = int(record["mon_port"])
        self.ic_port = int(record["ic_port"])


class RemoteHarnessCluster:
    """Cluster description used by a nemesis process that does not own the harness."""

    def __init__(self, manifest: dict, control: HarnessControlClient) -> None:
        self.control = control
        self.nodes = {
            int(record["node_id"]): _RemoteNode(record)
            for record in manifest["nodes"]
        }
        self.slots = {
            int(record["slot_idx"]): _RemoteSlot(record)
            for record in manifest.get("slots", [])
        }
        self.config = SimpleNamespace(
            static_erasure=str(manifest["static_erasure"]),
            pdisks_info=list(manifest.get("pdisks", [])),
        )
        self.client = _RemoteDriveClient(self)
