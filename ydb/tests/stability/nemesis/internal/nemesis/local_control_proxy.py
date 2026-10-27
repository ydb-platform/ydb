"""Owner-only Unix-socket transport for narrow local harness capabilities."""

from __future__ import annotations

import json
import os
import socket
import socketserver
import stat
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import (
    require_cluster,
)
from ydb.tests.stability.nemesis.internal.nemesis.local_harness_controller import (
    HarnessNodeController,
    NodeRestart,
)


_PROTOCOL_VERSION = 1
_MAX_REQUEST_BYTES = 4096
_controller_override: Any | None = None
_override_lock = threading.RLock()


class _ControlRequestHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        raw = self.rfile.readline(_MAX_REQUEST_BYTES + 1)
        if not raw or len(raw) > _MAX_REQUEST_BYTES or not raw.endswith(b"\n"):
            self._reply(error="invalid request framing")
            return
        try:
            request = json.loads(raw)
            if not isinstance(request, dict):
                raise ValueError("request must be an object")
            if set(request) != {"version", "operation", "node_id"}:
                raise ValueError("request fields are not allowed")
            if request["version"] != _PROTOCOL_VERSION:
                raise ValueError("unsupported protocol version")
            if request["operation"] != "kill_and_restart":
                raise ValueError("operation is not allowed")
            node_id = request["node_id"]
            if isinstance(node_id, bool):
                raise ValueError("node_id must be an integer")
            result = self.server.controller.kill_and_restart(int(node_id))
            self._reply(
                result={
                    "node_id": result.node_id,
                    "previous_pid": result.previous_pid,
                    "current_pid": result.current_pid,
                }
            )
        except Exception as error:
            self._reply(error=str(error))

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
    def __init__(self, path: str, controller: HarnessNodeController) -> None:
        self.controller = controller
        super().__init__(path, _ControlRequestHandler)
        os.chmod(path, 0o600)


class HarnessControlProxy:
    """Serve one allowlisted controller operation on an owner-only Unix socket."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        controller: HarnessNodeController,
    ) -> None:
        self.path = Path(path)
        self._server = _UnixControlServer(str(self.path), controller)
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
    """Client implementing the controller's proven kill-and-restart capability."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        timeout: float = 10,
    ) -> None:
        self.path = Path(path)
        self.timeout = float(timeout)

    def kill_and_restart(self, node_id: int) -> NodeRestart:
        self._validate_socket()
        request = {
            "version": _PROTOCOL_VERSION,
            "operation": "kill_and_restart",
            "node_id": int(node_id),
        }
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(self.timeout)
            client.connect(str(self.path))
            client.sendall(
                json.dumps(request, separators=(",", ":")).encode("utf-8") + b"\n"
            )
            with client.makefile("rb") as response_file:
                raw = response_file.readline(_MAX_REQUEST_BYTES + 1)
        if not raw or len(raw) > _MAX_REQUEST_BYTES:
            raise RuntimeError("local control proxy returned an invalid response")
        response = json.loads(raw)
        if not isinstance(response, dict) or response.get("ok") is not True:
            error = response.get("error") if isinstance(response, dict) else None
            raise RuntimeError(f"local control proxy rejected request: {error}")
        result = response.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("local control proxy omitted its result")
        return NodeRestart(
            node_id=int(result["node_id"]),
            previous_pid=int(result["previous_pid"]),
            current_pid=int(result["current_pid"]),
        )

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
    with _override_lock:
        controller = _controller_override
    if controller is not None:
        return controller
    return HarnessNodeController(require_cluster())


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
    "local_node_controller",
    "use_local_node_controller",
]
