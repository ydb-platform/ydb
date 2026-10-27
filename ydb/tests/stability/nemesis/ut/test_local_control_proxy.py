import json
import socket
from types import SimpleNamespace

import pytest

from ydb.tests.stability.nemesis.internal.nemesis.local_control_proxy import (
    HarnessControlClient,
    HarnessControlProxy,
)
from ydb.tests.stability.nemesis.internal.nemesis.local_harness_controller import (
    HarnessNodeController,
)


class _Node:
    def __init__(self, pid):
        self.pid = pid
        self.kill_calls = 0

    def kill(self):
        self.kill_calls += 1
        self.pid += 1000

    def is_alive(self):
        return True


def _cluster():
    return SimpleNamespace(
        config=SimpleNamespace(static_erasure="block-4-2"),
        nodes={node_id: _Node(100 + node_id) for node_id in range(1, 9)},
    )


def test_unix_proxy_restarts_exact_node_and_removes_socket(tmp_path):
    cluster = _cluster()
    path = tmp_path / "control.sock"

    with HarnessControlProxy(
        path,
        HarnessNodeController(cluster),
    ):
        result = HarnessControlClient(path).kill_and_restart(3)
        assert result.node_id == 3
        assert result.previous_pid == 103
        assert result.current_pid == 1103
        assert path.stat().st_mode & 0o777 == 0o600

    assert not path.exists()
    assert cluster.nodes[3].kill_calls == 1
    assert all(
        node.kill_calls == (1 if node_id == 3 else 0)
        for node_id, node in cluster.nodes.items()
    )


def test_unix_proxy_rejects_unlisted_operation_without_side_effect(tmp_path):
    cluster = _cluster()
    path = tmp_path / "control.sock"

    with HarnessControlProxy(
        path,
        HarnessNodeController(cluster),
    ):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(path))
            client.sendall(
                json.dumps(
                    {
                        "version": 1,
                        "operation": "stop",
                        "node_id": 3,
                    }
                ).encode("utf-8")
                + b"\n"
            )
            response = json.loads(client.makefile("rb").readline())

    assert response["ok"] is False
    assert "not allowed" in response["error"]
    assert all(node.kill_calls == 0 for node in cluster.nodes.values())


def test_unix_client_rejects_group_accessible_socket(tmp_path):
    cluster = _cluster()
    path = tmp_path / "control.sock"

    with HarnessControlProxy(
        path,
        HarnessNodeController(cluster),
    ):
        path.chmod(0o660)
        with pytest.raises(RuntimeError, match="permissions are not owner-only"):
            HarnessControlClient(path).kill_and_restart(3)

    assert all(node.kill_calls == 0 for node in cluster.nodes.values())
