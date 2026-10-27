from types import SimpleNamespace

import pytest

from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import use_cluster
from ydb.tests.stability.nemesis.internal.nemesis.local_harness_controller import (
    HarnessNodeController,
)
from ydb.tests.stability.nemesis.internal.nemesis.runners import node_local
from ydb.tests.stability.nemesis.internal.nemesis.runners.node_local import (
    KillNodeNemesis,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import (
    ChaosTarget,
)


class _Node:
    def __init__(self, pid):
        self.pid = pid
        self.kill_calls = 0
        self.alive = True
        self.ic_port = pid + 10000
        self.grpc_port = pid + 11000
        self.mon_port = pid + 12000
        self.grpc_ssl_port = None
        self.ydbd_log_file_path = f"log-{pid}"

    def kill(self):
        self.kill_calls += 1
        self.pid += 1000

    def is_alive(self):
        return self.alive

    def start(self):
        self.alive = True
        self.pid += 1000

    def set_log_file_prefix(self, prefix):
        self.ydbd_log_file_path = f"{prefix}test.log"


class _Socket:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def _cluster(*, erasure="block-4-2", nodes=8):
    return SimpleNamespace(
        config=SimpleNamespace(static_erasure=erasure),
        nodes={node_id: _Node(100 + node_id) for node_id in range(1, nodes + 1)},
    )


def test_controller_restarts_exactly_one_harness_node():
    cluster = _cluster()

    restarted = HarnessNodeController(cluster).kill_and_restart(3)

    assert restarted.node_id == 3
    assert restarted.previous_pid == 103
    assert restarted.current_pid == 1103
    assert cluster.nodes[3].kill_calls == 1
    assert all(
        node.kill_calls == (1 if node_id == 3 else 0)
        for node_id, node in cluster.nodes.items()
    )


@pytest.mark.parametrize(
    "cluster",
    [
        _cluster(erasure="none"),
        _cluster(nodes=7),
    ],
)
def test_controller_refuses_unsafe_cluster_shape(cluster):
    with pytest.raises(RuntimeError, match="at least 8 block-4-2 nodes"):
        HarnessNodeController(cluster).kill_and_restart(1)
    assert all(node.kill_calls == 0 for node in cluster.nodes.values())


def test_controller_holds_stopped_node_by_id_and_restarts_idempotently(
    monkeypatch,
):
    cluster = _cluster()
    sockets = {
        cluster.nodes[3].ic_port: _Socket(),
        cluster.nodes[3].grpc_port: _Socket(),
    }
    monkeypatch.setattr(
        HarnessNodeController,
        "_kill_without_restart",
        lambda self, node: setattr(node, "alive", False),
    )
    monkeypatch.setattr(
        HarnessNodeController,
        "_hold_node_ports",
        lambda self, node: dict(sockets),
    )
    controller = HarnessNodeController(cluster)

    stopped = controller.stop(3)
    assert stopped.node_id == 3
    assert stopped.previous_pid == 103
    assert stopped.held_ports == tuple(sorted(sockets))
    assert controller.stop(3) is stopped
    assert controller.stopped_node_ids() == (3,)
    assert cluster.nodes[3].is_alive() is False

    started = controller.start(3)
    assert started.previous_pid == 103
    assert started.current_pid == 1103
    assert started.log_file.startswith("local_nemesis_node_3_001_")
    assert all(sock.closed for sock in sockets.values())
    assert controller.stopped_node_ids() == ()
    assert controller.start(3) is None


def test_kill_node_runner_uses_harness_controller_without_shell(monkeypatch):
    cluster = _cluster()
    shell_calls = []
    monkeypatch.setattr(
        node_local.subprocess,
        "check_call",
        lambda *args, **kwargs: shell_calls.append((args, kwargs)),
    )
    runner = KillNodeNemesis()
    runner.on_success_inject_fault = lambda: None
    target = ChaosTarget.for_node("node-4", node_id=4, ic_port=19004)

    with use_cluster(cluster):
        runner.inject_fault({"chaos_target": target.to_dict()})

    assert cluster.nodes[4].kill_calls == 1
    assert shell_calls == []
