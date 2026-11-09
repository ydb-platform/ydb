"""Delivery contracts shared by manual, boundary and legacy execution."""

from types import SimpleNamespace

import pytest
import requests
from flask import Flask

from ydb.tests.stability.nemesis.internal.agent.nemesis.service import AgentExecutionService
from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import dispatch
from ydb.tests.stability.nemesis.internal.nemesis.command_transport import AgentCommandTransport
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.command_dispatcher import CommandDispatcher
from ydb.tests.stability.nemesis.routers import agent_router, orchestrator_router


class RecordingManager:
    def __init__(self):
        self.calls = []

    def start_process(self, process_type, runner, action, *, payload):
        self.calls.append((process_type, runner, action, payload))

    def get_all(self):
        return []


class RecordingMetrics:
    def __init__(self):
        self.events = []

    def fault_started(self, **fields):
        self.events.append(("started", fields))

    def fault_extract_dispatched(self, **fields):
        self.events.append(("extract", fields))

    def fault_dispatch_failed(self, **fields):
        self.events.append(("failed", fields))


@pytest.fixture
def delivery(monkeypatch):
    manager = RecordingManager()
    service = AgentExecutionService(manager, {"KillNodeNemesis": {"runner": object()}})
    monkeypatch.setattr(agent_router, "execution_service", service)
    app = Flask(__name__)
    app.register_blueprint(agent_router.blueprint)
    client = app.test_client()
    posts = []

    def post(url, *, json, timeout):
        posts.append((url, json, timeout))
        response = client.post("/api/processes", json=json)

        def raise_for_status():
            if response.status_code >= 400:
                raise requests.HTTPError(str(response.status_code))

        return SimpleNamespace(json=response.get_json, raise_for_status=raise_for_status)

    def transport(local):
        return AgentCommandTransport(
            is_local_host=lambda host: local,
            start_local=service.start,
            get_app_port=lambda: 31434,
            resolve_http_host=lambda host: "[::1]",
            post=post,
        )

    return SimpleNamespace(manager=manager, service=service, client=client, posts=posts, transport=transport)


@pytest.mark.parametrize("local", [True, False])
@pytest.mark.parametrize("target", [
    ChaosTarget.for_host("h1"),
    ChaosTarget.for_node("h1", node_id=1, ic_port=19001),
    ChaosTarget.for_slot("h1", slot_idx=2, node_id=22, ic_port=19002),
    ChaosTarget.for_disk("h1", node_id=1, disk_path="/run/cluster/pdisk1"),
])
def test_local_and_http_preserve_exact_target(delivery, local, target):
    payload = {"signal": "SIGSTOP"}
    command = dispatch("KillNodeNemesis", target, "inject", payload)
    delivery.transport(local).send(command)
    assert len(delivery.manager.calls) == 1
    process_type, _, action, actual = delivery.manager.calls[0]
    assert (process_type, action) == (command.nemesis_type, "inject")
    assert actual == {"signal": "SIGSTOP", "host": "h1", "chaos_target": target.to_dict()}
    assert payload == {"signal": "SIGSTOP"}  # routing metadata must not mutate the plan
    assert len(delivery.posts) == (0 if local else 1)
    if not local:
        url, body, timeout = delivery.posts[0]
        assert url == "http://[::1]:31434/api/processes" and timeout == 5
        assert body["target"] == target.to_dict()


@pytest.mark.parametrize("local", [True, False])
def test_multiple_nodes_on_one_host_are_not_collapsed(delivery, local):
    transport = delivery.transport(local)
    for node_id in (1, 2):
        transport.send(dispatch(
            "KillNodeNemesis", ChaosTarget.for_node("localhost", node_id=node_id, ic_port=19000 + node_id),
            "inject", {},
        ))
    assert [row[3]["chaos_target"]["node_id"] for row in delivery.manager.calls] == [1, 2]


@pytest.mark.parametrize("local", [True, False])
def test_rejected_type_is_not_reported_as_started(delivery, local):
    metrics = RecordingMetrics()
    sender = CommandDispatcher(delivery.transport(local), metrics=metrics)
    assert not sender.dispatch(dispatch("MissingType", "h1", "inject", {}), track_history=True)
    assert delivery.manager.calls == []
    assert sender.recent_history() == []
    assert [name for name, _ in metrics.events] == ["failed"]


@pytest.mark.parametrize("failure", ["timeout_after_acceptance", "http_error", "rejection", "invalid_json"])
def test_delivery_errors_are_not_retried_or_recorded_as_success(failure):
    calls = []

    def post(*args, **kwargs):
        calls.append(kwargs["json"])
        if failure == "timeout_after_acceptance":
            # The server may already have started the action. Do not retry it.
            raise requests.Timeout("response lost")

        def raise_for_status():
            if failure == "http_error":
                raise requests.HTTPError("503")

        def result():
            if failure == "invalid_json":
                raise ValueError("invalid response")
            return {"status": "error", "message": "rejected"}

        return SimpleNamespace(raise_for_status=raise_for_status, json=result)

    metrics = RecordingMetrics()
    sender = CommandDispatcher(AgentCommandTransport(
        is_local_host=lambda host: False, start_local=None, get_app_port=lambda: 31434,
        resolve_http_host=lambda host: host, post=post,
    ), metrics=metrics)
    assert not sender.dispatch(dispatch("KillNodeNemesis", "h1", "inject", {}), track_history=True)
    assert len(calls) == 1
    assert sender.recent_history() == []
    assert [name for name, _ in metrics.events] == ["failed"]


def test_shared_history_and_metrics_keep_existing_api(delivery, monkeypatch):
    metrics = RecordingMetrics()
    sender = CommandDispatcher(delivery.transport(True), metrics=metrics, history_limit=2)
    target = ChaosTarget.for_node("h1", node_id=1, ic_port=19001)
    commands = [dispatch("KillNodeNemesis", target, action, {}, lease_id="lease")
                for action in ("inject", "inject", "extract")]
    for command in commands:
        assert sender.dispatch(command, track_history=True)
    # Manual dispatch retains the existing opt-out from scheduled history.
    assert sender.dispatch(commands[0], track_history=False)
    assert [name for name, _ in metrics.events] == ["started", "started", "extract", "started"]
    assert all(fields["lease_id"] == "lease" for _, fields in metrics.events)
    monkeypatch.setattr(orchestrator_router, "command_dispatcher", sender)
    app = Flask(__name__)
    app.register_blueprint(orchestrator_router.blueprint)
    rows = app.test_client().get("/api/history").get_json()
    assert [row["execution_id"] for row in rows] == [c.execution_id for c in reversed(commands[1:])]
    assert set(rows[0]) == {"type", "action", "host", "target", "execution_id", "scenario_id", "timestamp"}
    assert rows[0]["target"] == target.to_dict()


def test_agent_api_and_local_service_share_execution(delivery):
    assert delivery.client.post("/api/processes", json={"type": "KillNodeNemesis"}).get_json() == {
        "status": "started",
    }
    assert delivery.service.start("KillNodeNemesis", "extract") == {"status": "started"}
    assert [row[2] for row in delivery.manager.calls] == ["inject", "extract"]
    assert delivery.client.get("/api/processes").get_json() == []
    assert delivery.service.wait_for_processes(timeout=0) == 0


@pytest.mark.parametrize("local", [True, False])
def test_existing_manual_api_uses_shared_dispatcher(delivery, monkeypatch, local):
    sender = CommandDispatcher(delivery.transport(local))
    from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.fault_engine import FaultEngine
    from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import GuardMode
    engine = FaultEngine(
        guard=None, inventory=None,
        plan_inject=lambda name, target: [dispatch(name, target, "inject", {})],
        dispatch=lambda cmd: sender.dispatch(cmd, track_history=False),
        mode_for=lambda name: GuardMode.BYPASS)
    monkeypatch.setattr(orchestrator_router, "fault_engine", engine)
    target = ChaosTarget.for_node("h1", node_id=2, ic_port=19002)
    monkeypatch.setattr(orchestrator_router, "hosts", ["h1"])
    monkeypatch.setattr(orchestrator_router, "command_dispatcher", sender)
    monkeypatch.setattr(orchestrator_router, "chaos_store", SimpleNamespace(
        plan_manual=lambda name, host, action: [dispatch(name, host, action, {})],
    ))
    monkeypatch.setattr(orchestrator_router, "failure_guard", None)
    monkeypatch.setattr(orchestrator_router, "cluster_inventory", None)
    app = Flask(__name__)
    app.register_blueprint(orchestrator_router.blueprint)
    response = app.test_client().post("/api/hosts/process", json={
        "type": "KillNodeNemesis", "host": "h1", "action": "inject", "target": target.to_dict(),
    })
    assert response.status_code == 200
    body = response.get_json()
    assert body["status"] == "ok" and len(body["executions"]) == 1
    assert body["executions"][0]["target"] == target.to_dict()
    assert delivery.manager.calls[0][3]["chaos_target"] == target.to_dict()
    assert sender.recent_history() == []
