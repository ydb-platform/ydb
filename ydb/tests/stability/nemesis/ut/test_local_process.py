"""Real isolated child processes exercise discovery, signals and SIGTERM restore."""
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import threading
import time
from dataclasses import replace

import pytest
import requests
import yaml
from flask import Flask, jsonify

from ydb.tests.stability.nemesis.internal.agent.nemesis.local_process import (
    LocalProcessRunner, ProcessController, ProcessSpec, load_process_specs, local_definitions,
)
from ydb.tests.stability.nemesis.internal.agent.nemesis.runner import NemesisManager
from ydb.tests.stability.nemesis.internal.agent.nemesis.service import AgentExecutionService
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.cluster_inventory import ClusterInventory
from ydb.tests.stability.nemesis.internal.shutdown import ShutdownCoordinator, serve_until_stopped


@pytest.fixture
def processes(tmp_path):
    python = shutil.which("python3")
    if not python or not hasattr(os, "pidfd_open"):
        pytest.skip("Linux pidfd and a standalone python3 are required")
    children = []

    def spawn(port=19001):
        argv = [python, "-c", "import time; time.sleep(3600)", "--ic-port", str(port)]
        child = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        children.append(child)
        spec = ProcessSpec("localhost", "node", port, None, port, str(Path(python).resolve()),
                           "r1", "dc1", 0, os.getuid(), {"argv": argv, "cwd": str(tmp_path)})
        controller = ProcessController([spec], timeout=3)
        controller._wait(lambda: controller._read(child.pid, spec) is not None, "test child did not start")
        return child, spec

    yield spawn
    for child in children:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)


def target(spec):
    return ChaosTarget.for_node(spec.host, node_id=spec.node_id, ic_port=spec.ic_port)


def test_suspend_exact_process_and_idempotent_restore(processes):
    child, spec = processes(19001)
    other, other_spec = processes(190010 % 65536)
    controller = ProcessController([spec, other_spec], timeout=3)
    controller.inject(target(spec), "suspend")
    assert controller.discover(spec).state == "T"
    assert controller.discover(other_spec).state != "T"
    controller.restore(target(spec))
    controller.restore(target(spec))
    assert controller.discover(spec).state != "T"
    assert child.poll() is None and other.poll() is None


def test_ambiguous_process_and_wrong_target_are_rejected(processes):
    _, spec = processes()
    processes()
    controller = ProcessController([spec], timeout=3)
    with pytest.raises(RuntimeError, match="Ambiguous"):
        controller.inject(target(spec), "suspend")
    with pytest.raises(ValueError, match="exactly one"):
        controller.spec_for(ChaosTarget.for_node("localhost", node_id=999, ic_port=spec.ic_port))


def test_pid_reuse_is_rejected_before_signal(processes, monkeypatch):
    _, spec = processes()
    controller = ProcessController([spec])
    ref = controller.discover(spec)
    sent = []
    monkeypatch.setattr(controller, "_read", lambda *a: replace(ref, starttime=ref.starttime + 1))
    monkeypatch.setattr(signal, "pidfd_send_signal", lambda *a: sent.append(a))
    with pytest.raises(RuntimeError, match="identity changed"):
        controller._signal(spec, ref, signal.SIGSTOP)
    assert not sent


@pytest.mark.parametrize("mode", ["kill", "stop"])
def test_restart_uses_recipe_and_does_not_duplicate_supervisor_restart(processes, mode):
    child, spec = processes()
    commands = []

    def run(cmd, **kwargs):
        commands.append(cmd)
        processes()

    controller = ProcessController([spec], run=run, timeout=3)
    controller.inject(target(spec), mode)
    if mode == "stop":
        assert controller.discover(spec) is None
        controller.restore(target(spec))
    assert controller.discover(spec).pid != child.pid
    assert len(commands) == 1 and commands[0][0] == "systemd-run"
    assert "--service-type=exec" in commands[0]  # restart lives outside Nemesis cgroup
    controller.restore(target(spec))
    assert len(commands) == 1


def test_supervisor_replacement_is_adopted(processes):
    _, spec = processes()
    controller = ProcessController([spec], run=lambda *a, **k: pytest.fail("duplicate restart"), timeout=3)
    controller.inject(target(spec), "stop")
    processes()
    controller.restore(target(spec))


def test_inventory_loads_without_harness_and_keeps_piles(tmp_path, processes, monkeypatch):
    _, spec = processes()
    row = {**spec.__dict__, "restart": spec.restart}
    path = tmp_path / "processes.yaml"
    other = {**row, "node_id": 2, "ic_port": 19002, "bridge_pile_id": 1,
             "restart": {"unit": "ydb-node-2.service"}}
    path.write_text(yaml.safe_dump({"processes": [row, other]}))
    specs = load_process_specs(path)
    monkeypatch.setattr(ClusterInventory, "_load_from_harness", lambda *a: pytest.fail("harness used"))
    inventory = ClusterInventory(type("Topology", (), {"hosts": {"localhost": None}})(), process_specs=specs)
    assert [n.bridge_pile_id for n in inventory.nodes.values()] == [0, 1]
    assert local_definitions({"NetworkNemesis": {}, "KillNodeNemesis": {}}, ProcessController(specs)).keys() == {"KillNodeNemesis"}
    row["restart"] = {"argv": [spec.executable, "--ic-port", "9999"], "cwd": str(tmp_path)}
    path.write_text(yaml.safe_dump({"processes": [row]}))
    with pytest.raises(ValueError, match="same executable"):
        load_process_specs(path)


def test_manager_waits_for_inject_then_restores_and_rejects_new_inject():
    entered, release = threading.Event(), threading.Event()
    calls = []

    class Runner:
        def inject_fault(self, payload):
            entered.set()
            release.wait(2)
            calls.append("inject")

        def extract_fault(self, payload):
            calls.append("restore")

    service = AgentExecutionService(NemesisManager(), {"toggle": {"runner": Runner(), "recovery": "extract"}})
    assert service.start("toggle")["status"] == "started"
    assert entered.wait(1)
    service.begin_shutdown()
    assert service.start("toggle")["status"] == "error"
    release.set()
    assert service.shutdown(3)["status"] == "ok"
    assert service.shutdown(3)["status"] == "ok"
    assert calls == ["inject", "restore"]


def test_failed_restore_is_not_successful_shutdown(tmp_path):
    class Runner:
        def inject_fault(self, payload):
            pass

        def extract_fault(self, payload):
            raise RuntimeError("restore unavailable")

        def on_failed_extract_fault(self):
            pass

    service = AgentExecutionService(NemesisManager(), {"toggle": {"runner": Runner(), "recovery": "extract"}})
    service.start("toggle")
    coordinator = ShutdownCoordinator(service.begin_shutdown, lambda: service.shutdown(2),
                                      report_path=tmp_path / "shutdown.json", timeout=3)
    assert coordinator.shutdown()["status"] == "error"
    assert json.loads((tmp_path / "shutdown.json").read_text())["unrestored"] == ["toggle"]


def test_shutdown_deadline_is_persisted_and_idempotent(tmp_path):
    blocked = threading.Event()
    coordinator = ShutdownCoordinator(lambda: None, lambda: blocked.wait(2),
                                      report_path=tmp_path / "shutdown.json", timeout=0.01)
    try:
        first = coordinator.shutdown()
        assert first["status"] == "error"
        assert coordinator.shutdown() == first
        assert json.loads((tmp_path / "shutdown.json").read_text()) == first
    finally:
        blocked.set()


def test_sigterm_during_injection_restores_real_process(processes, tmp_path):
    _, spec = processes()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    report = tmp_path / "shutdown.json"

    def run_server():
        controller = ProcessController([spec], timeout=3)

        class DelayedSuspend(LocalProcessRunner):
            def inject_fault(self, payload):
                time.sleep(0.3)  # SIGTERM arrives while this accepted inject is in flight.
                super().inject_fault(payload)

        service = AgentExecutionService(NemesisManager(), {
            "suspend": {"runner": DelayedSuspend(controller, "suspend"), "recovery": "extract"},
        })
        coordinator = ShutdownCoordinator(service.begin_shutdown, lambda: service.shutdown(5),
                                          report_path=report, timeout=6)
        app = Flask(__name__)

        @app.route("/health")
        def health():
            return "ok"

        @app.route("/inject", methods=["POST"])
        def inject():
            return jsonify(service.start("suspend", payload={"chaos_target": target(spec).to_dict()}))

        raise SystemExit(serve_until_stopped(app, coordinator.shutdown, host="127.0.0.1", port=port))

    server = multiprocessing.get_context("fork").Process(target=run_server)
    server.start()
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                requests.get(f"http://127.0.0.1:{port}/health", timeout=0.2).raise_for_status()
                break
            except requests.RequestException:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)
        assert requests.post(f"http://127.0.0.1:{port}/inject", timeout=1).json()["status"] == "started"
        os.kill(server.pid, signal.SIGTERM)
        server.join(10)
        assert server.exitcode == 0
        assert json.loads(report.read_text())["status"] == "ok"
        assert ProcessController([spec]).discover(spec).state != "T"
    finally:
        if server.is_alive():
            server.kill()
        server.join(5)


def test_agent_can_resume_after_remote_drain_but_not_sigterm():
    service = AgentExecutionService(NemesisManager(), {})
    assert service.shutdown(1)["status"] == "ok"
    service.resume()
    service.begin_shutdown()
    assert service.shutdown(1)["status"] == "ok"
    with pytest.raises(RuntimeError, match="cannot resume"):
        service.resume()


def test_extract_is_not_duplicated_between_probe_and_shutdown():
    calls = []

    class Runner:
        def inject_fault(self, payload):
            calls.append("inject")

        def extract_fault(self, payload):
            time.sleep(0.05)
            calls.append("extract")

    service = AgentExecutionService(NemesisManager(), {"toggle": {"runner": Runner(), "recovery": "extract"}})
    service.start("toggle")
    service.wait_for_processes(timeout=1, poll_interval=0.01)
    service.start("toggle", "extract")
    assert service.shutdown(2)["status"] == "ok"
    assert calls == ["inject", "extract"]


def test_local_app_wires_process_inventory_and_engine_without_harness(tmp_path, processes, monkeypatch):
    from types import SimpleNamespace
    from ydb.tests.stability.nemesis import app as module
    from ydb.tests.stability.nemesis.routers import agent_router, orchestrator_router
    from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import ClusterTopologyModel
    from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.fault_engine import FaultRejected

    _, spec = processes()
    config = tmp_path / "processes.yaml"
    config.write_text(yaml.safe_dump({"processes": [spec.__dict__]}))
    topology_file = tmp_path / "cluster.yaml"
    topology_file.write_text(yaml.safe_dump({"static_erasure": "block-4-2", "hosts": [
        {"name": "localhost", "location": {"rack": "r1", "data_center": "dc1"}},
    ]}))
    settings = SimpleNamespace(local_process_config=str(config), nemesis_type="orchestrator",
                               install_root=str(tmp_path), shutdown_timeout_sec=5, mon_port=8765)
    monkeypatch.setattr(module, "get_settings", lambda: settings)
    monkeypatch.setattr(module, "_shutdown_coordinator", None)
    monkeypatch.setattr(module, "AgentWardenChecker", lambda **k: None)
    monkeypatch.setattr(module, "OrchestratorWardenChecker", lambda **k: None)
    monkeypatch.setattr(module, "HealthCheckReporter", lambda *a, **k: SimpleNamespace(
        start_healthchecks=lambda: None, stop_healthchecks=lambda: None,
        last_results={}, last_update=None,
    ))
    monkeypatch.setattr(module.RecoveryProbe, "start", lambda self: None)
    monkeypatch.setattr(ClusterInventory, "_load_from_harness", lambda self: pytest.fail("harness used"))
    for name in ("hosts", "host_endpoints", "mon_port", "healthcheck_reporter", "chaos_problems",
                 "failure_guard", "cluster_inventory", "nemesis_metrics", "chaos_store",
                 "orchestrator_warden_checker", "recovery_probe", "command_dispatcher",
                 "nemesis_schedule", "fault_engine", "nemesis_scheduler"):
        monkeypatch.setattr(orchestrator_router, name, getattr(orchestrator_router, name))
    monkeypatch.setattr(agent_router, "execution_service", None)
    monkeypatch.setattr(agent_router, "warden_checker", None)
    flask_app = Flask(__name__)
    flask_app.config["NEMESIS_TOPOLOGY"] = ClusterTopologyModel(str(topology_file))
    with flask_app.app_context():
        module.initialize_app()
    assert orchestrator_router.cluster_inventory.nodes[spec.node_id].ic_port == spec.ic_port
    assert "NetworkNemesis" not in orchestrator_router.nemesis_scheduler.status()["enabled_types"]
    orchestrator_router.fault_engine.inject("SuspendNodeNemesis", target(spec))
    assert agent_router.execution_service.wait_for_processes(timeout=2, poll_interval=0.01) == 0
    assert ProcessController([spec]).discover(spec).state == "T"
    assert module.cleanup_app()["status"] == "ok"
    assert ProcessController([spec]).discover(spec).state != "T"
    with pytest.raises(FaultRejected, match="stopping"):
        orchestrator_router.fault_engine.inject("SuspendNodeNemesis", target(spec))


def test_install_root_environment_controls_shutdown_report_directory(monkeypatch):
    from ydb.tests.stability.nemesis.internal.config import Settings
    monkeypatch.setenv("NEMESIS_INSTALL_ROOT", "/tmp/nemesis-test-install")
    assert Settings().install_root == "/tmp/nemesis-test-install"
