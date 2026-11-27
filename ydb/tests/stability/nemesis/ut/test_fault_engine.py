"""One admission path, conservative delivery, per-pile recovery and old HTTP API."""
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from flask import Flask

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import dispatch
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.fault_engine import (
    FaultEngine, FaultRejected, DispatchFailed, RecoveryUnavailable,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import GuardMode, ImpactScope
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import RecoveryProbe
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.runtime_topology import RuntimeNodePiles
from ydb.tests.stability.nemesis.routers import orchestrator_router
from test_pile_budget import cluster, slot_target


def setup_engine(tmp_path, *, toggle=False, send=None, admission=None):
    guard, inventory, _, _ = cluster(tmp_path, local=True, slots=(10, 10))
    now = [100.]
    reporter = SimpleNamespace(last_results={}, last_update=100.)
    membership = {s.node_id: s.bridge_pile_id for s in inventory.slots.values()}
    probe = RecoveryProbe(guard=guard, hc_source=reporter, clock=lambda: now[0],
                          min_hold_sec=0, node_piles_source=lambda: dict(membership))
    delivered = []
    engine = FaultEngine(
        guard=guard, inventory=inventory, recovery_probe=probe,
        plan_inject=lambda n, t: [dispatch(n, t, "inject", {})],
        plan_extract=lambda n, t: [dispatch(n, t, "extract", {})],
        dispatch=send or (lambda c: delivered.append(c)),
        scope_for=lambda n: ImpactScope.SLOT, kind_for=lambda n: TargetKind.SLOT,
        mode_for=lambda n: GuardMode.FULL,
        recovery_mode_for=lambda n: "extract" if toggle else "self",
        recovery_sec_for=lambda n: 60, admission=admission)

    def publish(ids):
        now[0] += 1
        reporter.last_update = now[0]
        reporter.last_results = {"localhost": {"self_check_result": "GOOD", "database_status": [
            {"compute": {"nodes": [{"id": str(i), "pools": [{"name": "System"}]} for i in ids]}}
        ]}}
    publish(list(membership))
    return SimpleNamespace(
        engine=engine, guard=guard, inventory=inventory, probe=probe,
        membership=membership, publish=publish, now=now, delivered=delivered)


def test_other_pile_growth_cannot_confirm_recovery_and_new_runtime_id_can(tmp_path):
    e = setup_engine(tmp_path)
    t = slot_target(e.inventory, 0, 0)
    e.engine.inject("slot", t)
    del e.membership[t.node_id]
    e.membership[9000] = 1  # replaces total count but not the affected pile
    e.publish(e.membership)
    e.probe.tick()
    assert e.guard.snapshot()["piles"]["0"]["impaired_slots"] == 1
    e.membership[9001] = 0  # restarted slot has a new runtime id
    e.publish(e.membership)
    e.probe.tick()
    assert e.guard.snapshot()["tracked_executions"] == 0


def test_missing_runtime_membership_blocks_admission_and_release(tmp_path):
    e = setup_engine(tmp_path)
    t = slot_target(e.inventory, 0, 0)
    e.engine.inject("slot", t)
    e.membership.clear()
    e.probe.tick()
    assert e.guard.snapshot()["tracked_executions"] == 1
    with pytest.raises(RecoveryUnavailable):
        e.engine.inject("slot", slot_target(e.inventory, 1, 0))


def test_manual_restore_keeps_lease_until_new_healthcheck(tmp_path):
    e = setup_engine(tmp_path, toggle=True)
    t = slot_target(e.inventory, 0, 0)
    e.engine.inject("slot", t, source="manual")
    e.now[0] += 1
    e.engine.extract("slot", t)
    assert [c.action for c in e.delivered] == ["inject", "extract"]
    e.probe.tick()
    assert e.guard.snapshot()["tracked_executions"] == 1
    e.engine.extract("slot", t)  # no duplicate restore while confirming
    assert len(e.delivered) == 2
    e.publish(e.membership)
    e.probe.tick()
    assert e.guard.snapshot()["tracked_executions"] == 0


@pytest.mark.parametrize("raises", [False, True])
def test_first_delivery_failure_holds_budget_and_tracks_restore(tmp_path, raises):
    def fail(cmd):
        if raises:
            raise TimeoutError("accepted but reply lost")
        return False
    e = setup_engine(tmp_path, send=fail, toggle=True)
    with pytest.raises((TimeoutError, DispatchFailed)):
        e.engine.inject("slot", slot_target(e.inventory, 0, 0))
    assert e.guard.snapshot()["tracked_executions"] == 1
    assert len(e.probe.pending()) == 1
    assert not e.probe.pending()[0].dispatching


def test_empty_plan_does_not_reserve_or_dispatch(tmp_path):
    e = setup_engine(tmp_path)
    e.engine._plan_inject = lambda n, t: []
    with pytest.raises(FaultRejected):
        e.engine.inject("slot", slot_target(e.inventory, 0, 0))
    assert not e.delivered and not e.probe.pending()
    assert e.guard.snapshot()["tracked_executions"] == 0


def test_manual_and_scheduler_compete_for_one_budget(tmp_path):
    e = setup_engine(tmp_path)

    def inject(i):
        try:
            e.engine.inject("slot", slot_target(e.inventory, 0, i),
                            source="manual" if i % 2 else "boundary")
            return True
        except FaultRejected:
            return False
    with ThreadPoolExecutor(max_workers=6) as pool:
        assert sum(pool.map(inject, range(10))) == 3
    assert len(e.delivered) == 3
    assert e.guard.snapshot()["piles"]["1"]["impaired_slots"] == 0


def test_admission_extension_sees_complete_plan_and_can_veto_bypass(tmp_path):
    plans = []

    def check(plan):
        plans.append(plan)
        raise FaultRejected("CMS denied")
    e = setup_engine(tmp_path, admission=SimpleNamespace(check=check))
    e.engine._mode_for = lambda n: GuardMode.BYPASS
    with pytest.raises(FaultRejected):
        e.engine.inject("slot", slot_target(e.inventory, 0, 0))
    assert len(plans[0].commands) == 1
    assert not e.delivered and not e.probe.pending()


def test_old_manual_api_reserves_before_dispatch_and_extract_does_not_release(tmp_path, monkeypatch):
    e = setup_engine(tmp_path, toggle=True)
    t = slot_target(e.inventory, 0, 0)
    monkeypatch.setattr(orchestrator_router, "fault_engine", e.engine)
    monkeypatch.setattr(orchestrator_router, "hosts", ["localhost"])
    monkeypatch.setattr(orchestrator_router, "command_dispatcher", object())
    monkeypatch.setattr(orchestrator_router, "chaos_store", object())
    monkeypatch.setattr(orchestrator_router, "cluster_inventory", None)
    app = Flask(__name__)
    app.register_blueprint(orchestrator_router.blueprint)
    client = app.test_client()
    payload = {"type": "KillSlotDaemonNemesis", "host": "localhost", "target": t.to_dict()}
    response = client.post("/api/hosts/process", json=payload)
    assert response.status_code == 200 and response.json["executions"][0]["target"] == t.to_dict()
    assert client.post("/api/hosts/process", json={**payload, "force": True}).status_code == 409
    assert client.post("/api/hosts/process", json={**payload, "action": "extract"}).status_code == 200
    assert e.guard.snapshot()["tracked_executions"] == 1
    assert client.post("/api/hosts/process", json={**payload, "action": "typo"}).status_code == 400


def test_runtime_membership_uses_location_not_host_and_refreshes_ids():
    rows = [{"NodeId": 11, "SystemState": {"Host": "localhost", "Location": {"BridgePileName": "a"}}},
            {"NodeId": 22, "SystemState": {"Host": "localhost", "Location": {"BridgePileName": "b"}}}]
    source = RuntimeNodePiles(
        ["http://localhost:8765"], {0: "a", 1: "b"},
        get=lambda *a, **k: SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"Nodes": rows}))
    assert source() == {11: 0, 22: 1}
    rows[0]["NodeId"] = 33
    assert source() == {33: 0, 22: 1}
    rows.clear()
    assert source() == {}


def test_recovery_does_not_release_while_delivery_is_running(tmp_path):
    e = setup_engine(tmp_path)

    def during_delivery(cmd):
        e.publish(e.membership)
        e.probe.tick()
        assert e.guard.snapshot()["tracked_executions"] == 1
        assert e.probe.pending()[0].dispatching

    e.engine._dispatch = during_delivery
    e.engine.inject("slot", slot_target(e.inventory, 0, 0))
    assert not e.probe.pending()[0].dispatching


def test_fanout_accounts_and_restores_every_destination(tmp_path):
    e = setup_engine(tmp_path, toggle=True)
    targets = [slot_target(e.inventory, p, 0) for p in (0, 1)]
    e.engine._plan_inject = lambda n, t: [dispatch(n, target, "inject", {}) for target in targets]
    e.engine.inject("slot", targets[0])
    assert [p["impaired_slots"] for p in e.guard.snapshot()["piles"].values()] == [1, 1]
    e.engine._plan_inject = lambda n, t: [dispatch(n, t, "inject", {})]
    with pytest.raises(FaultRejected):
        e.engine.inject("slot", targets[1])
    e.engine.extract("slot", targets[0])
    assert [c.target for c in e.delivered if c.action == "extract"] == targets
    e.membership.pop(targets[1].node_id)
    e.publish(e.membership)
    e.probe.tick()
    assert e.guard.snapshot()["tracked_executions"] == 1


def test_bridge_storage_missing_from_healthcheck_is_not_recovered(tmp_path):
    from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.hc_model import HcSnapshot, hc_predicate_for
    from test_pile_budget import node_target
    e = setup_engine(tmp_path)
    t = node_target(e.inventory, 0, 0)
    recovered = hc_predicate_for(t, kind=t.kind, scope=ImpactScope.NODE, inventory=e.inventory)
    snap = HcSnapshot(fresh=True, data_at=1, answering=frozenset({"localhost"}),
                      storage_by_node={11: {"11-1": "GREEN"}})
    assert not recovered(snap)  # only the other pile reported healthy storage
    snap.storage_by_node[t.node_id] = {f"{t.node_id}-1": "GREEN"}
    assert recovered(snap)


def test_async_restore_failure_retries_before_health_release(tmp_path):
    e = setup_engine(tmp_path, toggle=True)
    status = ["running"]
    sends = []

    def send(cmd):
        sends.append(cmd.action)
        return (lambda: status[0]) if cmd.action == "extract" else True

    e.engine._dispatch = send
    e.engine.inject("slot", slot_target(e.inventory, 0, 0))
    operation = e.engine.operations()[0]["operation_id"]
    e.engine.restore_operation(operation)
    e.publish(e.membership)
    e.probe.tick()
    assert sends.count("extract") == 1 and e.probe.pending()
    status[0] = "error"
    e.probe.tick()
    assert e.engine.operations()[0]["can_restore"]
    status[0] = "finished"
    e.engine.restore_operation(operation)
    assert sends.count("extract") == 2 and e.probe.pending()
    e.publish(e.membership)
    e.probe.tick()
    assert not e.probe.pending()


def test_failed_remote_resume_can_restore_retained_lease(tmp_path):
    from unittest.mock import Mock
    import requests
    from ydb.tests.stability.nemesis.internal.agent.nemesis.runner import NemesisManager
    from ydb.tests.stability.nemesis.internal.agent.nemesis.service import AgentExecutionService
    from ydb.tests.stability.nemesis.internal.nemesis.command_transport import AgentCommandTransport

    service = AgentExecutionService(NemesisManager(), {"slot": {"runner": Mock(), "recovery": "extract"}})
    assert service.shutdown()["status"] == "ok"
    resumes = []

    def post(url, json=None, **kwargs):
        if url.endswith("/resume"):
            resumes.append(url)
            if len(resumes) == 1:
                raise requests.Timeout("resume not delivered")
            service.resume()
            result = {"status": "ok"}
        else:
            result = service.start(json["type"], json["action"], json["payload"])
        return Mock(json=lambda: result)

    transport = AgentCommandTransport(is_local_host=lambda h: False, start_local=None,
                                      get_app_port=lambda: 31434, resolve_http_host=lambda h: h,
                                      post=post, resume_remote=True,
                                      get=lambda url, **kw: Mock(json=lambda: service.result(url.rsplit("/", 1)[1])))
    e = setup_engine(tmp_path, toggle=True, send=transport.send)
    with pytest.raises(requests.Timeout):
        e.engine.inject("slot", slot_target(e.inventory, 0, 0))
    assert e.probe.pending() and e.guard.snapshot()["tracked_executions"] == 1
    e.engine.restore_operation(e.engine.operations()[0]["operation_id"])
    assert service.wait_for_processes() == 0
    e.probe.tick()
    e.publish(e.membership)
    e.probe.tick()
    assert len(resumes) == 2 and not e.probe.pending()
    assert e.guard.snapshot()["tracked_executions"] == 0


@pytest.mark.parametrize("result", ["finished", "error"])
def test_execution_result_survives_recovery_and_preserves_command_identity(tmp_path, result):
    e = setup_engine(tmp_path)
    status = {"status": "running"}
    e.engine._dispatch = lambda cmd: lambda: dict(status)
    e.engine.inject("slot", slot_target(e.inventory, 0, 0))
    e.publish(e.membership)
    e.probe.tick()
    assert e.probe.pending()  # HC must not overtake an in-flight inject
    e.engine.poll_executions()
    assert e.engine.operations()[0]["execution"]["status"] == "running"
    status.update(status=result, ret_code=int(result == "error"), error="failed action" if result == "error" else None)
    e.engine.poll_executions()
    e.publish(e.membership)
    e.probe.tick()
    row = e.engine.operations()[0]
    assert row["state"] == "recovered"
    assert row["execution"]["status"] == ("succeeded" if result == "finished" else "failed")
    assert row["commands"][0]["ret_code"] == int(result == "error")


def test_sql_failure_and_pre_restore_success_hold_both_pile_budgets(tmp_path):
    from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.workload_probe import WorkloadProbe
    e = setup_engine(tmp_path, toggle=True)
    failure = []

    def check():
        if failure:
            raise TimeoutError("SQL timed out")
    workload = WorkloadProbe(check, clock=lambda: e.now[0], max_age=10)
    e.probe._workload = workload
    with pytest.raises(RecoveryUnavailable):
        e.engine.inject("slot", slot_target(e.inventory, 0, 0))
    for pile in (0, 1):
        workload.tick()
        e.engine.inject("slot", slot_target(e.inventory, pile, 0))
    workload.tick()  # success before restore must not release either pile
    e.now[0] += 1
    for row in e.engine.operations():
        e.engine.restore_operation(row["operation_id"])
    e.publish(e.membership)
    e.probe.tick()
    assert e.guard.snapshot()["impaired_slots"] == 2
    failure.append(True)
    workload.tick()
    e.probe.tick()
    assert e.guard.snapshot()["impaired_slots"] == 2
    assert all(row["recovery"]["workload_recovery"]["status"] == "failed" for row in e.engine.operations())
    failure.clear()
    workload.tick()
    e.probe.tick()
    assert not e.probe.pending()
    e.now[0] += 11
    with pytest.raises(RecoveryUnavailable):
        e.engine.inject("slot", slot_target(e.inventory, 0, 0))


def test_completed_receipt_cannot_end_fanout_delivery(tmp_path):
    e = setup_engine(tmp_path)
    targets = [slot_target(e.inventory, p, 0) for p in (0, 1)]
    e.engine._plan_inject = lambda n, t: [dispatch(n, target, "inject", {}) for target in targets]
    calls = []

    def send(cmd):
        calls.append(cmd)
        if len(calls) == 2:
            e.engine.poll_executions()
            e.publish(e.membership)
            e.probe.tick()
            assert e.probe.pending()[0].dispatching
            assert e.guard.snapshot()["impaired_slots"] == 2
        return lambda: {"status": "finished", "ret_code": 0}

    e.engine._dispatch = send
    e.engine.inject("slot", targets[0])
    e.engine.poll_executions()
    e.publish(e.membership)
    e.probe.tick()
    assert not e.probe.pending()


def test_completion_during_health_predicate_requires_new_evidence(tmp_path):
    e = setup_engine(tmp_path, send=lambda cmd: lambda: {"status": "finished"})
    e.engine.inject("slot", slot_target(e.inventory, 0, 0))
    pending = e.probe.pending()[0]
    predicate = pending.recovered

    def raced(snap):
        e.now[0] += 1
        e.engine.poll_executions()
        return predicate(snap)

    pending.recovered = raced
    e.publish(e.membership)
    e.probe.tick()
    assert e.probe.pending()
    pending.recovered = predicate
    e.publish(e.membership)
    e.probe.tick()
    assert not e.probe.pending()
