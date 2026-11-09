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
