"""Manual operation lifecycle and retained stability client contracts."""
import pytest
from flask import Flask

from ydb.tests.stability.nemesis.internal.nemesis.catalog import NEMESIS_TYPES
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import GuardMode
from ydb.tests.stability.nemesis.routers import orchestrator_router as router
from test_fault_engine import setup_engine
from test_pile_budget import slot_target


@pytest.fixture
def operations(tmp_path, monkeypatch):
    e = setup_engine(tmp_path, toggle=True)
    name = "TestSlotToggle"
    monkeypatch.setitem(NEMESIS_TYPES, name, {"target_kind": TargetKind.SLOT, "recovery": "extract"})
    e.inventory.entities = lambda kind: [slot_target(e.inventory, p, i) for p in (0, 1) for i in range(10)]
    monkeypatch.setattr(router, "fault_engine", e.engine)
    monkeypatch.setattr(router, "cluster_inventory", e.inventory)
    monkeypatch.setattr(router, "available_fault_types", {name})
    monkeypatch.setattr(router, "failure_guard", e.guard)
    app = Flask(__name__)
    app.register_blueprint(router.blueprint)
    return e, app.test_client(), name


def inject(client, name, target, **fields):
    response = client.post("/api/operations", json={"type": name, "target": target.to_dict(), **fields})
    assert response.status_code == 201, response.json
    return response.json["operation_id"]


def test_exact_targets_show_pile_and_unavailable_faults_have_reasons(operations):
    e, client, name = operations
    rows = client.get("/api/targets", query_string={"type": name}).json
    assert len(rows) == 20
    assert rows[0]["placement"] == [["localhost", "0", "dc", "0"]]
    assert rows[10]["placement"] == [["localhost", "1", "dc", "0"]]
    catalog = {row["name"]: row for row in client.get("/api/faults").json}
    assert catalog[name]["available"]
    assert not catalog["NetworkNemesis"]["available"] and catalog["NetworkNemesis"]["unavailable_reason"]
    wrong = slot_target(e.inventory, 0, 0).to_dict()
    wrong["ic_port"] += 1
    assert client.post("/api/operations", json={"type": name, "target": wrong}).status_code == 400
    assert not e.delivered


def test_restore_id_cannot_restore_later_operation_on_same_target(operations):
    e, client, name = operations
    t0, t1 = slot_target(e.inventory, 0, 0), slot_target(e.inventory, 1, 0)
    first = inject(client, name, t0, hold_sec=7)
    second = inject(client, name, t1)
    assert [p.extract_after_sec for p in e.probe.pending()] == [7, 60]
    assert client.post(f"/api/operations/{first}/restore", json={}).status_code == 200
    assert [c.target for c in e.delivered if c.action == "extract"] == [t0]
    assert e.guard.snapshot()["impaired_slots"] == 2
    snap = e.probe.snapshot()
    assert snap["piles"]["0"]["confirming"] == 1 and snap["piles"]["1"]["confirming"] == 0
    e.publish(e.membership)
    e.probe.tick()
    rows = {row["operation_id"]: row for row in client.get("/api/operations").json["operations"]}
    assert rows[first]["state"] == "recovered" and not rows[first]["can_restore"]
    assert rows[second]["piles"] == [1] and rows[second]["can_restore"]
    third = inject(client, name, t0)
    assert third != first
    assert client.post(f"/api/operations/{first}/restore", json={}).status_code == 200
    assert len([c for c in e.delivered if c.action == "extract"]) == 1
    assert client.post("/api/operations/missing/restore", json={}).status_code == 404


def test_delivery_failure_remains_visible_and_restorable(operations):
    e, client, name = operations
    e.engine._dispatch = lambda cmd: False
    response = client.post("/api/operations", json={"type": name, "target": slot_target(e.inventory, 0, 0).to_dict()})
    assert response.status_code == 502
    row = client.get("/api/operations").json["operations"][0]
    assert row["operation_id"] == response.json["operation_id"]
    assert row["delivery"] == "failed" and row["error"] and row["can_restore"]
    assert e.guard.snapshot()["impaired_slots"] == 1


@pytest.mark.parametrize("hold", [0, -1, 86401, True, "7", float("inf")])
def test_invalid_per_operation_parameters_do_not_dispatch(operations, hold):
    e, client, name = operations
    response = client.post("/api/operations", json={"type": name, "target": slot_target(e.inventory, 0, 0).to_dict(), "hold_sec": hold})
    assert response.status_code == 409
    assert not e.delivered


def test_stale_pile_counts_are_unknown_not_zero(operations):
    e, client, name = operations
    inject(client, name, slot_target(e.inventory, 0, 0))
    e.now[0] += 1000
    row = client.get("/api/operations").json["operations"][0]
    assert row["recovery"]["pile_recovery"]["0"] == {"baseline": 10, "alive": None}
    assert e.probe.snapshot()["piles"]["0"]["alive_compute"] is None


def test_history_bound_keeps_active_operations(operations):
    e, client, name = operations
    active = inject(client, name, slot_target(e.inventory, 0, 0))
    e.engine._mode_for = lambda n: GuardMode.BYPASS
    for _ in range(105):
        inject(client, name, slot_target(e.inventory, 1, 0))
    rows = client.get("/api/operations").json["operations"]
    assert len(rows) == 101
    assert rows[-1]["operation_id"] == active
    assert rows[-1]["can_restore"]
    assert not rows[0]["tracked"] and rows[0]["state"] == "accepted"


def test_old_per_type_scheduler_routes_are_removed(operations):
    _, client, _ = operations
    for path in ("/api/schedule", "/api/schedule/all", "/api/schedule/history"):
        assert client.get(path).status_code == 404
        assert client.post(path, json={"enabled": True}).status_code == 404
