"""Orchestrator NemesisMetrics: budget + fault lifecycle events."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import yaml

from ydb.tests.tools.nemesis.library import monitor as nemesis_monitor
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import (
    ChaosTarget,
    TargetKind,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelGuard,
    ImpactScope,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.hc_model import hc_predicate_for
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.metrics import (
    EVENT_BUDGET_ACQUIRED,
    EVENT_BUDGET_ACQUIRE_REJECTED,
    EVENT_BUDGET_RELEASED,
    EVENT_FAULT_ENDED,
    EVENT_FAULT_STARTED,
    EVENT_FAULT_STUCK,
    NemesisMetrics,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import RecoveryProbe


def _topology() -> ClusterTopologyModel:
    hosts = [
        {"name": f"h{i}", "location": {"rack": f"r{i}", "data_center": "dc1"}} for i in (1, 2, 3, 4)
    ]
    fd, path = tempfile.mkstemp(suffix=".yaml")
    os.close(fd)
    Path(path).write_text(
        yaml.safe_dump({"static_erasure": "block-4-2", "hosts": hosts}), encoding="utf-8"
    )
    return ClusterTopologyModel(path)


def _metrics() -> NemesisMetrics:
    return NemesisMetrics(mon=nemesis_monitor.Monitor(), run_id="test-run")


def _events(m: NemesisMetrics, name: str) -> list[dict]:
    return [e for e in m.recent_events(100) if e["event"] == name]


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class StubReporter:
    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock
        self.last_results: dict = {}
        self.last_update: float | None = None

    def publish(self, results: dict) -> None:
        self.last_results = results
        self.last_update = self._clock()


def _vdisk(entity_id: str, overall: str = "GREEN") -> dict:
    pid = "-".join(entity_id.split("-")[:2])
    return {"id": entity_id, "overall": overall, "pdisk": {"id": pid, "overall": overall}}


def _compute_node(i: int) -> dict:
    return {"id": str(i), "overall": "GREEN", "pools": [{"name": "System", "usage": 0.01}]}


def _hc(host: str, vdisks=(), compute=()) -> dict:
    return {
        "self_check_result": "GOOD",
        "database_status": [
            {
                "name": "/Root",
                "overall": "GREEN",
                "storage": {
                    "overall": "GREEN",
                    "pools": [
                        {
                            "id": "static",
                            "overall": "GREEN",
                            "groups": [{"id": "0", "overall": "GREEN", "vdisks": list(vdisks)}],
                        }
                    ],
                },
                "compute": {
                    "overall": "GREEN",
                    "nodes": list(compute),
                    "clock_skew": {"overall": "GREEN"},
                },
            }
        ],
        "location": {"id": 1, "host": host, "port": 19001},
    }


def _healthy_results() -> dict:
    compute = [_compute_node(i) for i in range(1, 9)]
    vdisks = [_vdisk(f"{n}-1-0") for n in (1, 2, 3, 4)]
    return {f"h{i}": _hc(f"h{i}", vdisks=vdisks, compute=compute) for i in (1, 2, 3, 4)}


def _red_node1() -> dict:
    compute = [_compute_node(i) for i in range(1, 9)]
    vdisks = [_vdisk("1-1-0", "RED")] + [_vdisk(f"{n}-1-0") for n in (2, 3, 4)]
    return {f"h{i}": _hc(f"h{i}", vdisks=vdisks, compute=compute) for i in (1, 2, 3, 4)}


class TestNemesisMetricsEmitter:
    def test_fault_lifecycle_events(self):
        m = _metrics()
        target = ChaosTarget.for_node("h1", node_id=1)
        m.fault_started(
            target=target,
            nemesis_type="KillNodeNemesis",
            execution_id="e1",
            lease_id="lease1",
            source="dispatch",
        )
        started = _events(m, EVENT_FAULT_STARTED)
        assert len(started) == 1
        assert started[0]["target"]["host"] == "h1"
        assert started[0]["lease_id"] == "lease1"
        assert started[0]["run_id"] == "test-run"

        m.fault_ended(
            target=target,
            nemesis_type="KillNodeNemesis",
            reason="recovered",
            lease_id="lease1",
            held_sec=42.5,
            source="probe",
        )
        ended = _events(m, EVENT_FAULT_ENDED)
        assert ended[0]["held_sec"] == 42.5
        assert ended[0]["reason"] == "recovered"

    def test_budget_acquired_always_has_nemesis_label(self, caplog):
        """Monium legends use {{nemesis}}; missing label renders as literal {{nemesis}}."""
        import logging

        m = _metrics()
        guard = FailureModelGuard(_topology(), metrics=m)
        target = ChaosTarget.for_node("h1", node_id=1)
        with caplog.at_level(logging.INFO, logger="ydb.tests.stability.nemesis.internal.orchestrator.nemesis.metrics"):
            lease = guard.reserve(
                guard.footprint_for(target, ImpactScope.NODE),
                identity_key=target.identity_key(),
                target=target,
                nemesis_type="KillNodeNemesis",
                source="boundary",
            )
        assert lease
        acquired = _events(m, EVENT_BUDGET_ACQUIRED)[0]
        assert acquired["nemesis_type"] == "KillNodeNemesis"
        # Counter labels must include nemesis (checked via cache key).
        assert any(
            name == "NemesisBudgetAcquired" and dict(labels).get("nemesis") == "KillNodeNemesis"
            for (name, labels) in m._counter_cache
        )
        assert any("budget acquired: KillNodeNemesis" in r.message for r in caplog.records)
        assert any(r.message.startswith("nemesis_metric ") for r in caplog.records)

    def test_budget_acquired_without_type_uses_unknown_nemesis_label(self):
        m = _metrics()
        guard = FailureModelGuard(_topology(), metrics=m)
        target = ChaosTarget.for_node("h1", node_id=1)
        lease = guard.reserve(
            guard.footprint_for(target, ImpactScope.NODE),
            identity_key=target.identity_key(),
        )
        assert lease
        assert any(
            name == "NemesisBudgetAcquired" and dict(labels).get("nemesis") == "unknown"
            for (name, labels) in m._counter_cache
        )


class TestGuardBudgetMetrics:
    def test_reserve_release_emit_budget_events(self):
        m = _metrics()
        guard = FailureModelGuard(_topology(), metrics=m)
        target = ChaosTarget.for_node("h1", node_id=1)
        fp = guard.footprint_for(target, ImpactScope.NODE)
        lease = guard.reserve(
            fp,
            identity_key=target.identity_key(),
            target=target,
            nemesis_type="KillNodeNemesis",
            source="boundary",
        )
        assert lease
        acquired = _events(m, EVENT_BUDGET_ACQUIRED)
        assert len(acquired) == 1
        assert acquired[0]["lease_id"] == lease
        assert "dc1/r1" in acquired[0]["footprint"]["racks"]
        assert acquired[0]["budget_after"]["impaired_racks"] == ["dc1/r1"]

        assert guard.release(
            lease, reason="recovered", target=target, nemesis_type="KillNodeNemesis", source="probe"
        )
        released = _events(m, EVENT_BUDGET_RELEASED)
        assert released[0]["reason"] == "recovered"
        assert released[0]["budget_after"]["impaired_racks"] == []

    def test_reserve_rejected_emits_event(self):
        m = _metrics()
        guard = FailureModelGuard(_topology(), metrics=m)
        for i in (1, 2):
            t = ChaosTarget.for_node(f"h{i}", node_id=i)
            assert guard.reserve(guard.footprint_for(t, ImpactScope.NODE), identity_key=t.identity_key())
        t3 = ChaosTarget.for_node("h3", node_id=3)
        assert (
            guard.reserve(
                guard.footprint_for(t3, ImpactScope.NODE),
                identity_key=t3.identity_key(),
                target=t3,
                source="boundary",
            )
            is None
        )
        assert _events(m, EVENT_BUDGET_ACQUIRE_REJECTED)

    def test_record_inject_extract_emit_budget_events(self):
        m = _metrics()
        guard = FailureModelGuard(_topology(), metrics=m)
        target = ChaosTarget.for_node("h1", node_id=1)
        guard.record_inject(
            "exec-1", target, ImpactScope.NODE, nemesis_type="KillNodeNemesis", source="legacy"
        )
        assert _events(m, EVENT_BUDGET_ACQUIRED)[0]["lease_id"] == "exec-1"
        guard.record_extract(
            "exec-1", target, ImpactScope.NODE, nemesis_type="KillNodeNemesis", source="legacy"
        )
        assert _events(m, EVENT_BUDGET_RELEASED)[0]["reason"] == "extract"

    def test_tablet_footprint_skips_budget_metrics(self):
        m = _metrics()
        guard = FailureModelGuard(_topology(), metrics=m)
        tablet = ChaosTarget.for_tablet("h1", tablet_id=42)
        guard.record_inject("t1", tablet, ImpactScope.NODE, source="manual")
        assert _events(m, EVENT_BUDGET_ACQUIRED) == []


class TestProbeFaultMetrics:
    def test_release_emits_fault_ended(self):
        m = _metrics()
        guard = FailureModelGuard(_topology(), metrics=m)
        clock = FakeClock()
        rep = StubReporter(clock)
        probe = RecoveryProbe(
            guard=guard, hc_source=rep, metrics=m, min_hold_sec=0.0, clock=clock
        )
        rep.publish(_healthy_results())
        clock.advance(5.0)
        target = ChaosTarget.for_node("h1", node_id=1)
        lease = guard.reserve(
            guard.footprint_for(target, ImpactScope.NODE),
            identity_key=target.identity_key(),
            target=target,
            nemesis_type="KillNodeNemesis",
            source="boundary",
        )
        probe.track(
            lease,
            target,
            "KillNodeNemesis",
            recovered=hc_predicate_for(target, kind=TargetKind.NODE, scope=ImpactScope.NODE),
            stuck_timeout_sec=100.0,
        )
        rep.publish(_healthy_results())
        probe.tick()
        ended = _events(m, EVENT_FAULT_ENDED)
        assert ended and ended[0]["reason"] == "recovered"
        assert ended[0]["lease_id"] == lease
        assert _events(m, EVENT_BUDGET_RELEASED)

    def test_stuck_emits_event(self):
        m = _metrics()
        guard = FailureModelGuard(_topology(), metrics=m)
        clock = FakeClock()
        rep = StubReporter(clock)
        probe = RecoveryProbe(
            guard=guard, hc_source=rep, metrics=m, min_hold_sec=0.0, clock=clock
        )
        rep.publish(_red_node1())
        clock.advance(1.0)
        target = ChaosTarget.for_node("h1", node_id=1)
        lease = guard.reserve(
            guard.footprint_for(target, ImpactScope.NODE),
            identity_key=target.identity_key(),
        )
        probe.track(
            lease,
            target,
            "KillNodeNemesis",
            recovered=hc_predicate_for(target, kind=TargetKind.NODE, scope=ImpactScope.NODE),
            stuck_timeout_sec=5.0,
        )
        clock.advance(10.0)
        rep.publish(_red_node1())
        stuck = probe.tick()
        assert stuck
        assert _events(m, EVENT_FAULT_STUCK)
