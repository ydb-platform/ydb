"""Fact-based recovery of reserved failure budget: hc-model, probe phases, blindness."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import yaml

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import (
    ChaosTarget,
    TargetKind,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelGuard,
    ImpactScope,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.hc_model import (
    build_snapshot,
    datacenter_predicate,
    hc_predicate_for,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import (
    PHASE_CONFIRM,
    RecoveryProbe,
    StuckFault,
)


def _guard() -> FailureModelGuard:
    hosts = [
        {"name": f"h{i}", "location": {"rack": f"r{i}", "data_center": "dc1"}} for i in (1, 2, 3, 4)
    ]
    fd, path = tempfile.mkstemp(suffix=".yaml")
    os.close(fd)
    Path(path).write_text(
        yaml.safe_dump({"static_erasure": "block-4-2", "hosts": hosts}), encoding="utf-8"
    )
    return FailureModelGuard(ClusterTopologyModel(path))


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


def _vdisk(entity_id: str, overall: str = "GREEN", pdisk_overall: str = "GREEN") -> dict:
    pid = "-".join(entity_id.split("-")[:2])
    return {"id": entity_id, "overall": overall, "pdisk": {"id": pid, "overall": pdisk_overall}}


def _compute_node(i: int, alive: bool = True) -> dict:
    return {
        "id": str(i),
        "overall": "GREEN",
        "pools": [{"name": "System", "usage": 0.01}] if alive else [],
    }


def _hc(host: str, vdisks=(), compute=(), skew: str = "GREEN", result: str = "GOOD") -> dict:
    return {
        "self_check_result": result,
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
                    "clock_skew": {"overall": skew},
                },
            }
        ],
        "location": {"id": 1, "host": host, "port": 19001},
    }


def _error_entry() -> dict:
    return {"self_check_result": "HC_REQUEST_ERROR"}


def _healthy_results(compute_count: int = 8) -> dict:
    compute = [_compute_node(i) for i in range(1, compute_count + 1)]
    vdisks = [_vdisk(f"{n}-1-0") for n in (1, 2, 3, 4)]
    return {f"h{i}": _hc(f"h{i}", vdisks=vdisks, compute=compute) for i in (1, 2, 3, 4)}


def _reserve(guard, host="h1", node_id=1):
    target = ChaosTarget.for_node(host, node_id=node_id)
    lease = guard.reserve(
        guard.footprint_for(target, ImpactScope.NODE),
        identity_key=target.identity_key(),
    )
    return target, lease


def _node_predicate(target):
    return hc_predicate_for(target, kind=TargetKind.NODE, scope=ImpactScope.NODE)


class TestSnapshot:
    def test_snapshot_merge_rules(self):
        snap = build_snapshot(
            {"h1": _error_entry(), "h2": _hc("h2")}, now=10.0, last_update=10.0, max_age_sec=180.0
        )
        assert snap.fresh and snap.answering == frozenset({"h2"})

        results = {
            "h1": _hc("h1", vdisks=[_vdisk("1-1-0", "GREEN")]),
            "h2": _hc("h2", vdisks=[_vdisk("1-1-0", "BLUE")]),
        }
        snap = build_snapshot(results, now=10.0, last_update=10.0, max_age_sec=180.0)
        assert not snap.storage_green(1) and snap.storage_blockers(1) == ["1-1-0=BLUE"]

        # Alive = non-empty pools; count is the most lagging view.
        results = {
            "h1": _hc("h1", compute=[_compute_node(1), _compute_node(2), _compute_node(3, alive=False)]),
            "h2": _hc("h2", compute=[_compute_node(1)]),
        }
        snap = build_snapshot(results, now=10.0, last_update=10.0, max_age_sec=180.0)
        assert snap.alive_compute == 1

        snap = build_snapshot(
            {"h1": _hc("h1", skew="ORANGE")}, now=10.0, last_update=10.0, max_age_sec=180.0
        )
        assert not snap.clock_skew_green
        assert not build_snapshot(
            {"h1": _hc("h1")}, now=1000.0, last_update=10.0, max_age_sec=180.0
        ).fresh
        assert not build_snapshot(
            {"h1": _error_entry()}, now=10.0, last_update=10.0, max_age_sec=180.0
        ).fresh

        # HOST kind is shared; only TimeSkew waits on clock_skew.
        host = ChaosTarget.for_host("h1")
        skew_bad = build_snapshot(
            {"h1": _hc("h1", skew="ORANGE")}, now=10.0, last_update=10.0, max_age_sec=180.0
        )
        assert not hc_predicate_for(
            host, kind=TargetKind.HOST, scope=ImpactScope.NODE, nemesis_type="TimeSkewNemesis"
        )(skew_bad)
        assert hc_predicate_for(
            host, kind=TargetKind.HOST, scope=ImpactScope.NODE, nemesis_type="NetworkNemesis"
        )(skew_bad)


class TestNodeAndSlotRecovery:
    def test_node_prefault_blue_stuck_then_green(self):
        # Pre-fault data must not release.
        guard, clock = _guard(), FakeClock()
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        rep.publish(_healthy_results())
        clock.advance(5.0)
        target, lease = _reserve(guard)
        probe.track(lease, target, "KillNode", recovered=_node_predicate(target),
                    stuck_timeout_sec=100.0)
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"]
        rep.publish(_healthy_results())
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == []

        # BLUE holds; stuck once while down; GREEN releases.
        guard, clock, seen = _guard(), FakeClock(), []
        rep = StubReporter(clock)
        probe = RecoveryProbe(
            guard=guard, hc_source=rep, on_stuck=seen.append, min_hold_sec=0.0, clock=clock
        )
        target, lease = _reserve(guard)
        probe.track(lease, target, "KillNode", recovered=_node_predicate(target),
                    stuck_timeout_sec=100.0)

        results = _healthy_results()
        results["h2"] = _hc("h2", vdisks=[_vdisk("1-1-0", "BLUE")])
        rep.publish(results)
        clock.advance(1.0)
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"]

        rep.publish({"h1": _error_entry(), "h2": _hc("h2", vdisks=[_vdisk("1-1-0", "RED")])})
        clock.advance(120.0)
        rep.publish(rep.last_results)
        stuck = probe.tick()
        assert len(stuck) == 1 and stuck[0].phase == "hold"
        assert probe.tick() == [] and len(seen) == 1

        rep.publish(_healthy_results())
        clock.advance(20.0)
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == [] and probe.pending() == []

    def test_slot_baseline_and_count_recovery(self):
        guard, clock = _guard(), FakeClock()
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        assert probe.alive_compute_baseline() is None  # blind

        target = ChaosTarget.for_slot("h1", slot_idx=3)
        assert hc_predicate_for(target, kind=TargetKind.SLOT, scope=ImpactScope.SLOT)(
            build_snapshot(_healthy_results(), now=0.0, last_update=0.0, max_age_sec=180.0)
        ) is False

        # Empty DC ids / no inventory → never recovered (all([]) must not win).
        snap = build_snapshot(_healthy_results(), now=0.0, last_update=0.0, max_age_sec=180.0)
        assert datacenter_predicate(["h1"], [])(snap) is False
        assert hc_predicate_for(
            ChaosTarget.for_datacenter("h1", "dc1"),
            kind=TargetKind.DATACENTER, scope=ImpactScope.DATACENTER, inventory=None,
        )(snap) is False

        rep.publish(_healthy_results(compute_count=8))
        baseline = probe.alive_compute_baseline()
        assert baseline == 8
        slot = ChaosTarget.for_slot("h1", slot_idx=3, node_id=1)
        lease = guard.reserve(
            guard.footprint_for(slot, ImpactScope.SLOT), identity_key=slot.identity_key()
        )
        probe.track(
            lease, slot, "KillSlot",
            recovered=hc_predicate_for(
                slot, kind=TargetKind.SLOT, scope=ImpactScope.SLOT, baseline=baseline
            ),
            stuck_timeout_sec=300.0,
        )
        rep.publish(_healthy_results(compute_count=7))
        clock.advance(20.0)
        probe.tick()
        assert guard.snapshot()["impaired_slots"] == 1
        rep.publish(_healthy_results(compute_count=8))
        clock.advance(20.0)
        probe.tick()
        assert guard.snapshot()["impaired_slots"] == 0 and probe.pending() == []


class TestToggleAndDrain:
    def _track_toggle(self, probe, target, lease, extracted, extract_after=90.0, confirm=300.0):
        probe.track(
            lease, target, "StopStart",
            recovered=_node_predicate(target),
            stuck_timeout_sec=1800.0,
            recover_action=lambda: extracted.append(target.host),
            extract_after_sec=extract_after,
            confirm_timeout_sec=confirm,
        )

    def test_toggle_extract_confirm_stuck_and_blind(self):
        guard, clock, extracted = _guard(), FakeClock(), []
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        target, lease = _reserve(guard)
        self._track_toggle(probe, target, lease, extracted, extract_after=90.0, confirm=100.0)

        # Pre-extract healthy data must not confirm after extract fires.
        rep.publish(_healthy_results())
        clock.advance(100.0)
        probe.tick()
        assert extracted == ["h1"]
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"]
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "pre-extract data ignored"

        rep.publish(_healthy_results())
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == [] and probe.pending() == []

        # Failed confirm → stuck in confirm phase, budget held.
        guard, clock, extracted = _guard(), FakeClock(), []
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        target, lease = _reserve(guard)
        self._track_toggle(probe, target, lease, extracted, extract_after=90.0, confirm=100.0)
        clock.advance(100.0)
        probe.tick()
        rep.publish({"h1": _error_entry(), "h2": _hc("h2", vdisks=[_vdisk("1-1-0", "RED")])})
        clock.advance(150.0)
        rep.publish(rep.last_results)
        stuck = probe.tick()
        assert len(stuck) == 1 and stuck[0].phase == PHASE_CONFIRM
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"]

        # Blind: extract still fires, no release.
        guard, clock, extracted = _guard(), FakeClock(), []
        probe = RecoveryProbe(
            guard=guard, hc_source=StubReporter(clock), min_hold_sec=0.0, clock=clock
        )
        target, lease = _reserve(guard)
        self._track_toggle(probe, target, lease, extracted, extract_after=90.0)
        clock.advance(100.0)
        probe.tick()
        assert extracted == ["h1"]
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"]

    def test_failed_extract_is_retried_then_confirmed(self):
        guard, clock, calls = _guard(), FakeClock(), {"n": 0}
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        target, lease = _reserve(guard)

        def flaky_extract():
            calls["n"] += 1
            if calls["n"] < 2:
                raise RuntimeError("agent unreachable")

        probe.track(
            lease, target, "StopStart",
            recovered=_node_predicate(target),
            stuck_timeout_sec=1800.0,
            recover_action=flaky_extract,
            extract_after_sec=90.0,
            confirm_timeout_sec=300.0,
        )
        clock.advance(100.0)
        probe.tick()
        assert calls["n"] == 1 and probe.pending()[0].extract_ok is False
        assert probe.pending()[0].phase == PHASE_CONFIRM

        probe.tick()  # retry
        assert calls["n"] == 2 and probe.pending()[0].extract_ok is True

        rep.publish(_healthy_results())
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == [] and probe.pending() == []

        # Drain also retries a failed extract (CONFIRM without extract_ok).
        guard, clock, calls = _guard(), FakeClock(), {"n": 0}
        probe = RecoveryProbe(
            guard=guard, hc_source=StubReporter(clock), min_hold_sec=0.0, clock=clock
        )
        target, lease = _reserve(guard)

        def always_fail_then_ok():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom")

        probe.track(
            lease, target, "StopStart",
            recovered=_node_predicate(target),
            stuck_timeout_sec=1800.0,
            recover_action=always_fail_then_ok,
            extract_after_sec=90.0,
            confirm_timeout_sec=300.0,
        )
        clock.advance(100.0)
        probe.tick()
        assert calls["n"] == 1 and probe.pending()[0].extract_ok is False
        assert probe.drain_extracts() == 1 and calls["n"] == 2
        assert probe.pending()[0].extract_ok is True
        assert probe.drain_extracts() == 0

    def test_drain_and_untrack(self):
        guard, clock, extracted, seen = _guard(), FakeClock(), [], []
        rep = StubReporter(clock)
        probe = RecoveryProbe(
            guard=guard, hc_source=rep, on_stuck=seen.append, min_hold_sec=0.0, clock=clock
        )
        toggle, toggle_lease = _reserve(guard, "h1", 1)
        self_healing, self_lease = _reserve(guard, "h2", 2)
        self._track_toggle(probe, toggle, toggle_lease, extracted, extract_after=600.0)
        probe.track(self_lease, self_healing, "KillNode",
                    recovered=_node_predicate(self_healing), stuck_timeout_sec=100.0)

        assert probe.drain_extracts() == 1 and extracted == ["h1"]
        assert probe.drain_extracts() == 0  # already confirming
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1", "dc1/r2"]

        # Explicit extract untracks; no stuck over a released lease.
        clock.advance(200.0)
        rep.publish({"h1": _error_entry(), "h2": _hc("h2")})
        guard.record_extract("manual", self_healing, ImpactScope.NODE)
        assert probe.untrack_identity(self_healing.identity_key()) == 1
        assert probe.tick() == [] and seen == []

        rep.publish(_healthy_results())
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == [] and probe.pending() == []


class TestBlindness:
    def test_startup_grace_then_lose_and_regain_sight(self):
        guard, clock = _guard(), FakeClock()
        blind_calls, sighted_calls = [], []
        rep = StubReporter(clock)
        probe = RecoveryProbe(
            guard=guard, hc_source=rep,
            on_blind=lambda: blind_calls.append(1),
            on_sighted=lambda: sighted_calls.append(1),
            min_hold_sec=0.0, clock=clock,
        )
        target, lease = _reserve(guard)
        probe.track(lease, target, "KillNode", recovered=_node_predicate(target),
                    stuck_timeout_sec=100.0)

        probe.tick()
        assert blind_calls == [] and probe.snapshot()["blind"] is True
        clock.advance(200.0)
        probe.tick()
        assert blind_calls == [1]

        rep.publish(_healthy_results())
        probe.tick()
        assert probe.snapshot()["blind"] is False
        assert guard.snapshot()["impaired_racks"] == []

        clock.advance(200.0)
        probe.tick()
        assert blind_calls == [1, 1], "losing sight after being sighted reports at once"
        rep.publish(_healthy_results())
        probe.tick()
        assert sighted_calls == [1, 1]


def test_stuck_fault_carries_phase():
    fault = StuckFault(
        lease_id="l1",
        nemesis_type="KillNode",
        target=ChaosTarget.for_node("h1", node_id=1),
        held_sec=400.0,
        timeout_sec=300.0,
        phase=PHASE_CONFIRM,
    )
    assert fault.target.identity_key() == "node:1:h1" and fault.phase == "confirm"
