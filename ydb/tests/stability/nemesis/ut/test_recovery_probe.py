"""Unit tests for fact-based recovery of reserved failure budget."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import yaml

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelGuard,
    ImpactScope,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import (
    RecoveryProbe,
    StuckFault,
    healthcheck_recovery,
)


def _guard() -> FailureModelGuard:
    hosts = [
        {"name": f"h{i}", "location": {"rack": f"r{i}", "data_center": "dc1"}}
        for i in (1, 2, 3, 4)
    ]
    fd, path = tempfile.mkstemp(suffix=".yaml")
    os.close(fd)
    Path(path).write_text(
        yaml.safe_dump({"static_erasure": "block-4-2", "hosts": hosts}), encoding="utf-8"
    )
    return FailureModelGuard(ClusterTopologyModel(path))


class FakeClock:
    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def _reserve(guard, host, node_id):
    target = ChaosTarget.for_node(host, node_id=node_id)
    fp = guard.footprint_for(target, ImpactScope.NODE)
    lease = guard.reserve(fp, recovery_sec=None, identity_key=target.identity_key())
    return target, lease


class TestRecoveryProbe:
    def test_recovered_fault_releases_budget(self):
        guard = _guard()
        clock = FakeClock()
        recovered_hosts = set()
        probe = RecoveryProbe(
            guard=guard,
            recovered=lambda t: t.host in recovered_hosts,
            min_hold_sec=30.0,
            default_timeout_sec=300.0,
            clock=clock,
        )
        target, lease = _reserve(guard, "h1", 1)
        probe.track(lease, target, "KillNode", timeout_sec=300.0)
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"]

        # Still within min-hold: recovery signal must be ignored.
        recovered_hosts.add("h1")
        clock.advance(10.0)
        assert probe.tick() == []
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "must not release before min-hold"

        # Past min-hold and recovered -> budget released.
        clock.advance(40.0)
        assert probe.tick() == []
        assert guard.snapshot()["impaired_racks"] == [], (
            f"recovered fault must release budget; snapshot={guard.snapshot()}"
        )
        assert probe.pending() == []

    def test_stuck_fault_reports_once_and_holds_budget(self):
        guard = _guard()
        clock = FakeClock()
        stuck_seen: list[StuckFault] = []
        probe = RecoveryProbe(
            guard=guard,
            recovered=lambda t: False,  # never recovers
            on_stuck=stuck_seen.append,
            min_hold_sec=30.0,
            default_timeout_sec=100.0,
            clock=clock,
        )
        target, lease = _reserve(guard, "h1", 1)
        probe.track(lease, target, "KillNode", timeout_sec=100.0)

        clock.advance(50.0)  # past min-hold, before timeout
        assert probe.tick() == [], "no stuck report before timeout"

        clock.advance(60.0)  # now past 100s timeout
        stuck = probe.tick()
        assert len(stuck) == 1 and stuck[0].target.host == "h1", (
            f"a fault past its timeout must be reported stuck; got {stuck}"
        )
        assert len(stuck_seen) == 1, "on_stuck must fire exactly once"
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], (
            "a stuck fault must KEEP holding budget, never silently release"
        )

        # Subsequent ticks must not re-report the same stuck fault.
        clock.advance(60.0)
        assert probe.tick() == []
        assert len(stuck_seen) == 1, "stuck fault must not be re-reported every tick"
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"]

    def test_forget_stops_tracking(self):
        guard = _guard()
        clock = FakeClock()
        probe = RecoveryProbe(
            guard=guard, recovered=lambda t: False, clock=clock, min_hold_sec=0.0
        )
        target, lease = _reserve(guard, "h1", 1)
        probe.track(lease, target, "KillNode", timeout_sec=10.0)
        probe.forget(lease)
        clock.advance(1000.0)
        assert probe.tick() == [], "a forgotten lease must not be polled or reported"


class TestToggleAutoExtract:
    def test_toggle_fault_extracts_then_releases_after_hold(self):
        guard = _guard()
        clock = FakeClock()
        extracted: list[str] = []
        probe = RecoveryProbe(
            guard=guard,
            recovered=lambda t: (_ for _ in ()).throw(AssertionError("healthcheck must not run")),
            min_hold_sec=30.0,
            clock=clock,
        )
        target, lease = _reserve(guard, "h1", 1)
        probe.track(
            lease, target, "TimeSkewNemesis", timeout_sec=90.0,
            recover_action=lambda: extracted.append(target.host),
        )
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"]

        clock.advance(60.0)  # past min-hold, before the hold window closes
        assert probe.tick() == []
        assert extracted == [], "must not extract before the hold elapses"
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "budget held through the hold window"

        clock.advance(40.0)  # now past the 90s hold
        assert probe.tick() == []
        assert extracted == ["h1"], "hold elapsed -> extract dispatched exactly once"
        assert guard.snapshot()["impaired_racks"] == [], "extract must release the budget"
        assert probe.pending() == []

    def test_toggle_fault_never_reports_stuck(self):
        guard = _guard()
        clock = FakeClock()
        stuck_seen: list[StuckFault] = []
        probe = RecoveryProbe(
            guard=guard,
            recovered=lambda t: False,
            on_stuck=stuck_seen.append,
            min_hold_sec=0.0,
            clock=clock,
        )
        target, lease = _reserve(guard, "h1", 1)
        probe.track(
            lease, target, "StopStartNodeNemesis", timeout_sec=50.0,
            recover_action=lambda: None,
        )
        clock.advance(1000.0)  # far past the hold
        assert probe.tick() == [], "a toggle fault is recovered by extract, never reported stuck"
        assert stuck_seen == []
        assert guard.snapshot()["impaired_racks"] == [], "released by extract, not held as stuck"


class TestDrainExtracts:
    """Switching chaos off must not leave toggle faults applied."""

    def test_drain_extracts_pending_toggle_faults_before_their_hold_elapsed(self):
        guard = _guard()
        clock = FakeClock()
        extracted: list[str] = []
        probe = RecoveryProbe(
            guard=guard, recovered=lambda t: False, min_hold_sec=30.0, clock=clock
        )
        target, lease = _reserve(guard, "h1", 1)
        probe.track(
            lease, target, "StopStartNodeNemesis", timeout_sec=600.0,
            recover_action=lambda: extracted.append(target.host),
        )
        clock.advance(5.0)  # still deep inside the hold window
        assert probe.tick() == [] and extracted == [], "nothing is due yet"

        assert probe.drain_extracts() == 1, "the pending toggle fault must be drained"
        assert extracted == ["h1"], (
            "drain must dispatch the extract even though the hold window has not elapsed"
        )
        assert guard.snapshot()["impaired_racks"] == [], (
            f"drained fault must release its budget; snapshot={guard.snapshot()}"
        )
        assert probe.pending() == [], "drained faults must stop being tracked"

    def test_drain_keeps_self_recovering_faults_tracked(self):
        guard = _guard()
        clock = FakeClock()
        probe = RecoveryProbe(
            guard=guard, recovered=lambda t: False, min_hold_sec=0.0, clock=clock
        )
        target, lease = _reserve(guard, "h1", 1)
        probe.track(lease, target, "KillNodeNemesis", timeout_sec=300.0)  # no recover_action

        assert probe.drain_extracts() == 0, (
            "a self-recovering fault has nothing to extract — it must not be drained"
        )
        assert [p.lease_id for p in probe.pending()] == [lease], (
            "self-recovering faults stay tracked so a restarted probe keeps polling them"
        )
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], (
            "draining must not release a fault that is still down"
        )

    def test_drain_is_idempotent(self):
        guard = _guard()
        extracted: list[str] = []
        probe = RecoveryProbe(
            guard=guard, recovered=lambda t: False, min_hold_sec=0.0, clock=FakeClock()
        )
        target, lease = _reserve(guard, "h1", 1)
        probe.track(
            lease, target, "TimeSkewNemesis", timeout_sec=120.0,
            recover_action=lambda: extracted.append(target.host),
        )
        assert probe.drain_extracts() == 1
        assert probe.drain_extracts() == 0, "a second drain must not re-dispatch extracts"
        assert extracted == ["h1"]


class TestHealthcheckRecovery:
    def test_recovered_when_endpoint_answers(self):
        reporter = type("R", (), {"last_results": {}})()
        recovered = healthcheck_recovery(reporter)
        t = ChaosTarget.for_node("h1", node_id=1)

        assert recovered(t) is False, "no healthcheck data yet must read as not-recovered"

        reporter.last_results = {"h1": {"self_check_result": "HC_REQUEST_ERROR"}}
        assert recovered(t) is False, "endpoint error means the host is still down"

        reporter.last_results = {"h1": {"self_check_result": "GOOD"}}
        assert recovered(t) is True, "endpoint answering GOOD means recovered"

        reporter.last_results = {"h1": {"self_check_result": "DEGRADED"}}
        assert recovered(t) is True, "a degraded-but-answering endpoint still counts as back"
