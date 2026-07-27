"""Fact-based recovery of reserved failure budget."""

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


def _reserve(guard, host="h1", node_id=1):
    target = ChaosTarget.for_node(host, node_id=node_id)
    lease = guard.reserve(
        guard.footprint_for(target, ImpactScope.NODE),
        recovery_sec=None,
        identity_key=target.identity_key(),
    )
    return target, lease


class TestSelfRecovery:
    def test_budget_is_released_once_the_target_is_back(self):
        guard, clock, recovered = _guard(), FakeClock(), set()
        probe = RecoveryProbe(
            guard=guard, recovered=lambda t: t.host in recovered, min_hold_sec=30.0, clock=clock
        )
        target, lease = _reserve(guard)
        probe.track(lease, target, "KillNode", timeout_sec=300.0)

        recovered.add("h1")
        clock.advance(10.0)
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "min-hold ignores early signals"

        clock.advance(40.0)
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == [] and probe.pending() == []

    def test_a_stuck_fault_keeps_the_budget_and_is_reported_once(self):
        guard, clock, seen = _guard(), FakeClock(), []
        probe = RecoveryProbe(
            guard=guard, recovered=lambda t: False, on_stuck=seen.append,
            min_hold_sec=30.0, clock=clock,
        )
        target, lease = _reserve(guard)
        probe.track(lease, target, "KillNode", timeout_sec=100.0)

        clock.advance(50.0)
        assert probe.tick() == [], "not stuck before the timeout"

        clock.advance(60.0)
        stuck = probe.tick()
        assert len(stuck) == 1 and stuck[0].target.host == "h1"

        clock.advance(60.0)
        assert probe.tick() == [] and len(seen) == 1, "reported once, not every tick"
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "never silently released"


class TestToggleRecovery:
    def test_extract_runs_after_the_hold_and_releases(self):
        guard, clock, extracted = _guard(), FakeClock(), []
        probe = RecoveryProbe(
            guard=guard,
            recovered=lambda t: (_ for _ in ()).throw(AssertionError("healthcheck must not run")),
            min_hold_sec=30.0,
            clock=clock,
        )
        target, lease = _reserve(guard)
        probe.track(
            lease, target, "TimeSkewNemesis", timeout_sec=90.0,
            recover_action=lambda: extracted.append(target.host),
        )

        clock.advance(60.0)
        probe.tick()
        assert extracted == [] and guard.snapshot()["impaired_racks"] == ["dc1/r1"]

        clock.advance(40.0)
        probe.tick()
        assert extracted == ["h1"] and guard.snapshot()["impaired_racks"] == []

    def test_toggle_faults_are_never_reported_stuck(self):
        guard, clock, seen = _guard(), FakeClock(), []
        probe = RecoveryProbe(
            guard=guard, recovered=lambda t: False, on_stuck=seen.append,
            min_hold_sec=0.0, clock=clock,
        )
        target, lease = _reserve(guard)
        probe.track(lease, target, "StopStart", timeout_sec=50.0, recover_action=lambda: None)
        clock.advance(1000.0)
        assert probe.tick() == [] and seen == []


class TestDrainExtracts:
    def test_drain_extracts_toggles_early_and_leaves_the_rest_tracked(self):
        guard, clock, extracted = _guard(), FakeClock(), []
        probe = RecoveryProbe(
            guard=guard, recovered=lambda t: False, min_hold_sec=30.0, clock=clock
        )
        toggle, toggle_lease = _reserve(guard, "h1", 1)
        self_healing, self_lease = _reserve(guard, "h2", 2)
        probe.track(
            toggle_lease, toggle, "StopStart", timeout_sec=600.0,
            recover_action=lambda: extracted.append(toggle.host),
        )
        probe.track(self_lease, self_healing, "KillNode", timeout_sec=300.0)

        assert probe.drain_extracts() == 1, "only the toggle has something to extract"
        assert extracted == ["h1"], "dispatched even though the hold window is far from over"
        assert [p.lease_id for p in probe.pending()] == [self_lease], "self-healing stays tracked"
        assert guard.snapshot()["impaired_racks"] == ["dc1/r2"], "and keeps its budget"

        assert probe.drain_extracts() == 0 and extracted == ["h1"], "idempotent"


class TestHealthcheckRecovery:
    def test_recovered_when_the_endpoint_answers(self):
        reporter = type("R", (), {"last_results": {}})()
        recovered = healthcheck_recovery(reporter)
        target = ChaosTarget.for_node("h1", node_id=1)

        assert recovered(target) is False, "no data yet is not recovery"
        reporter.last_results = {"h1": {"self_check_result": "HC_REQUEST_ERROR"}}
        assert recovered(target) is False
        reporter.last_results = {"h1": {"self_check_result": "DEGRADED"}}
        assert recovered(target) is True, "degraded but answering counts as back"


def test_stuck_fault_is_serializable_for_the_problem_log():
    fault = StuckFault(
        lease_id="l1",
        nemesis_type="KillNode",
        target=ChaosTarget.for_node("h1", node_id=1),
        held_sec=400.0,
        timeout_sec=300.0,
    )
    assert fault.target.identity_key() == "node:1:h1"
