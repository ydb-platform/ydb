"""The legacy per-type loop: probe-tracked accounting for injects/extracts."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import yaml

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import dispatch as build_dispatch
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelGuard,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.schedule_loop import (
    OrchestratorNemesisSchedule,
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


class RecordingProbe:
    def __init__(self, baseline: int | None = 8) -> None:
        self.tracked: list = []
        self.untracked: list = []
        self._baseline = baseline

    def alive_compute_baseline(self):
        return self._baseline

    def track(self, lease_id, target, nemesis_type, **kwargs):
        self.tracked.append((lease_id, target, nemesis_type, kwargs))

    def untrack_identity(self, identity_key: str) -> int:
        self.untracked.append(identity_key)
        return 1


class ChaosStoreStub:
    def __init__(self) -> None:
        self.extract_plans: list = []

    def plan_extract_target(self, nemesis_type, target):
        self.extract_plans.append((nemesis_type, target))
        return [build_dispatch(nemesis_type, target, "extract", {})]


def _schedule(guard, probe, dispatched, store) -> OrchestratorNemesisSchedule:
    sched = OrchestratorNemesisSchedule(
        chaos_store=store,
        get_hosts=lambda: ["h1", "h2", "h3", "h4"],
        is_local_host=lambda h: False,
        get_app_port=lambda: 0,
        failure_guard=guard,
        recovery_probe=probe,
    )

    def _fake_dispatch(cmd, track_history):
        dispatched.append(cmd)
        return True

    sched.dispatch_command = _fake_dispatch
    return sched


class TestLegacyLoopAccounting:
    def test_inject_extract_and_self_healing_wiring(self):
        guard, probe, dispatched, store = _guard(), RecordingProbe(), [], ChaosStoreStub()
        sched = _schedule(guard, probe, dispatched, store)
        target = ChaosTarget.for_node("h1", node_id=1)

        # Toggle: tracked with a probe-driven extract; lease holds until confirm.
        sched._dispatch_and_record(build_dispatch("StopStartNodeNemesis", target, "inject", {}))
        assert [c.action for c in dispatched] == ["inject"]
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"]
        lease, _, ntype, kwargs = probe.tracked[0]
        assert ntype == "StopStartNodeNemesis" and kwargs["extract_after_sec"] == 90.0
        kwargs["recover_action"]()
        assert [c.action for c in dispatched] == ["inject", "extract"]

        # Explicit extract releases by identity and untracks the probe.
        sched._dispatch_and_record(build_dispatch("StopStartNodeNemesis", target, "extract", {}))
        assert guard.snapshot()["impaired_racks"] == []
        assert probe.untracked == [target.identity_key()]

        # Self-healing: tracked without recover_action.
        probe.tracked.clear()
        sched._dispatch_and_record(
            build_dispatch("KillNodeNemesis", ChaosTarget.for_node("h2", node_id=2), "inject", {})
        )
        _, _, _, kwargs = probe.tracked[0]
        assert "recover_action" not in kwargs and kwargs["stuck_timeout_sec"] > 0

    def test_blind_slot_inject_is_skipped(self):
        guard, probe, dispatched, store = _guard(), RecordingProbe(baseline=None), [], ChaosStoreStub()
        sched = _schedule(guard, probe, dispatched, store)
        sched._dispatch_and_record(
            build_dispatch("KillSlotDaemonNemesis", ChaosTarget.for_slot("h1", slot_idx=1), "inject", {})
        )
        assert dispatched == [] and probe.tracked == []
        assert guard.snapshot()["impaired_slots"] == 0
