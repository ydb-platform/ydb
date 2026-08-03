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
    def __init__(self) -> None:
        self.tracked: list = []
        self.untracked: list = []

    def alive_compute_baseline(self):
        return 8

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
    sched.dispatch_command = lambda cmd, track_history: dispatched.append(cmd)
    return sched


class TestLegacyLoopAccounting:
    def test_toggle_inject_is_tracked_with_a_probe_driven_extract(self):
        # Lease blocks planner toggle-back, so the probe must drive the extract.
        guard, probe, dispatched, store = _guard(), RecordingProbe(), [], ChaosStoreStub()
        sched = _schedule(guard, probe, dispatched, store)
        target = ChaosTarget.for_node("h1", node_id=1)
        cmd = build_dispatch("StopStartNodeNemesis", target, "inject", {})

        sched._dispatch_and_record(cmd)

        assert [c.action for c in dispatched] == ["inject"]
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "held until the probe confirms"
        lease, tracked_target, ntype, kwargs = probe.tracked[0]
        assert lease == cmd.execution_id and ntype == "StopStartNodeNemesis"
        assert kwargs["extract_after_sec"] == 90.0
        assert kwargs["confirm_timeout_sec"] > 0 and kwargs["stuck_timeout_sec"] > 0
        kwargs["recover_action"]()
        assert store.extract_plans == [("StopStartNodeNemesis", target)]
        assert [c.action for c in dispatched] == ["inject", "extract"]

    def test_self_healing_inject_is_tracked_without_an_extract(self):
        guard, probe, dispatched, store = _guard(), RecordingProbe(), [], ChaosStoreStub()
        sched = _schedule(guard, probe, dispatched, store)
        cmd = build_dispatch("KillNodeNemesis", ChaosTarget.for_node("h1", node_id=1), "inject", {})

        sched._dispatch_and_record(cmd)

        _, _, _, kwargs = probe.tracked[0]
        assert "recover_action" not in kwargs and kwargs["stuck_timeout_sec"] > 0

    def test_extract_releases_and_untracks_by_identity(self):
        guard, probe, dispatched, store = _guard(), RecordingProbe(), [], ChaosStoreStub()
        sched = _schedule(guard, probe, dispatched, store)
        target = ChaosTarget.for_node("h1", node_id=1)
        sched._dispatch_and_record(build_dispatch("StopStartNodeNemesis", target, "inject", {}))

        sched._dispatch_and_record(build_dispatch("StopStartNodeNemesis", target, "extract", {}))

        assert guard.snapshot()["impaired_racks"] == []
        assert probe.untracked == [target.identity_key()]

    def test_blind_slot_inject_is_skipped(self):
        class BlindProbe(RecordingProbe):
            def alive_compute_baseline(self):
                return None

        guard, probe, dispatched, store = _guard(), BlindProbe(), [], ChaosStoreStub()
        sched = _schedule(guard, probe, dispatched, store)
        cmd = build_dispatch(
            "KillSlotDaemonNemesis", ChaosTarget.for_slot("h1", slot_idx=1), "inject", {}
        )

        sched._dispatch_and_record(cmd)

        assert dispatched == []
        assert probe.tracked == []
        assert guard.snapshot()["impaired_slots"] == 0
