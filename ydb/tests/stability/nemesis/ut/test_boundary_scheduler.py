"""The boundary-walking scheduler: tick, recovery wiring, lifecycle."""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import pytest
import yaml

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import dispatch as build_dispatch
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.boundary_scheduler import (
    BoundaryNemesisScheduler,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_state import (
    datacenter_inject_fanout,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelGuard,
    Footprint,
    GuardMode,
    ImpactScope,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import RecoveryProbe


def _guard(erasure: str, **kwargs) -> FailureModelGuard:
    hosts = [
        {"name": f"h{i}", "location": {"rack": f"r{i}", "data_center": "dc1"}} for i in (1, 2, 3, 4)
    ]
    fd, path = tempfile.mkstemp(suffix=".yaml")
    os.close(fd)
    Path(path).write_text(
        yaml.safe_dump({"static_erasure": erasure, "hosts": hosts}), encoding="utf-8"
    )
    return FailureModelGuard(ClusterTopologyModel(path), **kwargs)


class FakeInventory:
    def __init__(self, targets: list[ChaosTarget]) -> None:
        self._targets = list(targets)

    def entities(self, kind: TargetKind) -> list[ChaosTarget]:
        return [t for t in self._targets if t.kind is kind]


class ScriptedRandom:
    """Deterministic stand-in: fixed randint / uniform, always first menu entry."""

    def __init__(self, *, randint: int = 1, uniform: float = 0.0) -> None:
        self._randint = randint
        self._uniform = uniform

    def randint(self, a: int, b: int) -> int:
        return max(a, min(b, self._randint))

    def uniform(self, a: float, b: float) -> float:
        return self._uniform

    def choice(self, seq):
        return seq[0]


class RecordingProbe:
    def __init__(self, baseline: int | None = 8) -> None:
        self.tracked: list = []
        self._baseline = baseline

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def drain_extracts(self) -> int:
        return 0

    def snapshot(self) -> dict:
        return {"tracked": len(self.tracked)}

    def alive_compute_baseline(self) -> int | None:
        return self._baseline

    def track(self, lease_id, target, nemesis_type, **kwargs):
        self.tracked.append((lease_id, target, nemesis_type, kwargs))


class LiveReporter:
    def __init__(self) -> None:
        self.last_results: dict = {}
        self.last_update: float | None = None

    def publish(self, results: dict) -> None:
        self.last_results = results
        self.last_update = time.monotonic()


def _healthy(*hosts: str) -> dict:
    return {h: {"self_check_result": "GOOD", "database_status": []} for h in hosts}


def _make_scheduler(guard, inventory, dispatched, **overrides):
    kwargs = dict(
        guard=guard,
        inventory=inventory,
        plan_inject=lambda ntype, target: [build_dispatch(ntype, target, "inject", {})],
        dispatch=lambda cmd: dispatched.append(cmd),
        enabled_types=["KillNode"],
        scope_for=lambda t: ImpactScope.NODE,
        kind_for=lambda t: TargetKind.NODE,
        mode_for=lambda t: GuardMode.FULL,
        recovery_sec_for=lambda t: None,
        recovery_probe=RecordingProbe(),
    )
    kwargs.update(overrides)
    return BoundaryNemesisScheduler(**kwargs)


def _nodes() -> list[ChaosTarget]:
    return [ChaosTarget.for_node(f"h{i}", node_id=i) for i in (1, 2, 3, 4)]


class TestTick:
    @pytest.mark.parametrize(
        "erasure,max_per_tick,expected",
        [
            ("block-4-2", 5, 2),   # budget is the limit
            ("mirror-3-dc", 3, 3),  # one DC realm fits under the fuse
            ("mirror-3-dc", 2, 2),  # fuse bounds a roomier budget
        ],
    )
    def test_tick_respects_budget_and_fuse(self, erasure, max_per_tick, expected):
        dispatched: list = []
        sched = _make_scheduler(
            _guard(erasure), FakeInventory(_nodes()), dispatched,
            max_per_tick=max_per_tick, rng=ScriptedRandom(randint=max_per_tick),
        )
        assert sched.tick() == expected
        assert len({c.target.host for c in dispatched}) == expected

    def test_slot_budget_fills_then_idles(self):
        guard = _guard("block-4-2", total_slots=10)  # max_slots = 3
        dispatched: list = []
        sched = _make_scheduler(
            guard, FakeInventory([ChaosTarget.for_slot("h1", slot_idx=i) for i in range(5)]),
            dispatched,
            enabled_types=["KillSlot"],
            scope_for=lambda t: ImpactScope.SLOT,
            kind_for=lambda t: TargetKind.SLOT,
            max_per_tick=16, rng=ScriptedRandom(randint=1),
        )
        assert sched.tick() == 3
        assert guard.snapshot()["impaired_racks"] == []
        assert sched.tick() == 0, "budget full"

    def test_menu_mutes_and_offers(self):
        # Enabled types appear; FULL without probe / toggle without extract are muted.
        sched = _make_scheduler(
            _guard("block-4-2"), FakeInventory(_nodes()), [], enabled_types=["A", "B"]
        )
        assert {item[0] for item in sched._menu()} == {"A", "B"}

        muted = _make_scheduler(
            _guard("block-4-2"), FakeInventory(_nodes()), [], recovery_probe=None,
        )
        assert muted._menu() == [] and muted.tick() == 0

        toggle = _make_scheduler(
            _guard("block-4-2"), FakeInventory(_nodes()), [],
            enabled_types=["Toggle"], recovery_mode_for=lambda t: "extract",
        )
        assert toggle._menu() == [] and toggle.tick() == 0

    def test_blind_slots_pause_without_blocking_other_types(self):
        dispatched: list = []
        sched = _make_scheduler(
            _guard("block-4-2", total_slots=10),
            FakeInventory(_nodes() + [ChaosTarget.for_slot("h1", slot_idx=1)]),
            dispatched,
            enabled_types=["KillSlot", "KillNode"],
            scope_for=lambda t: ImpactScope.SLOT if t == "KillSlot" else ImpactScope.NODE,
            kind_for=lambda t: TargetKind.SLOT if t == "KillSlot" else TargetKind.NODE,
            recovery_probe=RecordingProbe(baseline=None),
            max_per_tick=1, rng=ScriptedRandom(randint=1),
        )
        assert sched.tick() == 1
        assert dispatched[0].nemesis_type == "KillNode"

        slots_only = _make_scheduler(
            _guard("block-4-2", total_slots=10),
            FakeInventory([ChaosTarget.for_slot("h1", slot_idx=i) for i in range(3)]),
            [],
            enabled_types=["KillSlot"],
            scope_for=lambda t: ImpactScope.SLOT,
            kind_for=lambda t: TargetKind.SLOT,
            recovery_probe=RecordingProbe(baseline=None),
            max_per_tick=5, rng=ScriptedRandom(randint=5),
        )
        assert slots_only.tick() == 0

    def test_dispatch_failures_release_only_when_nothing_landed(self):
        # Planner boom before any dispatch → lease freed.
        guard = _guard("block-4-2")
        sched = _make_scheduler(
            guard, FakeInventory(_nodes()), [],
            plan_inject=lambda n, t: (_ for _ in ()).throw(RuntimeError("planner")),
            max_per_tick=1, rng=ScriptedRandom(randint=1),
        )
        with pytest.raises(RuntimeError):
            sched.tick()
        assert guard.snapshot()["tracked_executions"] == 0

        # Partial fanout then failure → budget stays charged and tracked.
        guard, probe, dispatched, calls = _guard("block-4-2"), RecordingProbe(), [], {"n": 0}

        def flaky_fanout(ntype, target):
            return [
                build_dispatch(ntype, ChaosTarget.for_node("h1", node_id=1), "inject", {}),
                build_dispatch(ntype, ChaosTarget.for_node("h2", node_id=2), "inject", {}),
            ]

        def flaky_dispatch(cmd):
            calls["n"] += 1
            dispatched.append(cmd)
            if calls["n"] >= 2:
                raise RuntimeError("agent unreachable")

        sched = _make_scheduler(
            guard, FakeInventory(_nodes()), dispatched,
            plan_inject=flaky_fanout, dispatch=flaky_dispatch, recovery_probe=probe,
            max_per_tick=1, rng=ScriptedRandom(randint=1),
        )
        with pytest.raises(RuntimeError):
            sched.tick()
        assert len(dispatched) == 2
        assert guard.snapshot()["tracked_executions"] == 1
        assert len(probe.tracked) == 1


class TestProfile:
    @pytest.mark.parametrize("uniform,expected", [(-0.5, 30.0), (0.5, 90.0)])
    def test_sleep_and_status(self, uniform, expected):
        sched = _make_scheduler(
            _guard("block-4-2"), FakeInventory(_nodes()), [],
            base_interval=60.0, jitter=0.5, rng=ScriptedRandom(uniform=uniform),
        )
        assert sched._sleep_seconds() == pytest.approx(expected)
        sched.set_profile(enabled=["X"], base_interval=10.0, jitter=0.25, max_per_tick=7)
        status = sched.status()
        assert (status["enabled_types"], status["base_interval"], status["max_per_tick"]) == (
            ["X"], 10.0, 7,
        )


class TestDatacenterFanout:
    def test_chosen_dc_fans_out_to_its_own_hosts(self):
        cmds = datacenter_inject_fanout(
            "DataCenterStopNodesNemesis",
            ChaosTarget.for_datacenter("h1", "dc1"),
            FakeInventory([
                ChaosTarget.for_datacenter("h1", "dc1"),
                ChaosTarget.for_datacenter("h2", "dc1"),
                ChaosTarget.for_datacenter("h3", "dc2"),
            ]),
        )
        assert {c.target.host for c in cmds} == {"h1", "h2"}
        assert len({c.scenario_id for c in cmds}) == 1


class TestToggleRecovery:
    def _toggle_scheduler(self, guard, dispatched, probe, hold_sec=90.0):
        return _make_scheduler(
            guard, FakeInventory(_nodes()), dispatched,
            enabled_types=["Toggle"],
            recovery_mode_for=lambda t: "extract",
            recovery_sec_for=lambda t: hold_sec,
            plan_extract=lambda ntype, target: [build_dispatch(ntype, target, "extract", {})],
            recovery_probe=probe,
            max_per_tick=1, rng=ScriptedRandom(randint=1),
        )

    def test_toggle_tracks_extract_and_stop_drains_to_confirm(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        probe = RecordingProbe()
        assert self._toggle_scheduler(guard, dispatched, probe).tick() == 1
        assert len(guard.snapshot()["impaired_racks"]) == 1
        _, target, ntype, kwargs = probe.tracked[0]
        assert ntype == "Toggle" and kwargs["extract_after_sec"] == 90.0
        kwargs["recover_action"]()
        assert [c.action for c in dispatched if c.action == "extract"]

        # Live probe: stop drains extract; budget waits for HC confirm.
        guard, dispatched = _guard("block-4-2"), []
        rep = LiveReporter()
        live = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, poll_interval=3600.0)
        sched = self._toggle_scheduler(guard, dispatched, live, hold_sec=600.0)
        assert sched.tick() == 1
        sched.stop()
        assert [c.action for c in dispatched] == ["inject", "extract"]
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"]
        rep.publish(_healthy("h1", "h2", "h3", "h4"))
        live.tick()
        assert guard.snapshot()["impaired_racks"] == [] and live.pending() == []


class TestBypassAndFill:
    def test_bypass_cap_and_dedup_with_budget_fill(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        sched = _make_scheduler(
            guard,
            FakeInventory(_nodes() + [ChaosTarget.for_tablet("h1")]),
            dispatched,
            enabled_types=["KillNode", "KillHive", "ReBalance"],
            kind_for=lambda t: (
                TargetKind.TABLET if t in ("KillHive", "ReBalance") else TargetKind.NODE
            ),
            mode_for=lambda t: (
                GuardMode.BYPASS if t in ("KillHive", "ReBalance") else GuardMode.FULL
            ),
            max_per_tick=16, max_bypass_per_tick=1, rng=ScriptedRandom(randint=1),
        )
        assert sched.tick() == 3
        kinds = [c.nemesis_type for c in dispatched]
        assert kinds.count("KillNode") == 2
        assert sum(1 for k in kinds if k in ("KillHive", "ReBalance")) == 1

        # Exhausted budget still lets bypass fire; each tablet type once.
        for rack in ("r1", "r2"):
            guard.reserve(Footprint(racks=frozenset({rack})), identity_key=rack)
        dispatched.clear()
        bypass = _make_scheduler(
            guard, FakeInventory([ChaosTarget.for_tablet("h1")]), dispatched,
            enabled_types=["KillHive", "ReBalance"],
            kind_for=lambda t: TargetKind.TABLET,
            mode_for=lambda t: GuardMode.BYPASS,
            max_per_tick=3, max_bypass_per_tick=3, rng=ScriptedRandom(randint=3),
        )
        assert bypass.tick() == 2
        assert {c.nemesis_type for c in dispatched} == {"KillHive", "ReBalance"}
        assert len(guard.snapshot()["impaired_racks"]) == 2


class TestLifecycle:
    def test_start_then_stop_terminates_the_thread(self):
        dispatched: list = []
        sched = _make_scheduler(
            _guard("block-4-2"), FakeInventory(_nodes()), dispatched,
            base_interval=1000.0, jitter=0.0, max_per_tick=2, rng=ScriptedRandom(randint=2),
        )
        sched.start()
        deadline = time.monotonic() + 2.0
        while not dispatched and time.monotonic() < deadline:
            time.sleep(0.01)
        sched.stop()
        assert dispatched and sched._thread is not None and not sched._thread.is_alive()
