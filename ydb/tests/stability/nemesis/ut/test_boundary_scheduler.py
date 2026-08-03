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
    """Deterministic stand-in for random.Random: always the same cap and the first menu entry."""

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
    """Duck-typed hc_source stamping real monotonic time."""

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
    def test_tick_stops_at_the_budget(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        sched = _make_scheduler(
            guard, FakeInventory(_nodes()), dispatched,
            max_per_tick=5, rng=ScriptedRandom(randint=5),
        )
        assert sched.tick() == 2, "block-4-2 tolerates 2 domains, whatever the cap"
        assert len({c.target.host for c in dispatched}) == 2, "and not twice the same host"

    def test_tick_fills_the_cap_while_the_budget_allows(self):
        # mirror-3-dc over 4 racks of one DC: the whole realm may be sacrificed.
        dispatched: list = []
        sched = _make_scheduler(
            _guard("mirror-3-dc"), FakeInventory(_nodes()), dispatched,
            max_per_tick=3, rng=ScriptedRandom(randint=3),
        )
        assert sched.tick() == 3

    def test_menu_offers_every_enabled_type(self):
        sched = _make_scheduler(
            _guard("block-4-2"), FakeInventory(_nodes()), [], enabled_types=["A", "B"]
        )
        assert {item[0] for item in sched._menu()} == {"A", "B"}, "no weights, no muting"

    def test_slot_tick_caps_at_the_slot_budget(self):
        guard = _guard("block-4-2", total_slots=10)  # max_slots = 3
        dispatched: list = []
        sched = _make_scheduler(
            guard, FakeInventory([ChaosTarget.for_slot("h1", slot_idx=i) for i in range(5)]),
            dispatched,
            enabled_types=["KillSlot"],
            scope_for=lambda t: ImpactScope.SLOT,
            kind_for=lambda t: TargetKind.SLOT,
            max_per_tick=5, rng=ScriptedRandom(randint=5),
        )
        assert sched.tick() == 3
        assert guard.snapshot()["impaired_racks"] == [], "slots cost no fail domain"

    def test_full_types_are_muted_without_a_probe(self):
        # No observability -> no chaos: without the probe a lease is never released.
        sched = _make_scheduler(
            _guard("block-4-2"), FakeInventory(_nodes()), [], recovery_probe=None,
        )
        assert sched._menu() == [] and sched.tick() == 0

    def test_slot_chaos_pauses_when_the_probe_is_blind(self):
        sched = _make_scheduler(
            _guard("block-4-2", total_slots=10),
            FakeInventory([ChaosTarget.for_slot("h1", slot_idx=i) for i in range(5)]),
            [],
            enabled_types=["KillSlot"],
            scope_for=lambda t: ImpactScope.SLOT,
            kind_for=lambda t: TargetKind.SLOT,
            recovery_probe=RecordingProbe(baseline=None),  # blind: no fresh healthcheck data
            max_per_tick=5, rng=ScriptedRandom(randint=5),
        )
        assert sched.tick() == 0, "a slot inject without a baseline is not observable"

    def test_blind_slots_do_not_block_other_types(self):
        dispatched: list = []
        sched = _make_scheduler(
            _guard("block-4-2", total_slots=10),
            FakeInventory(_nodes() + [ChaosTarget.for_slot("h1", slot_idx=1)]),
            dispatched,
            enabled_types=["KillSlot", "KillNode"],
            scope_for=lambda t: ImpactScope.SLOT if t == "KillSlot" else ImpactScope.NODE,
            kind_for=lambda t: TargetKind.SLOT if t == "KillSlot" else TargetKind.NODE,
            recovery_probe=RecordingProbe(baseline=None),
            max_per_tick=1, rng=ScriptedRandom(randint=1),  # choice() takes the first menu entry
        )
        assert sched.tick() == 1
        assert dispatched[0].nemesis_type == "KillNode", "the paused slot type steps aside"

    def test_lease_is_released_when_planning_raises(self):
        guard = _guard("block-4-2")

        def bad_plan(ntype, target):
            raise RuntimeError("planner exploded")

        sched = _make_scheduler(
            guard, FakeInventory(_nodes()), [],
            plan_inject=bad_plan,
            max_per_tick=1, rng=ScriptedRandom(randint=1),
        )
        with pytest.raises(RuntimeError):
            sched.tick()
        assert guard.snapshot()["tracked_executions"] == 0, "no silent lease leak"

    def test_partial_fanout_keeps_budget_and_tracks(self):
        # Partial fanout must keep the budget charged and stay tracked.
        guard = _guard("block-4-2")
        probe = RecordingProbe()
        dispatched: list = []
        calls = {"n": 0}

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
            plan_inject=flaky_fanout,
            dispatch=flaky_dispatch,
            recovery_probe=probe,
            max_per_tick=1, rng=ScriptedRandom(randint=1),
        )
        with pytest.raises(RuntimeError):
            sched.tick()
        assert len(dispatched) == 2
        assert guard.snapshot()["tracked_executions"] == 1, "budget stays charged"
        assert len(probe.tracked) == 1, "probe still watches the partial fault"

    def test_track_failure_after_dispatch_keeps_budget(self):
        guard = _guard("block-4-2")
        dispatched: list = []

        class BoomProbe(RecordingProbe):
            def track(self, *args, **kwargs):
                raise RuntimeError("probe.track exploded")

        sched = _make_scheduler(
            guard, FakeInventory(_nodes()), dispatched,
            recovery_probe=BoomProbe(),
            max_per_tick=1, rng=ScriptedRandom(randint=1),
        )
        with pytest.raises(RuntimeError):
            sched.tick()
        assert len(dispatched) == 1
        assert guard.snapshot()["tracked_executions"] == 1, "do not free budget after a landed fault"


class TestProfile:
    @pytest.mark.parametrize("uniform,expected", [(-0.5, 30.0), (0.5, 90.0)])
    def test_sleep_applies_jitter(self, uniform, expected):
        sched = _make_scheduler(
            _guard("block-4-2"), FakeInventory([]), [],
            base_interval=60.0, jitter=0.5, rng=ScriptedRandom(uniform=uniform),
        )
        assert sched._sleep_seconds() == pytest.approx(expected)

    def test_set_profile_and_status(self):
        sched = _make_scheduler(_guard("block-4-2"), FakeInventory(_nodes()), [])
        sched.set_profile(enabled=["X"], base_interval=10.0, jitter=0.25, max_per_tick=7)
        status = sched.status()
        assert status["running"] is False
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
        assert {c.target.host for c in cmds} == {"h1", "h2"}, "not the round-robin planner's DC"
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

    def test_toggle_holds_budget_and_tracks_its_extract(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        probe = RecordingProbe()
        assert self._toggle_scheduler(guard, dispatched, probe).tick() == 1
        assert len(guard.snapshot()["impaired_racks"]) == 1, "held, not expiring on a timer"

        lease, target, ntype, kwargs = probe.tracked[0]
        assert ntype == "Toggle"
        assert kwargs["extract_after_sec"] == 90.0
        assert kwargs["recover_action"] is not None
        assert kwargs["stuck_timeout_sec"] > 0 and kwargs["confirm_timeout_sec"] > 0
        kwargs["recover_action"]()
        extracts = [c for c in dispatched if c.action == "extract"]
        assert len(extracts) == 1 and extracts[0].target.host == target.host

    def test_toggle_is_muted_when_nothing_can_extract(self):
        # Without a plan_extract it would stay broken forever, so it must not be offered.
        sched = _make_scheduler(
            _guard("block-4-2"), FakeInventory(_nodes()), [],
            enabled_types=["Toggle"], recovery_mode_for=lambda t: "extract",
        )
        assert sched._menu() == [] and sched.tick() == 0

    def test_stop_extracts_what_is_still_held(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        rep = LiveReporter()
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, poll_interval=3600.0)
        sched = self._toggle_scheduler(guard, dispatched, probe, hold_sec=600.0)
        assert sched.tick() == 1
        assert [c.action for c in dispatched] == ["inject"]

        sched.stop()
        assert [c.action for c in dispatched] == ["inject", "extract"], "stop must not leave it applied"
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], (
            "the drain is not a release: the budget waits for the healthcheck confirm"
        )

        rep.publish(_healthy("h1", "h2", "h3", "h4"))
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == [] and probe.pending() == []


class TestFillSemantics:
    """A tick fills the budget up to the boundary; the cap is only a burst fuse."""

    def test_a_tick_fills_the_whole_slot_budget_and_then_goes_idle(self):
        guard = _guard("block-4-2", total_slots=12)  # max_slots = 3 (30%)
        dispatched: list = []
        sched = _make_scheduler(
            guard,
            FakeInventory([ChaosTarget.for_slot("h1", slot_idx=i) for i in range(8)]),
            dispatched,
            enabled_types=["KillSlot"],
            scope_for=lambda t: ImpactScope.SLOT,
            kind_for=lambda t: TargetKind.SLOT,
            max_per_tick=16, rng=ScriptedRandom(randint=1),
        )
        assert sched.tick() == 3, "the budget is the limit, not the old random cap"
        assert sched.tick() == 0, "budget full — nothing left to inject"

    def test_bypass_cap_is_independent_from_the_budget_fill(self):
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
        added = sched.tick()
        kinds = [c.nemesis_type for c in dispatched]
        assert kinds.count("KillNode") == 2, "budget filled (block-4-2: 2 domains)"
        assert sum(1 for k in kinds if k in ("KillHive", "ReBalance")) == 1, "bypass capped at 1"
        assert added == 3

    def test_the_fuse_bounds_the_burst_when_the_budget_is_roomier(self):
        # mirror-3-dc over 4 racks of one DC: the whole realm fits, but the fuse says 2.
        dispatched: list = []
        sched = _make_scheduler(
            _guard("mirror-3-dc"), FakeInventory(_nodes()), dispatched,
            max_per_tick=2, rng=ScriptedRandom(randint=1),
        )
        assert sched.tick() == 2


class TestBypass:
    def _bypass_scheduler(self, guard, dispatched, enabled, **overrides):
        return _make_scheduler(
            guard, FakeInventory([ChaosTarget.for_tablet("h1")]), dispatched,
            enabled_types=enabled,
            kind_for=lambda t: TargetKind.TABLET,
            mode_for=lambda t: GuardMode.BYPASS,
            **overrides,
        )

    def test_bypass_fires_with_the_budget_exhausted_and_reserves_nothing(self):
        guard = _guard("block-4-2")
        for rack in ("r1", "r2"):
            guard.reserve(Footprint(racks=frozenset({rack})), identity_key=rack)
        dispatched: list = []
        sched = self._bypass_scheduler(
            guard, dispatched, ["KillHive"], max_per_tick=1, rng=ScriptedRandom(randint=1)
        )
        assert sched.tick() == 1 and len(dispatched) == 1
        assert len(guard.snapshot()["impaired_racks"]) == 2, "no extra budget spent"

    def test_dedup_is_per_type_and_target(self):
        # Tablet types share one control-host target: each fires once, all of them fire.
        dispatched: list = []
        sched = self._bypass_scheduler(
            _guard("block-4-2"), dispatched, ["KillHive", "ReBalance"],
            max_per_tick=3, max_bypass_per_tick=3, rng=ScriptedRandom(randint=3),
        )
        assert sched.tick() == 2
        assert {c.nemesis_type for c in dispatched} == {"KillHive", "ReBalance"}


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
        assert dispatched, "the thread must have ticked"
        assert sched._thread is not None and not sched._thread.is_alive()
