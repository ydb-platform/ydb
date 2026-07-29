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
        return list(self._targets)


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
    def __init__(self) -> None:
        self.tracked: list = []

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def drain_extracts(self) -> int:
        return 0

    def snapshot(self) -> dict:
        return {"tracked": len(self.tracked)}

    def track(self, lease_id, target, nemesis_type, timeout_sec=None, recover_action=None):
        self.tracked.append((lease_id, target, nemesis_type, timeout_sec, recover_action))


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
        default_recovery_sec=300.0,
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

        lease, target, ntype, timeout, action = probe.tracked[0]
        assert (ntype, timeout) == ("Toggle", 90.0) and action is not None
        action()
        extracts = [c for c in dispatched if c.action == "extract"]
        assert len(extracts) == 1 and extracts[0].target.host == target.host

    def test_toggle_is_muted_when_nothing_can_extract(self):
        # Without a probe / plan_extract it would stay broken forever, so it must not be offered.
        sched = _make_scheduler(
            _guard("block-4-2"), FakeInventory(_nodes()), [],
            enabled_types=["Toggle"], recovery_mode_for=lambda t: "extract",
        )
        assert sched._menu() == [] and sched.tick() == 0

    def test_stop_extracts_what_is_still_held(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        probe = RecoveryProbe(
            guard=guard, recovered=lambda t: False, min_hold_sec=0.0, poll_interval=3600.0
        )
        sched = self._toggle_scheduler(guard, dispatched, probe, hold_sec=600.0)
        assert sched.tick() == 1
        assert [c.action for c in dispatched] == ["inject"]

        sched.stop()
        assert [c.action for c in dispatched] == ["inject", "extract"], "stop must not leave it applied"
        assert guard.snapshot()["impaired_racks"] == [] and probe.pending() == []


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
            guard.reserve(Footprint(racks=frozenset({rack})), recovery_sec=None, identity_key=rack)
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
            max_per_tick=3, rng=ScriptedRandom(randint=3),
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
