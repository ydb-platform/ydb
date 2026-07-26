"""Unit tests for the single-threaded weighted nemesis scheduler."""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import pytest
import yaml

from ydb.tests.stability.nemesis.internal.nemesis.catalog import weight_for as catalog_weight_for
from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import dispatch as build_dispatch
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_state import (
    datacenter_inject_fanout,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelGuard,
    ImpactScope,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.weighted_scheduler import (
    WeightedNemesisScheduler,
)


def _write_topology(hosts: list[dict], erasure: str) -> str:
    fd, path = tempfile.mkstemp(suffix=".yaml")
    os.close(fd)
    Path(path).write_text(
        yaml.safe_dump({"static_erasure": erasure, "hosts": hosts}), encoding="utf-8"
    )
    return path


def _guard(erasure: str) -> FailureModelGuard:
    hosts = [
        {"name": f"h{i}", "location": {"rack": f"r{i}", "data_center": "dc1"}}
        for i in (1, 2, 3, 4)
    ]
    return FailureModelGuard(ClusterTopologyModel(_write_topology(hosts, erasure)))


class FakeInventory:
    def __init__(self, targets: list[ChaosTarget]) -> None:
        self._targets = list(targets)

    def entities(self, kind: TargetKind) -> list[ChaosTarget]:
        return list(self._targets)


class ScriptedRandom:
    """Deterministic stand-in for random.Random with scripted return values."""

    def __init__(self, *, randint: int = 1, randoms=None, uniform: float = 0.0) -> None:
        self._randint = randint
        self._randoms = list(randoms if randoms is not None else [])
        self._uniform = uniform

    def randint(self, a: int, b: int) -> int:
        return max(a, min(b, self._randint))

    def random(self) -> float:
        return self._randoms.pop(0) if self._randoms else 0.0

    def uniform(self, a: float, b: float) -> float:
        return self._uniform

    def choice(self, seq):
        return seq[0]


def _make_scheduler(guard, inventory, dispatched, **overrides):
    kwargs = dict(
        guard=guard,
        inventory=inventory,
        plan_inject=lambda ntype, target: [build_dispatch(ntype, target, "inject", {})],
        dispatch=lambda cmd: dispatched.append(cmd),
        enabled_types=["KillNode"],
        scope_for=lambda t: ImpactScope.NODE,
        kind_for=lambda t: TargetKind.NODE,
        recovery_sec_for=lambda t: None,
        default_recovery_sec=300.0,
    )
    kwargs.update(overrides)
    return WeightedNemesisScheduler(**kwargs)


def _nodes() -> list[ChaosTarget]:
    return [ChaosTarget.for_node(f"h{i}", node_id=i) for i in (1, 2, 3, 4)]


class TestTick:
    def test_single_tick_respects_block42_ceiling(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        sched = _make_scheduler(
            guard, FakeInventory(_nodes()), dispatched,
            max_per_tick=5, rng=ScriptedRandom(randint=5),
        )
        injected = sched.tick()
        assert injected == 2, (
            "a single tick must stop at the block-4-2 budget of 2 fail domains even with a "
            f"cap of 5; injected={injected}, snapshot={guard.snapshot()}"
        )
        assert len(dispatched) == 2, f"exactly two commands must be dispatched; got {len(dispatched)}"
        assert len({c.target.host for c in dispatched}) == 2, (
            "the two injects must hit two distinct hosts (identity exclusion), not the same one; "
            f"hosts={[c.target.host for c in dispatched]}"
        )
        assert len(guard.snapshot()["impaired_racks"]) == 2

    def test_disabled_guard_fills_cap_with_distinct_targets(self):
        guard = _guard("no-such-erasure")  # unknown -> guard disabled (fail-open)
        assert not guard.enabled, "unknown erasure must leave the guard disabled"
        dispatched: list = []
        sched = _make_scheduler(
            guard, FakeInventory(_nodes()), dispatched,
            max_per_tick=3, rng=ScriptedRandom(randint=3, randoms=[0.0, 0.3, 0.6]),
        )
        injected = sched.tick()
        assert injected == 3, f"a disabled guard must never block; cap of 3 must fill; got {injected}"
        assert len(dispatched) == 3
        assert len({c.target.host for c in dispatched}) == 3, (
            "with a walking rng the disabled scheduler should spread across distinct hosts; "
            f"hosts={[c.target.host for c in dispatched]}"
        )

    def test_zero_weight_type_is_never_offered(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        sched = _make_scheduler(
            guard, FakeInventory(_nodes()), dispatched,
            enabled_types=["Muted", "Active"],
            weight_for=lambda t: 0.0 if t == "Muted" else 1.0,
            max_per_tick=1, rng=ScriptedRandom(randint=1),
        )
        menu = sched._menu()
        assert menu, "the weighted (Active) type must still produce a menu"
        assert all(item[0] == "Active" for item in menu), (
            f"a zero-weight type must never appear in the menu; got {[m[0] for m in menu]}"
        )


class TestWeightedChoice:
    def _menu(self):
        t = ChaosTarget.for_node("h1", node_id=1)
        fs = frozenset({"r1"})
        return [("A", t, fs, 1.0), ("B", t, fs, 3.0)]

    def test_low_draw_picks_first_bucket(self):
        sched = _make_scheduler(_guard("block-4-2"), FakeInventory([]), [],
                                rng=ScriptedRandom(randoms=[0.1]))
        picked = sched._weighted_choice(self._menu())
        assert picked[0] == "A", f"r=0.4 lands in A's [0,1] bucket; got {picked[0]}"

    def test_high_draw_picks_weighted_bucket(self):
        sched = _make_scheduler(_guard("block-4-2"), FakeInventory([]), [],
                                rng=ScriptedRandom(randoms=[0.5]))
        picked = sched._weighted_choice(self._menu())
        assert picked[0] == "B", f"r=2.0 lands in B's (1,4] bucket; got {picked[0]}"

    def test_all_zero_weight_menu_falls_back_to_choice(self):
        sched = _make_scheduler(_guard("block-4-2"), FakeInventory([]), [],
                                rng=ScriptedRandom())
        t = ChaosTarget.for_node("h1", node_id=1)
        menu = [("A", t, frozenset({"r1"}), 0.0), ("B", t, frozenset({"r2"}), 0.0)]
        picked = sched._weighted_choice(menu)
        assert picked[0] == "A", "with zero total weight, choice() (first element) is used"


class TestSleep:
    def test_sleep_low_extreme(self):
        sched = _make_scheduler(_guard("block-4-2"), FakeInventory([]), [],
                                base_interval=60.0, jitter=0.5, rng=ScriptedRandom(uniform=-0.5))
        assert sched._sleep_seconds() == pytest.approx(30.0)

    def test_sleep_high_extreme(self):
        sched = _make_scheduler(_guard("block-4-2"), FakeInventory([]), [],
                                base_interval=60.0, jitter=0.5, rng=ScriptedRandom(uniform=0.5))
        assert sched._sleep_seconds() == pytest.approx(90.0)

    def test_sleep_has_floor(self):
        sched = _make_scheduler(_guard("block-4-2"), FakeInventory([]), [],
                                base_interval=0.1, jitter=0.0, rng=ScriptedRandom(uniform=0.0))
        assert sched._sleep_seconds() == pytest.approx(0.5), "sleep must never drop below the 0.5s floor"


class TestProfile:
    def test_set_profile_updates_weights_and_bounds(self):
        sched = _make_scheduler(_guard("block-4-2"), FakeInventory(_nodes()), [])
        sched.set_profile(enabled=["X"], weights={"X": 2.0}, base_interval=10.0,
                          jitter=0.25, max_per_tick=7)
        assert sched._weight_for("X") == 2.0
        assert sched._weight_for("unlisted") == 1.0, "unlisted types default to weight 1.0"
        assert sched._base_interval == 10.0
        assert sched._max_per_tick == 7


class TestCatalogWeights:
    def test_weight_for_reads_registry_and_defaults(self):
        assert catalog_weight_for("KillNodeNemesis") == 3.0, "annotated weight must be read from the registry"
        assert catalog_weight_for("no-such-nemesis") == 1.0, "unknown types default to weight 1.0"


class TestDatacenterFanout:
    def test_dc_target_fans_out_to_every_host_in_the_chosen_dc(self):
        dc_targets = [
            ChaosTarget.for_datacenter("h1", "dc1"),
            ChaosTarget.for_datacenter("h2", "dc1"),
            ChaosTarget.for_datacenter("h3", "dc2"),
        ]
        cmds = datacenter_inject_fanout(
            "DataCenterStopNodesNemesis",
            ChaosTarget.for_datacenter("h1", "dc1"),
            FakeInventory(dc_targets),
        )
        assert {c.target.host for c in cmds} == {"h1", "h2"}, (
            "a chosen DC must fan out to exactly that DC's hosts, not the round-robin planner's; "
            f"got {[c.target.host for c in cmds]}"
        )
        assert all(c.action == "inject" and c.target.kind is TargetKind.DATACENTER for c in cmds)
        assert all(c.nemesis_type == "DataCenterStopNodesNemesis" for c in cmds)
        assert len({c.scenario_id for c in cmds}) == 1, "one fanout must share a single scenario id"


class TestStatus:
    def test_status_reports_running_and_profile(self):
        sched = _make_scheduler(_guard("block-4-2"), FakeInventory(_nodes()), [],
                                base_interval=42.0, jitter=0.25, max_per_tick=4)
        st = sched.status()
        assert st["running"] is False, "a scheduler that never started must report running=False"
        assert st["base_interval"] == 42.0
        assert st["jitter"] == 0.25
        assert st["max_per_tick"] == 4
        assert st["enabled_types"] == ["KillNode"]
        assert "recovery_probe" not in st, "no probe wired -> no recovery_probe key"

    def test_status_includes_probe_snapshot_when_wired(self):
        class FakeProbe:
            def start(self):
                pass

            def stop(self):
                pass

            def snapshot(self):
                return {"tracked": 0, "stuck": 0}
        sched = _make_scheduler(_guard("block-4-2"), FakeInventory(_nodes()), [],
                                recovery_probe=FakeProbe())
        assert sched.status()["recovery_probe"] == {"tracked": 0, "stuck": 0}


class TestLifecycle:
    def test_start_then_stop_terminates_thread(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        sched = _make_scheduler(
            guard, FakeInventory(_nodes()), dispatched,
            base_interval=1000.0, jitter=0.0, max_per_tick=2, rng=ScriptedRandom(randint=2),
        )
        sched.start()
        deadline = time.monotonic() + 2.0
        while not dispatched and time.monotonic() < deadline:
            time.sleep(0.01)
        sched.stop()
        assert sched._thread is not None and not sched._thread.is_alive(), (
            "stop() must join the daemon thread"
        )
        assert dispatched, "the scheduler thread must have run at least one tick before stopping"
