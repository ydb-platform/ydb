"""Unit tests for the single-threaded boundary-walking nemesis scheduler."""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import pytest
import yaml

from ydb.tests.stability.nemesis.internal.nemesis.catalog import (
    NEMESIS_TYPES,
    guard_mode_for,
    recovery_mode_for,
)
from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import dispatch as build_dispatch
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
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.boundary_scheduler import (
    BoundaryNemesisScheduler,
    _STABILITY_PROFILE,
    default_enabled_types,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import RecoveryProbe


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

    def __init__(self, *, randint: int = 1, uniform: float = 0.0) -> None:
        self._randint = randint
        self._uniform = uniform

    def randint(self, a: int, b: int) -> int:
        return max(a, min(b, self._randint))

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
        mode_for=lambda t: GuardMode.FULL,
        recovery_sec_for=lambda t: None,
        default_recovery_sec=300.0,
    )
    kwargs.update(overrides)
    return BoundaryNemesisScheduler(**kwargs)


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

    def test_cap_fills_while_the_budget_allows(self):
        # mirror-3-dc over 4 racks in one DC: the whole DC may be sacrificed, so 3 injects fit.
        guard = _guard("mirror-3-dc")
        dispatched: list = []
        sched = _make_scheduler(
            guard, FakeInventory(_nodes()), dispatched,
            max_per_tick=3, rng=ScriptedRandom(randint=3),
        )
        injected = sched.tick()
        assert injected == 3, f"a cap of 3 must fill while the budget allows; got {injected}"
        assert len(dispatched) == 3

    def test_menu_offers_every_enabled_type_uniformly(self):
        guard = _guard("block-4-2")
        sched = _make_scheduler(
            guard, FakeInventory(_nodes()), [],
            enabled_types=["A", "B"],
            max_per_tick=1, rng=ScriptedRandom(randint=1),
        )
        menu = sched._menu()
        assert {item[0] for item in menu} == {"A", "B"}, (
            f"every enabled type must be offered (no weights, no muting); got {[m[0] for m in menu]}"
        )
        assert all(len(item) == 3 for item in menu), "menu entries are (type, target, racks) triples"


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
    def test_set_profile_updates_enabled_and_bounds(self):
        sched = _make_scheduler(_guard("block-4-2"), FakeInventory(_nodes()), [])
        sched.set_profile(enabled=["X"], base_interval=10.0, jitter=0.25, max_per_tick=7)
        assert sched._enabled == ["X"]
        assert sched._base_interval == 10.0
        assert sched._jitter == 0.25
        assert sched._max_per_tick == 7


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


class RecordingProbe:
    def __init__(self) -> None:
        self.tracked: list = []

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def snapshot(self) -> dict:
        return {"tracked": len(self.tracked)}

    def track(self, lease_id, target, nemesis_type, timeout_sec=None, recover_action=None):
        self.tracked.append((lease_id, target, nemesis_type, timeout_sec, recover_action))


class TestToggleRecovery:
    def _toggle_scheduler(self, guard, dispatched, probe, **overrides):
        return _make_scheduler(
            guard, FakeInventory(_nodes()), dispatched,
            enabled_types=["Toggle"],
            recovery_mode_for=lambda t: "extract",
            recovery_sec_for=lambda t: 90.0,
            plan_extract=lambda ntype, target: [build_dispatch(ntype, target, "extract", {})],
            recovery_probe=probe,
            max_per_tick=1, rng=ScriptedRandom(randint=1),
            **overrides,
        )

    def test_toggle_tick_holds_budget_and_tracks_extract_action(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        probe = RecordingProbe()
        sched = self._toggle_scheduler(guard, dispatched, probe)

        assert sched.tick() == 1
        injects = [c for c in dispatched if c.action == "inject"]
        assert len(injects) == 1, f"exactly one inject must be dispatched; got {dispatched}"
        assert len(guard.snapshot()["impaired_racks"]) == 1, (
            "a toggle fault must hold the budget (recovery_sec=None), not expire on a timer"
        )
        assert len(probe.tracked) == 1
        lease, target, ntype, timeout, action = probe.tracked[0]
        assert ntype == "Toggle" and timeout == 90.0
        assert action is not None, "toggle faults must be tracked with a recover_action"

        # Running the recover_action must dispatch an extract for the same target.
        action()
        extracts = [c for c in dispatched if c.action == "extract"]
        assert len(extracts) == 1 and extracts[0].target.host == target.host, (
            f"recover_action must extract the injected target; got {dispatched}"
        )

    def test_toggle_type_muted_when_extract_not_wired(self):
        guard = _guard("block-4-2")
        # No plan_extract / no probe -> a toggle fault can never auto-extract, so it must never
        # be offered (else it would stay broken forever).
        sched = _make_scheduler(
            guard, FakeInventory(_nodes()), [],
            enabled_types=["Toggle"],
            recovery_mode_for=lambda t: "extract",
        )
        assert sched._menu() == [], "toggle types must be filtered out with no way to extract"
        assert sched.tick() == 0


class TestBypass:
    """BYPASS types (tablet chaos) inject without spending the failure-model budget."""

    def _bypass_scheduler(self, guard, dispatched, targets, enabled, **overrides):
        return _make_scheduler(
            guard, FakeInventory(targets), dispatched,
            enabled_types=enabled,
            kind_for=lambda t: TargetKind.TABLET,
            mode_for=lambda t: GuardMode.BYPASS,
            **overrides,
        )

    def test_bypass_fires_even_when_budget_is_full(self):
        guard = _guard("block-4-2")
        # Reserve the whole block-4-2 budget so no budgeted fault could fit.
        assert guard.reserve(Footprint(racks=frozenset({"r1"})), recovery_sec=None, identity_key="x1")
        assert guard.reserve(Footprint(racks=frozenset({"r2"})), recovery_sec=None, identity_key="x2")
        dispatched: list = []
        sched = self._bypass_scheduler(
            guard, dispatched, [ChaosTarget.for_tablet("h1")], ["KillHive"],
            max_per_tick=1, rng=ScriptedRandom(randint=1),
        )
        assert sched.tick() == 1, "a BYPASS fault must fire even with the failure budget exhausted"
        assert len(dispatched) == 1
        assert len(guard.snapshot()["impaired_racks"]) == 2, (
            "BYPASS injection must not reserve any additional budget"
        )

    def test_multiple_bypass_types_share_one_target_in_a_tick(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        sched = self._bypass_scheduler(
            guard, dispatched, [ChaosTarget.for_tablet("h1")], ["KillHive", "ReBalance"],
            max_per_tick=2, rng=ScriptedRandom(randint=2),
        )
        assert sched.tick() == 2, (
            "two BYPASS types on one control-host target must both fire (dedup is per "
            "(type, target), not per target)"
        )
        assert {c.nemesis_type for c in dispatched} == {"KillHive", "ReBalance"}

    def test_bypass_type_fires_at_most_once_per_tick(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        sched = self._bypass_scheduler(
            guard, dispatched, [ChaosTarget.for_tablet("h1")], ["KillHive"],
            max_per_tick=3, rng=ScriptedRandom(randint=3),
        )
        assert sched.tick() == 1, (
            "one BYPASS type on one target fires once per tick even at a higher cap; the "
            "(type, target) dedup empties the menu after the first inject"
        )


class TestSlotScheduling:
    """Slot kills draw from the separate 30% slot budget, not the erasure/rack budget."""

    def _slots(self, n: int) -> list[ChaosTarget]:
        return [ChaosTarget.for_slot("h1", slot_idx=i) for i in range(n)]

    def _slot_scheduler(self, guard, dispatched, targets, **overrides):
        return _make_scheduler(
            guard, FakeInventory(targets), dispatched,
            enabled_types=["KillSlot"],
            scope_for=lambda t: ImpactScope.SLOT,
            kind_for=lambda t: TargetKind.SLOT,
            mode_for=lambda t: GuardMode.FULL,
            **overrides,
        )

    def test_tick_caps_slots_at_thirty_percent(self):
        # 10 slots -> max_slots = floor(0.3*10) = 3, even with 5 candidates and a cap of 5.
        guard = FailureModelGuard(
            ClusterTopologyModel(_write_topology(
                [{"name": "h1", "location": {"rack": "r1", "data_center": "dc1"}}], "block-4-2",
            )),
            total_slots=10,
        )
        dispatched: list = []
        sched = self._slot_scheduler(
            guard, dispatched, self._slots(5),
            max_per_tick=5, rng=ScriptedRandom(randint=5),
        )
        injected = sched.tick()
        assert injected == 3, (
            f"a single tick must stop at the 30% slot budget (3 of 10); injected={injected}, "
            f"snapshot={guard.snapshot()}"
        )
        assert guard.snapshot()["impaired_slots"] == 3
        assert guard.snapshot()["impaired_racks"] == [], (
            "slot kills must never consume an erasure/rack fail-domain"
        )

    def test_slot_kill_does_not_spend_rack_budget(self):
        # A full slot budget must leave the block-4-2 rack budget completely untouched.
        guard = FailureModelGuard(
            ClusterTopologyModel(_write_topology(
                [{"name": "h1", "location": {"rack": "r1", "data_center": "dc1"}}], "block-4-2",
            )),
            total_slots=3,  # max_slots = 1
        )
        dispatched: list = []
        sched = self._slot_scheduler(
            guard, dispatched, self._slots(3),
            max_per_tick=3, rng=ScriptedRandom(randint=3),
        )
        assert sched.tick() == 1, "the slot budget of 1 caps a single slot kill per tick"
        assert guard.fits(Footprint(racks=frozenset({"r1"}))), (
            "a rack fault must still fit while the slot budget is exhausted (independent budgets)"
        )


class TestDefaultProfile:
    def test_default_profile_is_curated_and_registered(self):
        enabled = default_enabled_types()
        assert enabled, "the default profile must not be empty"
        assert set(enabled) <= set(_STABILITY_PROFILE)
        assert all(t in NEMESIS_TYPES for t in enabled), (
            "every profile entry must be a registered nemesis type"
        )
        for t in ("KillNodeNemesis", "KillSlotDaemonNemesis", "StopStartNodeNemesis",
                  "SafelyBreakDiskNemesis", "TimeSkewNemesis"):
            assert t in enabled, f"{t} must be in the default stability profile"

    def test_tablet_chaos_is_in_profile_and_bypass(self):
        enabled = default_enabled_types()
        for t in ("KillHiveNemesis", "KillCoordinatorNemesis", "ReBalanceTabletsNemesis"):
            assert t in enabled, f"tablet chaos type {t} must be in the default profile"
            assert guard_mode_for(t) is GuardMode.BYPASS, f"{t} must be BYPASS (no budget spent)"
        # Kicking tablets kills the node, so it stays budgeted like any other node fault.
        assert "KickTabletsFromNodeNemesis" in enabled
        assert guard_mode_for("KickTabletsFromNodeNemesis") is GuardMode.FULL

    def test_toggle_members_are_extract_mode(self):
        for t in ("StopStartNodeNemesis", "SafelyBreakDiskNemesis",
                  "SafelyCleanupDisksNemesis", "TimeSkewNemesis"):
            assert recovery_mode_for(t) == "extract", f"{t} must recover via extract"
        for t in ("KillNodeNemesis", "KillSlotDaemonNemesis"):
            assert recovery_mode_for(t) == "self", f"{t} is self-recovering"


class TestStopDrainsToggleFaults:
    """``stop()`` (deploy.py's "disable nemesis") must not leave toggle faults applied."""

    def _toggle_scheduler_with_real_probe(self, guard, dispatched):
        # poll_interval is huge and the probe is never started, so only stop() can extract.
        probe = RecoveryProbe(
            guard=guard,
            recovered=lambda t: False,
            min_hold_sec=0.0,
            poll_interval=3600.0,
        )
        sched = _make_scheduler(
            guard, FakeInventory(_nodes()), dispatched,
            enabled_types=["Toggle"],
            recovery_mode_for=lambda t: "extract",
            recovery_sec_for=lambda t: 600.0,  # hold window far longer than the test
            plan_extract=lambda ntype, target: [build_dispatch(ntype, target, "extract", {})],
            recovery_probe=probe,
            max_per_tick=1, rng=ScriptedRandom(randint=1),
        )
        return sched, probe

    def test_stop_extracts_faults_still_inside_their_hold_window(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        sched, probe = self._toggle_scheduler_with_real_probe(guard, dispatched)

        assert sched.tick() == 1
        assert [c.action for c in dispatched] == ["inject"], (
            f"the tick must only inject so far; got {[c.action for c in dispatched]}"
        )
        injected_target = dispatched[0].target

        sched.stop()

        assert [c.action for c in dispatched] == ["inject", "extract"], (
            "stopping the scheduler must extract the toggle fault it left applied; "
            f"got {[c.action for c in dispatched]}"
        )
        assert dispatched[-1].target.identity_key() == injected_target.identity_key(), (
            "the extract must target exactly what was injected; "
            f"injected={injected_target.identity_key()}, extracted={dispatched[-1].target.identity_key()}"
        )
        assert guard.snapshot()["impaired_racks"] == [], (
            f"draining on stop must release the budget too; snapshot={guard.snapshot()}"
        )
        assert probe.pending() == [], "nothing may stay tracked after stop"

    def test_stop_without_pending_faults_dispatches_nothing(self):
        guard = _guard("block-4-2")
        dispatched: list = []
        sched, _probe = self._toggle_scheduler_with_real_probe(guard, dispatched)
        sched.stop()
        assert dispatched == [], "stop() on an idle scheduler must not dispatch anything"


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
