"""Whole-loop properties: the budget must never be exceeded, and restart must keep chaos running."""

from __future__ import annotations

import os
import random
import tempfile
import time
from pathlib import Path

import pytest
import yaml

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import dispatch as build_dispatch
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.boundary_scheduler import (
    BoundaryNemesisScheduler,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelGuard,
    GuardMode,
    ImpactScope,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import RecoveryProbe

# 3 realms × 3 racks with labels repeating per DC; 2 nodes and 6 slots worth of targets per host.
HOSTS = [
    {"name": f"{dc}-r{r}", "location": {"rack": str(r), "data_center": dc}}
    for dc in ("dc1", "dc2", "dc3")
    for r in (1, 2, 3)
]
NODES = [ChaosTarget.for_node(h["name"], node_id=i + 1) for i, h in enumerate(HOSTS)]
SLOTS = [
    ChaosTarget.for_slot(h["name"], slot_idx=i * 10 + s)
    for i, h in enumerate(HOSTS)
    for s in range(6)
]
DCS = [ChaosTarget.for_datacenter(h["name"], h["location"]["data_center"]) for h in HOSTS]
TABLETS = [ChaosTarget.for_tablet(HOSTS[0]["name"])]

# type -> (kind, scope, guard mode, recovery mode, recovery window)
TYPES = {
    "KillNode": (TargetKind.NODE, ImpactScope.NODE, GuardMode.FULL, "self", 120.0),
    "KillSlot": (TargetKind.SLOT, ImpactScope.SLOT, GuardMode.FULL, "self", 90.0),
    "StopStart": (TargetKind.NODE, ImpactScope.NODE, GuardMode.FULL, "extract", 90.0),
    "DCStop": (TargetKind.DATACENTER, ImpactScope.DATACENTER, GuardMode.FULL, "self", 240.0),
    "KillHive": (TargetKind.TABLET, ImpactScope.NODE, GuardMode.BYPASS, "self", 60.0),
}
BY_KIND = {
    TargetKind.NODE: NODES,
    TargetKind.SLOT: SLOTS,
    TargetKind.DATACENTER: DCS,
    TargetKind.TABLET: TABLETS,
}


class Inventory:
    def entities(self, kind):
        return list(BY_KIND.get(kind, []))


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture(scope="module")
def topology():
    fd, path = tempfile.mkstemp(suffix=".yaml")
    os.close(fd)
    Path(path).write_text(
        yaml.safe_dump({"static_erasure": "mirror-3-dc", "hosts": HOSTS}), encoding="utf-8"
    )
    return ClusterTopologyModel(path)


def _scheduler(guard, inventory, dispatched, probe, rng, **overrides):
    kwargs = dict(
        guard=guard,
        inventory=inventory,
        plan_inject=lambda n, t: [build_dispatch(n, t, "inject", {})],
        plan_extract=lambda n, t: [build_dispatch(n, t, "extract", {})],
        dispatch=dispatched.append,
        recovery_probe=probe,
        enabled_types=list(TYPES),
        scope_for=lambda n: TYPES[n][1],
        kind_for=lambda n: TYPES[n][0],
        mode_for=lambda n: TYPES[n][2],
        recovery_sec_for=lambda n: TYPES[n][4],
        recovery_mode_for=lambda n: TYPES[n][3],
        max_per_tick=6,
        rng=rng,
    )
    kwargs.update(overrides)
    return BoundaryNemesisScheduler(**kwargs)


def _assert_within_budget(guard, where: str) -> None:
    snap = guard.snapshot()
    domains = set(snap["impaired_racks"])
    assert guard._is_tolerable(domains), f"{where}: erasure budget exceeded: {sorted(domains)}"
    assert snap["impaired_slots"] <= snap["max_slots"], f"{where}: slot budget exceeded: {snap}"


class AlwaysFreshReporter:
    """hc_source that is always fresh; the simulation's predicates ignore the snapshot."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self.last_results = {"h1": {"self_check_result": "GOOD", "database_status": []}}

    @property
    def last_update(self) -> float:
        return self._clock()


@pytest.mark.parametrize("seed", range(3))
def test_budget_is_never_exceeded_across_many_ticks(topology, seed):
    rng = random.Random(seed)
    guard = FailureModelGuard(topology, total_slots=len(SLOTS))
    clock, recovered, dispatched = Clock(), set(), []
    probe = RecoveryProbe(
        guard=guard, hc_source=AlwaysFreshReporter(clock), min_hold_sec=0.0, clock=clock
    )
    sched = _scheduler(
        guard, Inventory(), dispatched, probe, rng,
        predicate_for=lambda target, **kw: (lambda snap: target.host in recovered),
    )

    injected = 0
    for i in range(200):
        injected += sched.tick()
        _assert_within_budget(guard, f"tick {i}")
        clock.t += rng.uniform(5, 60)
        recovered.clear()
        recovered.update(h["name"] for h in HOSTS if rng.random() < 0.35)
        probe.tick()
        _assert_within_budget(guard, f"probe {i}")

    assert injected > 100, f"the simulation must actually inject chaos; got {injected}"

    toggles = [c for c in dispatched if c.action == "inject" and c.nemesis_type == "StopStart"]
    probe.drain_extracts()
    extracts = [c for c in dispatched if c.action == "extract"]
    assert len(extracts) == len(toggles), "every toggle inject must end in exactly one extract"


class ProbeStub:
    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def drain_extracts(self) -> int:
        return 0

    def snapshot(self) -> dict:
        return {"tracked": 0, "stuck": 0}

    def alive_compute_baseline(self) -> int:
        return 0

    def track(self, *args, **kwargs) -> None:
        pass


def test_restart_after_a_slow_stop(topology):
    """A start() on top of a thread still winding down used to be a silent no-op: the API said
    "ok" while the stop flag killed the old thread mid-dispatch."""
    sched = _scheduler(
        FailureModelGuard(topology, total_slots=len(SLOTS)),
        Inventory(),
        [],
        ProbeStub(),
        random.Random(0),
        dispatch=lambda cmd: time.sleep(3.0),
        enabled_types=["KillNode"],
        base_interval=1000.0,
        jitter=0.0,
        max_per_tick=1,
        stop_join_sec=0.5,      # stop() gives up while the tick is still dispatching
        restart_join_sec=10.0,  # start() waits the dying thread out
    )

    sched.start()
    time.sleep(0.4)  # a tick is in flight, blocked in dispatch
    sched.stop()
    assert sched.running(), "precondition: stop() gave up on the slow tick"

    sched.start()
    assert sched.running() and not sched._stop.is_set(), "must run again, with the flag cleared"
    sched.stop()
