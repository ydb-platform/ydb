"""Fact-based recovery of reserved failure budget: hc-model, probe phases, blindness."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import yaml

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import (
    ChaosTarget,
    TargetKind,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelGuard,
    ImpactScope,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.hc_model import (
    build_snapshot,
    hc_predicate_for,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import (
    PHASE_CONFIRM,
    RecoveryProbe,
    StuckFault,
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


class StubReporter:
    """Duck-typed hc_source: publish() stamps last_update with the probe's own clock."""

    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock
        self.last_results: dict = {}
        self.last_update: float | None = None

    def publish(self, results: dict) -> None:
        self.last_results = results
        self.last_update = self._clock()


# -- healthcheck fixtures -------------------------------------------------------


def _vdisk(entity_id: str, overall: str = "GREEN", pdisk_overall: str = "GREEN") -> dict:
    pid = "-".join(entity_id.split("-")[:2])
    return {"id": entity_id, "overall": overall, "pdisk": {"id": pid, "overall": pdisk_overall}}


def _compute_node(i: int, alive: bool = True) -> dict:
    return {
        "id": str(i),
        "overall": "GREEN",
        "pools": [{"name": "System", "usage": 0.01}] if alive else [],
    }


def _hc(host: str, vdisks=(), compute=(), skew: str = "GREEN", result: str = "GOOD") -> dict:
    return {
        "self_check_result": result,
        "database_status": [
            {
                "name": "/Root",
                "overall": "GREEN",
                "storage": {
                    "overall": "GREEN",
                    "pools": [
                        {
                            "id": "static",
                            "overall": "GREEN",
                            "groups": [{"id": "0", "overall": "GREEN", "vdisks": list(vdisks)}],
                        }
                    ],
                },
                "compute": {
                    "overall": "GREEN",
                    "nodes": list(compute),
                    "clock_skew": {"overall": skew},
                },
            }
        ],
        "location": {"id": 1, "host": host, "port": 19001},
    }


def _error_entry() -> dict:
    return {"self_check_result": "HC_REQUEST_ERROR"}


def _healthy_results(compute_count: int = 8) -> dict:
    """All four hosts answer; every node's storage is GREEN; ``compute_count`` alive slots."""
    compute = [_compute_node(i) for i in range(1, compute_count + 1)]
    vdisks = [_vdisk(f"{n}-1-0") for n in (1, 2, 3, 4)]
    return {f"h{i}": _hc(f"h{i}", vdisks=vdisks, compute=compute) for i in (1, 2, 3, 4)}


def _reserve(guard, host="h1", node_id=1):
    target = ChaosTarget.for_node(host, node_id=node_id)
    lease = guard.reserve(
        guard.footprint_for(target, ImpactScope.NODE),
        identity_key=target.identity_key(),
    )
    return target, lease


def _node_predicate(target):
    return hc_predicate_for(target, kind=TargetKind.NODE, scope=ImpactScope.NODE)


class TestSnapshot:
    def test_endpoint_error_marks_the_host_not_answering(self):
        snap = build_snapshot(
            {"h1": _error_entry(), "h2": _hc("h2")}, now=10.0, last_update=10.0, max_age_sec=180.0
        )
        assert snap.fresh and snap.answering == frozenset({"h2"})

    def test_storage_merges_worst_status_across_hosts(self):
        results = {
            "h1": _hc("h1", vdisks=[_vdisk("1-1-0", "GREEN")]),
            "h2": _hc("h2", vdisks=[_vdisk("1-1-0", "BLUE")]),  # replication in progress
        }
        snap = build_snapshot(results, now=10.0, last_update=10.0, max_age_sec=180.0)
        assert not snap.storage_green(1), "one BLUE view blocks GREEN"
        assert snap.storage_blockers(1) == ["1-1-0=BLUE"]
        assert snap.storage_green(2), "no entities observed for another node"

    def test_alive_compute_requires_non_empty_pools(self):
        # A dead dynamic node shows GREEN with empty pools (the RED report is commented out
        # in health_check.cpp) — overall must not be trusted, pools are the liveness marker.
        compute = [_compute_node(1), _compute_node(2, alive=False)]
        snap = build_snapshot(
            {"h1": _hc("h1", compute=compute)}, now=10.0, last_update=10.0, max_age_sec=180.0
        )
        assert snap.alive_compute == 1

    def test_alive_compute_takes_the_most_lagging_view(self):
        # Pessimistic merge, like the worst-status storage merge: one host's stale
        # "still alive" must not release a slot lease early.
        results = {
            "h1": _hc("h1", compute=[_compute_node(1), _compute_node(2)]),
            "h2": _hc("h2", compute=[_compute_node(1)]),
        }
        snap = build_snapshot(results, now=10.0, last_update=10.0, max_age_sec=180.0)
        assert snap.alive_compute == 1

    def test_clock_skew_and_staleness(self):
        snap = build_snapshot(
            {"h1": _hc("h1", skew="ORANGE")}, now=10.0, last_update=10.0, max_age_sec=180.0
        )
        assert not snap.clock_skew_green
        stale = build_snapshot({"h1": _hc("h1")}, now=1000.0, last_update=10.0, max_age_sec=180.0)
        assert not stale.fresh, "old data is blind"
        blind = build_snapshot({"h1": _error_entry()}, now=10.0, last_update=10.0, max_age_sec=180.0)
        assert not blind.fresh, "zero answering endpoints is blind"


class TestNodeRecovery:
    def test_released_once_the_endpoint_answers_and_storage_is_green(self):
        guard, clock = _guard(), FakeClock()
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=30.0, clock=clock)
        target, lease = _reserve(guard)
        probe.track(lease, target, "KillNode", recovered=_node_predicate(target),
                    stuck_timeout_sec=300.0)

        # The node is down: its endpoint errors, peers see its vdisks RED.
        rep.publish({
            "h1": _error_entry(),
            "h2": _hc("h2", vdisks=[_vdisk("1-1-0", "RED")]),
        })
        clock.advance(40.0)
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"]

        rep.publish(_healthy_results())
        clock.advance(20.0)
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == [] and probe.pending() == []

    def test_replication_in_progress_blocks_the_release(self):
        guard, clock = _guard(), FakeClock()
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        target, lease = _reserve(guard)
        probe.track(lease, target, "KillNode", recovered=_node_predicate(target),
                    stuck_timeout_sec=300.0)

        # The endpoint answers, but the node's vdisk is still replicating (BLUE).
        results = _healthy_results()
        results["h2"] = _hc("h2", vdisks=[_vdisk("1-1-0", "BLUE")])
        rep.publish(results)
        clock.advance(1.0)
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "BLUE must hold the budget"

        rep.publish(_healthy_results())
        clock.advance(20.0)
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == []

    def test_pre_fault_data_is_not_read_as_recovery(self):
        guard, clock = _guard(), FakeClock()
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        rep.publish(_healthy_results())  # published BEFORE the fault
        clock.advance(5.0)
        target, lease = _reserve(guard)
        probe.track(lease, target, "KillNode", recovered=_node_predicate(target),
                    stuck_timeout_sec=300.0)
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "stale data must not release"

        rep.publish(_healthy_results())  # fresh, post-fault data
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == []

    def test_a_stuck_fault_keeps_the_budget_and_is_reported_once(self):
        guard, clock, seen = _guard(), FakeClock(), []
        rep = StubReporter(clock)
        probe = RecoveryProbe(
            guard=guard, hc_source=rep, on_stuck=seen.append, min_hold_sec=0.0, clock=clock
        )
        target, lease = _reserve(guard)
        probe.track(lease, target, "KillNode", recovered=_node_predicate(target),
                    stuck_timeout_sec=100.0)
        rep.publish({"h1": _error_entry(), "h2": _hc("h2", vdisks=[_vdisk("1-1-0", "RED")])})

        clock.advance(50.0)
        assert probe.tick() == [], "not stuck before the timeout"
        clock.advance(60.0)
        rep.publish(rep.last_results)  # keep the data fresh
        stuck = probe.tick()
        assert len(stuck) == 1 and stuck[0].target.host == "h1" and stuck[0].phase == "hold"

        clock.advance(60.0)
        rep.publish(rep.last_results)
        assert probe.tick() == [] and len(seen) == 1, "reported once, not every tick"
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "never silently released"


class TestSlotRecovery:
    def test_released_when_the_alive_count_returns_to_baseline(self):
        guard, clock = _guard(), FakeClock()
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        rep.publish(_healthy_results(compute_count=8))
        baseline = probe.alive_compute_baseline()
        assert baseline == 8

        target = ChaosTarget.for_slot("h1", slot_idx=3, node_id=1)
        lease = guard.reserve(
            guard.footprint_for(target, ImpactScope.SLOT), identity_key=target.identity_key()
        )
        predicate = hc_predicate_for(
            target, kind=TargetKind.SLOT, scope=ImpactScope.SLOT, baseline=baseline
        )
        probe.track(lease, target, "KillSlot", recovered=predicate, stuck_timeout_sec=300.0)

        rep.publish(_healthy_results(compute_count=7))  # the slot is down
        clock.advance(20.0)
        probe.tick()
        assert guard.snapshot()["impaired_slots"] == 1

        rep.publish(_healthy_results(compute_count=8))  # restarted (with a new runtime id)
        clock.advance(20.0)
        probe.tick()
        assert guard.snapshot()["impaired_slots"] == 0 and probe.pending() == []

    def test_baseline_is_none_when_blind(self):
        guard, clock = _guard(), FakeClock()
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, clock=clock)
        assert probe.alive_compute_baseline() is None, "no data published yet"

    def test_slot_predicate_without_a_baseline_never_recovers(self):
        target = ChaosTarget.for_slot("h1", slot_idx=3)
        predicate = hc_predicate_for(target, kind=TargetKind.SLOT, scope=ImpactScope.SLOT)
        snap = build_snapshot(
            _healthy_results(compute_count=8), now=0.0, last_update=0.0, max_age_sec=180.0
        )
        assert predicate(snap) is False, "an unobservable slot fault must surface as stuck"


class TestDatacenterRecovery:
    def test_empty_node_ids_never_count_as_recovered(self):
        # all([]) is True — empty lists must not silently release a DC lease.
        from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.hc_model import (
            datacenter_predicate,
        )

        snap = build_snapshot(
            _healthy_results(), now=0.0, last_update=0.0, max_age_sec=180.0
        )
        assert datacenter_predicate(["h1", "h2"], [])(snap) is False
        assert datacenter_predicate([], [1, 2])(snap) is False

    def test_predicate_without_inventory_nodes_never_recovers(self):
        target = ChaosTarget.for_datacenter("h1", "dc1")
        predicate = hc_predicate_for(
            target, kind=TargetKind.DATACENTER, scope=ImpactScope.DATACENTER, inventory=None
        )
        snap = build_snapshot(
            _healthy_results(), now=0.0, last_update=0.0, max_age_sec=180.0
        )
        assert predicate(snap) is False


class TestToggleRecovery:
    def _track_toggle(self, probe, target, lease, extracted, extract_after=600.0, confirm=300.0):
        probe.track(
            lease, target, "StopStart",
            recovered=_node_predicate(target),
            stuck_timeout_sec=1800.0,
            recover_action=lambda: extracted.append(target.host),
            extract_after_sec=extract_after,
            confirm_timeout_sec=confirm,
        )

    def test_extract_fires_then_healthcheck_confirm_releases(self):
        guard, clock, extracted = _guard(), FakeClock(), []
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        target, lease = _reserve(guard)
        self._track_toggle(probe, target, lease, extracted, extract_after=90.0)

        clock.advance(100.0)
        probe.tick()
        assert extracted == ["h1"], "extract dispatched after the hold"
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "extract is not a release anymore"

        rep.publish(_healthy_results())
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == [], "confirmed by healthcheck"
        assert probe.pending() == []

    def test_failed_extract_turns_stuck_instead_of_leaking_the_budget(self):
        guard, clock, extracted = _guard(), FakeClock(), []
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        target, lease = _reserve(guard)
        self._track_toggle(probe, target, lease, extracted, extract_after=90.0, confirm=100.0)

        clock.advance(100.0)
        probe.tick()
        assert extracted == ["h1"]

        # The extract did not take: the node stays down, healthcheck keeps seeing it.
        rep.publish({"h1": _error_entry(), "h2": _hc("h2", vdisks=[_vdisk("1-1-0", "RED")])})
        clock.advance(150.0)
        rep.publish(rep.last_results)
        stuck = probe.tick()
        assert len(stuck) == 1 and stuck[0].phase == PHASE_CONFIRM
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "budget must not leak"

    def test_extract_fires_while_blind_but_nothing_is_released(self):
        guard, clock, extracted = _guard(), FakeClock(), []
        rep = StubReporter(clock)  # never published -> blind
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        target, lease = _reserve(guard)
        self._track_toggle(probe, target, lease, extracted, extract_after=90.0)

        clock.advance(100.0)
        probe.tick()
        assert extracted == ["h1"], "blindness must not extend the fault"
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "but no release without facts"

    def test_extract_fires_once_between_tick_and_drain(self):
        guard, clock, extracted = _guard(), FakeClock(), []
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        target, lease = _reserve(guard)
        self._track_toggle(probe, target, lease, extracted, extract_after=10.0)

        clock.advance(20.0)
        probe.tick()
        assert extracted == ["h1"]
        assert probe.drain_extracts() == 0, "already in confirm — no double extract"
        assert extracted == ["h1"]

    def test_confirm_requires_post_extract_data(self):
        guard, clock, extracted = _guard(), FakeClock(), []
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        target, lease = _reserve(guard)
        self._track_toggle(probe, target, lease, extracted, extract_after=90.0)

        rep.publish(_healthy_results())  # healthy data from BEFORE the extract
        clock.advance(100.0)
        probe.tick()
        assert extracted == ["h1"]
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "pre-extract data must not confirm"

        rep.publish(_healthy_results())  # fresh, post-extract
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == []


class TestBlindness:
    def test_startup_blindness_waits_out_the_healthcheck_grace(self):
        """The first healthcheck tick takes up to ~65s, so a just-started probe is blind by
        construction; reporting that would latch a spurious problem entry on every boot."""
        guard, clock, blind_calls = _guard(), FakeClock(), []
        rep = StubReporter(clock)
        probe = RecoveryProbe(
            guard=guard, hc_source=rep, on_blind=lambda: blind_calls.append(1),
            min_hold_sec=0.0, clock=clock,
        )
        target, lease = _reserve(guard)
        probe.track(lease, target, "KillNode", recovered=_node_predicate(target),
                    stuck_timeout_sec=100.0)

        probe.tick()
        probe.tick()
        assert blind_calls == [], "startup grace: no report before max_hc_age"
        assert probe.snapshot()["blind"] is True, "the live state stays honest"

        clock.advance(200.0)  # past the grace (180s default), still blind
        probe.tick()
        assert blind_calls == [1], "persistent blindness is reported"
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1"], "no stuck transitions while blind"

        probe.tick()
        assert blind_calls == [1], "once per episode"

        rep.publish(_healthy_results())
        probe.tick()
        assert probe.snapshot()["blind"] is False
        assert guard.snapshot()["impaired_racks"] == [], "evaluation resumes with sight"

    def test_losing_sight_reports_blind_at_once_and_sighting_resolves(self):
        guard, clock = _guard(), FakeClock()
        blind_calls, sighted_calls = [], []
        rep = StubReporter(clock)
        probe = RecoveryProbe(
            guard=guard, hc_source=rep,
            on_blind=lambda: blind_calls.append(1),
            on_sighted=lambda: sighted_calls.append(1),
            min_hold_sec=0.0, clock=clock,
        )
        rep.publish(_healthy_results())
        probe.tick()
        assert probe.snapshot()["blind"] is False

        clock.advance(200.0)  # data goes stale: last_update falls behind
        probe.tick()
        assert blind_calls == [1], "losing sight after being sighted reports at once"

        rep.publish(_healthy_results())
        probe.tick()
        assert sighted_calls == [1], "sight returns -> resolve callback"


class TestDrainExtracts:
    def test_drain_moves_toggles_to_confirm_and_leaves_the_rest_tracked(self):
        guard, clock, extracted = _guard(), FakeClock(), []
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        toggle, toggle_lease = _reserve(guard, "h1", 1)
        self_healing, self_lease = _reserve(guard, "h2", 2)
        probe.track(
            toggle_lease, toggle, "StopStart",
            recovered=_node_predicate(toggle), stuck_timeout_sec=1800.0,
            recover_action=lambda: extracted.append(toggle.host), extract_after_sec=600.0,
            confirm_timeout_sec=300.0,
        )
        probe.track(self_lease, self_healing, "KillNode",
                    recovered=_node_predicate(self_healing), stuck_timeout_sec=300.0)

        assert probe.drain_extracts() == 1, "only the toggle has something to extract"
        assert extracted == ["h1"], "dispatched even though the hold window is far from over"
        assert guard.snapshot()["impaired_racks"] == ["dc1/r1", "dc1/r2"], (
            "the drain is not a release: the toggle's budget waits for the healthcheck confirm"
        )

        rep.publish(_healthy_results())
        probe.tick()
        assert guard.snapshot()["impaired_racks"] == [] and probe.pending() == []
        assert probe.drain_extracts() == 0 and extracted == ["h1"], "idempotent"


class TestUntrackIdentity:
    def test_explicit_extract_untracks_the_pending_lease(self):
        guard, clock = _guard(), FakeClock()
        rep = StubReporter(clock)
        probe = RecoveryProbe(guard=guard, hc_source=rep, min_hold_sec=0.0, clock=clock)
        target, lease = _reserve(guard)
        probe.track(lease, target, "KillNode", recovered=_node_predicate(target),
                    stuck_timeout_sec=100.0)

        # A manual extract releases by identity (its execution id is fresh) — the probe
        # must stop waiting for the target, or it would cry stuck over a released lease.
        guard.record_extract("manual-extract", target, ImpactScope.NODE)
        assert probe.untrack_identity(target.identity_key()) == 1
        assert probe.pending() == [] and guard.snapshot()["impaired_racks"] == []
        assert probe.untrack_identity(target.identity_key()) == 0, "idempotent"

    def test_an_untracked_lease_does_not_report_stuck(self):
        guard, clock, seen = _guard(), FakeClock(), []
        rep = StubReporter(clock)
        probe = RecoveryProbe(
            guard=guard, hc_source=rep, on_stuck=seen.append, min_hold_sec=0.0, clock=clock
        )
        target, lease = _reserve(guard)
        probe.track(lease, target, "KillNode", recovered=_node_predicate(target),
                    stuck_timeout_sec=100.0)

        clock.advance(200.0)  # past the stuck timeout
        rep.publish({"h1": _error_entry(), "h2": _hc("h2")})
        probe.untrack_identity(target.identity_key())  # explicit extract raced the tick
        assert probe.tick() == [] and seen == [], "no stuck report over a released lease"


def test_stuck_fault_is_serializable_for_the_problem_log():
    fault = StuckFault(
        lease_id="l1",
        nemesis_type="KillNode",
        target=ChaosTarget.for_node("h1", node_id=1),
        held_sec=400.0,
        timeout_sec=300.0,
        phase=PHASE_CONFIRM,
    )
    assert fault.target.identity_key() == "node:1:h1" and fault.phase == "confirm"
