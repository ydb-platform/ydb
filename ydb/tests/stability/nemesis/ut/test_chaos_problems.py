"""Unit tests for the nemesis problem log and the scheduler-profile validation.

Both exist so that chaos-side trouble reaches the stability test report instead of a log line:
``ChaosProblemStore`` collects it, ``GET /api/problems`` serves it, and a rejected profile keeps a
typo from silently disabling all chaos.
"""

from __future__ import annotations

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_problems import (
    KIND_INVENTORY_DEGRADED,
    KIND_STUCK_FAULT,
    ChaosProblemStore,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import StuckFault
from ydb.tests.stability.nemesis.routers.orchestrator_router import _validated_profile


def _stuck(host: str = "h1", nemesis_type: str = "StopStartNodeNemesis") -> StuckFault:
    return StuckFault(
        lease_id="lease-1",
        nemesis_type=nemesis_type,
        target=ChaosTarget.for_node(host, node_id=1),
        held_sec=420.0,
        timeout_sec=300.0,
    )


class TestChaosProblemStore:
    def test_stuck_fault_is_recorded_with_target_and_numbers(self):
        store = ChaosProblemStore()
        store.record_stuck_fault(_stuck())
        problems = store.snapshot()
        assert len(problems) == 1, f"one stuck fault must produce one problem; got {problems}"
        problem = problems[0]
        assert problem["kind"] == KIND_STUCK_FAULT
        assert problem["host"] == "h1" and problem["target"] == "node:1:h1", (
            f"the problem must name the target that did not recover; got {problem}"
        )
        assert problem["details"]["held_sec"] == 420.0
        assert problem["details"]["timeout_sec"] == 300.0
        assert "did not recover" in problem["summary"], (
            f"summary must be readable in a test report; got {problem['summary']!r}"
        )

    def test_repeated_problem_is_deduplicated_with_a_counter(self):
        store = ChaosProblemStore()
        for _ in range(3):
            store.record_stuck_fault(_stuck())
        problems = store.snapshot()
        assert len(problems) == 1, "the same fault must not pile up as separate problems"
        assert problems[0]["count"] == 3, f"repeats must bump count; got {problems[0]}"
        assert problems[0]["last_seen"] >= problems[0]["first_seen"]

    def test_distinct_targets_stay_separate(self):
        store = ChaosProblemStore()
        store.record_stuck_fault(_stuck(host="h1"))
        store.record_stuck_fault(_stuck(host="h2"))
        store.record_inventory_degraded("cluster harness unavailable")
        assert store.counts_by_kind() == {KIND_STUCK_FAULT: 2, KIND_INVENTORY_DEGRADED: 1}, (
            f"different targets/kinds must be separate problems; got {store.counts_by_kind()}"
        )

    def test_store_is_bounded(self):
        store = ChaosProblemStore(limit=2)
        for i in range(5):
            store.record_stuck_fault(_stuck(host=f"h{i}"))
        assert len(store.snapshot()) == 2, "the store must not grow without bound"
        assert store.dropped == 3, f"dropped problems must be counted; got {store.dropped}"

    def test_clear_resets(self):
        store = ChaosProblemStore()
        store.record_stuck_fault(_stuck())
        store.clear()
        assert store.snapshot() == [] and store.dropped == 0


class TestSchedulerProfileValidation:
    def test_empty_body_is_a_valid_no_op(self):
        profile, error = _validated_profile({})
        assert (profile, error) == ({}, None), "an empty body must keep the current profile"

    def test_unknown_type_is_rejected(self):
        profile, error = _validated_profile({"enabled": ["KillNodeNemesis", "NoSuchNemesis"]})
        assert profile == {} and error is not None, "an unknown type must not be accepted"
        assert "NoSuchNemesis" in error, f"the error must name the offender; got {error!r}"

    def test_string_instead_of_list_is_rejected(self):
        # list("KillNodeNemesis") would silently become 15 single-character "types".
        profile, error = _validated_profile({"enabled": "KillNodeNemesis"})
        assert profile == {} and error is not None, "a bare string must not be split into types"

    def test_known_types_pass_through(self):
        profile, error = _validated_profile({"enabled": ["KillNodeNemesis"]})
        assert error is None and profile == {"enabled": ["KillNodeNemesis"]}

    def test_numeric_bounds(self):
        good, error = _validated_profile(
            {"base_interval": 30, "jitter": 0.25, "max_per_tick": 4}
        )
        assert error is None
        assert good == {"base_interval": 30.0, "jitter": 0.25, "max_per_tick": 4}

        for body in (
            {"jitter": 1.5},
            {"jitter": "wide"},
            {"base_interval": 0},
            {"max_per_tick": 0},
            {"max_per_tick": "many"},
        ):
            profile, error = _validated_profile(body)
            assert profile == {} and error is not None, f"{body} must be rejected"

    def test_unknown_field_is_rejected(self):
        profile, error = _validated_profile({"max_pertick": 3})
        assert profile == {} and error is not None, (
            "a misspelled field must not be silently ignored"
        )
        assert "max_pertick" in error
