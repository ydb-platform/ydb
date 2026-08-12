"""The chaos-side problem log behind ``GET /api/problems``."""

from __future__ import annotations

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_problems import (
    KIND_INVENTORY_DEGRADED,
    KIND_PROBE_BLIND,
    KIND_STUCK_FAULT,
    ChaosProblemStore,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import StuckFault


def _stuck(host: str = "h1") -> StuckFault:
    return StuckFault(
        lease_id="lease-1",
        nemesis_type="StopStartNodeNemesis",
        target=ChaosTarget.for_node(host, node_id=1),
        held_sec=420.0,
        timeout_sec=300.0,
    )


def test_stuck_fault_names_its_target_and_numbers():
    store = ChaosProblemStore()
    store.record_stuck_fault(_stuck())
    problem = store.snapshot()[0]
    assert problem["kind"] == KIND_STUCK_FAULT
    assert (problem["target"], problem["host"]) == ("node:1:h1", "h1")
    assert problem["details"] == {
        "held_sec": 420.0,
        "timeout_sec": 300.0,
        "lease_id": "lease-1",
        "phase": "hold",
    }
    assert "did not recover" in problem["summary"]


def test_repeats_are_counted_and_distinct_problems_kept_apart():
    store = ChaosProblemStore()
    for _ in range(3):
        store.record_stuck_fault(_stuck("h1"))
    store.record_stuck_fault(_stuck("h2"))
    store.record_inventory_degraded("cluster harness unavailable")

    assert store.counts_by_kind() == {KIND_STUCK_FAULT: 2, KIND_INVENTORY_DEGRADED: 1}
    by_host = {p["host"]: p for p in store.snapshot() if p["host"]}
    assert by_host["h1"]["count"] == 3 and by_host["h2"]["count"] == 1


def test_store_is_bounded():
    store = ChaosProblemStore(limit=2)
    for i in range(5):
        store.record_stuck_fault(_stuck(f"h{i}"))
    assert len(store.snapshot()) == 2 and store.dropped == 3


def test_resolve_kind_drops_only_matching_entries():
    # Probe blindness is self-healing: once the probe can see again, its entry must not
    # keep saying "broken" in the UI — unlike stuck faults, which stay latched for a human.
    store = ChaosProblemStore()
    store.record_probe_blind()
    store.record_stuck_fault(_stuck())
    assert store.resolve_kind(KIND_PROBE_BLIND) == 1
    assert store.counts_by_kind() == {KIND_STUCK_FAULT: 1}
    assert store.resolve_kind(KIND_PROBE_BLIND) == 0, "idempotent"
