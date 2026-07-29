"""Catalog annotations must match how the runners actually behave."""

from __future__ import annotations

import pytest

from ydb.tests.stability.nemesis.internal.nemesis.catalog import (
    NEMESIS_TYPES,
    impairment_hold_sec_for,
    recovery_mode_for,
    recovery_sec_for,
    supports_boundary_scheduler,
    target_kind_for,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.boundary_scheduler import (
    _STABILITY_PROFILE,
    default_enabled_types,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import TargetKind

# Faults that stay applied until something extracts them.
TOGGLE_TYPES = (
    "StopStartNodeNemesis",
    "SuspendNodeNemesis",
    "SafelyBreakDiskNemesis",
    "SafelyCleanupDisksNemesis",
    "TimeSkewNemesis",
)


@pytest.mark.parametrize("field", ["target_kind", "impact_scope", "guard_mode"])
def test_every_type_annotates_the_guard_fields(field):
    # Implicit defaults would silently exempt custom-planner types from the budget.
    missing = sorted(name for name, spec in NEMESIS_TYPES.items() if field not in spec)
    assert missing == [], missing


@pytest.mark.parametrize("nemesis_type", TOGGLE_TYPES)
def test_toggle_faults_recover_by_extract(nemesis_type):
    # Annotated self-recovering, they would wait for a healthcheck that never arrives.
    assert recovery_mode_for(nemesis_type) == "extract"
    assert recovery_sec_for(nemesis_type) is not None, "needs a hold window before the extract"


def test_kills_are_self_recovering():
    assert recovery_mode_for("KillNodeNemesis") == "self"
    assert recovery_mode_for("KillSlotDaemonNemesis") == "self"


class TestImpairmentHold:
    def test_paired_extract_holds_a_toggle_until_extracted(self):
        assert impairment_hold_sec_for("StopStartNodeNemesis", paired_extract=True) is None

    def test_unpaired_toggle_is_held_for_its_pulse(self):
        # The legacy loop toggles the fault back with its next inject, so the hold covers the gap.
        hold = impairment_hold_sec_for("StopStartNodeNemesis", paired_extract=False)
        assert hold is not None and hold >= float(NEMESIS_TYPES["StopStartNodeNemesis"]["schedule"])

    def test_self_recovering_types_ignore_pairing(self):
        expected = recovery_sec_for("KillNodeNemesis")
        assert impairment_hold_sec_for("KillNodeNemesis", paired_extract=True) == expected
        assert impairment_hold_sec_for("KillNodeNemesis", paired_extract=False) == expected


class TestBoundarySchedulerCompatibility:
    def test_whole_default_profile_is_usable(self):
        enabled = default_enabled_types()
        assert enabled and set(enabled) == {t for t in _STABILITY_PROFILE if t in NEMESIS_TYPES}, (
            "dropping a profile member would silently shrink chaos"
        )

    def test_custom_planners_must_opt_in(self):
        # Fail closed: a planner with its own targets would inject what the guard never reserved.
        assert supports_boundary_scheduler("ClusterRollingRestartNemesis") is False
        assert supports_boundary_scheduler("NoSuchNemesis") is False
        for name, spec in NEMESIS_TYPES.items():
            custom = spec.get("planner_cls") is not None or spec.get("planner_factory") is not None
            derived_safe = (
                target_kind_for(name) is TargetKind.DATACENTER
                or recovery_mode_for(name) == "extract"
            )
            if custom and not derived_safe and "boundary_safe" not in spec:
                assert supports_boundary_scheduler(name) is False, name

    def test_default_planner_and_opted_in_types_are_usable(self):
        for name in ("KillNodeNemesis", "KillHiveNemesis", "SerialKillNodeNemesis"):
            assert supports_boundary_scheduler(name) is True, name
