"""Catalog annotations must match how the runners actually behave."""

from __future__ import annotations

import pytest

from ydb.tests.stability.nemesis.internal.nemesis.catalog import (
    DEFAULT_STUCK_TIMEOUT_SEC,
    NEMESIS_TYPES,
    confirm_timeout_for,
    recovery_mode_for,
    recovery_sec_for,
    stuck_timeout_for,
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
    "NetworkNemesis",
    "DnsNemesis",
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


class TestRecoveryTimeouts:
    """The probe's stuck/confirm budgets: scaled to what the recovery actually takes."""

    def test_stuck_timeout_defaults_by_scope(self):
        assert stuck_timeout_for("KillNodeNemesis") == DEFAULT_STUCK_TIMEOUT_SEC
        assert stuck_timeout_for("KillSlotDaemonNemesis") == DEFAULT_STUCK_TIMEOUT_SEC
        # DISK scope waits out re-replication (BLUE blocks GREEN), not just a process restart.
        assert stuck_timeout_for("SafelyBreakDiskNemesis") == 3600.0

    def test_cleanup_disks_confirm_waits_for_a_full_resync(self):
        # Obliterate wipes the node's disks; re-replication takes far longer than the default.
        assert confirm_timeout_for("SafelyCleanupDisksNemesis") == 7200.0

    def test_unknown_type_uses_defaults(self):
        assert stuck_timeout_for("NoSuchNemesis") == DEFAULT_STUCK_TIMEOUT_SEC
        assert confirm_timeout_for("NoSuchNemesis") == DEFAULT_STUCK_TIMEOUT_SEC


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
