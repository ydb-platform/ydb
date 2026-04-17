#!/usr/bin/env python3
"""
Load mute / unmute / quarantine thresholds from JSON and evaluate default rules.

Canonical numbers live in .github/config/mute_coordinator_thresholds.json only.
"""

from __future__ import annotations

import json
import os
from typing import Dict, FrozenSet

_CONFIG_REL = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "mute_coordinator_thresholds.json")
)

_REQUIRED_KEYS: FrozenSet[str] = frozenset(
    {
        # Default mute / unmute / delete aggregation (create_new_muted_ya + coordinator)
        "default_mute_window_days",
        "default_unmute_window_days",
        "default_delete_window_days",
        "default_unmute_min_runs",
        "default_mute_total_runs_branch_boundary",
        "default_mute_min_fails_high_volume",
        "default_mute_min_fails_low_volume",
        "default_mute_min_fails_medium_volume",
        "default_mute_medium_volume_total_runs_gt_exclusive",
        # Quarantine (mute_coordinator only)
        "quarantine_new_test_window_days",
        "quarantine_user_fixed_window_days",
        "quarantine_manual_unmute_window_days",
        "quarantine_manual_mute_window_days",
        "quarantine_min_runs",
    }
)


def mute_coordinator_thresholds_path() -> str:
    return _CONFIG_REL


def load_mute_coordinator_thresholds() -> Dict[str, int]:
    with open(_CONFIG_REL, "r", encoding="utf-8") as f:
        raw = json.load(f)
    out = {k: int(v) for k, v in raw.items()}
    missing = sorted(_REQUIRED_KEYS - out.keys())
    if missing:
        raise KeyError(f"{_CONFIG_REL}: missing required keys: {missing}")
    return out


def passes_default_mute(pass_count: int, fail_count: int, thresholds: Dict[str, int]) -> bool:
    """Whether aggregated pass/fail counts justify automated mute."""
    total_runs = int(pass_count) + int(fail_count)
    fc = int(fail_count)
    bound = int(thresholds["default_mute_total_runs_branch_boundary"])
    hi = int(thresholds["default_mute_min_fails_high_volume"])
    lo = int(thresholds["default_mute_min_fails_low_volume"])
    if (fc >= hi and total_runs > bound) or (fc >= lo and total_runs <= bound):
        return True
    # Fills the gap: (fail>=3 & runs>10) OR (fail>=2 & runs<=10) never matched (2 fails, runs>10).
    # Set medium fails to 0 in config to disable this branch.
    med = int(thresholds["default_mute_min_fails_medium_volume"])
    med_ex = int(thresholds["default_mute_medium_volume_total_runs_gt_exclusive"])
    if med <= 0:
        return False
    return fc >= med and total_runs > med_ex


def is_delete_candidate_counts(
    pass_count: int,
    fail_count: int,
    mute_count: int,
    skip_count: int,
    *,
    is_muted: bool,
) -> bool:
    """Same rules as create_new_muted_ya.is_delete_candidate: remove-from-mutes / no-signal test."""
    p, f, m, s = int(pass_count), int(fail_count), int(mute_count), int(skip_count)
    total_runs = p + f + m + s
    only_skipped_while_muted = (
        is_muted and s > 0 and p == 0 and f == 0 and m == 0
    )
    return total_runs == 0 or only_skipped_while_muted


def passes_default_unmute(
    pass_count: int,
    fail_count: int,
    mute_count: int,
    thresholds: Dict[str, int],
) -> bool:
    """Whether aggregated stats justify automated unmute."""
    min_runs = int(thresholds["default_unmute_min_runs"])
    total_runs = int(pass_count) + int(fail_count) + int(mute_count)
    total_fails = int(fail_count) + int(mute_count)
    return total_runs >= min_runs and total_fails == 0
