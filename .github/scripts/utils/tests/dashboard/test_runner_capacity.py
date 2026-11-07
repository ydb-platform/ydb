"""Tests for measured runner capacity (dashboard limits / recommendations)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import DASHBOARD, add_product_paths

add_product_paths(DASHBOARD)

from runner_capacity import (  # noqa: E402
    MEM_BUDGET_FRACTION,
    YA_MAKE_MEM_FRACTION,
    capacity_from_measured,
    enrich_resources_overlay,
    mem_cpu_tiers_for,
)


def test_mem_cpu_tiers_for():
    assert mem_cpu_tiers_for(64) == (1, 2, 4, 8, 16, 32, 48, 64)
    assert mem_cpu_tiers_for(96) == (1, 2, 4, 8, 16, 32, 48, 64, 96)


def test_capacity_from_measured_requires_both():
    assert capacity_from_measured(cpu_cores=None, ram_gb=124.0) is None
    assert capacity_from_measured(cpu_cores=64, ram_gb=None) is None
    assert capacity_from_measured(cpu_cores=0, ram_gb=124.0) is None


def test_capacity_from_measured():
    cap = capacity_from_measured(cpu_cores=64, ram_gb=124.42)
    assert cap is not None
    assert cap.vcpu == 64
    assert abs(cap.ram_gb - 124.42) < 1e-6
    assert abs(cap.mem_budget_gb - 124.42 * MEM_BUDGET_FRACTION) < 1e-6
    assert abs(cap.ya_make_mem_limit_gb - 124.42 * YA_MAKE_MEM_FRACTION) < 1e-6
    assert 64 in cap.mem_cpu_tiers


def test_enrich_resources_overlay_measured_limits():
    overlay = {
        "xs_evlog_sec": [0.0, 1.0],
        "cpu_total_cores": [10.0, 20.0],
        "ram_gb": [100.0, 120.0],
        "cpu_cores": 64,
        "ram_total_gb": 124.42,
    }
    records = [{"cpu_cores": 64, "ram_total_gb": 124.42}]
    out, cap = enrich_resources_overlay(overlay, records=records)
    assert out is not None
    assert cap is not None
    assert out["runner_limits"]["cpu_cores_max"] == 64
    assert abs(out["runner_limits"]["ram_gb_max"] - 124.42) < 1e-6
    assert out["measured"]["ram_gb"] == 124.42
    assert out["runner_capacity"]["vcpu"] == 64


def test_enrich_resources_overlay_no_limits_without_monitor():
    overlay = {
        "xs_evlog_sec": [0.0],
        "cpu_total_cores": [1.0],
        "ram_gb": [1.0],
    }
    out, cap = enrich_resources_overlay(overlay, records=[])
    assert out is not None
    assert cap is None
    assert "runner_limits" not in out
    assert "runner_capacity" not in out


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for fn in (
        test_mem_cpu_tiers_for,
        test_capacity_from_measured_requires_both,
        test_capacity_from_measured,
        test_enrich_resources_overlay_measured_limits,
        test_enrich_resources_overlay_no_limits_without_monitor,
    ):
        suite.addTest(unittest.FunctionTestCase(fn))
    return suite


if __name__ == "__main__":
    test_mem_cpu_tiers_for()
    test_capacity_from_measured_requires_both()
    test_capacity_from_measured()
    test_enrich_resources_overlay_measured_limits()
    test_enrich_resources_overlay_no_limits_without_monitor()
    print("OK")
