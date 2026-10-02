"""Tests for runner_footprint config loader."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import DASHBOARD, add_product_paths

add_product_paths(DASHBOARD)

from runner_footprint import (  # noqa: E402
    enrich_resources_overlay,
    footprint_key_for_preset,
    mem_cpu_tiers_for,
    resolve_runner_footprint,
)


def test_footprint_key_for_preset():
    assert footprint_key_for_preset("release-asan") == "build-preset-release-asan"
    assert footprint_key_for_preset("build-preset-release-asan") == "build-preset-release-asan"


def test_resolve_release_asan_footprint():
    fp = resolve_runner_footprint(build_preset="release-asan")
    assert fp.vcpu == 64
    assert fp.ram_gb == 320.0
    assert fp.mem_budget_gb == 320.0 * 0.70
    assert fp.ya_make_mem_limit_gb == 320.0 * 0.95
    assert fp.footprint_key == "build-preset-release-asan"
    assert 64 in fp.mem_cpu_tiers


def test_resolve_relwithdebinfo_footprint():
    fp = resolve_runner_footprint(build_preset="relwithdebinfo")
    assert fp.vcpu == 64
    assert fp.ram_gb == 256.0


def test_mem_cpu_tiers_for():
    assert mem_cpu_tiers_for(64) == (1, 2, 4, 8, 16, 32, 48, 64)
    assert mem_cpu_tiers_for(96) == (1, 2, 4, 8, 16, 32, 48, 64, 96)


def test_enrich_resources_overlay_adds_limits():
    fp = resolve_runner_footprint(build_preset="release-asan")
    overlay = {
        "xs_evlog_sec": [0.0, 1.0],
        "cpu_total_cores": [10.0, 20.0],
        "ram_gb": [100.0, 120.0],
        "cpu_cores": 64,
    }
    records = [{"ram_total_gb": 280.5}]
    out = enrich_resources_overlay(overlay, fp, records=records)
    assert out is not None
    assert out["runner_limits"]["cpu_cores_max"] == 64
    assert out["runner_limits"]["ram_gb_max"] == 320.0
    assert out["measured"]["ram_gb"] == 280.5
    assert out["runner_footprint"]["build_preset"] == "release-asan"


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for fn in (
        test_footprint_key_for_preset,
        test_resolve_release_asan_footprint,
        test_resolve_relwithdebinfo_footprint,
        test_mem_cpu_tiers_for,
        test_enrich_resources_overlay_adds_limits,
    ):
        suite.addTest(unittest.FunctionTestCase(fn))
    return suite


if __name__ == "__main__":
    test_footprint_key_for_preset()
    test_resolve_release_asan_footprint()
    test_resolve_relwithdebinfo_footprint()
    test_mem_cpu_tiers_for()
    test_enrich_resources_overlay_adds_limits()
    print("OK")
