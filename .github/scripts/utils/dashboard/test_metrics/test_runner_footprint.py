"""Tests for runner_footprint config loader."""

from __future__ import annotations

import sys
from pathlib import Path

_DASHBOARD_DIR = Path(__file__).resolve().parent.parent
if str(_DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(_DASHBOARD_DIR))

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
    assert fp.vcpu == 96
    assert fp.ram_gb == 288.0
    assert fp.mem_budget_gb == 288.0 * 0.70
    assert fp.ya_make_mem_limit_gb == 288.0 * 0.95
    assert fp.footprint_key == "build-preset-release-asan"
    assert 96 in fp.mem_cpu_tiers


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
        "cpu_cores": 96,
    }
    records = [{"ram_total_gb": 280.5}]
    out = enrich_resources_overlay(overlay, fp, records=records)
    assert out is not None
    assert out["runner_limits"]["cpu_cores_max"] == 96
    assert out["runner_limits"]["ram_gb_max"] == 288.0
    assert out["measured"]["ram_gb"] == 280.5
    assert out["runner_footprint"]["build_preset"] == "release-asan"


if __name__ == "__main__":
    test_footprint_key_for_preset()
    test_resolve_release_asan_footprint()
    test_resolve_relwithdebinfo_footprint()
    test_mem_cpu_tiers_for()
    test_enrich_resources_overlay_adds_limits()
    print("OK")
