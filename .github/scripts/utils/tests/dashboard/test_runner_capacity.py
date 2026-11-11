"""Behavioral tests for measured host capacity on the resources overlay."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import DASHBOARD, add_product_paths

add_product_paths(DASHBOARD)

from runner_capacity import (  # noqa: E402
    DEFAULT_TUNING,
    DashboardTuning,
    enrich_resources_overlay,
    load_dashboard_tuning,
    measured_host_from_records,
)

try:
    import yaml
except ImportError:
    yaml = None

_TUNING = DashboardTuning(
    mem_budget_fraction=0.70,
    ya_make_mem_fraction=0.95,
    mem_min_chunk_gb=24.0,
    cpu_saturation_pct=90.0,
    near_budget_factor=1.10,
)


def test_enrich_attaches_single_host_block():
    overlay = {
        "xs_evlog_sec": [0.0, 1.0],
        "cpu_total_cores": [10.0, 20.0],
        "ram_gb": [100.0, 120.0],
        # series-normalization sentinel must not become host.vcpu
        "cpu_cores": 1,
    }
    records = [{"cpu_cores": 64, "ram_total_gb": 124.42}]
    out, host = enrich_resources_overlay(overlay, records=records, tuning=_TUNING)
    assert host is not None
    assert host.vcpu == 64
    assert out["host"] == {
        "vcpu": 64,
        "ram_gb": 124.4,
        "mem_budget_gb": 87.1,
        "ya_make_mem_limit_gb": 118.2,
    }
    assert "runner_limits" not in out
    assert "runner_capacity" not in out
    assert "measured" not in out


def test_no_host_from_overlay_sentinel_or_partial_records():
    overlay = {"xs_evlog_sec": [0.0], "cpu_cores": 1, "ram_total_gb": 124.0}
    out, host = enrich_resources_overlay(overlay, records=[], tuning=_TUNING)
    assert host is None
    assert "host" not in out

    # RAM without CPU (or vice versa) is not enough.
    assert measured_host_from_records([{"ram_total_gb": 124.0}]) is None
    assert measured_host_from_records([{"cpu_cores": 64}]) is None
    # Overlay scalars alone must never invent a host.
    out2, host2 = enrich_resources_overlay(
        {"cpu_cores": 64, "ram_total_gb": 124.0},
        records=[{"ram_used_kb": 1}],
        tuning=_TUNING,
    )
    assert host2 is None
    assert "host" not in out2


def test_load_tuning_from_repo_config():
    if yaml is None:
        raise unittest.SkipTest("PyYAML is not installed")
    assert load_dashboard_tuning() == DEFAULT_TUNING


def test_load_tuning_falls_back_when_config_missing():
    assert load_dashboard_tuning(Path("/nonexistent/tests_resource_dashboard.yml")) == DEFAULT_TUNING


def test_load_tuning_rejects_bad_value():
    if yaml is None:
        raise unittest.SkipTest("PyYAML is not installed")

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "bad.yml"
        path.write_text(
            "mem_budget_fraction: 0\n"
            "ya_make_mem_fraction: 0.95\n"
            "mem_min_chunk_gb: 24.0\n"
            "cpu_saturation_pct: 90.0\n"
            "near_budget_factor: 1.10\n",
            encoding="utf-8",
        )
        try:
            load_dashboard_tuning(path)
        except ValueError:
            return
        raise AssertionError("expected ValueError for mem_budget_fraction: 0")


def _all_tests():
    return [
        test_enrich_attaches_single_host_block,
        test_no_host_from_overlay_sentinel_or_partial_records,
        test_load_tuning_from_repo_config,
        test_load_tuning_falls_back_when_config_missing,
        test_load_tuning_rejects_bad_value,
    ]


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for fn in _all_tests():
        suite.addTest(unittest.FunctionTestCase(fn))
    return suite


if __name__ == "__main__":
    failures = 0
    for t in _all_tests():
        try:
            t()
            print(f"PASS {t.__name__}")
        except unittest.SkipTest as e:
            print(f"SKIP {t.__name__}: {e}")
        except AssertionError as e:
            failures += 1
            print(f"FAIL {t.__name__}: {e}")
    sys.exit(1 if failures else 0)
