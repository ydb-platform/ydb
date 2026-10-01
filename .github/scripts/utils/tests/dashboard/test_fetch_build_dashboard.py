"""Tests for fetch_and_build_dashboard URL helpers."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import TEST_METRICS, add_product_paths

add_product_paths(TEST_METRICS)

from fetch_and_build_dashboard import build_preset_from_config_segment  # noqa: E402
import fetch_and_build_dashboard as fetch_mod  # noqa: E402


def test_build_preset_from_config_segment():
    assert build_preset_from_config_segment("ya-main-x86-64") == "relwithdebinfo"
    assert build_preset_from_config_segment("ya-x86-64") == "relwithdebinfo"
    assert build_preset_from_config_segment("ya-main-x86-64-asan") == "release-asan"
    assert build_preset_from_config_segment("ya-main-x86-64-tsan") == "release-tsan"
    assert build_preset_from_config_segment("ya-main-x86-64-msan") == "release-msan"


def test_dry_run_skips_fetch():
    def boom(*_args, **_kwargs):
        raise AssertionError("dry-run must not fetch")

    old_fetch = fetch_mod.fetch_text
    old_argv = sys.argv
    fetch_mod.fetch_text = boom
    try:
        sys.argv = [
            "fetch_and_build_dashboard.py",
            "--dry-run",
            "https://example.com/PR-check/1/ya-x86-64/index.html",
        ]
        fetch_mod.main()
    finally:
        fetch_mod.fetch_text = old_fetch
        sys.argv = old_argv


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for fn in (test_build_preset_from_config_segment, test_dry_run_skips_fetch):
        suite.addTest(unittest.FunctionTestCase(fn))
    return suite


if __name__ == "__main__":
    test_build_preset_from_config_segment()
    test_dry_run_skips_fetch()
    print("OK")
