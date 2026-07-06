"""Tests for fetch_and_build_dashboard URL helpers."""

from __future__ import annotations

import sys
from pathlib import Path

_METRICS_DIR = Path(__file__).resolve().parent
if str(_METRICS_DIR) not in sys.path:
    sys.path.insert(0, str(_METRICS_DIR))

from fetch_and_build_dashboard import build_preset_from_config_segment  # noqa: E402


def test_build_preset_from_config_segment():
    assert build_preset_from_config_segment("ya-main-x86-64") == "relwithdebinfo"
    assert build_preset_from_config_segment("ya-x86-64") == "relwithdebinfo"
    assert build_preset_from_config_segment("ya-main-x86-64-asan") == "release-asan"
    assert build_preset_from_config_segment("ya-main-x86-64-tsan") == "release-tsan"
    assert build_preset_from_config_segment("ya-main-x86-64-msan") == "release-msan"


if __name__ == "__main__":
    test_build_preset_from_config_segment()
    print("OK")
