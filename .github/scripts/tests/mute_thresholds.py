#!/usr/bin/env python3

import json
import math
import os
from functools import lru_cache


_REQUIRED_KEYS = (
    "mute_window_days",
    "default_unmute_window_days",
    "delete_window_days",
    "manual_fast_unmute_window_days",
    "control_comment_part_max_tests",
)


def _thresholds_path():
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, "..", "..", "config", "mute_thresholds.json")


@lru_cache(maxsize=1)
def load_thresholds():
    path = _thresholds_path()
    try:
        with open(path, "r", encoding="utf-8") as fp:
            payload = json.load(fp)
    except FileNotFoundError:
        raise RuntimeError(f"Thresholds config not found: {path}")

    if not isinstance(payload, dict):
        raise RuntimeError(f"Thresholds config must be JSON object: {path}")

    raw = dict(payload)

    # Backward-compatible aliases for older key names.
    if "mute_days" in raw and "mute_window_days" not in raw:
        raw["mute_window_days"] = raw["mute_days"]
    if "delete_days" in raw and "delete_window_days" not in raw:
        raw["delete_window_days"] = raw["delete_days"]
    if (
        "manual_fast_unmute_wait_hours" in raw
        and "manual_fast_unmute_window_days" not in raw
    ):
        wait_hours = int(raw["manual_fast_unmute_wait_hours"])
        raw["manual_fast_unmute_window_days"] = max(1, math.ceil(wait_hours / 24))

    missing = [key for key in _REQUIRED_KEYS if key not in raw]
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise RuntimeError(f"Missing threshold keys in {path}: {missing_text}")

    thresholds = {}
    for key in _REQUIRED_KEYS:
        thresholds[key] = int(raw[key])

    # Derived value (single source of truth is manual_fast_unmute_window_days).
    thresholds["manual_fast_unmute_wait_hours"] = int(thresholds["manual_fast_unmute_window_days"]) * 24
    return thresholds


def get_thresholds():
    return load_thresholds()
