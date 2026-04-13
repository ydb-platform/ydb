#!/usr/bin/env python3

import json
import os
from functools import lru_cache


_REQUIRED_KEYS = (
    "mute_window_days",
    "default_unmute_window_days",
    "delete_window_days",
    "manual_fast_unmute_window_days",
    "manual_fast_unmute_min_passes",
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

    missing = [key for key in _REQUIRED_KEYS if key not in payload]
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise RuntimeError(f"Missing threshold keys in {path}: {missing_text}")

    thresholds = {}
    for key in _REQUIRED_KEYS:
        thresholds[key] = int(payload[key])
    return thresholds


def get_thresholds():
    return load_thresholds()
