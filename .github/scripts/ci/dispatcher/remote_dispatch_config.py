#!/usr/bin/env python3
"""Thin shim: map CONFIG_JSON onto env for dispatch/ingest entrypoints."""
from __future__ import annotations

import os
import sys
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import apply_config_env, die, load_config_json  # noqa: E402, F401


def load_config(raw: str | None = None) -> dict[str, Any] | None:
    cfg = load_config_json(raw)
    return cfg or None


def apply_to_environ(cfg: dict[str, Any] | None = None, *, overwrite: bool = False) -> dict[str, Any] | None:
    # overwrite kept for call-site compat; apply_config_env always sets from config.
    _ = overwrite
    return apply_config_env(cfg)
