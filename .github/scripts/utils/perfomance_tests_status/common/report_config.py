"""Load per-report defaults from ``<report_dir>/report_config.json``."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


CONFIG_NAME = "report_config.json"


def load_report_config(report_dir: Path | str) -> dict[str, Any]:
    """Read report defaults. Missing file → empty dict (callers keep code fallbacks)."""
    path = Path(report_dir) / CONFIG_NAME
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit(f"{path}: expected JSON object")
    return data


def cfg_int(cfg: dict[str, Any], key: str, default: int) -> int:
    v = cfg.get(key, default)
    return int(v)


def cfg_float(cfg: dict[str, Any], key: str, default: float) -> float:
    v = cfg.get(key, default)
    return float(v)


def cfg_str(cfg: dict[str, Any], key: str, default: str) -> str:
    v = cfg.get(key, default)
    return str(v) if v is not None else default
