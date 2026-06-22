#!/usr/bin/env python3
"""Resolve build/test preset defaults from .github/config/ci_presets.json."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any


def _write_output(name: str, value: str) -> None:
    out = os.environ.get("GITHUB_OUTPUT")
    if not out:
        raise RuntimeError("GITHUB_OUTPUT is not set")
    with open(out, "a", encoding="utf-8") as handle:
        handle.write(f"{name}={value}\n")


def _pick(config: dict[str, Any], build_preset: str, profile: str) -> dict[str, Any]:
    preset_cfg = config.get(build_preset)
    if preset_cfg is None:
        raise KeyError(f"Unknown build_preset: {build_preset}")

    profile_cfg = preset_cfg.get(profile)
    if profile_cfg is None:
        raise KeyError(f"Unknown profile {profile!r} for build_preset {build_preset!r}")

    return profile_cfg


def resolve(
    config_path: Path,
    profile: str,
    build_preset: str,
    *,
    override_threads: str = "",
    override_size: str = "",
    override_type: str = "",
    override_link: str = "",
    override_timeout: str = "",
) -> dict[str, str]:
    with config_path.open(encoding="utf-8") as handle:
        config = json.load(handle)

    preset = _pick(config, build_preset, profile)

    return {
        "test_threads": override_threads.strip() or str(preset["test_threads"]),
        "test_size": override_size.strip() or str(preset["test_size"]),
        "test_type": override_type.strip() or str(preset["test_type"]),
        "link_threads": override_link.strip() or str(preset["link_threads"]),
        "timeout_minutes": override_timeout.strip() or str(preset["timeout_minutes"]),
    }


def main() -> None:
    repo_root = Path(os.environ.get("REPO_ROOT", ".")).resolve()
    config_path = repo_root / ".github/config/ci_presets.json"

    resolved = resolve(
        config_path,
        os.environ["PROFILE"],
        os.environ["BUILD_PRESET"],
        override_threads=os.environ.get("OVERRIDE_THREADS", ""),
        override_size=os.environ.get("OVERRIDE_SIZE", ""),
        override_type=os.environ.get("OVERRIDE_TYPE", ""),
        override_link=os.environ.get("OVERRIDE_LINK", ""),
        override_timeout=os.environ.get("OVERRIDE_TIMEOUT", ""),
    )

    for key, value in resolved.items():
        _write_output(key, value)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 - surface resolver errors in CI logs
        print(f"resolve_ci_preset failed: {exc}", file=sys.stderr)
        raise
