#!/usr/bin/env python3
"""Resolve build/test preset defaults from CI_PRESET_DEFAULTS repo variable (JSON)."""

from __future__ import annotations

import json
import os
import sys
from typing import Any

# Used when vars.CI_PRESET_DEFAULTS is unset (forks, local runs).
_BUILTIN_DEFAULTS: dict[str, Any] = {
    "relwithdebinfo": {
        "ci": {
            "test_threads": 52,
            "test_size": "small,medium",
            "link_threads": 12,
            "test_type": "",
            "timeout_minutes": 600,
        },
        "run_tests": {
            "test_threads": 52,
            "test_size": "small,medium,large",
            "link_threads": 12,
            "test_type": "",
            "timeout_minutes": 1200,
        },
    },
    "release-asan": {
        "ci": {
            "test_threads": 52,
            "test_size": "small,medium",
            "link_threads": 12,
            "test_type": "",
            "timeout_minutes": 600,
        },
        "run_tests": {
            "test_threads": 20,
            "test_size": "small,medium,large",
            "link_threads": 12,
            "test_type": "",
            "timeout_minutes": 1200,
        },
    },
    "release-tsan": {
        "ci": {
            "test_threads": 52,
            "test_size": "small,medium",
            "link_threads": 12,
            "test_type": "",
            "timeout_minutes": 600,
        },
        "run_tests": {
            "test_threads": 18,
            "test_size": "small,medium,large",
            "link_threads": 12,
            "test_type": "",
            "timeout_minutes": 1200,
        },
    },
    "release-msan": {
        "ci": {
            "test_threads": 52,
            "test_size": "small,medium",
            "link_threads": 12,
            "test_type": "",
            "timeout_minutes": 600,
        },
        "run_tests": {
            "test_threads": 5,
            "test_size": "small,medium,large",
            "link_threads": 12,
            "test_type": "",
            "timeout_minutes": 1200,
        },
    },
}


def _write_output(name: str, value: str) -> None:
    out = os.environ.get("GITHUB_OUTPUT")
    if not out:
        raise RuntimeError("GITHUB_OUTPUT is not set")
    with open(out, "a", encoding="utf-8") as handle:
        handle.write(f"{name}={value}\n")


def _load_config() -> dict[str, Any]:
    raw = os.environ.get("CI_PRESET_DEFAULTS", "").strip()
    if not raw:
        return _BUILTIN_DEFAULTS
    config = json.loads(raw)
    if not isinstance(config, dict):
        raise TypeError("CI_PRESET_DEFAULTS must be a JSON object")
    return config


def _pick(config: dict[str, Any], build_preset: str, profile: str) -> dict[str, Any]:
    preset_cfg = config.get(build_preset)
    if preset_cfg is None:
        raise KeyError(f"Unknown build_preset: {build_preset}")

    profile_cfg = preset_cfg.get(profile)
    if profile_cfg is None:
        raise KeyError(f"Unknown profile {profile!r} for build_preset {build_preset!r}")

    return profile_cfg


def _resolve_test_threads(preset: dict[str, Any], *, override_threads: str = "") -> str:
    if override_threads.strip():
        return override_threads.strip()
    return str(preset["test_threads"])


def resolve(
    config: dict[str, Any],
    profile: str,
    build_preset: str,
    *,
    override_threads: str = "",
    override_size: str = "",
    override_type: str = "",
    override_link: str = "",
    override_timeout: str = "",
) -> dict[str, str]:
    preset = _pick(config, build_preset, profile)

    return {
        "test_threads": _resolve_test_threads(preset, override_threads=override_threads),
        "test_size": override_size.strip() or str(preset["test_size"]),
        "test_type": override_type.strip() or str(preset["test_type"]),
        "link_threads": override_link.strip() or str(preset["link_threads"]),
        "timeout_minutes": override_timeout.strip() or str(preset["timeout_minutes"]),
    }


def main() -> None:
    config = _load_config()

    resolved = resolve(
        config,
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
