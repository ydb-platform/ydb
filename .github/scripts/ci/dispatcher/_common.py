#!/usr/bin/env python3
"""Shared helpers for platform remote dispatcher / ingest scripts."""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, NoReturn


def die(msg: str) -> NoReturn:
    print(f"::error::{msg}", file=sys.stderr)
    sys.exit(1)


def env(name: str) -> str:
    v = os.environ.get(name)
    if v is None or v == "":
        die(f"missing env {name}")
    return v


def env_opt(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def quote_url(url: str) -> str:
    return urllib.parse.quote(url, safe=":/%")


def http_get_bytes(url: str, timeout: int = 60) -> bytes | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read()
    except Exception as e:
        print(f"GET {url} failed: {e}")
        return None


def http_get_text(url: str, timeout: int = 60) -> str | None:
    raw = http_get_bytes(url, timeout=timeout)
    return None if raw is None else raw.decode()


def append_summary(text: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(text if text.endswith("\n") else text + "\n")


def set_outputs(**kwargs: Any) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        for k, v in kwargs.items():
            s = str(v)
            if "\n" in s or "\r" in s:
                delim = f"ghadelimiter_{os.urandom(8).hex()}"
                f.write(f"{k}<<{delim}\n{s}\n{delim}\n")
            else:
                f.write(f"{k}={s}\n")


def load_config_json() -> dict[str, Any]:
    text = (os.environ.get("CONFIG_JSON") or "").strip()
    if not text:
        die("CONFIG_JSON empty")
    try:
        cfg = json.loads(text)
    except json.JSONDecodeError as e:
        die(f"CONFIG_JSON is not valid JSON: {e}")
    if not isinstance(cfg, dict):
        die("CONFIG_JSON must be an object")
    return cfg


def cfg_str(cfg: dict[str, Any], key: str) -> str:
    v = cfg.get(key)
    if v is None:
        die(f"CONFIG_JSON.{key} missing")
    s = str(v).strip()
    if not s:
        die(f"CONFIG_JSON.{key} missing")
    return s


def cfg_bool(cfg: dict[str, Any], key: str) -> bool:
    if key not in cfg:
        die(f"CONFIG_JSON.{key} missing")
    v = cfg[key]
    if isinstance(v, bool):
        return v
    die(f"CONFIG_JSON.{key} must be bool, got {type(v).__name__}: {v!r}")


def host_s3(cfg: dict[str, Any]) -> tuple[str, str]:
    ep = cfg_str(cfg, "host_s3_endpoint").rstrip("/")
    bucket = cfg_str(cfg, "host_s3_bucket")
    return ep, bucket


def object_url(endpoint: str, bucket: str, key: str) -> str:
    return quote_url(f"{endpoint.rstrip('/')}/{bucket}/{key.lstrip('/')}")


def discover_attempts(public_dir: Path) -> list[dict[str, Any]]:
    """Scan try_*/report.json under public_dir; each try dir must have started_at."""
    found: list[dict[str, Any]] = []
    if not public_dir.is_dir():
        return found
    for report in sorted(public_dir.glob("try_*/report.json")):
        m = re.match(r"try_(\d+)$", report.parent.name)
        if not m:
            continue
        started_file = report.parent / "started_at"
        if not started_file.is_file():
            die(f"missing {started_file}")
        started_at = started_file.read_text(encoding="utf-8").strip()
        if not started_at:
            die(f"empty {started_file}")
        found.append(
            {
                "retry": int(m.group(1)),
                "started_at": started_at,
                "report_relpath": str(report.relative_to(public_dir)),
                "html_relpath": str((report.parent / "ya-test.html").relative_to(public_dir)),
            }
        )
    return found

