"""
Library to read reserved CPU/RAM/SIZE from ya.make for a test suite.

Supports conditional branches by sanitizer:
  IF (SANITIZER_TYPE)
  IF (SANITIZER_TYPE == "thread")
  IF (SANITIZER_TYPE OR WITH_VALGRIND)

Usage:
  from .ya_make_requirements import get_requirements_for_suite, build_requirements_cache

  req = get_requirements_for_suite(repo_root, "ydb/tests/functional/blobstorage", sanitizer="thread")
  # -> {"ram_gb": 32, "cpu_cores": 4, "size": "LARGE"} (fields may be missing)
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Optional

# REQUIREMENTS(...) / SIZE(...) / IF/ELSE/ENDIF — from ya.make
RE_REQUIREMENTS_LINE = re.compile(r"^\s*REQUIREMENTS\s*\((.*)\)\s*$")
RE_REQ_RAM = re.compile(r"\bram\s*:\s*(\d+)\b", re.IGNORECASE)
RE_REQ_CPU = re.compile(r"\bcpu\s*:\s*(\w+)\b", re.IGNORECASE)
RE_SIZE = re.compile(r"^\s*SIZE\s*\(\s*(\w+)\s*\)\s*$")
RE_IF = re.compile(r"^\s*IF\s*\((.*)\)\s*$")
RE_ELSE = re.compile(r"^\s*ELSE\s*\(\s*\)\s*$")
RE_ENDIF = re.compile(r"^\s*ENDIF\s*\(\s*\)\s*$")
RE_SAN_EQ = re.compile(r'SANITIZER_TYPE\s*==\s*"([^"]+)"')
RE_SAN_NE = re.compile(r'SANITIZER_TYPE\s*!=\s*"([^"]+)"')

# Strip /partN for normalized suite path (same as in generate_chunk_trace)
PART_SUFFIX_RE = re.compile(r"/part\d+$")


def normalize_suite_path(path: str) -> str:
    """Merge partitioned suites like .../part7 into one suite path."""
    return PART_SUFFIX_RE.sub("", path)


def _has_sanitizer(sanitizer: Optional[str]) -> bool:
    return bool(sanitizer and str(sanitizer).strip() and str(sanitizer).strip().lower() not in ("none", "off", "false", "0"))


def _eval_condition(cond: str, sanitizer: Optional[str]) -> bool:
    """
    Evaluate a small subset of ya.make IF conditions for SANITIZER_TYPE branches.
    Unknown identifiers default to False.
    """
    has_san = _has_sanitizer(sanitizer)
    san = (sanitizer or "").strip().lower()
    expr = cond.strip()

    # Replace explicit SANITIZER_TYPE comparisons first.
    expr = RE_SAN_EQ.sub(lambda m: "True" if san == m.group(1).strip().lower() else "False", expr)
    expr = RE_SAN_NE.sub(lambda m: "True" if san != m.group(1).strip().lower() else "False", expr)

    # Replace known identifiers.
    expr = re.sub(r"\bSANITIZER_TYPE\b", "True" if has_san else "False", expr)
    expr = re.sub(r"\bWITH_VALGRIND\b", "False", expr)

    # Boolean operators.
    expr = re.sub(r"\bOR\b", "or", expr)
    expr = re.sub(r"\bAND\b", "and", expr)
    expr = re.sub(r"\bNOT\b", "not", expr)

    try:
        return bool(eval(expr, {"__builtins__": {}}, {}))
    except Exception:
        return has_san if "SANITIZER_TYPE" in cond else False


def _parse_active_attrs(text: str, sanitizer: Optional[str]) -> dict[str, Any]:
    """
    Evaluate ya.make with IF/ELSE/ENDIF for the selected sanitizer and collect
    REQUIREMENTS + SIZE from active lines.
    """
    attrs: dict[str, Any] = {}
    # Stack frames: (parent_active, if_result, current_active, seen_else)
    stack: list[tuple[bool, bool, bool, bool]] = []
    current_active = True

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue

        m_if = RE_IF.match(line)
        if m_if:
            parent_active = current_active
            if_res = _eval_condition(m_if.group(1), sanitizer)
            current_active = parent_active and if_res
            stack.append((parent_active, if_res, current_active, False))
            continue

        if RE_ELSE.match(line):
            if not stack:
                continue
            parent_active, if_res, _cur, seen_else = stack.pop()
            if seen_else:
                stack.append((parent_active, if_res, _cur, seen_else))
                continue
            current_active = parent_active and (not if_res)
            stack.append((parent_active, if_res, current_active, True))
            continue

        if RE_ENDIF.match(line):
            if not stack:
                continue
            stack.pop()
            current_active = stack[-1][2] if stack else True
            continue

        if not current_active:
            continue

        m_req = RE_REQUIREMENTS_LINE.match(line)
        if m_req:
            body = m_req.group(1)
            m_ram = RE_REQ_RAM.search(body)
            m_cpu = RE_REQ_CPU.search(body)
            if m_ram:
                attrs["ram_gb"] = int(m_ram.group(1))
            if m_cpu:
                cpu_val = m_cpu.group(1).strip().lower()
                if cpu_val != "all":
                    try:
                        attrs["cpu_cores"] = int(cpu_val)
                    except ValueError:
                        pass
            continue

        m_size = RE_SIZE.match(line)
        if m_size:
            attrs["size"] = m_size.group(1).upper()
            continue

    return attrs


def get_requirements_for_suite(repo_root: Path, suite_path: str, sanitizer: Optional[str] = None) -> Optional[dict[str, Any]]:
    """
    Read active REQUIREMENTS/SIZE from repo_root/suite_path/ya.make, optionally
    selecting SANITIZER_TYPE branch. Returns subset of:
      {"ram_gb": int, "cpu_cores": int, "size": str}
    or None if file doesn't exist or nothing relevant found.
    """
    suite_path = normalize_suite_path(suite_path)
    ya_make = repo_root / suite_path / "ya.make"
    if not ya_make.exists():
        return None
    try:
        text = ya_make.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None
    attrs = _parse_active_attrs(text, sanitizer)
    return attrs or None


def build_requirements_cache(repo_root: Path, suite_paths: list[str], sanitizer: Optional[str] = None) -> dict[str, dict[str, Any]]:
    """
    Build a mapping suite_path (normalized) -> attrs from active ya.make branch:
      {"ram_gb": int?, "cpu_cores": int?, "size": str?}
    """
    cache: dict[str, dict[str, Any]] = {}
    for suite_path in suite_paths:
        norm = normalize_suite_path(suite_path)
        if norm in cache:
            continue
        req = get_requirements_for_suite(repo_root, norm, sanitizer=sanitizer)
        if req is not None:
            cache[norm] = req
    return cache
