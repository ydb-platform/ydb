#!/usr/bin/env python3
"""Best-effort extraction of test full_name entries from ya make graph JSON."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

_TEST_PATH_RE = re.compile(r"^ydb/.+")


def normalize_target_prefix(prefix: str | None) -> str | None:
    if prefix is None:
        return None
    value = prefix.strip().rstrip("/")
    return value or None


def in_target_scope(full_name: str, target_prefix: str | None) -> bool:
    if target_prefix is None:
        return True
    return full_name == target_prefix or full_name.startswith(f"{target_prefix}/")


def _looks_like_test_full_name(value: str) -> bool:
    if "/" not in value:
        return False
    tail = value.rsplit("/", 1)[1]
    return "." in tail or "::" in tail


def _maybe_add(
    value: str,
    tests: set[str],
    target_prefix: str | None,
    *,
    require_test_name: bool = True,
) -> None:
    value = value.strip()
    if not value:
        return
    if not _TEST_PATH_RE.match(value):
        return
    if require_test_name and not _looks_like_test_full_name(value):
        return
    if not in_target_scope(value, target_prefix):
        return
    tests.add(value)


def _add_test_node(path: str, subtest: str, tests: set[str], target_prefix: str | None) -> None:
    path = path.strip()
    subtest = subtest.strip()
    if not path or not subtest:
        return
    _maybe_add(f"{path}/{subtest}", tests, target_prefix, require_test_name=False)


def _walk(obj, tests: set[str], target_prefix: str | None) -> None:
    if isinstance(obj, dict):
        path = obj.get("path") or obj.get("target") or obj.get("name")
        subtest = obj.get("subtest_name") or obj.get("test_filter")
        if isinstance(path, str) and isinstance(subtest, str):
            _add_test_node(path, subtest, tests, target_prefix)
        for key in ("full_name", "test_name"):
            val = obj.get(key)
            if isinstance(val, str):
                _maybe_add(val, tests, target_prefix)
        for value in obj.values():
            _walk(value, tests, target_prefix)
    elif isinstance(obj, list):
        for item in obj:
            _walk(item, tests, target_prefix)
    elif isinstance(obj, str):
        _maybe_add(obj, tests, target_prefix)


def extract_tests(graph: dict, target_prefix: str | None = None) -> list[str]:
    prefix = normalize_target_prefix(target_prefix)
    tests: set[str] = set()
    _walk(graph, tests, prefix)
    return sorted(tests)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("graph", type=Path, help="graph.json from graph_compare")
    parser.add_argument(
        "--target-prefix",
        default="ydb/",
        help="Only include tests under this ya.make path (default: ydb/)",
    )
    parser.add_argument("-o", "--output", type=Path, help="Write one full_name per line")
    args = parser.parse_args()

    graph = json.loads(args.graph.read_text(encoding="utf-8"))
    tests = extract_tests(graph, args.target_prefix)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text("\n".join(tests) + ("\n" if tests else ""), encoding="utf-8")
    else:
        for test in tests:
            print(test)
    prefix = normalize_target_prefix(args.target_prefix) or "ydb"
    print(f"Extracted {len(tests)} tests under {prefix}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
