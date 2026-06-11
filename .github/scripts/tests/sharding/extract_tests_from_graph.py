#!/usr/bin/env python3
"""Best-effort extraction of test full_name entries from ya make graph JSON."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

_TEST_PATH_RE = re.compile(r"^ydb/.+")


def _maybe_add(value: str, tests: set[str]) -> None:
    value = value.strip()
    if not value:
        return
    if _TEST_PATH_RE.match(value):
        tests.add(value)


def _walk(obj, tests: set[str]) -> None:
    if isinstance(obj, dict):
        path = obj.get("path") or obj.get("target") or obj.get("name")
        subtest = obj.get("subtest_name") or obj.get("test_filter")
        if isinstance(path, str) and isinstance(subtest, str) and subtest:
            _maybe_add(f"{path}/{subtest}", tests)
        elif isinstance(path, str):
            _maybe_add(path, tests)
        for key in ("full_name", "test_name", "target", "uid"):
            val = obj.get(key)
            if isinstance(val, str):
                _maybe_add(val, tests)
        for value in obj.values():
            _walk(value, tests)
    elif isinstance(obj, list):
        for item in obj:
            _walk(item, tests)
    elif isinstance(obj, str):
        _maybe_add(obj, tests)


def extract_tests(graph: dict) -> list[str]:
    tests: set[str] = set()
    _walk(graph, tests)
    return sorted(tests)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("graph", type=Path, help="graph.json from graph_compare")
    parser.add_argument("-o", "--output", type=Path, help="Write one full_name per line")
    args = parser.parse_args()

    graph = json.loads(args.graph.read_text(encoding="utf-8"))
    tests = extract_tests(graph)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text("\n".join(tests) + ("\n" if tests else ""), encoding="utf-8")
    else:
        for test in tests:
            print(test)
    print(f"Extracted {len(tests)} tests", file=__import__("sys").stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
