#!/usr/bin/env python3
"""Extract test suite directories from ya make graph JSON."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

_TEST_PATH_RE = re.compile(r"^ydb/.+")
_TEST_MODULE_TAGS = frozenset(
    {
        "py3test_program",
        "py2test_program",
        "gtest_program",
        "unittest_program",
    }
)
_BUILD_ARTIFACT_TAIL_RE = re.compile(
    r"(?:\.(?:a|o|so|global\.a|py-|proto|pb\.cc|pb\.h)|^objcopy_|^lib.*\.a$)",
    re.IGNORECASE,
)


def normalize_target_prefix(prefix: str | None) -> str | None:
    if prefix is None:
        return None
    value = prefix.strip().rstrip("/")
    return value or None


def in_target_scope(path: str, target_prefix: str | None) -> bool:
    prefix = normalize_target_prefix(target_prefix)
    if prefix is None:
        return True
    return path == prefix or path.startswith(f"{prefix}/")


def is_test_module_tag(tag: str | None) -> bool:
    if not tag:
        return False
    if tag in _TEST_MODULE_TAGS:
        return True
    return tag.endswith("test_program")


def is_suite_directory(path: str) -> bool:
    if not _TEST_PATH_RE.match(path):
        return False
    tail = path.rsplit("/", 1)[-1]
    if not tail or tail.startswith("lib") and tail.endswith(".a"):
        return False
    return _BUILD_ARTIFACT_TAIL_RE.search(tail) is None


def extract_suite_paths(graph: dict, target_prefix: str | None = None) -> list[str]:
    prefix = normalize_target_prefix(target_prefix)
    suites: set[str] = set()

    stack: list[object] = [graph]
    while stack:
        obj = stack.pop()
        if isinstance(obj, dict):
            module_dir = obj.get("module_dir")
            module_tag = obj.get("module_tag")
            if isinstance(module_dir, str) and is_test_module_tag(str(module_tag)):
                path = module_dir.strip()
                if is_suite_directory(path) and in_target_scope(path, prefix):
                    suites.add(path)
            stack.extend(obj.values())
        elif isinstance(obj, list):
            stack.extend(obj)

    return sorted(suites)


def drop_redundant_scope_roots(suites: list[str], target_prefix: str | None) -> list[str]:
    """Drop scope root suite when nested suites exist (e.g. ydb/tests/olap + children)."""
    prefix = normalize_target_prefix(target_prefix)
    if not prefix or prefix not in suites:
        return suites
    if any(s.startswith(f"{prefix}/") for s in suites):
        return sorted(s for s in suites if s != prefix)
    return suites


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("graph", type=Path, help="graph.json from ya make --save-graph-to")
    parser.add_argument(
        "--target-prefix",
        default="ydb/",
        help="Only include suites under this ya.make path (default: ydb/)",
    )
    parser.add_argument("-o", "--output", type=Path, help="Write one suite path per line")
    args = parser.parse_args()

    graph = json.loads(args.graph.read_text(encoding="utf-8"))
    suites = extract_suite_paths(graph, args.target_prefix)
    suites = drop_redundant_scope_roots(suites, args.target_prefix)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text("\n".join(suites) + ("\n" if suites else ""), encoding="utf-8")
    else:
        for suite in suites:
            print(suite)
    prefix = normalize_target_prefix(args.target_prefix) or "ydb"
    print(f"Extracted {len(suites)} test suites under {prefix}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
