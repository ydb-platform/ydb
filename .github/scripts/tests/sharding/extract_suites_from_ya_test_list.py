#!/usr/bin/env python3
"""Extract test suite paths and counts from ``./ya test -L`` output."""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

_RUNNABLE_TYPES = frozenset(
    {
        "py3test",
        "gtest",
        "unittest",
        "py2test",
        "pytest",
    }
)
_SUITE_HEADER_RE = re.compile(r"^([^\s<]+)\s+<([^>]+)>(.*)$")
_SIZE_RE = re.compile(r"\[size:(\w+)\]")
_TOTAL_SUITES_RE = re.compile(r"^Total\s+(\d+)\s+suites?\s*$", re.IGNORECASE)
_TOTAL_TESTS_RE = re.compile(r"^Total\s+(\d+)\s+tests?\s*$", re.IGNORECASE)
SIZE_WEIGHT_SMALL = 60
SIZE_WEIGHT_MEDIUM = 600


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


def drop_redundant_scope_roots(
    suites: list[str],
    target_prefix: str | None,
    *,
    suite_test_counts: dict[str, int] | None = None,
) -> list[str]:
    """Drop scope root when nested suites exist, unless the root has direct tests.

    A scope root with its own runnable tests (e.g. ydb/tests/olap py3test files
    in the root ya.make) must stay in the plan even when child suites exist;
    otherwise those direct tests are never scheduled.
    """
    prefix = normalize_target_prefix(target_prefix)
    if not prefix or prefix not in suites:
        return suites
    if not any(s.startswith(f"{prefix}/") for s in suites):
        return suites
    if suite_test_counts and (suite_test_counts.get(prefix) or 0) > 0:
        return sorted(suites)
    return sorted(s for s in suites if s != prefix)


def _parse_sizes(rest: str) -> set[str]:
    return {match.group(1).lower() for match in _SIZE_RE.finditer(rest)}


def _is_manual(rest: str) -> bool:
    return "ya:manual" in rest


@dataclass
class SuiteInfo:
    path: str
    test_type: str
    sizes: set[str] = field(default_factory=set)
    small_test_count: int = 0
    medium_test_count: int = 0
    test_names: list[str] = field(default_factory=list)

    @property
    def test_count(self) -> int:
        return self.small_test_count + self.medium_test_count

    @property
    def weight(self) -> int:
        return self.small_test_count * SIZE_WEIGHT_SMALL + self.medium_test_count * SIZE_WEIGHT_MEDIUM


def _increment_test_count(suite: SuiteInfo, active_sizes: set[str]) -> None:
    if "medium" in active_sizes:
        suite.medium_test_count += 1
    else:
        # No [size:*] tag defaults to small (same as ya SIZE(SMALL) semantics).
        suite.small_test_count += 1


def parse_ya_test_list(
    text: str,
    *,
    target_prefix: str | None = None,
    allowed_sizes: set[str] | None = None,
) -> tuple[list[SuiteInfo], int | None, int | None]:
    suites: dict[str, SuiteInfo] = {}
    current_path: str | None = None
    current_sizes: set[str] = set()
    reported_suites: int | None = None
    reported_tests: int | None = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            continue

        suites_match = _TOTAL_SUITES_RE.match(stripped)
        if suites_match:
            reported_suites = int(suites_match.group(1))
            continue
        tests_match = _TOTAL_TESTS_RE.match(stripped)
        if tests_match:
            reported_tests = int(tests_match.group(1))
            continue

        header_match = _SUITE_HEADER_RE.match(stripped)
        if header_match:
            path, test_type, rest = header_match.groups()
            test_type = test_type.strip().lower()
            if test_type not in _RUNNABLE_TYPES:
                current_path = None
                continue
            sizes = _parse_sizes(rest)
            if allowed_sizes and sizes and sizes.isdisjoint(allowed_sizes):
                current_path = None
                continue
            if _is_manual(rest):
                current_path = None
                continue
            if not in_target_scope(path, target_prefix):
                current_path = None
                continue
            suite = suites.setdefault(
                path,
                SuiteInfo(path=path, test_type=test_type),
            )
            suite.test_type = test_type
            suite.sizes.update(sizes)
            current_path = path
            current_sizes = sizes
            continue

        if current_path is None:
            continue
        if not line.startswith("  "):
            continue
        test_name = stripped
        if not test_name or test_name.endswith("::flake8"):
            continue
        if " chunk]" in test_name or test_name.endswith(" chunk"):
            continue
        suites[current_path].test_names.append(test_name)
        _increment_test_count(suites[current_path], current_sizes)

    result = [suite for suite in suites.values() if suite.test_count > 0]
    result.sort(key=lambda item: item.path)
    paths = [suite.path for suite in result]
    suite_test_counts = {suite.path: suite.test_count for suite in result}
    dropped_roots = _dropped_scope_roots(paths, target_prefix, suite_test_counts=suite_test_counts)
    paths = drop_redundant_scope_roots(paths, target_prefix, suite_test_counts=suite_test_counts)
    path_set = set(paths)
    result = [suite for suite in result if suite.path in path_set]
    for dropped_path in dropped_roots:
        dropped_suite = suites.get(dropped_path)
        if dropped_suite and dropped_suite.test_count > 0:
            print(
                f"warning: dropped scope root {dropped_path} with "
                f"{dropped_suite.test_count} direct tests; nested suites are sharded instead",
                file=sys.stderr,
            )
    return result, reported_suites, reported_tests


def _dropped_scope_roots(
    paths: list[str],
    target_prefix: str | None,
    *,
    suite_test_counts: dict[str, int] | None = None,
) -> list[str]:
    prefix = normalize_target_prefix(target_prefix)
    if not prefix or prefix not in paths:
        return []
    if not any(path.startswith(f"{prefix}/") for path in paths):
        return []
    if suite_test_counts and (suite_test_counts.get(prefix) or 0) > 0:
        return []
    return [prefix]


def build_summary(
    suites: list[SuiteInfo],
    reported_suites: int | None,
    reported_tests: int | None,
) -> dict:
    total_tests = sum(suite.test_count for suite in suites)
    total_weight = sum(suite.weight for suite in suites)
    return {
        "total_suites": len(suites),
        "total_tests": total_tests,
        "total_weight": total_weight,
        "size_weights": {
            "small": SIZE_WEIGHT_SMALL,
            "medium": SIZE_WEIGHT_MEDIUM,
        },
        "reported_suites": reported_suites,
        "reported_tests": reported_tests,
        "suites": [
            {
                "path": suite.path,
                "test_type": suite.test_type,
                "sizes": sorted(suite.sizes) if suite.sizes else ["small"],
                "small_test_count": suite.small_test_count,
                "medium_test_count": suite.medium_test_count,
                "test_count": suite.test_count,
                "weight": suite.weight,
            }
            for suite in suites
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("list_log", type=Path, help="Output of ./ya test -L")
    parser.add_argument(
        "--target-prefix",
        default="ydb/",
        help="Only include suites under this ya.make path (default: ydb/)",
    )
    parser.add_argument(
        "--test-size",
        action="append",
        default=["small", "medium"],
        help="Include suites with these test sizes (repeatable; default: small+medium)",
    )
    parser.add_argument("-o", "--output", type=Path, help="Write one suite path per line")
    parser.add_argument(
        "--summary-json",
        type=Path,
        help="Write suite paths, per-suite test counts, and totals as JSON",
    )
    args = parser.parse_args()

    if not args.list_log.is_file():
        print(f"error: missing list log: {args.list_log}", file=sys.stderr)
        return 2

    allowed_sizes = {size.strip().lower() for size in args.test_size if size.strip()}
    suites, reported_suites, reported_tests = parse_ya_test_list(
        args.list_log.read_text(encoding="utf-8"),
        target_prefix=args.target_prefix,
        allowed_sizes=allowed_sizes or None,
    )
    if not suites:
        print("error: no runnable test suites found in list log", file=sys.stderr)
        return 2

    summary = build_summary(suites, reported_suites, reported_tests)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            "\n".join(suite.path for suite in suites) + "\n",
            encoding="utf-8",
        )
    else:
        for suite in suites:
            print(suite.path)

    if args.summary_json:
        args.summary_json.parent.mkdir(parents=True, exist_ok=True)
        args.summary_json.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    prefix = normalize_target_prefix(args.target_prefix) or "ydb"
    print(
        f"Extracted {summary['total_suites']} suites, {summary['total_tests']} tests "
        f"(weight {summary['total_weight']}) under {prefix} "
        f"(ya test -L reported {reported_tests} tests in {reported_suites} suites)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
