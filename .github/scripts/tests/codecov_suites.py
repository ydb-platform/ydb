#!/usr/bin/env python3
"""Shared suite table for C++ Codecov CI (detect + run_clang_codecov)."""

from __future__ import annotations

import json
import sys

# Product-only exclude (same idea as SDK --coverage-exclude-regexp).
COVERAGE_EXCLUDE_REGEXP = r"/(ut|tests)/|_(ut|it)\.(c|cc|cpp|cxx|h|hh|hpp|hxx)"

# Infra changes validate the pipeline for every suite.
SHARED_PATH_PREFIXES = [
    ".github/actions/run_clang_codecov/",
    ".github/scripts/tests/codecov_suites.py",
    ".github/scripts/tests/detect_codecov_matrix.py",
    ".github/scripts/tests/export_coverage_lcov.py",
    ".github/scripts/tests/generate_coverage_landing.py",
    ".github/workflows/cpp_codecov.yml",
    ".github/codecov.yml",
]

SUITES: dict[str, dict] = {
    "cpp_sdk": {
        "flag": "cpp_sdk",
        "regexp": "ydb/public/sdk/cpp",
        "targets": ["ydb/public/sdk/cpp"],
        "lcov_prefixes": ["ydb/public/sdk/cpp/"],
        "path_prefixes": ["ydb/public/sdk/cpp/"],
        "labels": ["coverage/sdk"],
    },
    "cli": {
        "flag": "cli",
        "regexp": "ydb/apps/ydb/|ydb/public/lib/ydb_cli",
        "targets": [
            "ydb/apps/ydb",
            "ydb/public/lib/ydb_cli",
            "ydb/tests/functional/ydb_cli",
        ],
        "lcov_prefixes": ["ydb/apps/ydb/", "ydb/public/lib/ydb_cli/"],
        "path_prefixes": [
            "ydb/apps/ydb/",
            "ydb/public/lib/ydb_cli/",
            "ydb/tests/functional/ydb_cli/",
        ],
        "labels": ["coverage/cli"],
    },
    "cli_workload": {
        "flag": "cli_workload",
        "regexp": "ydb/library/workload",
        "targets": ["ydb/library/workload"],
        "lcov_prefixes": ["ydb/library/workload/"],
        "path_prefixes": ["ydb/library/workload/"],
        "labels": ["coverage/workload"],
    },
}

ALL_COVERAGE_LABELS = {
    "coverage/sdk",
    "coverage/cli",
    "coverage/workload",
    "coverage/all",
}

LABEL_TO_SUITES = {
    "coverage/sdk": ["cpp_sdk"],
    "coverage/cli": ["cli"],
    "coverage/workload": ["cli_workload"],
    "coverage/all": ["cpp_sdk", "cli", "cli_workload"],
}


def _normalize_repo_path(path: str) -> str:
    while path.startswith("./"):
        path = path[2:]
    return path


def _matches_prefix(norm: str, prefixes: list[str]) -> bool:
    return any(norm.startswith(p) or p.rstrip("/") == norm for p in prefixes)


def suites_from_paths(changed_files: list[str]) -> list[str]:
    found: set[str] = set()
    for path in changed_files:
        norm = _normalize_repo_path(path)
        if _matches_prefix(norm, SHARED_PATH_PREFIXES):
            found.update(SUITES.keys())
            continue
        for name, cfg in SUITES.items():
            if _matches_prefix(norm, cfg["path_prefixes"]):
                found.add(name)
    return sorted(found)


def suites_from_labels(labels: list[str]) -> list[str]:
    found: set[str] = set()
    for lab in labels:
        found.update(LABEL_TO_SUITES.get(lab, []))
    return sorted(found)


def coverage_labels_present(labels: list[str]) -> list[str]:
    return sorted({lab for lab in labels if lab in ALL_COVERAGE_LABELS})


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: codecov_suites.py {list|json-suite <name>|exclude-regexp}", file=sys.stderr)
        return 2
    cmd = sys.argv[1]
    if cmd == "list":
        print("\n".join(SUITES.keys()))
        return 0
    if cmd == "exclude-regexp":
        print(COVERAGE_EXCLUDE_REGEXP)
        return 0
    if cmd == "json-suite" and len(sys.argv) == 3:
        name = sys.argv[2]
        if name not in SUITES:
            print(f"unknown suite: {name}", file=sys.stderr)
            return 1
        print(json.dumps(SUITES[name]))
        return 0
    print(f"unknown command: {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
