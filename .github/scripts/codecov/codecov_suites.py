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
    ".github/actions/setup_ci_ydb_service_account_key_file_credentials/",
    ".github/scripts/codecov/",
]

# Unit tests validate coverage helpers but do not change the runtime pipeline.
CHECK_ONLY_PATH_PREFIXES = [".github/scripts/codecov/tests/"]

SHARED_PATHS = {
    ".github/workflows/cpp_codecov.yml",
    ".github/codecov.yml",
}

SUITES: dict[str, dict] = {
    "cpp_sdk": {
        "flag": "cpp_sdk",
        "regexp": r"^ydb/public/sdk/cpp(?:/|$)",
        "targets": ["ydb/public/sdk/cpp"],
        "lcov_prefixes": ["ydb/public/sdk/cpp/"],
        "path_prefixes": ["ydb/public/sdk/cpp/"],
    },
    "cli": {
        "flag": "cli",
        "regexp": r"^(?:ydb/apps/ydb|ydb/public/lib/ydb_cli)(?:/|$)",
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
    },
    "cli_workload": {
        "flag": "cli_workload",
        "regexp": r"^ydb/library/workload(?:/|$)",
        "targets": ["ydb/library/workload"],
        "lcov_prefixes": ["ydb/library/workload/"],
        "path_prefixes": ["ydb/library/workload/"],
    },
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
        if _matches_prefix(norm, CHECK_ONLY_PATH_PREFIXES):
            continue
        if norm in SHARED_PATHS or _matches_prefix(norm, SHARED_PATH_PREFIXES):
            found.update(SUITES.keys())
            continue
        for name, cfg in SUITES.items():
            if _matches_prefix(norm, cfg["path_prefixes"]):
                found.add(name)
    return sorted(found)


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
