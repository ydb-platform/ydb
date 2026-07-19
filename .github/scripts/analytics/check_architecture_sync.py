#!/usr/bin/env python3
"""Fail if analytics code changed without ARCHITECTURE.md update.

Usage:
  python3 check_architecture_sync.py              # vs HEAD (staged + unstaged)
  python3 check_architecture_sync.py --base origin/main
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
ARCHITECTURE = REPO_ROOT / ".github/scripts/analytics/ARCHITECTURE.md"

WATCH_PREFIXES = (
    ".github/scripts/analytics/",
    ".github/workflows/collect_analytics",
    ".github/workflows/update_muted_ya.yml",
    ".github/workflows/create_issues_for_muted_tests.yml",
    ".github/workflows/monitoring_queries.yml",
    ".github/config/ydb_qa_config.json",
)

SKIP_UNDER_ANALYTICS = (
    ".github/scripts/analytics/check_architecture_sync.py",
)


def changed_files(base: str | None) -> set[str]:
    if base:
        out = subprocess.check_output(
            ["git", "diff", "--name-only", f"{base}...HEAD"],
            cwd=REPO_ROOT,
            text=True,
        )
    else:
        out = subprocess.check_output(
            ["git", "diff", "--name-only", "HEAD"], cwd=REPO_ROOT, text=True
        )
        staged = subprocess.check_output(
            ["git", "diff", "--name-only", "--cached"], cwd=REPO_ROOT, text=True
        )
        untracked = subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard"],
            cwd=REPO_ROOT,
            text=True,
        )
        out = out + staged + untracked
    return {line.strip() for line in out.splitlines() if line.strip()}


def touches_analytics(paths: set[str]) -> bool:
    for path in paths:
        if path in SKIP_UNDER_ANALYTICS:
            continue
        if any(path.startswith(prefix) for prefix in WATCH_PREFIXES):
            return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--base",
        default=None,
        help="Compare branch to base (e.g. origin/main). Default: working tree vs HEAD",
    )
    args = parser.parse_args()

    paths = changed_files(args.base)
    if not touches_analytics(paths):
        print("OK: no watched analytics paths changed")
        return 0

    arch_rel = ARCHITECTURE.relative_to(REPO_ROOT).as_posix()
    if arch_rel in paths:
        print(f"OK: {arch_rel} updated together with analytics changes")
        return 0

    print(
        f"ERROR: analytics paths changed but {arch_rel} was not updated.\n"
        f"Changed files:\n  "
        + "\n  ".join(
            sorted(p for p in paths if any(p.startswith(w) for w in WATCH_PREFIXES))
        ),
        file=sys.stderr,
    )
    print(f"\nUpdate {arch_rel} (diagram + table) in the same PR.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
