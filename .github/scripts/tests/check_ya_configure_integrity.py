#!/usr/bin/env python3
"""Fail CI on configure integrity errors that `ya make -k` may otherwise swallow.

Keeps PR-check resilient to noisy configure warnings, but blocks merges when the
build graph is invalid (e.g. conflicting PROVIDES / BadDep).
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Extend this list carefully: only errors that mean the graph must not land in main.
FATAL_CONFIGURE_PATTERNS = (
    re.compile(r"(?:Error|Warning)\[-WBadDep\]"),
    re.compile(r"PROVIDES same feature"),
)


def find_hits(text: str) -> list[str]:
    hits: list[str] = []
    for line in text.splitlines():
        if any(pat.search(line) for pat in FATAL_CONFIGURE_PATTERNS):
            hits.append(line.rstrip())
    return hits


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "log_file",
        nargs="?",
        help="ya configure/make log path (default: stdin)",
    )
    args = parser.parse_args(argv)

    if args.log_file:
        text = Path(args.log_file).read_text(encoding="utf-8", errors="replace")
    else:
        text = sys.stdin.read()

    hits = find_hits(text)
    if not hits:
        return 0

    print(
        "Configure integrity errors (not allowed even with ya -k):",
        file=sys.stderr,
    )
    for line in hits[:50]:
        print(f"  {line}", file=sys.stderr)
    if len(hits) > 50:
        print(f"  ... and {len(hits) - 50} more", file=sys.stderr)
    print(
        "See https://github.com/ydb-platform/ydb/issues/47524",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
