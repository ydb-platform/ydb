#!/usr/bin/env python3
"""Decide which C++ Codecov suites to run from changed repository paths."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from codecov_suites import SUITES, suites_from_paths


def write_output(name: str, value: str) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        print(f"{name}={value}")
        return
    with open(path, "a", encoding="utf-8") as fh:
        if "\n" in value:
            fh.write(f"{name}<<EOF\n{value}\nEOF\n")
        else:
            fh.write(f"{name}={value}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--changed-files",
        default="",
        help="Newline-separated list of changed paths",
    )
    parser.add_argument(
        "--changed-files-file",
        default="",
        help="File with newline-separated changed paths",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Select every suite (used for a self-contained main baseline)",
    )
    args = parser.parse_args()

    files: list[str] = []
    if args.changed_files_file:
        with open(args.changed_files_file, encoding="utf-8") as fh:
            files = [ln.strip() for ln in fh if ln.strip()]
    elif args.changed_files:
        files = [ln.strip() for ln in args.changed_files.splitlines() if ln.strip()]

    suites = sorted(SUITES) if args.all else suites_from_paths(files)

    matrix = json.dumps(suites)
    should_run = "true" if suites else "false"
    write_output("matrix", matrix)
    write_output("should_run", should_run)
    print(f"suites={suites}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
