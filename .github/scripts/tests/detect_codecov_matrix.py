#!/usr/bin/env python3
"""Decide which Codecov suites to run for a PR (paths and/or labels)."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from codecov_suites import (
    ALL_COVERAGE_LABELS,
    coverage_labels_present,
    suites_from_labels,
    suites_from_paths,
)


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
        "--mode",
        choices=("paths", "labels"),
        required=True,
        help="paths: changed-files only; labels: coverage/* labels only",
    )
    parser.add_argument(
        "--changed-files",
        default="",
        help="Newline-separated list of changed paths (paths mode)",
    )
    parser.add_argument(
        "--changed-files-file",
        default="",
        help="File with newline-separated changed paths",
    )
    parser.add_argument(
        "--labels",
        default="",
        help="JSON array or newline-separated PR labels",
    )
    args = parser.parse_args()

    files: list[str] = []
    if args.changed_files_file:
        with open(args.changed_files_file, encoding="utf-8") as fh:
            files = [ln.strip() for ln in fh if ln.strip()]
    elif args.changed_files:
        files = [ln.strip() for ln in args.changed_files.splitlines() if ln.strip()]

    labels: list[str] = []
    raw = args.labels.strip()
    if raw:
        if raw.startswith("["):
            labels = list(json.loads(raw))
        else:
            labels = [ln.strip() for ln in raw.splitlines() if ln.strip()]

    hanging = coverage_labels_present(labels)

    if args.mode == "labels":
        suites = suites_from_labels(hanging)
    else:
        suites = suites_from_paths(files)

    matrix = json.dumps(suites)
    should_run = "true" if suites else "false"
    write_output("matrix", matrix)
    write_output("should_run", should_run)
    write_output("hanging_labels", json.dumps(hanging))
    write_output("all_coverage_labels", json.dumps(sorted(ALL_COVERAGE_LABELS)))
    print(f"mode={args.mode} suites={suites} hanging_labels={hanging}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
