#!/usr/bin/env python3
"""List the commits of a period that touch the given paths, one line per commit.

Usage:
    python3 list_commits.py --repo DIR --ref REF --since YYYY-MM-DD --until YYYY-MM-DD [--path P ...]

Days are UTC; --until is inclusive. Merge commits are skipped. Pass the base you fetched as --ref.
Output columns, tab-separated: sha, commit date, author, PR number, files, insertions, deletions,
tests_only (yes when every changed file is under a ut/, ut_*/ or tests/ directory),
audit (yes when a non-test file changed under AUDIT_PATHS), subject.
"""

import argparse
import datetime
import re
import subprocess
import sys

DEFAULT_PATHS = [
    "ydb/core/tx/columnshard",
    "ydb/core/formats/arrow",
    "ydb/library/formats/arrow",
    "ydb/core/tx/conveyor",
    "ydb/core/tx/conveyor_composite",
    "ydb/core/tx/limiter",
    "ydb/core/tx/general_cache",
    "ydb/core/tx/tiering",
    "ydb/core/tx/program",
    "ydb/core/tx/data_events",
    "ydb/core/tx/schemeshard/olap",
    "ydb/core/kqp/query_compiler/kqp_olap_compiler.cpp",
    "ydb/core/kqp/opt/physical",
    "ydb/core/kqp/compute_actor",
    "ydb/core/kqp/runtime/kqp_write_actor.cpp",
    "ydb/core/kqp/ut/olap",
]

# Code where object lifetime, threads and row counts meet; every commit touching it gets an audit subagent.
AUDIT_PATHS = [
    "ydb/core/tx/columnshard/engines/reader/",
    "ydb/core/tx/columnshard/engines/portions/",
    "ydb/core/tx/columnshard/data_accessor/",
    "ydb/core/tx/columnshard/blobs_action/",
    "ydb/core/tx/columnshard/blob_cache",
    "ydb/core/tx/conveyor",
    "ydb/core/tx/limiter/",
    "ydb/core/tx/general_cache/",
    "ydb/core/formats/arrow/",
    "ydb/library/formats/arrow/",
]

TEST_DIR = re.compile(r"(^|/)(ut|ut_[^/]*|tests?)/")
PR_NUMBER = re.compile(r"\(#(\d+)\)\s*$")


def git(repo, *args):
    return subprocess.run(["git", "-C", repo] + list(args), check=True, capture_output=True, text=True).stdout


def parse_numstat(text):
    files, added, deleted, tests_only, audit = 0, 0, 0, True, False
    for line in text.splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        files += 1
        added += int(parts[0]) if parts[0].isdigit() else 0
        deleted += int(parts[1]) if parts[1].isdigit() else 0
        if not TEST_DIR.search(parts[2]):
            tests_only = False
            if any(parts[2].startswith(prefix) for prefix in AUDIT_PATHS):
                audit = True
    return files, added, deleted, tests_only and files > 0, audit


def pr_number(subject):
    match = PR_NUMBER.search(subject)
    return match.group(1) if match else ""


def parse_day(text):
    try:
        return datetime.datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("expected a date YYYY-MM-DD, got: " + text)


def list_commits(repo, ref, since, until, paths):
    first, last = parse_day(since), parse_day(until)
    if first > last:
        raise ValueError("--since is after --until")
    log = git(repo, "log", "--no-merges", "--format=%H%x09%cs%x09%an%x09%s",
              "--since={}T00:00:00+00:00".format(first), "--until={}T23:59:59+00:00".format(last), ref, "--", *paths)
    rows = []
    for line in log.splitlines():
        sha, date, author, subject = line.split("\t", 3)
        numstat = git(repo, "show", "--numstat", "--format=", sha)
        files, added, deleted, tests_only, audit = parse_numstat(numstat)
        rows.append([sha[:11], date, author, pr_number(subject), str(files), str(added), str(deleted),
                     "yes" if tests_only else "no", "yes" if audit else "no", subject])
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--since", required=True)
    parser.add_argument("--until", required=True)
    parser.add_argument("--ref", required=True, help="the fetched base, for example upstream/main")
    parser.add_argument("--path", action="append", help="repeat for several paths; default: ColumnShard and its dependencies")
    args = parser.parse_args(argv)
    try:
        rows = list_commits(args.repo, args.ref, args.since, args.until, args.path or DEFAULT_PATHS)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    for row in rows:
        print("\t".join(row))
    return 0


if __name__ == "__main__":
    sys.exit(main())
