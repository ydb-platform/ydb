#!/usr/bin/env python3
"""List existing instruction files for AI agents, and the files that mention a topic.

Usage:
    python3 find.py [TOPIC] [--root DIR]

Prints every AGENTS.md, SKILL.md, RULES.md and rules/*.md outside contrib/
and vendor/ (tracked or not yet added, but not ignored). With TOPIC it also prints the README.md files and the
files under ydb/docs/en/core/contributor/ that contain TOPIC (case does not
matter). Read the printed files before you write a new instruction.

Written for Python 3.8 with the standard library only.
"""
import argparse
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check  # noqa: E402

INSTRUCTION_RE = re.compile(r"(^|/)(AGENTS\.md|SKILL\.md|RULES\.md|rules/[^/]+\.md)$")
SKIP_PREFIXES = ("contrib/", "vendor/")


def tracked_files(root):
    out = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard"], cwd=root, capture_output=True, text=True, check=False)
    if out.returncode != 0:
        return None
    return [line for line in out.stdout.splitlines() if line and not line.startswith(SKIP_PREFIXES)]


def instruction_files(files):
    return [path for path in files if INSTRUCTION_RE.search(path)]


def files_mentioning(files, topic, root):
    needle = topic.lower()
    found = []
    for path in files:
        if not (path.endswith("/README.md") or path.startswith("ydb/docs/en/core/contributor/") or INSTRUCTION_RE.search(path)):
            continue
        try:
            text = check.read_text(os.path.join(root, path))
        except (OSError, UnicodeDecodeError):
            continue
        if needle in text.lower():
            found.append(path)
    return found


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("topic", nargs="?", metavar="TOPIC", help="word or phrase to look for, for example a directory or component name")
    parser.add_argument("--root", help="repository root (default: found with git)")
    args = parser.parse_args(argv)

    root = os.path.realpath(args.root) if args.root else check.find_repo_root(os.getcwd())
    if root is None:
        print("cannot find the repository root; pass --root DIR", file=sys.stderr)
        return 2
    files = tracked_files(root)
    if files is None:
        print("git ls-files failed in %s" % root, file=sys.stderr)
        return 2

    print("instruction files:")
    for path in instruction_files(files):
        print("  " + path)
    if args.topic:
        print("files that mention %r:" % args.topic)
        for path in files_mentioning(files, args.topic, root):
            print("  " + path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
