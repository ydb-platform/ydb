#!/usr/bin/env python3
"""List existing instruction files for AI agents, the files that mention a topic, and the skills.

Usage:
    python3 find.py [TOPIC] [--skills] [--root DIR]

Without options: prints every AGENTS.md, SKILL.md, RULES.md and rules/*.md
outside contrib/ and vendor/ (tracked or not yet added, but not ignored).
With TOPIC: also prints the README.md files, the .md files under
ydb/docs/en/core/contributor/ and the instruction files that contain TOPIC
(case does not matter).
With --skills: prints the skills, one per line as "name<TAB>directory", and
then the component directories ydb/<layer>/<component>, so a reader can
compare a new skill name with the existing names.

Written for Python 3.9 with the standard library only.
"""
import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check  # noqa: E402

INSTRUCTION_RE = re.compile(r"(^|/)(AGENTS\.md|SKILL\.md|RULES\.md|rules/[^/]+\.md)$")
SKILL_RE = re.compile(r"(^|/)\.agents/skills/([^/]+)/SKILL\.md$")
SKIP_PREFIXES = ("contrib/", "vendor/")
DOCS_DIR = "ydb/docs/en/core/contributor/"


def tracked_files(root):
    """Tracked and new (not ignored) files; None when git is not installed."""
    out = check.run_git(["ls-files", "--cached", "--others", "--exclude-standard"], root)
    if out is None or out.returncode != 0:
        return None
    return [line for line in out.stdout.splitlines() if line and not line.startswith(SKIP_PREFIXES)]


def is_instruction(path):
    return bool(INSTRUCTION_RE.search(path))


def instruction_files(files):
    return [path for path in files if is_instruction(path)]


def skills(files):
    """(name, directory the skill is about) for every SKILL.md."""
    found = []
    for path in files:
        match = SKILL_RE.search(path)
        if match:
            owner = path[: match.start()] or "."
            found.append((match.group(2), owner.rstrip("/") or "."))
    return found


def components(files):
    """Directories ydb/<layer>/<component> that hold tracked files."""
    found = set()
    for path in files:
        parts = path.split("/")
        if len(parts) >= 4 and parts[0] == "ydb" and not parts[1].startswith(".") and not parts[2].startswith("."):
            found.add("/".join(parts[:3]))
    return sorted(found)


def files_mentioning(files, topic, root):
    needle = topic.lower()
    found = []
    for path in files:
        if not path.endswith(".md"):
            continue
        if not (path.endswith("/README.md") or path.startswith(DOCS_DIR) or is_instruction(path)):
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
    parser.add_argument("--skills", action="store_true", help="print skill names with their directories, then the component directories")
    parser.add_argument("--root", help="repository root (default: found with git)")
    args = parser.parse_args(argv)

    root = os.path.realpath(args.root) if args.root else check.find_repo_root(os.getcwd())
    if root is None:
        print("cannot find the repository root; pass --root DIR", file=sys.stderr)
        return 2
    files = tracked_files(root)
    if files is None:
        print("git is not installed or %s is not a git repository" % root, file=sys.stderr)
        return 2

    if args.skills:
        print("skills:")
        for name, owner in skills(files):
            print("  %s\t%s" % (name, owner))
        print("components:")
        for path in components(files):
            print("  " + path)
        return 0
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
