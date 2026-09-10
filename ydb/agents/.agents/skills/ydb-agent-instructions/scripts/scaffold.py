#!/usr/bin/env python3
"""Create the files for a new agent skill in one directory.

Usage:
    python3 scaffold.py DIR SKILL_NAME --description TEXT [--dry-run] [--root DIR]

DIR is the directory the skill is about. SKILL_NAME starts with ydb- and
uses lowercase letters, digits and dashes. The script creates:

    DIR/.agents/skills/SKILL_NAME/SKILL.md   skill text with TODO lines
    DIR/AGENTS.md                            short router, only when absent
    DIR/CLAUDE.md                            the single line @./AGENTS.md

It never overwrites a file. When anything conflicts it writes nothing and
exits with code 1. A second run with the same arguments does nothing.
After writing it runs check.py on DIR; exit code 1 then means check.py
found errors in the files (the files stay in place, fix them).
Exit code 2 means a bad SKILL_NAME, a missing DIR, or no repository root.

Written for Python 3.8 with the standard library only.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check  # noqa: E402

SKILL_TEMPLATE = """---
name: {name}
description: "{description}"
---

# {title}

TODO: one or two sentences: what this skill is for and which directory it covers.

## Source map

TODO: table "Responsibility | Source" for the main files in {dir_text}. Point to existing docs or README files instead of copying text.

## Trace the changed contract

TODO: what to read on both sides of a change (producer and consumer), and which invariants to keep.

## Validation

TODO: the smallest test target to run first, then wider targets.{guide_note}
"""

AGENTS_TEMPLATE = """# {title}

These instructions apply to {dir_text}.

For work in this directory, read {skill_link}.{guide_line}
"""

CLAUDE_CONTENT = check.CLAUDE_INCLUDE + "\n"


def title_from_name(name):
    return " ".join(part.capitalize() for part in name.split("-"))


def relative_link(from_dir, to_path):
    return os.path.relpath(to_path, from_dir).replace(os.sep, "/")


def yaml_double_quoted(text):
    """One-line YAML double-quoted scalar body: collapse whitespace, escape backslash and quote."""
    return " ".join(text.split()).replace("\\", "\\\\").replace('"', '\\"')


def plan_actions(args, root):
    """Return (actions, conflicts, notes). Each action is (path, content)."""
    actions = []
    conflicts = []
    notes = []
    target_dir = os.path.realpath(args.dir)
    skill_dir = os.path.join(target_dir, ".agents", "skills", args.skill_name)
    skill_md = os.path.join(skill_dir, "SKILL.md")
    agents_md = os.path.join(target_dir, "AGENTS.md")
    claude_md = os.path.join(target_dir, "CLAUDE.md")
    rel_dir = relative_link(root, target_dir)
    dir_text = "the repo root" if rel_dir == "." else "`%s/`" % rel_dir
    guide = os.path.join(root, "ydb", "agents", "GUIDE.md")
    has_guide = os.path.isfile(guide)
    skill_link = relative_link(target_dir, skill_md)
    fields = {
        "name": args.skill_name,
        "title": title_from_name(args.skill_name),
        "description": yaml_double_quoted(args.description),
        "dir_text": dir_text,
        "skill_link": skill_link,
        "guide_line": "",
        "guide_note": "",
    }
    if has_guide:
        fields["guide_line"] = "\nBuild and test commands: ydb/agents/GUIDE.md."
        fields["guide_note"] = " Build and test commands are in ydb/agents/GUIDE.md; do not copy them here."
    else:
        notes.append("no ydb/agents/GUIDE.md in this repo; the build and test link is left out")

    if os.path.exists(skill_md):
        notes.append("exists, not touched: %s" % skill_md)
    else:
        actions.append((skill_md, SKILL_TEMPLATE.format(**fields)))

    if os.path.isfile(agents_md):
        if skill_link in check.read_text(agents_md):
            notes.append("exists and points to the skill: %s" % agents_md)
        else:
            notes.append("MANUAL STEP: add this line to %s:\n    For work in this directory, read %s." % (agents_md, skill_link))
    elif os.path.exists(agents_md):
        conflicts.append("%s exists but is not a file" % agents_md)
    else:
        actions.append((agents_md, AGENTS_TEMPLATE.format(**fields)))

    if os.path.isfile(claude_md):
        if check.CLAUDE_INCLUDE in check.read_text(claude_md):
            notes.append("exists with the include line: %s" % claude_md)
        else:
            conflicts.append("%s exists without the line %s; add it or move its rules to AGENTS.md" % (claude_md, check.CLAUDE_INCLUDE))
    elif os.path.exists(claude_md):
        conflicts.append("%s exists but is not a file" % claude_md)
    else:
        actions.append((claude_md, CLAUDE_CONTENT))

    names = check.repo_skill_names(root)
    taken = None if names is None else names.get(args.skill_name)
    if taken and os.path.realpath(taken) != os.path.realpath(skill_md):
        conflicts.append("skill name %r is already used by %s" % (args.skill_name, os.path.relpath(taken, root)))
    return actions, conflicts, notes


def apply_actions(actions):
    for path, content in actions:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "x", encoding="utf-8") as handle:
            handle.write(content)
        print("created %s" % path)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dir", metavar="DIR", help="directory the skill is about")
    parser.add_argument("skill_name", metavar="SKILL_NAME", help="ydb- plus lowercase words joined by dashes")
    parser.add_argument("--description", required=True, help="one sentence: component, task types, what it is not for")
    parser.add_argument("--dry-run", action="store_true", help="print the plan and exit")
    parser.add_argument("--root", help="repository root (default: found with git)")
    args = parser.parse_args(argv)

    if not check.SKILL_NAME_RE.match(args.skill_name) or len(args.skill_name) > check.SKILL_NAME_MAX_CHARS:
        print("SKILL_NAME must be ydb- followed by words of lowercase letters and digits joined by single dashes, at most %d characters" % check.SKILL_NAME_MAX_CHARS, file=sys.stderr)
        return 2
    if not os.path.isdir(args.dir):
        print("DIR does not exist: %s" % args.dir, file=sys.stderr)
        return 2
    root = os.path.realpath(args.root) if args.root else check.find_repo_root(args.dir)
    if root is None:
        print("cannot find the repository root; pass --root DIR", file=sys.stderr)
        return 2
    target = os.path.realpath(args.dir)
    if not (target == root or target.startswith(root + os.sep)):
        print("DIR must be inside the repository root %s" % root, file=sys.stderr)
        return 2

    actions, conflicts, notes = plan_actions(args, root)
    for note in notes:
        print(note)
    if conflicts:
        print("conflicts, nothing written:")
        for item in conflicts:
            print("  " + item)
        return 1
    if not actions:
        print("nothing to do")
        return 0
    if args.dry_run:
        print("would create:")
        for path, _ in actions:
            print("  " + path)
        return 0
    apply_actions(actions)
    print("")
    print("next steps:")
    print("  1. replace every TODO line in the new SKILL.md")
    print("  2. python3 %s %s" % (os.path.join(os.path.dirname(os.path.abspath(__file__)), "check.py"), target))
    print("")
    return check.main([target, "--root", root])


if __name__ == "__main__":
    sys.exit(main())
