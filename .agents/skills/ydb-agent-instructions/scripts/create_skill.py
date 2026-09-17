#!/usr/bin/env python3
"""Create the files for a new agent skill in one directory.

Usage:
    python3 create_skill.py DIR SKILL_NAME --description TEXT [--dry-run] [--root DIR]

DIR is the directory the skill is about. SKILL_NAME starts with ydb- and
uses lowercase letters, digits and dashes. The script creates:

    DIR/.agents/skills/SKILL_NAME/SKILL.md   skill text with TODO lines
    DIR/AGENTS.md                            rules of the directory, only when absent
    DIR/CLAUDE.md                            @./AGENTS.md plus one line per skill with its path and description

The files are written in DIR itself, never in a parent directory: Claude
Code loads DIR/CLAUDE.md when it works on files in DIR. When DIR/CLAUDE.md
already exists, the script adds the line of the new skill to it and changes
nothing else. It never overwrites a file and never
removes a line. When anything conflicts it writes nothing and exits with
code 1. A second run with the same arguments does nothing.
After writing it runs check.py on DIR; exit code 1 then means check.py
found errors in the files (the files stay in place, fix them).
Exit code 2 means a bad SKILL_NAME, a missing DIR, or no repository root.

Written for Python 3.9 with the standard library only.
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

TODO: the smallest test target to run first, then wider targets.
"""

AGENTS_TEMPLATE = """# {title}

These instructions apply to {dir_text}.

TODO: one or two rules that always apply here; steps of a task go to the skill.
"""

def claude_with_skill(text, skill_line):
    """Text of an existing CLAUDE.md with skill_line in the list, or None when it is already there."""
    lines = text.splitlines()
    stripped = [line.strip() for line in lines]
    if skill_line in stripped:
        return None
    if check.CLAUDE_SKILLS_HEADER in stripped:
        start = stripped.index(check.CLAUDE_SKILLS_HEADER) + 1
        end = start
        while end < len(lines) and stripped[end].startswith("- .agents/skills/"):
            end += 1
        lines[start:end] = sorted(lines[start:end] + [skill_line])
    else:
        if lines and lines[-1].strip():
            lines.append("")
        lines += [check.CLAUDE_SKILLS_HEADER, skill_line]
    return "\n".join(lines) + "\n"


def claude_content(owner_dir, name, description):
    """CLAUDE.md for owner_dir: the include, the header and one line per skill (the new one included)."""
    lines = check.skill_lines(owner_dir)
    lines[name] = "- .agents/skills/%s/SKILL.md: %s" % (name, description)
    return check.CLAUDE_INCLUDE + "\n\n" + check.CLAUDE_SKILLS_HEADER + "\n" + "\n".join(lines[key] for key in sorted(lines)) + "\n"


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
    fields = {
        "name": args.skill_name,
        "title": title_from_name(args.skill_name),
        "description": yaml_double_quoted(args.description),
        "dir_text": dir_text,
    }

    if os.path.exists(skill_md):
        notes.append("exists, not touched: %s" % skill_md)
    else:
        actions.append(("create", skill_md, SKILL_TEMPLATE.format(**fields)))

    if os.path.isfile(agents_md):
        notes.append("exists, not touched: %s" % agents_md)
    elif os.path.exists(agents_md):
        conflicts.append("%s exists but is not a file" % agents_md)
    else:
        actions.append(("create", agents_md, AGENTS_TEMPLATE.format(**fields)))

    skill_line = "- .agents/skills/%s/SKILL.md: %s" % (args.skill_name, " ".join(args.description.split()))
    if os.path.isfile(claude_md):
        text = check.read_text(claude_md)
        if check.CLAUDE_INCLUDE not in [line.strip() for line in text.splitlines()]:
            conflicts.append("%s exists without the line %s; add it or move its rules to AGENTS.md" % (claude_md, check.CLAUDE_INCLUDE))
        else:
            updated = claude_with_skill(text, skill_line)
            if updated is None:
                notes.append("exists and lists the skill: %s" % claude_md)
            else:
                actions.append(("update", claude_md, updated))
    elif os.path.exists(claude_md):
        conflicts.append("%s exists but is not a file" % claude_md)
    else:
        actions.append(("create", claude_md, claude_content(target_dir, args.skill_name, " ".join(args.description.split()))))

    names = check.repo_skill_names(root)
    taken = None if names is None else names.get(args.skill_name)
    if taken and os.path.realpath(taken) != os.path.realpath(skill_md):
        conflicts.append("skill name %r is already used by %s" % (args.skill_name, os.path.relpath(taken, root)))
    return actions, conflicts, notes


def apply_actions(actions):
    for kind, path, content in actions:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if kind == "create":
            with open(path, "x", encoding="utf-8") as handle:
                handle.write(content)
            print("created %s" % path)
        else:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(content)
            print("updated %s" % path)


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
        print("would write:")
        for kind, path, _ in actions:
            print("  %s %s" % (kind, path))
        return 0
    apply_actions(actions)
    print("")
    print("next steps:")
    print("  1. replace every TODO line in the new SKILL.md and AGENTS.md")
    print("  2. python3 %s %s" % (os.path.join(os.path.dirname(os.path.abspath(__file__)), "check.py"), target))
    print("")
    return check.main([target, "--root", root])


if __name__ == "__main__":
    sys.exit(main())
