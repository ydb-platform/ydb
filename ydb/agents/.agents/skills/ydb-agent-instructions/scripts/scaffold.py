#!/usr/bin/env python3
"""Create the files for a new agent skill in one directory.

Usage:
    python3 scaffold.py DIR SKILL_NAME [--description TEXT] [--no-symlink] [--dry-run] [--root DIR]

DIR is the directory the skill is about. SKILL_NAME is lowercase letters,
digits and dashes. The script creates:

    DIR/.agents/skills/SKILL_NAME/SKILL.md   skill text with TODO lines
    DIR/AGENTS.md                            short router, only when absent
    DIR/CLAUDE.md                            the single line @./AGENTS.md
    DIR/.claude -> .agents                   symlink, so Claude Code sees the skill

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

TODO: table "Responsibility | Source" for the main files in {dir_text}. Link to existing docs or README files instead of copying text.

## Trace the changed contract

TODO: what to read on both sides of a change (producer and consumer), and which invariants to keep.

## Validation

TODO: the smallest test target to run first, then wider targets.{guide_note}
"""

AGENTS_TEMPLATE = """# {title}

These instructions apply to {dir_text}.

For work in this directory, read the [{name} skill]({skill_link}).{guide_line}
"""

CLAUDE_CONTENT = check.CLAUDE_INCLUDE + "\n"
DEFAULT_DESCRIPTION = "TODO: name the component and directory, the task types this skill covers, and what it is not for."


def title_from_name(name):
    return " ".join(part.capitalize() for part in name.split("-"))


def relative_link(from_dir, to_path):
    return os.path.relpath(to_path, from_dir).replace(os.sep, "/")


def plan_actions(args, root):
    """Return (actions, conflicts, notes). Each action is (kind, path, content)."""
    actions = []
    conflicts = []
    notes = []
    target_dir = os.path.realpath(args.dir)
    skill_dir = os.path.join(target_dir, ".agents", "skills", args.skill_name)
    skill_md = os.path.join(skill_dir, "SKILL.md")
    agents_md = os.path.join(target_dir, "AGENTS.md")
    claude_md = os.path.join(target_dir, "CLAUDE.md")
    claude_link = os.path.join(target_dir, ".claude")
    rel_dir = relative_link(root, target_dir)
    dir_text = "the repo root" if rel_dir == "." else "`%s/`" % rel_dir
    guide = os.path.join(root, "ydb", "agents", "GUIDE.md")
    has_guide = os.path.isfile(guide)
    skill_link = relative_link(target_dir, skill_md)
    fields = {
        "name": args.skill_name,
        "title": title_from_name(args.skill_name),
        "description": " ".join((args.description or DEFAULT_DESCRIPTION).replace('"', "'").split()),
        "dir_text": dir_text,
        "skill_link": skill_link,
        "guide_line": "\nBuild and test commands: [`GUIDE.md`](%s)." % relative_link(target_dir, guide) if has_guide else "",
        "guide_note": " Build and test commands are in [GUIDE.md](%s); do not copy them here." % relative_link(skill_dir, guide) if has_guide else "",
    }
    if not has_guide:
        notes.append("no ydb/agents/GUIDE.md in this repo; the build and test link is left out")

    if os.path.exists(skill_md):
        notes.append("exists, not touched: %s" % skill_md)
    else:
        actions.append(("file", skill_md, SKILL_TEMPLATE.format(**fields)))

    if os.path.isfile(agents_md):
        if skill_link in check.read_text(agents_md):
            notes.append("exists and links to the skill: %s" % agents_md)
        else:
            notes.append("MANUAL STEP: add this line to %s:\n    For work in this directory, read the [%s skill](%s)." % (agents_md, args.skill_name, skill_link))
    elif os.path.exists(agents_md):
        conflicts.append("%s exists but is not a file" % agents_md)
    else:
        actions.append(("file", agents_md, AGENTS_TEMPLATE.format(**fields)))

    if os.path.isfile(claude_md):
        if check.CLAUDE_INCLUDE in check.read_text(claude_md):
            notes.append("exists with the include line: %s" % claude_md)
        else:
            conflicts.append("%s exists without the line %s; add it or move its rules to AGENTS.md" % (claude_md, check.CLAUDE_INCLUDE))
    elif os.path.exists(claude_md):
        conflicts.append("%s exists but is not a file" % claude_md)
    else:
        actions.append(("file", claude_md, CLAUDE_CONTENT))

    if not args.no_symlink and target_dir == root and os.path.isdir(claude_link) and not os.path.islink(claude_link):
        notes.append("root .claude is a local directory, no symlink created; Claude Code reaches a root skill through the link in AGENTS.md")
    elif not args.no_symlink:
        if os.path.islink(claude_link):
            if os.readlink(claude_link) == ".agents":
                notes.append("symlink exists: %s -> .agents" % claude_link)
            else:
                conflicts.append("%s is a symlink to %r, not to .agents" % (claude_link, os.readlink(claude_link)))
        elif os.path.exists(claude_link):
            conflicts.append("%s exists and is not a symlink; remove or rename it first" % claude_link)
        else:
            actions.append(("symlink", claude_link, ".agents"))
    return actions, conflicts, notes


def apply_actions(actions):
    for kind, path, content in actions:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if kind == "file":
            with open(path, "x", encoding="utf-8") as handle:
                handle.write(content)
            print("created %s" % path)
        elif kind == "symlink":
            try:
                os.symlink(content, path)
                print("created symlink %s -> %s" % (path, content))
            except OSError as exc:
                print("WARNING: cannot create symlink %s (%s). Claude Code will not see this skill by itself. On Windows enable Developer Mode or run as administrator, or re-run with --no-symlink." % (path, exc))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dir", metavar="DIR", help="directory the skill is about")
    parser.add_argument("skill_name", metavar="SKILL_NAME", help="lowercase letters, digits and dashes")
    parser.add_argument("--description", help="one sentence: component, task types, what it is not for")
    parser.add_argument("--no-symlink", action="store_true", help="do not create the .claude -> .agents symlink")
    parser.add_argument("--dry-run", action="store_true", help="print the plan and exit")
    parser.add_argument("--root", help="repository root (default: found with git)")
    args = parser.parse_args(argv)

    if not check.SKILL_NAME_RE.match(args.skill_name):
        print("SKILL_NAME must start with ydb- and use only lowercase letters, digits and dashes", file=sys.stderr)
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
    names = check.repo_skill_names(root)
    taken = names.get(args.skill_name)
    if taken and os.path.realpath(taken) != os.path.realpath(os.path.join(target, ".agents", "skills", args.skill_name, "SKILL.md")):
        conflicts.append("skill name %r is already used by %s" % (args.skill_name, os.path.relpath(taken, root)))
    similar = check.similar_names(args.skill_name, names)
    if similar:
        notes.append("WARNING: name %r looks like %s; tell the human and propose other names" % (args.skill_name, ", ".join(similar)))
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
        for kind, path, content in actions:
            print("  %s %s%s" % (kind, path, (" -> " + content) if kind == "symlink" else ""))
        return 0
    apply_actions(actions)
    print("")
    print("next steps:")
    print("  1. replace every TODO line in the new SKILL.md")
    print("  2. python3 %s %s" % (os.path.join(os.path.dirname(os.path.abspath(__file__)), "check.py"), target))
    print("  3. run the discovery tests from references/tool-compatibility.md")
    print("")
    return check.main([target, "--root", root])


if __name__ == "__main__":
    sys.exit(main())
