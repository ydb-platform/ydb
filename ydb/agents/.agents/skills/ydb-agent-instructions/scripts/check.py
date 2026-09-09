#!/usr/bin/env python3
"""Check instruction files for AI agents: SKILL.md, AGENTS.md, CLAUDE.md.

Usage:
    python3 check.py PATH [PATH ...] [--root DIR] [--warnings-as-errors]

PATH is a directory or a file. The script checks only what is under PATH.
It prints one finding per line: "ERROR path:line: message" or
"WARN path:line: message". Exit code 0 means no errors.

Written for Python 3.8 with the standard library only. Keep it that way:
every YDB contributor has this Python because ./ya itself is Python.
"""
import argparse
import ast
import difflib
import importlib.util
import os
import re
import subprocess
import sys
import sysconfig

PY_VERSION = (3, 8)
SKILL_NAME_RE = re.compile(r"^ydb-[a-z0-9][a-z0-9-]*$")
SIMILAR_NAME_RATIO = 0.8
DESCRIPTION_MAX_CHARS = 1024
SKILL_SOFT_LINES = 200
SKILL_HARD_LINES = 500
AGENTS_MAX_LINES = 50
AGENTS_CHAIN_MAX_BYTES = 32 * 1024
SENTENCE_MAX_WORDS = 40
CLAUDE_INCLUDE = "@./AGENTS.md"
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
SKIP_DIRS = {".git", ".claude", "contrib", "vendor", "node_modules", "__pycache__"}


class Report(object):
    def __init__(self):
        self.items = []

    def error(self, path, line, msg):
        self.items.append(("ERROR", path, line, msg))

    def warn(self, path, line, msg):
        self.items.append(("WARN", path, line, msg))

    def counts(self):
        errors = sum(1 for i in self.items if i[0] == "ERROR")
        return errors, len(self.items) - errors

    def print_all(self, root):
        for level, path, line, msg in sorted(self.items, key=lambda i: (i[1], i[2], i[0])):
            rel = os.path.relpath(path, root)
            print("%s %s:%d: %s" % (level, rel, line, msg))


def find_repo_root(start):
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=start, capture_output=True, text=True, check=False,
        )
        if out.returncode == 0 and out.stdout.strip():
            return os.path.realpath(out.stdout.strip())
    except OSError:
        pass
    cur = os.path.realpath(start)
    while True:
        if os.path.exists(os.path.join(cur, ".git")):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            return None
        cur = parent


def read_text(path):
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def ignored_paths(paths, root):
    """Return the subset of paths that git ignores. Symlinks are checked as paths."""
    if not paths:
        return set()
    try:
        out = subprocess.run(
            ["git", "check-ignore", "--stdin"],
            cwd=root, input="\n".join(paths) + "\n", capture_output=True, text=True, check=False,
        )
    except OSError:
        return set()
    if out.returncode not in (0, 1):
        return set()
    return set(line for line in out.stdout.splitlines() if line)


def is_ignored(path, root):
    return path in ignored_paths([path], root)


def repo_skill_names(root):
    """Map skill folder name -> SKILL.md path for every skill in the repo (tracked or new, not ignored)."""
    try:
        out = subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", "*/.agents/skills/*/SKILL.md", ".agents/skills/*/SKILL.md"],
            cwd=root, capture_output=True, text=True, check=False,
        )
    except OSError:
        return {}
    names = {}
    for line in out.stdout.splitlines():
        if line.startswith(("contrib/", "vendor/")) or "/.claude/" in "/" + line:
            continue
        parts = line.split("/")
        if len(parts) >= 4 and parts[-4] == ".agents" and parts[-3] == "skills":
            names[parts[-2]] = os.path.join(root, line)
    return names


def similar_names(name, names):
    """Names that look like `name`: one contains the other, or the words mostly match."""
    def core(value):
        return value[4:] if value.startswith("ydb-") else value
    found = []
    for other in sorted(names):
        if other == name:
            continue
        left, right = core(name), core(other)
        if left in right or right in left or difflib.SequenceMatcher(None, left, right).ratio() >= SIMILAR_NAME_RATIO:
            found.append(other)
    return found


def check_skill_name(name, skill_md, root, report):
    names = repo_skill_names(root)
    other = names.get(name)
    if other and os.path.realpath(other) != os.path.realpath(skill_md):
        report.error(skill_md, 1, "skill name %r is already used by %s; names must be unique in the repo" % (name, os.path.relpath(other, root)))
    similar = similar_names(name, names)
    if similar:
        report.warn(skill_md, 1, "name %r looks like %s; tell the human and propose other names" % (name, ", ".join(similar)))


def parse_frontmatter(lines):
    """Return (fields, end_line, error). Supports key: value and block scalars."""
    if not lines or lines[0].strip() != "---":
        return None, 0, "frontmatter must start with --- on line 1"
    fields = {}
    key = None
    block = None
    quote = None
    index = 1
    while index < len(lines):
        line = lines[index]
        if quote is not None:
            fields[key] += " " + line.strip()
            if line.rstrip().endswith(quote):
                fields[key] = fields[key][:-1]
                quote = None
            index += 1
            continue
        if line.strip() == "---":
            if key is not None and block is not None:
                fields[key] = " ".join(block).strip()
            return fields, index + 1, None
        if line.startswith(" ") or line.startswith("\t"):
            if key is not None and block is not None:
                block.append(line.strip())
            index += 1
            continue
        if key is not None and block is not None:
            fields[key] = " ".join(block).strip()
            block = None
        match = re.match(r"^([A-Za-z_][A-Za-z0-9_-]*):\s*(.*)$", line)
        if match:
            key = match.group(1)
            value = match.group(2).strip()
            if value in (">", "|", ">-", "|-"):
                block = []
            elif len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                fields[key] = value[1:-1]
            elif value and value[0] in "\"'":
                quote = value[0]
                fields[key] = value[1:]
            else:
                fields[key] = value
        index += 1
    return None, index, "frontmatter is not closed with ---"


def check_links(path, text, report):
    base = os.path.dirname(path)
    in_code = False
    for number, line in enumerate(text.splitlines(), start=1):
        if line.strip().startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            continue
        line = re.sub(r"`[^`]*`", "", line)
        for target in LINK_RE.findall(line):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            target = target.split("#", 1)[0]
            if not target:
                continue
            resolved = os.path.normpath(os.path.join(base, target))
            if not os.path.exists(resolved):
                report.error(path, number, "link target does not exist: %s" % target)


def check_sentences(path, text, start_line, report):
    in_code = False
    for number, line in enumerate(text.splitlines(), start=1):
        if number < start_line:
            continue
        if line.strip().startswith("```"):
            in_code = not in_code
            continue
        if in_code or line.strip().startswith("|"):
            continue
        for sentence in re.split(r"(?<=[.!?])\s+", line):
            if len(sentence.split()) > SENTENCE_MAX_WORDS:
                report.warn(path, number, "sentence longer than %d words; split it" % SENTENCE_MAX_WORDS)


def is_stdlib_module(name):
    """True when the running interpreter finds `name` inside its standard library."""
    names = getattr(sys, "stdlib_module_names", None)
    if names is not None:
        return name in names
    if name in sys.builtin_module_names:
        return True
    try:
        spec = importlib.util.find_spec(name)
    except (ImportError, ValueError):
        return False
    if spec is None:
        return False
    if spec.origin in ("built-in", "frozen"):
        return True
    origin = os.path.realpath(spec.origin or "")
    stdlib = os.path.realpath(sysconfig.get_paths()["stdlib"])
    return origin.startswith(stdlib + os.sep) and "site-packages" not in origin and "dist-packages" not in origin


def check_python_script(path, report):
    text = read_text(path)
    if not text.startswith("#!/usr/bin/env python3"):
        report.warn(path, 1, "first line should be #!/usr/bin/env python3")
    try:
        tree = ast.parse(text, filename=path, feature_version=PY_VERSION)
    except SyntaxError as exc:
        report.error(path, exc.lineno or 0, "not valid Python %d.%d syntax: %s" % (PY_VERSION[0], PY_VERSION[1], exc.msg))
        return
    own_dir = os.path.dirname(path)
    local_modules = set()
    for folder in (own_dir, os.path.dirname(own_dir)):
        local_modules.update(entry[:-3] for entry in os.listdir(folder) if entry.endswith(".py"))
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names = [node.module]
        for name in names:
            top = name.split(".")[0]
            if top in local_modules:
                continue
            if not is_stdlib_module(top):
                report.error(path, node.lineno, "import %r is not in the standard library; use the standard library only" % top)


def skill_dir_parts(path):
    """For .../<dir>/.agents/skills/<name>/SKILL.md return (dir, name) or None."""
    skill_dir = os.path.dirname(path)
    skills_dir = os.path.dirname(skill_dir)
    agents_dir = os.path.dirname(skills_dir)
    if os.path.basename(skills_dir) != "skills" or os.path.basename(agents_dir) != ".agents":
        return None
    return os.path.dirname(agents_dir), os.path.basename(skill_dir)


def check_skill(path, root, report):
    text = read_text(path)
    lines = text.splitlines()
    fields, body_start, error = parse_frontmatter(lines)
    if error:
        report.error(path, 1, error)
        fields, body_start = {}, 0
    parts = skill_dir_parts(path)
    if parts is None:
        report.error(path, 0, "SKILL.md must be at <dir>/.agents/skills/<name>/SKILL.md")
        return
    owner_dir, folder_name = parts
    name = fields.get("name", "")
    description = fields.get("description", "")
    if error:
        pass
    elif not name:
        report.error(path, 1, "frontmatter needs name")
    elif name != folder_name:
        report.error(path, 1, "name %r must equal the folder name %r" % (name, folder_name))
    elif not SKILL_NAME_RE.match(name):
        report.error(path, 1, "name must start with ydb- and use only lowercase letters, digits and dashes")
    else:
        check_skill_name(name, path, root, report)
    if error:
        pass
    elif not description:
        report.error(path, 1, "frontmatter needs description")
    elif len(description) > DESCRIPTION_MAX_CHARS:
        report.error(path, 1, "description has %d characters; limit is %d" % (len(description), DESCRIPTION_MAX_CHARS))
    for key in fields:
        if key not in ("name", "description"):
            report.warn(path, 1, "frontmatter key %r is not portable; keep only name and description" % key)
    if len(lines) > SKILL_HARD_LINES:
        report.error(path, len(lines), "SKILL.md has %d lines; hard limit is %d" % (len(lines), SKILL_HARD_LINES))
    elif len(lines) > SKILL_SOFT_LINES:
        report.warn(path, len(lines), "SKILL.md has %d lines; move detail to references/" % len(lines))
    check_links(path, text, report)
    check_sentences(path, text, body_start + 1, report)
    for number, line in enumerate(lines, start=1):
        if re.match(r"^\s*(?:[-*]\s+)?TODO\b", line):
            report.warn(path, number, "TODO line left from the template; replace it")

    skill_dir = os.path.dirname(path)
    rel_skill = os.path.relpath(path, owner_dir)
    agents_md = os.path.join(owner_dir, "AGENTS.md")
    if not os.path.isfile(agents_md):
        report.error(agents_md, 0, "missing; every skill needs a sibling AGENTS.md that points to it")
    elif rel_skill not in read_text(agents_md):
        report.warn(agents_md, 0, "does not link to %s" % rel_skill)
    claude_md = os.path.join(owner_dir, "CLAUDE.md")
    if not os.path.isfile(claude_md):
        report.warn(claude_md, 0, "missing; Claude Code reads CLAUDE.md, create it with the single line %s" % CLAUDE_INCLUDE)
    claude_link = os.path.join(owner_dir, ".claude")
    if os.path.islink(claude_link):
        target = os.readlink(claude_link)
        if target != ".agents":
            report.error(claude_link, 0, "symlink points to %r; it must point to .agents" % target)
    elif os.path.isdir(claude_link):
        if os.path.realpath(owner_dir) != os.path.realpath(root):
            report.error(claude_link, 0, "is a real directory; make it a symlink to .agents so tools see one skill tree")
    elif os.path.exists(claude_link):
        report.error(claude_link, 0, "exists but is not a symlink to .agents")
    else:
        report.warn(claude_link, 0, "missing; create the symlink .claude -> .agents for Claude Code")
    for reference in sorted(glob_files(os.path.join(skill_dir, "references"), ".md")):
        reference_text = read_text(reference)
        check_links(reference, reference_text, report)
        check_sentences(reference, reference_text, 1, report)

    check_scripts(skill_dir, report)

    committed = [candidate for candidate in (agents_md, claude_md, claude_link) if os.path.lexists(candidate)]
    committed += glob_files(skill_dir)
    for ignored in sorted(ignored_paths(committed, root)):
        report.error(ignored, 0, "is ignored by git; it must be committed")


def glob_files(folder, suffix=""):
    """All files under folder (no symlinks followed) whose name ends with suffix."""
    found = []
    for dirpath, dirnames, filenames in os.walk(folder):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS)
        for filename in filenames:
            if filename.endswith(suffix):
                found.append(os.path.join(dirpath, filename))
    return found


def check_scripts(skill_dir, report):
    """Every scripts/<name>.py must parse as Python 3.8 and have scripts/tests/test_<name>.py."""
    scripts_dir = os.path.join(skill_dir, "scripts")
    if not os.path.isdir(scripts_dir):
        return
    scripts = [entry for entry in sorted(os.listdir(scripts_dir)) if entry.endswith(".py")]
    tests_dir = os.path.join(scripts_dir, "tests")
    tests = []
    if os.path.isdir(tests_dir):
        tests = [entry for entry in sorted(os.listdir(tests_dir)) if entry.startswith("test_") and entry.endswith(".py")]
    for entry in scripts:
        check_python_script(os.path.join(scripts_dir, entry), report)
        if entry != "__init__.py" and "test_" + entry not in tests:
            report.error(os.path.join(scripts_dir, entry), 0, "has no tests; add scripts/tests/test_%s" % entry)
    for entry in tests:
        check_python_script(os.path.join(tests_dir, entry), report)


def agents_chain_bytes(path, root):
    total = 0
    cur = os.path.dirname(path)
    root = os.path.realpath(root)
    while True:
        candidate = os.path.join(cur, "AGENTS.md")
        if os.path.isfile(candidate):
            total += os.path.getsize(candidate)
        if os.path.realpath(cur) == root or os.path.dirname(cur) == cur:
            break
        cur = os.path.dirname(cur)
    return total


def check_agents(path, root, report):
    text = read_text(path)
    lines = text.splitlines()
    if len(lines) > AGENTS_MAX_LINES:
        report.warn(path, len(lines), "has %d lines; budget is %d, move detail to a skill or reference" % (len(lines), AGENTS_MAX_LINES))
    chain = agents_chain_bytes(path, root)
    if chain > AGENTS_CHAIN_MAX_BYTES:
        report.error(path, 0, "AGENTS.md chain from repo root is %d bytes; Codex stops reading after 32 KiB" % chain)
    check_links(path, text, report)
    check_sentences(path, text, 1, report)


def check_claude(path, root, report):
    text = read_text(path)
    agents_md = os.path.join(os.path.dirname(path), "AGENTS.md")
    if text.strip() == CLAUDE_INCLUDE:
        if not os.path.isfile(agents_md):
            report.error(path, 1, "includes AGENTS.md but AGENTS.md does not exist next to it")
        return
    if CLAUDE_INCLUDE in text:
        report.warn(path, 1, "has extra content; keep only the line %s and put rules into AGENTS.md" % CLAUDE_INCLUDE)
    else:
        report.error(path, 1, "must contain the single line %s; rules belong in AGENTS.md" % CLAUDE_INCLUDE)


def classify(path):
    base = os.path.basename(path)
    if base == "SKILL.md":
        return "skill"
    if base == "AGENTS.md":
        return "agents"
    if base == "CLAUDE.md":
        return "claude"
    if base.endswith(".py"):
        return "python"
    return None


def collect(paths, report):
    found = []
    for item in paths:
        item = os.path.realpath(item)
        if os.path.isfile(item):
            kind = classify(item)
            if kind is None:
                report.warn(item, 0, "not an instruction file; nothing to check")
            else:
                found.append((kind, item))
            continue
        if not os.path.isdir(item):
            report.error(item, 0, "path does not exist")
            continue
        for dirpath, dirnames, filenames in os.walk(item):
            dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS)
            for filename in filenames:
                kind = classify(filename)
                # Python scripts are checked from their SKILL.md, not on their own.
                if kind in ("skill", "agents", "claude"):
                    found.append((kind, os.path.join(dirpath, filename)))
    return found


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("paths", nargs="+", metavar="PATH", help="directory or file to check")
    parser.add_argument("--root", help="repository root (default: found with git)")
    parser.add_argument("--warnings-as-errors", action="store_true", help="exit 1 when there are warnings")
    args = parser.parse_args(argv)

    root = os.path.realpath(args.root) if args.root else find_repo_root(args.paths[0] if os.path.isdir(args.paths[0]) else os.path.dirname(os.path.realpath(args.paths[0])) or ".")
    if root is None:
        print("cannot find the repository root; pass --root DIR", file=sys.stderr)
        return 2

    report = Report()
    targets = collect(args.paths, report)
    for kind, path in targets:
        if kind == "skill":
            check_skill(path, root, report)
        elif kind == "agents":
            check_agents(path, root, report)
        elif kind == "claude":
            check_claude(path, root, report)
        elif kind == "python":
            check_python_script(path, report)
        if kind != "skill" and os.path.isfile(path) and is_ignored(path, root):
            report.error(path, 0, "is ignored by git; it must be committed")

    report.print_all(root)
    errors, warnings = report.counts()
    print("%d errors, %d warnings, %d files checked" % (errors, warnings, len(targets)))
    if errors or (warnings and args.warnings_as_errors):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
