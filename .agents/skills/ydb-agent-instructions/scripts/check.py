#!/usr/bin/env python3
"""Check instruction files for AI agents: SKILL.md, AGENTS.md, CLAUDE.md.

Usage:
    python3 check.py PATH [PATH ...] [--root DIR] [--warnings-as-errors]

PATH is a directory or a file. The script checks only what is under PATH.
It prints one finding per line: "ERROR path:line: message" or
"WARN path:line: message". Exit code 0 means no errors, 1 means errors,
2 means a usage problem such as a missing PATH.

Written for Python 3.9 with the standard library only.
"""
import argparse
import ast
import importlib.util
import os
import re
import subprocess
import sys
import sysconfig

PY_VERSION = (3, 9)
SKILL_NAME_RE = re.compile(r"^ydb(-[a-z0-9]+)+$")
SKILL_NAME_MAX_CHARS = 64
DESCRIPTION_MAX_CHARS = 1024
SKILL_SOFT_LINES = 200
SKILL_HARD_LINES = 500
AGENTS_MAX_LINES = 50
AGENTS_CHAIN_MAX_BYTES = 32 * 1024
SENTENCE_MAX_WORDS = 40
CLAUDE_INCLUDE = "@./AGENTS.md"
CLAUDE_SKILLS_HEADER = "Skills, read the one that matches your task:"
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
PATH_RE = re.compile(r"(?<![\w@/.-])((?:\.\.?/)?[\w.-]+(?:/[\w.-]+)+\.(?:md|py))\b")
TODO_RE = re.compile(r"^\s*(?:[-*]\s+)?TODO\b")
SKIP_DIRS = {".git", ".claude", "contrib", "vendor", "node_modules", "__pycache__"}


class Report:
    def __init__(self):
        self.items = []

    def error(self, path, line, msg):
        self.items.append(("ERROR", path, line, msg))

    def warn(self, path, line, msg):
        self.items.append(("WARN", path, line, msg))

    def counts(self):
        errors = sum(1 for item in self.items if item[0] == "ERROR")
        return errors, len(self.items) - errors

    def print_all(self, root):
        for level, path, line, msg in sorted(self.items, key=lambda item: (item[1], item[2], item[0])):
            print("%s %s:%d: %s" % (level, os.path.relpath(path, root), line, msg))


def run_git(args, root, stdin=None):
    """Run git in root. Return the CompletedProcess, or None when git is not installed."""
    try:
        return subprocess.run(["git"] + args, cwd=root, input=stdin, capture_output=True, text=True, check=False)
    except OSError:
        return None


def find_repo_root(start):
    out = run_git(["rev-parse", "--show-toplevel"], start)
    if out is not None and out.returncode == 0 and out.stdout.strip():
        return os.path.realpath(out.stdout.strip())
    cur = os.path.realpath(start)
    while True:
        if os.path.exists(os.path.join(cur, ".git")):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            return None
        cur = parent


def read_text(path):
    """Text of a UTF-8 file (a BOM is allowed). Raises UnicodeDecodeError otherwise."""
    with open(path, "r", encoding="utf-8-sig") as handle:
        return handle.read()


def load_text(path, report):
    """Text of the file, or None after reporting that it is not UTF-8."""
    try:
        return read_text(path)
    except UnicodeDecodeError as exc:
        report.error(path, 0, "not valid UTF-8: %s" % exc.reason)
        return None


def ignored_paths(paths, root):
    """The subset of paths that git ignores; None when git is not installed."""
    if not paths:
        return set()
    out = run_git(["check-ignore", "--stdin"], root, stdin="\n".join(paths) + "\n")
    if out is None:
        return None
    if out.returncode not in (0, 1):
        return set()
    return set(line for line in out.stdout.splitlines() if line)


def repo_skill_names(root):
    """Map skill folder name -> SKILL.md path for every skill in the repo; None when git is not installed."""
    out = run_git(["ls-files", "--cached", "--others", "--exclude-standard", "--", "*/.agents/skills/*/SKILL.md", ".agents/skills/*/SKILL.md"], root)
    if out is None:
        return None
    names = {}
    for line in out.stdout.splitlines():
        if line.startswith(("contrib/", "vendor/")):
            continue
        parts = line.split("/")
        if len(parts) >= 4 and parts[-4] == ".agents" and parts[-3] == "skills":
            names[parts[-2]] = os.path.join(root, line)
    return names


def parse_frontmatter(lines):
    """Return (fields, end_line, error). Supports key: value, quoted values over several lines, and block scalars."""
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
        if line.startswith((" ", "\t")):
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


def path_targets(line):
    """Markdown link targets and bare paths in one line of text."""
    found = []
    for target in LINK_RE.findall(line):
        if target.startswith(("http://", "https://", "mailto:", "#")):
            continue
        target = target.split("#", 1)[0]
        if target:
            found.append(target)
    found.extend(PATH_RE.findall(LINK_RE.sub("", line)))
    return found


def check_paths(path, text, root, report):
    """Every file the text points to must exist, relative to the file or to the repo root.

    Covers markdown links [text](target) and bare paths such as ydb/core/blobstorage/README.md.
    Fenced code blocks and inline code are skipped: paths there are examples.
    Tokens with < > * { } are placeholders and are skipped too.
    """
    base = os.path.dirname(path)
    in_code = False
    for number, line in enumerate(text.splitlines(), start=1):
        if line.strip().startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            continue
        line = re.sub(r"`[^`]*`", "", line)
        for target in path_targets(line):
            if any(mark in target for mark in "<>*{}"):
                continue
            if os.path.exists(os.path.join(base, target)) or os.path.exists(os.path.join(root, target)):
                continue
            report.error(path, number, "path does not exist: %s" % target)


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


def check_python_script(path, local_modules, report):
    text = load_text(path, report)
    if text is None:
        return
    if not text.startswith("#!/usr/bin/env python3"):
        report.warn(path, 1, "first line should be #!/usr/bin/env python3")
    try:
        tree = ast.parse(text, filename=path, feature_version=PY_VERSION)
    except SyntaxError as exc:
        report.error(path, exc.lineno or 0, "not valid Python %d.%d syntax: %s" % (PY_VERSION[0], PY_VERSION[1], exc.msg))
        return
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names = [node.module]
        for name in names:
            top = name.split(".")[0]
            if top in local_modules or is_stdlib_module(top):
                continue
            report.error(path, node.lineno, "import %r is not in the standard library; use the standard library only" % top)


def glob_files(folder, suffix=""):
    """All files under folder (symlinks are not followed) whose name ends with suffix."""
    found = []
    for dirpath, dirnames, filenames in os.walk(folder):
        dirnames[:] = sorted(name for name in dirnames if name not in SKIP_DIRS)
        for filename in filenames:
            if filename.endswith(suffix):
                found.append(os.path.join(dirpath, filename))
    return found


def check_scripts(skill_dir, report):
    """Every .py under scripts/ must be Python 3.9 with standard imports; scripts/<name>.py needs scripts/tests/test_<name>.py."""
    scripts_dir = os.path.join(skill_dir, "scripts")
    if not os.path.isdir(scripts_dir):
        return
    all_scripts = sorted(glob_files(scripts_dir, ".py"))
    local_modules = {os.path.basename(script)[:-3] for script in all_scripts}
    tests_dir = os.path.join(scripts_dir, "tests")
    for script in all_scripts:
        check_python_script(script, local_modules, report)
        if os.path.dirname(script) == scripts_dir and os.path.basename(script) != "__init__.py":
            test = os.path.join(tests_dir, "test_" + os.path.basename(script))
            if not os.path.isfile(test):
                report.error(script, 0, "has no tests; add scripts/tests/test_%s" % os.path.basename(script))


def skill_dir_parts(path):
    """For .../<dir>/.agents/skills/<name>/SKILL.md return (dir, name) or None."""
    skill_dir = os.path.dirname(path)
    skills_dir = os.path.dirname(skill_dir)
    agents_dir = os.path.dirname(skills_dir)
    if os.path.basename(skills_dir) != "skills" or os.path.basename(agents_dir) != ".agents":
        return None
    return os.path.dirname(agents_dir), os.path.basename(skill_dir)


def check_skill_name(name, skill_md, root, report):
    if not SKILL_NAME_RE.match(name):
        report.error(skill_md, 1, "name must be ydb- followed by words of lowercase letters and digits joined by single dashes")
        return
    if len(name) > SKILL_NAME_MAX_CHARS:
        report.error(skill_md, 1, "name has %d characters; limit is %d" % (len(name), SKILL_NAME_MAX_CHARS))
    names = repo_skill_names(root)
    if names is None:
        return
    other = names.get(name)
    if other and os.path.realpath(other) != os.path.realpath(skill_md):
        report.error(skill_md, 1, "skill name %r is already used by %s; names must be unique in the repo" % (name, os.path.relpath(other, root)))


def check_skill(path, root, report):
    text = load_text(path, report)
    if text is None:
        return
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
    else:
        check_skill_name(name, path, root, report)
    if error:
        pass
    elif not description:
        report.error(path, 1, "frontmatter needs description")
    elif len(description) > DESCRIPTION_MAX_CHARS:
        report.error(path, 1, "description has %d characters; limit is %d" % (len(description), DESCRIPTION_MAX_CHARS))
    elif "TODO" in description:
        report.error(path, 1, "description still holds TODO; write the real description")
    if len(lines) > SKILL_HARD_LINES:
        report.error(path, len(lines), "SKILL.md has %d lines; hard limit is %d" % (len(lines), SKILL_HARD_LINES))
    elif len(lines) > SKILL_SOFT_LINES:
        report.warn(path, len(lines), "SKILL.md has %d lines; move detail to references/" % len(lines))
    check_paths(path, text, root, report)
    check_sentences(path, text, body_start + 1, report)
    check_todo_lines(path, lines, report)

    skill_dir = os.path.dirname(path)
    agents_md = os.path.join(owner_dir, "AGENTS.md")
    if not os.path.isfile(agents_md):
        report.error(agents_md, 0, "missing; every skill needs a sibling AGENTS.md with the rules of its directory")
    claude_md = os.path.join(owner_dir, "CLAUDE.md")
    if not os.path.isfile(claude_md):
        report.warn(claude_md, 0, "missing; Claude Code reads CLAUDE.md, create it with %s, the line %r and one line per skill" % (CLAUDE_INCLUDE, CLAUDE_SKILLS_HEADER))
    claude_dir = os.path.join(owner_dir, ".claude")
    if os.path.islink(claude_dir):
        report.error(claude_dir, 0, "is a symlink; symlinks are not used, Claude Code reaches the skill through CLAUDE.md")
    elif os.path.isdir(os.path.join(claude_dir, "skills")):
        report.error(claude_dir, 0, "holds skills; skills live only in .agents/skills")

    for reference in sorted(glob_files(os.path.join(skill_dir, "references"), ".md")):
        reference_text = load_text(reference, report)
        if reference_text is None:
            continue
        check_paths(reference, reference_text, root, report)
        check_sentences(reference, reference_text, 1, report)

    check_scripts(skill_dir, report)

    committed = [candidate for candidate in (agents_md, claude_md) if os.path.isfile(candidate)]
    committed += glob_files(skill_dir)
    ignored = ignored_paths(committed, root)
    for item in sorted(ignored or ()):
        report.error(item, 0, "is ignored by git; it must be committed")


def check_todo_lines(path, lines, report):
    for number, line in enumerate(lines, start=1):
        if TODO_RE.match(line):
            report.warn(path, number, "TODO line left from the template; replace it")


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
    text = load_text(path, report)
    if text is None:
        return
    lines = text.splitlines()
    if len(lines) > AGENTS_MAX_LINES:
        report.warn(path, len(lines), "has %d lines; budget is %d, move detail to a skill or reference" % (len(lines), AGENTS_MAX_LINES))
    chain = agents_chain_bytes(path, root)
    if chain > AGENTS_CHAIN_MAX_BYTES:
        report.error(path, 0, "AGENTS.md files from the repo root to here total %d bytes; Codex drops files once the total reaches 32 KiB" % chain)
    check_paths(path, text, root, report)
    check_sentences(path, text, 1, report)
    check_todo_lines(path, lines, report)


def skill_lines(owner_dir):
    """The CLAUDE.md line expected for every skill in <owner_dir>/.agents/skills."""
    skills_dir = os.path.join(owner_dir, ".agents", "skills")
    expected = {}
    if not os.path.isdir(skills_dir):
        return expected
    for name in sorted(os.listdir(skills_dir)):
        skill_md = os.path.join(skills_dir, name, "SKILL.md")
        if not os.path.isfile(skill_md):
            continue
        try:
            fields, _, error = parse_frontmatter(read_text(skill_md).splitlines())
        except UnicodeDecodeError:
            continue
        description = (fields or {}).get("description", "") if not error else ""
        expected[name] = "- .agents/skills/%s/SKILL.md: %s" % (name, description)
    return expected


def check_claude(path, root, report):
    text = load_text(path, report)
    if text is None:
        return
    owner_dir = os.path.dirname(path)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if CLAUDE_INCLUDE not in lines:
        report.error(path, 1, "must contain the line %s; rules belong in AGENTS.md" % CLAUDE_INCLUDE)
    elif not os.path.isfile(os.path.join(owner_dir, "AGENTS.md")):
        report.error(path, 1, "includes AGENTS.md but AGENTS.md does not exist next to it")
    expected = skill_lines(owner_dir)
    if expected and CLAUDE_SKILLS_HEADER not in lines:
        report.error(path, 1, "must contain the line: %s" % CLAUDE_SKILLS_HEADER)
    for name, line in expected.items():
        if line not in lines:
            report.error(path, 1, "must list the skill %s as: %s" % (name, line))
    allowed = {CLAUDE_INCLUDE, CLAUDE_SKILLS_HEADER} | set(expected.values())
    for line in lines:
        if line not in allowed:
            report.warn(path, 1, "has extra content; keep only the include and the skill lines, put rules into AGENTS.md")
            break


def classify(path):
    base = os.path.basename(path)
    if base == "SKILL.md":
        return "skill"
    if base == "AGENTS.md":
        return "agents"
    if base == "CLAUDE.md":
        return "claude"
    return None


def collect(paths, report):
    """Return (targets, missing). targets are (kind, path); missing are paths that do not exist."""
    found = []
    missing = []
    for item in paths:
        item = os.path.realpath(item)
        if os.path.isfile(item):
            kind = classify(item)
            if kind is None:
                report.warn(item, 0, "not an instruction file; nothing to check")
            else:
                found.append((kind, item))
        elif os.path.isdir(item):
            for path in glob_files(item):
                kind = classify(path)
                if kind is not None:
                    found.append((kind, path))
        else:
            missing.append(item)
    return sorted(found), missing


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("paths", nargs="+", metavar="PATH", help="directory or file to check")
    parser.add_argument("--root", help="repository root (default: found with git)")
    parser.add_argument("--warnings-as-errors", action="store_true", help="exit 1 when there are warnings")
    args = parser.parse_args(argv)

    first = args.paths[0]
    start = first if os.path.isdir(first) else os.path.dirname(os.path.realpath(first)) or "."
    root = os.path.realpath(args.root) if args.root else find_repo_root(start)
    if root is None:
        print("cannot find the repository root; pass --root DIR", file=sys.stderr)
        return 2

    report = Report()
    targets, missing = collect(args.paths, report)
    if missing:
        for item in missing:
            print("path does not exist: %s" % item, file=sys.stderr)
        return 2
    if run_git(["--version"], root) is None:
        report.warn(root, 0, "git is not installed; name uniqueness and git-ignore checks were skipped")
    for kind, path in targets:
        if kind == "skill":
            check_skill(path, root, report)
        elif kind == "agents":
            check_agents(path, root, report)
        elif kind == "claude":
            check_claude(path, root, report)
        if kind != "skill" and (ignored_paths([path], root) or ()):
            report.error(path, 0, "is ignored by git; it must be committed")

    report.print_all(root)
    errors, warnings = report.counts()
    print("%d errors, %d warnings, %d files checked" % (errors, warnings, len(targets)))
    if errors or (warnings and args.warnings_as_errors):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
