#!/usr/bin/env python3
"""Validator for YDB user-facing error tags (YDBE-XXXXX).

Checks three things, as described in the RFC:

  1. Format     - every tag in the registry matches ^YDBE-\\d{5}$.
  2. Uniqueness - the registry has no duplicate tags.
  3. Coverage   - every YDBE-XXXXX tag used in the source tree is present
                  in the registry (no "ghost" tag without a description).

Additionally it warns (without failing) about orphan tags: tags that are
listed in the registry but never used in the source tree.

Scan modes:

  * full (default)  - walk the whole source tree (good for a local audit;
                      takes a couple of seconds).
  * diff            - only scan the files listed in ``--changed-files-from``.
                      If the registry file itself is among the changed files we
                      fall back to a full scan, because a registry edit can
                      affect coverage of source files that did not change.

Exit code is non-zero if any check fails, so it can be wired directly into CI.
"""

import argparse
import os
import re
import sys

CODE_RE = re.compile(r"^YDBE-\d{5}$")
CODE_IN_SOURCE_RE = re.compile(r"YDBE-\d{5}(?!\d)")
REGISTRY_LINE_RE = re.compile(r"^(\S+)\s+(.+?)\s*$")
SOURCE_EXTENSIONS = {
    ".cpp", ".cc", ".cxx", ".c",
    ".h", ".hpp", ".hh", ".hxx",
}

SKIP_DIRS = {".git", "contrib", "build", "ya.make.gen"}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_REGISTRY = os.path.join(SCRIPT_DIR, "registry.txt")
DEFAULT_SCAN_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))


def parse_registry(path):
    """Return (codes_by_line, errors).

    codes_by_line: list of (code, lineno) in file order.
    errors: list of human-readable format/uniqueness problems.
    """
    codes = []
    errors = []
    seen = {}

    with open(path, encoding="utf-8") as f:
        for lineno, raw in enumerate(f, start=1):
            line = raw.rstrip("\n")
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            m = REGISTRY_LINE_RE.match(line)
            if not m:
                errors.append(
                    f"{path}:{lineno}: cannot parse line (expected '<code> <description>'): {line!r}"
                )
                continue

            code, description = m.group(1), m.group(2)

            if not CODE_RE.match(code):
                errors.append(
                    f"{path}:{lineno}: code {code!r} does not match ^YDBE-\\d{{5}}$"
                )
                continue

            if not description.strip():
                errors.append(f"{path}:{lineno}: code {code} has an empty description")

            if code in seen:
                errors.append(
                    f"{path}:{lineno}: duplicate code {code} (first defined at line {seen[code]})"
                )
            else:
                seen[code] = lineno

            codes.append((code, lineno))

    return codes, errors


def iter_all_source_files(root):
    """Yield every source file under root (skipping SKIP_DIRS)."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if os.path.splitext(name)[1] in SOURCE_EXTENSIONS:
                yield os.path.join(dirpath, name)


def scan_files(filepaths, registry_abspath):
    """Return {code: [list of "file:line" locations]} for the given files."""
    usages = {}
    for filepath in filepaths:
        if os.path.abspath(filepath) == registry_abspath:
            continue
        try:
            with open(filepath, encoding="utf-8", errors="replace") as f:
                for lineno, line in enumerate(f, start=1):
                    for match in CODE_IN_SOURCE_RE.finditer(line):
                        code = match.group(0)
                        usages.setdefault(code, []).append(f"{filepath}:{lineno}")
        except OSError as e:
            print(f"warning: cannot read {filepath}: {e}", file=sys.stderr)
    return usages


def read_file_list(path):
    """Read a newline-separated list of file paths, ignoring blanks."""
    items = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                items.append(s)
    return items


def main():
    parser = argparse.ArgumentParser(description="Validate YDB error tag registry.")
    parser.add_argument(
        "--registry", default=DEFAULT_REGISTRY,
        help=f"path to the registry file (default: {DEFAULT_REGISTRY})",
    )
    parser.add_argument(
        "--root", default=DEFAULT_SCAN_ROOT,
        help=f"source tree root to scan in full mode (default: {DEFAULT_SCAN_ROOT})",
    )
    parser.add_argument(
        "--changed-files-from", default=None, metavar="PATH",
        help="diff mode: read the newline-separated list of changed files from PATH "
             "(used by CI); without it, the whole tree is scanned.",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.registry):
        print(f"error: registry file not found: {args.registry}", file=sys.stderr)
        return 2

    registry_abspath = os.path.abspath(args.registry)

    codes, format_errors = parse_registry(args.registry)
    registry_codes = {code for code, _ in codes}

    changed = None
    if args.changed_files_from is not None:
        if not os.path.isfile(args.changed_files_from):
            print(
                f"error: changed-files list not found: {args.changed_files_from}",
                file=sys.stderr,
            )
            return 2
        changed = read_file_list(args.changed_files_from)

    full_scan = changed is None
    registry_changed = False
    if not full_scan:
        registry_changed = any(os.path.abspath(p) == registry_abspath for p in changed)

    if full_scan or registry_changed:
        mode = "full" if full_scan else "full (registry changed)"
        files = list(iter_all_source_files(args.root))
    else:
        mode = "diff"
        files = [
            p for p in changed
            if os.path.splitext(p)[1] in SOURCE_EXTENSIONS and os.path.isfile(p)
        ]

    usages = scan_files(files, registry_abspath)

    # Coverage: every code used in scanned source must be present in the registry.
    coverage_errors = []
    for code in sorted(usages):
        if code not in registry_codes:
            locations = usages[code]
            shown = ", ".join(locations[:3])
            more = f" (+{len(locations) - 3} more)" if len(locations) > 3 else ""
            coverage_errors.append(
                f"code {code} is used in sources but missing from the registry: {shown}{more}"
            )

    print(f"Registry:   {args.registry}")
    print(f"Mode:       {mode}")
    if mode == "diff":
        print(f"Scanned:    {len(files)} changed source file(s)")
    else:
        print(f"Scan root:  {args.root}")
    print(f"Registered: {len(registry_codes)} code(s)")
    print(f"Used:       {len(usages)} distinct code(s) in scanned sources")
    print()

    ok = True

    if format_errors:
        ok = False
        print("FORMAT/UNIQUENESS ERRORS:")
        for e in format_errors:
            print(f"  - {e}")
        print()

    if coverage_errors:
        ok = False
        print("COVERAGE ERRORS (code used but not registered):")
        for e in coverage_errors:
            print(f"  - {e}")
        print(
            "  -> Fix: add each code to ydb/library/error_tags/registry.txt "
            "(format: 'YDBE-XXXXX <one-line description>')."
        )
        print("     See ydb/library/error_tags/README.md for the component ranges.")
        print()

    # Orphans only make sense for a full scan; in diff mode we deliberately
    # looked at a subset of files, so unused-here codes are not real orphans.
    if full_scan or registry_changed:
        orphans = sorted(registry_codes - set(usages))
        if orphans:
            print("WARNINGS (registered but unused codes):")
            for code in orphans:
                print(f"  - {code} is in the registry but not found in sources")
            print()

    if ok:
        print("OK: all error tags are valid, unique and covered.")
        return 0

    print("FAILED: error tag validation found problems (see above).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
