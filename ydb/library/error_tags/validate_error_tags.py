#!/usr/bin/env python3
"""Validator for YDB user-facing error tags (YDBE-XXXXX).

Checks three things, as described in the RFC:

  1. Format     - every tag in the registry matches ^YDBE-\\d{5}$.
  2. Uniqueness - the registry has no duplicate tags.
  3. Coverage   - every YDBE-XXXXX tag used in the source tree is present
                  in the registry (no "ghost" tag without a description).

Additionally it warns (without failing) about orphan tags: tags that are
listed in the registry but never used in the source tree.

Exit code is non-zero if any check fails, so it can be wired directly into CI.
"""

import argparse
import os
import re
import sys

CODE_RE = re.compile(r"^YDBE-\d{5}$")
CODE_IN_SOURCE_RE = re.compile(r"YDBE-\d{5}")
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


def scan_sources(root, registry_path):
    """Return {code: [list of "file:line" locations]} for all codes used in sources."""
    registry_abspath = os.path.abspath(registry_path)
    usages = {}

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]

        for name in filenames:
            ext = os.path.splitext(name)[1]
            if ext not in SOURCE_EXTENSIONS:
                continue

            filepath = os.path.join(dirpath, name)
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


def main():
    parser = argparse.ArgumentParser(description="Validate YDB error code registry.")
    parser.add_argument(
        "--registry", default=DEFAULT_REGISTRY,
        help=f"path to the registry file (default: {DEFAULT_REGISTRY})",
    )
    parser.add_argument(
        "--root", default=DEFAULT_SCAN_ROOT,
        help=f"source tree root to scan for code usages (default: {DEFAULT_SCAN_ROOT})",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.registry):
        print(f"error: registry file not found: {args.registry}", file=sys.stderr)
        return 2

    codes, format_errors = parse_registry(args.registry)
    registry_codes = {code for code, _ in codes}

    usages = scan_sources(args.root, args.registry)

    # Coverage: every code used in source must be present in the registry.
    coverage_errors = []
    for code in sorted(usages):
        if code not in registry_codes:
            locations = usages[code]
            shown = ", ".join(locations[:3])
            more = f" (+{len(locations) - 3} more)" if len(locations) > 3 else ""
            coverage_errors.append(
                f"code {code} is used in sources but missing from the registry: {shown}{more}"
            )

    # Orphans: codes in the registry never used in source (warning only).
    orphans = sorted(registry_codes - set(usages))

    print(f"Registry:   {args.registry}")
    print(f"Scan root:  {args.root}")
    print(f"Registered: {len(registry_codes)} code(s)")
    print(f"Used:       {len(usages)} distinct code(s) in sources")
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
        print()

    if orphans:
        print("WARNINGS (registered but unused codes):")
        for code in orphans:
            print(f"  - {code} is in the registry but not found in sources")
        print()

    if ok:
        print("OK: all error codes are valid, unique and covered.")
        return 0

    print("FAILED: error code validation found problems (see above).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
