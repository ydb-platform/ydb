#!/usr/bin/env python3
"""Enumerate Python stdlib modules suitable for freezing into the python3 binary.

Walks the CPython Lib/ directory and emits a JSON array of objects:
    {"name": "json.encoder", "path": "<abs path>", "package": false}

The output is consumed by GN's exec_script() in contrib/tools/python3/bin/BUILD.gn
to generate per-module action targets.

A small skip-list is applied for modules that don't make sense as frozen
(test trees, GUI toolkits we don't ship C extensions for, __main__.py
entrypoints whose execution would happen at import time, etc.). The list
mirrors the spirit of CPython's own Tools/build/freeze_modules.py — we
err on the side of skipping rather than freezing something that will
fail to import at startup.
"""

import json
import os
import sys

# Top-level Lib subdirectories to skip entirely. These either:
#   - depend on C extensions we don't link in (tkinter -> _tkinter)
#   - are pure dev tooling not needed at runtime (idlelib, turtledemo)
#   - bundle their own data (ensurepip wheels)
#   - are inherently entry-point-shaped (venv scripts)
SKIP_TOP_DIRS = frozenset({
    "idlelib",
    "tkinter",
    "turtledemo",
    "ensurepip",
    "venv",
    "__phello__",
})

# Path components that indicate a test tree. Any module whose path contains
# one of these as a directory component is skipped.
SKIP_PATH_COMPONENTS = frozenset({
    "test",
    "tests",
    "idle_test",
    "__pycache__",
})

# Specific filenames to skip regardless of location. `__main__.py` files
# are NOT skipped — they are package entry points for `python -m pkg`
# (e.g. `_pyrepl/__main__.py` is what Python 3.13 spawns for the
# interactive REPL) and only execute when invoked explicitly, not at
# import time.
SKIP_FILENAMES = frozenset({
    "__hello__.py",
})


def is_skipped(rel_parts: tuple[str, ...]) -> bool:
    if not rel_parts:
        return True
    if rel_parts[0] in SKIP_TOP_DIRS:
        return True
    if rel_parts[-1] in SKIP_FILENAMES:
        return True
    for part in rel_parts[:-1]:
        if part in SKIP_PATH_COMPONENTS:
            return True
    return False


def module_name(rel_parts: tuple[str, ...]) -> tuple[str, bool]:
    """Return (dotted_name, is_package) for a path relative to Lib/."""
    if rel_parts[-1] == "__init__.py":
        pkg_parts = rel_parts[:-1]
        return ".".join(pkg_parts), True
    last = rel_parts[-1]
    assert last.endswith(".py"), last
    stem = last[:-3]
    return ".".join(rel_parts[:-1] + (stem,)), False


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: list_stdlib_modules.py <Lib dir>", file=sys.stderr)
        return 2
    lib_dir = os.path.abspath(sys.argv[1])
    if not os.path.isdir(lib_dir):
        print(f"not a directory: {lib_dir}", file=sys.stderr)
        return 1

    entries = []
    for root, dirs, files in os.walk(lib_dir):
        # Prune skipped directories in-place so os.walk doesn't descend into them.
        rel_root = os.path.relpath(root, lib_dir)
        rel_root_parts = () if rel_root == "." else tuple(rel_root.split(os.sep))
        dirs[:] = [
            d for d in dirs
            if d not in SKIP_PATH_COMPONENTS
            and not (not rel_root_parts and d in SKIP_TOP_DIRS)
        ]
        dirs.sort()
        for fname in sorted(files):
            if not fname.endswith(".py"):
                continue
            rel_parts = rel_root_parts + (fname,)
            if is_skipped(rel_parts):
                continue
            name, is_pkg = module_name(rel_parts)
            if not name:
                continue
            entries.append({
                "name": name,
                # GN target names cannot contain '.', so produce a sanitized
                # identifier callers can splice into target labels.
                "target_suffix": name.replace(".", "_"),
                "path": os.path.join(root, fname),
                "package": is_pkg,
            })

    entries.sort(key=lambda e: e["name"])
    json.dump(entries, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
