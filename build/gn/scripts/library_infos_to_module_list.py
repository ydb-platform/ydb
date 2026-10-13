#!/usr/bin/env python3
"""Translate python_library metadata into a freeze-ready module list.

Reads the `library_infos.json` GN produces from walking `library_info`
metadata. Each entry looks like:

    {
        "library_name": "jinja2",
        "source_root": "contrib/python/Jinja2/py3/jinja2",
        "sources": ["__init__.py", "environment.py", "ext/..."]
    }

and emits one `{"name", "path", "package"}` row per source, with the
dotted module name reconstructed from library_name + relative path. The
output JSON is what freeze_all_modules.py expects as its --list.

Also accepts a --main-source/--main-module pair so the binary's own
entry-point .py joins the same list under its target frozen name.
"""

import argparse
import json
import os
import sys


def parts_to_name(library: str, rel: str) -> tuple[str, bool]:
    """('jinja2', 'ext/__init__.py') -> ('jinja2.ext', True)."""
    no_ext, ext = os.path.splitext(rel)
    if ext != ".py":
        raise SystemExit(f"non-.py source in library {library!r}: {rel}")
    pieces = no_ext.replace(os.sep, ".").split(".")
    if pieces[-1] == "__init__":
        pieces = pieces[:-1]
        is_pkg = True
    else:
        is_pkg = False
    return ".".join([library] + pieces), is_pkg


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--library-infos", required=True,
                   help="JSON file produced by generated_file walking library_info")
    p.add_argument("--main-source",
                   help="Absolute path to the binary's entry-point .py file")
    p.add_argument("--main-module",
                   help="Frozen module name to register the entry point under "
                        "(e.g. 'main')")
    p.add_argument("--output", required=True,
                   help="Where to write the resulting module list JSON")
    args = p.parse_args()

    if bool(args.main_source) != bool(args.main_module):
        raise SystemExit("--main-source and --main-module must be set together")

    with open(args.library_infos) as f:
        infos = json.load(f)

    modules = []
    seen: set[str] = set()

    for info in infos:
        lib = info["library_name"]
        root = info["source_root"]
        for rel in info["sources"]:
            name, is_pkg = parts_to_name(lib, rel)
            if name in seen:
                print(f"duplicate module name from libraries: {name}",
                      file=sys.stderr)
                return 1
            seen.add(name)
            modules.append({
                "name": name,
                "path": os.path.join(root, rel),
                "package": is_pkg,
            })

    if args.main_source:
        name = args.main_module
        if name in seen:
            raise SystemExit(
                f"main module name {name!r} collides with a library module")
        modules.append({
            "name": name,
            "path": args.main_source,
            "package": False,
        })

    modules.sort(key=lambda m: m["name"])
    with open(args.output, "w") as f:
        json.dump(modules, f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
