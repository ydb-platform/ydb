#!/usr/bin/env python3
"""Backs the expand_template() GN template.

Copies --input to --output, replacing every @KEY@ placeholder with the matching
value from the repeated --define KEY=VALUE arguments (CMake @ONLY semantics).
Placeholders without a matching key are left untouched.

Also handles #cmakedefine lines, mirroring ya's build/scripts/configure_file.py
(NOT real CMake semantics!):
  #cmakedefine VAR ...  ->  #define VAR ...   (unconditionally, value ignored)
  #cmakedefine01 VAR    ->  #define VAR 1     (only if the value is "yes")
                        ->  #define VAR 0     (otherwise)
"""

import argparse
import os
import re
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--define", action="append", default=[])
    args = parser.parse_args()

    defines = {}
    for item in args.define:
        key, _, value = item.partition("=")
        defines[key] = value

    with open(args.input, encoding="utf-8") as f:
        text = f.read()

    def replace(match):
        key = match.group(1)
        if key in defines:
            return str(defines[key])
        return match.group(0)

    text = re.sub(r"@([A-Za-z0-9_]+)@", replace, text)

    def cmakedefine(match):
        indent, zero_one, key, rest = match.groups()
        if zero_one:
            value = 1 if str(defines.get(key, "")) == "yes" else 0
            return "%s#define %s %d" % (indent, key, value)
        return "%s#define %s%s" % (indent, key, rest)

    text = re.sub(
        r"^(\s*)#cmakedefine(01)?\s+([A-Za-z0-9_]+)(.*)$",
        cmakedefine,
        text,
        flags=re.MULTILINE,
    )

    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(text)


if __name__ == "__main__":
    sys.exit(main())
