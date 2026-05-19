#!/usr/bin/env python3
"""Print CODEOWNERS handles (owners) for a given repo-relative path.

Reads `.github/CODEOWNERS` by default, applies the full CODEOWNERS spec via
the `codeowners` PyPI package (the same package already used by analytics /
mute scripts in this repo), and prints one handle per line.

Exit code is always 0 even when no owners are matched; consumers can detect
"no match" by checking for empty output.
"""
import argparse
import sys

from codeowners import CodeOwners


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", help="Repo-relative path to resolve, e.g. .github/workflows/foo.yml")
    parser.add_argument("--codeowners", default=".github/CODEOWNERS", help="Path to the CODEOWNERS file")
    args = parser.parse_args()

    with open(args.codeowners) as f:
        co = CodeOwners(f.read())

    for _kind, owner in co.of(args.path):
        print(owner)
    return 0


if __name__ == "__main__":
    sys.exit(main())
