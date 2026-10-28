#!/usr/bin/env python3

import sys

from version import latest_version_section


def main() -> None:
    try:
        version, notes = latest_version_section(sys.stdin)
    except ValueError as error:
        raise SystemExit(str(error)) from error

    if not notes:
        raise SystemExit(f"CHANGELOG section for {version} is empty")

    print(notes)


if __name__ == "__main__":
    main()
