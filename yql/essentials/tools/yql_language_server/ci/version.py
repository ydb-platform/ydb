#!/usr/bin/env python3

import re
import sys
from collections.abc import Iterable


VERSION_HEADING = re.compile(
    r"^##[ \t]+(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?)\s*$"
)
SECTION_HEADING = re.compile(r"^##(?:[ \t]+|$)")


def latest_version_section(lines: Iterable[str]) -> tuple[str, str]:
    version = None
    body = []

    for line in lines:
        heading = line.rstrip("\r\n")
        if version is None:
            match = VERSION_HEADING.fullmatch(heading)
            if match:
                version = match.group(1)
            continue

        if SECTION_HEADING.match(heading):
            break
        body.append(line)

    if version is None:
        raise ValueError("no semantic version section found in CHANGELOG")

    return version, "".join(body).strip()


def main() -> None:
    try:
        version, _ = latest_version_section(sys.stdin)
    except ValueError as error:
        raise SystemExit(str(error)) from error

    print(version)


if __name__ == "__main__":
    main()
