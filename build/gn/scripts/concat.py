#!/usr/bin/env python3
"""concat.py <output> <input>... -- the inputs, one after another, into the
output: ya's RUN_PYTHON3(build/scripts/cat.py <inputs> STDOUT <output>)."""

import shutil
import sys


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: concat.py <output> <input>...", file=sys.stderr)
        return 2
    with open(sys.argv[1], "wb") as out:
        for path in sys.argv[2:]:
            with open(path, "rb") as f:
                shutil.copyfileobj(f, out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
