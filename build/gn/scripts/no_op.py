#!/usr/bin/env python3
"""Touch the declared outputs so ninja sees the action's stamp files.

Used by the `group_with_inputs` template (see build/gn/python.gni): the
action only exists to attach `inputs`/`sources` to a group for change
tracking, but GN still requires it to declare an `output`. If that output
is never created, ninja treats the target as perpetually dirty and reruns
the action on every build — including no-op rebuilds. Creating the stamp
files here keeps them up to date so the action stays clean.
"""

import os
import sys


def main(argv):
    for path in argv:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        # Update mtime if it exists, otherwise create an empty stamp.
        with open(path, "a"):
            os.utime(path, None)


if __name__ == "__main__":
    main(sys.argv[1:])
