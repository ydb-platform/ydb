#!/usr/bin/env python
import sys
from shutil import copyfileobj as copy
import os.path


PY3 = sys.version_info[0] == 3


if __name__ == '__main__':
    for filename in sys.argv[1:] or ["-"]:
        if filename == "-":
            copy(sys.stdin, sys.stdout)
        else:
            if os.path.exists(filename):
                with open(filename, 'r' if PY3 else 'rb') as file:
                    copy(file, sys.stdout)
            else:
                sys.stderr.write('cat.py: {0}: No such file or directory\n'.format(filename))
