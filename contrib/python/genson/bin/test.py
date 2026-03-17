#!/usr/bin/env python

import os
import subprocess
import sys


def main():
    return subprocess.call(
        ['python', 'setup.py', 'test'] + sys.argv[1:],
        cwd=os.path.join(os.path.dirname(__file__), os.pardir)
    )


if __name__ == '__main__':
    sys.exit(main())
