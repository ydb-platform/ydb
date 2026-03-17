#!/usr/bin/env python
"""This is an alternative to pythoni which tries to look like the
CPython prompt as much as possible, with the exception of allowing
multiline input and multiline history entries.
"""

import os
import sys

from pyrepl import readline
from pyrepl.simple_interact import run_multiline_interactive_console

sys.modules["readline"] = readline


def main():
    pythonstartup = os.getenv("PYTHONSTARTUP")
    if pythonstartup and os.path.exists(pythonstartup):
        with open(pythonstartup) as fh:
            code = fh.read()
        exec(code)

    print("Python", sys.version)
    run_multiline_interactive_console()


if __name__ == "__main__":
    main()
