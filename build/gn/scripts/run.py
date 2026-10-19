"""Runs a command: run.py [--stdout <file>] [--python] <command> [args...]

--stdout <file>: the command's stdout written to <file> (ya's STDOUT of
    RUN_PROGRAM / RUN_PYTHON3), removed if the command fails: no stale output
    for ninja to take as up to date.
--python: <command> is a Python script, run with this interpreter
    (RUN_PYTHON3).
"""

import os
import subprocess
import sys


def main():
    args, stdout, python = sys.argv[1:], None, False
    while args and args[0] in ("--stdout", "--python"):
        if args[0] == "--stdout":
            stdout, args = args[1], args[2:]
        else:
            python, args = True, args[1:]
    if python:
        args = [sys.executable] + args
    if stdout is None:
        return subprocess.call(args)
    with open(stdout, "wb") as out:
        rc = subprocess.call(args, stdout=out)
    if rc != 0:
        os.remove(stdout)
    return rc


if __name__ == "__main__":
    sys.exit(main())
