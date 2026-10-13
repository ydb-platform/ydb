#!/usr/bin/env python3
"""Thin shim so a GN action() can invoke a native executable.

GN's `action` template insists on a Python script as `script`; you can't
point it at an ELF directly. This wrapper takes a binary path as argv[1]
and execvp's it with the remaining arguments, propagating its exit code.
Use from a template like:

    action(target_name) {
        script = "//build/gn/scripts/run_binary.py"
        args = [ rebase_path(binary_path, root_build_dir) ] + invoker.args
        ...
    }
"""

import os
import sys


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: run_binary.py <binary> [args...]", file=sys.stderr)
        return 2
    binary = sys.argv[1]
    # rebase_path(.., root_build_dir) in GN gives us a path *relative to
    # the current working directory* (which is root_build_dir for actions).
    # For a binary that lives at the cwd's top level the result is just
    # its name, with no slashes — and execvp would then search $PATH
    # instead of running the file. Resolve to an absolute path up front so
    # we always exec the intended ELF.
    binary = os.path.abspath(binary)
    # execvp replaces this process; exit code goes straight to ninja.
    os.execvp(binary, [binary] + sys.argv[2:])


if __name__ == "__main__":
    sys.exit(main())
