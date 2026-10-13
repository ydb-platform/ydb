#!/usr/bin/env python3
"""Run a command from a given working directory.

GN's action() always runs its script from root_build_dir and offers no way to
set the child's cwd. antlr3 (via StringTemplate's STGroupFile) resolves its
backend code-generation templates — org/antlr/codegen/templates/<Lang>/<Lang>.stg
— as a plain file *relative to the process cwd*, so we must chdir into the
templates root before exec'ing java. Usage:

    script = "//build/gn/scripts/run_in_dir.py"
    args = [ rebase_path(cwd_dir) ] + command_argv   # cwd_dir and every path in
                                                      # command_argv absolute
"""

import os
import sys


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: run_in_dir.py <dir> <command> [args...]", file=sys.stderr)
        return 2
    os.chdir(sys.argv[1])
    os.execvp(sys.argv[2], sys.argv[2:])


if __name__ == "__main__":
    sys.exit(main())
