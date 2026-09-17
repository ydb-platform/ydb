#!/usr/bin/env python3

# Cuts the force-linked GLOBAL objects of the udf service policy libraries out of a link command:
# the host that loads a udf supplies the service functions of the udf ABI itself, and a second copy
# inside the .so hands the code an allocator the host knows nothing about.

import json
import os
import sys

POLICY_ROOT = 'yql/essentials/public/udf/service'

# An archive of its own on the ld platforms; on msvc the GLOBAL sources are handed over one object
# at a time. Objects reach a link command from SRCS_GLOBAL alone -- the rest of a library arrives
# packed into its archive -- so any object under a policy directory is one of these.
GLOBAL_SUFFIXES = ('.global.a', '.global.lib', '.o', '.obj')


def windows_normalize_path(arg):
    return arg.strip('"').replace('\\', '/')


def is_policy_global(arg):
    path = '/' + windows_normalize_path(arg)
    if '/' + POLICY_ROOT + '/' not in path:
        return False
    return os.path.basename(path).endswith(GLOBAL_SUFFIXES)


def remove_policy_globals(args):
    return [arg for arg in args if not is_policy_global(arg)]


def main():
    args = sys.argv[1:]

    for arg in args:
        is_response_file = arg.startswith('@')
        if is_response_file:
            # The linker reads it on its own, so it is rewritten rather than returned.
            with open(arg[1:]) as f:
                lines = f.read().splitlines()
            kept = remove_policy_globals(lines)
            if kept != lines:
                with open(arg[1:], 'w') as f:
                    f.write('\n'.join(kept) + '\n')

    # Returned as it came in:
    # link_exe.py passes its own path first and drops it back off,
    # link_dyn_lib.py and fix_msvc_output.py take the whole list.
    sys.stdout.write(json.dumps(remove_policy_globals(args)))


if __name__ == '__main__':
    main()
