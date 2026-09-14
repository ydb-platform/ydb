import subprocess
import sys
import os


def flt(args):
    for a in args:
        if a == '-o':
            yield '-Wl,-r'
            yield '-fuse-ld=lld'
            yield '-nodefaultlibs'
            yield '-nostartfiles'
            yield '-Wl,-no-pie'
            yield '-o'
        elif a.endswith('.o'):
            yield a
        elif '--ld' in a:
            yield a
        elif '--target' in a:
            yield a


if '-apple-macos' in str(sys.argv):
    cmd = sys.argv[1:]
elif '-apple-darwin' in str(sys.argv):
    cmd = sys.argv[1:]
else:
    cmd = [sys.argv[1]] + list(flt(sys.argv[2:]))


subprocess.check_call(cmd)
