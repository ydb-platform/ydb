import subprocess
import sys


def fix(path: str):
    flags = []
    return flags

if __name__ == '__main__':
    path = sys.argv[1]
    args = sys.argv[2:]
    cmd = args + fix(path)
    rc = subprocess.call(cmd, shell=False, stderr=sys.stderr, stdout=sys.stdout)
    sys.exit(rc)
