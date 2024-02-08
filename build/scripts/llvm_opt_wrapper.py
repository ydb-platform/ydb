import subprocess
import sys


def fix(s):
    # we use '#' instead of ',' because ymake always splits args by comma
    if s.startswith('-internalize-public-api-list'):
        return s.replace('#', ',')

    # Dirty hack to eliminate double quotes from value of passes option.
    # Note that these double quoted are required by cmake.
    if s.startswith('-passes'):
        name, value = s.split('=', 1)
        value = value.strip('"')
        return '='.join([name, value])

    return s


if __name__ == '__main__':
    path = sys.argv[1]
    args = [fix(s) for s in [path] + sys.argv[2:]]

    rc = subprocess.call(args, shell=False, stderr=sys.stderr, stdout=sys.stdout)
    sys.exit(rc)
