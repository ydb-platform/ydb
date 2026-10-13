"""Runs bison on a grammar (.y, .ypp) as ya's _SRC("y") does, then turns off
in the C++ it wrote the warnings ya turns off for the file
(_LANG_CFLAGS_BISON): gn has no flags per source.

    bison.py --bison B --m4 M --data D --output O --header H [--flag=F ...] SRC
"""

import argparse
import os
import subprocess
import sys

WARNINGS_OFF = ("-Wunused-but-set-variable", "-Wdeprecated-copy")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bison", required=True)
    p.add_argument("--m4", required=True)
    p.add_argument("--data", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--header", required=True)
    p.add_argument("--flag", action="append", default=[])
    p.add_argument("source")
    a = p.parse_args()

    env = dict(os.environ, M4=a.m4, BISON_PKGDATADIR=a.data)
    rc = subprocess.call([a.bison] + a.flag + ["--defines=" + a.header, "-o", a.output, a.source], env=env)
    if rc:
        return rc

    with open(a.output, encoding="utf-8") as f:
        code = f.read()
    with open(a.output, "w", encoding="utf-8") as f:
        f.writelines('#pragma GCC diagnostic ignored "%s"\n' % w for w in WARNINGS_OFF)
        f.write(code)
    return 0


if __name__ == "__main__":
    sys.exit(main())
