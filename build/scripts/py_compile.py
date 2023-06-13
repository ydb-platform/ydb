#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import, division

import marshal
import sys


def main():
    srcpathx, in_fname, out_fname = sys.argv[1:]
    srcpath = srcpathx[:-1]

    with open(in_fname, 'r') as in_file:
        source = in_file.read()

    code = compile(source, srcpath, 'exec', dont_inherit=True)

    with open(out_fname, 'wb') as out_file:
        marshal.dump(code, out_file)


if __name__ == "__main__":
    main()
