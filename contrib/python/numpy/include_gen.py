#!/usr/bin/env python3
import os
import shutil
from os.path import dirname, exists, join, relpath

template = '''\
#ifdef USE_PYTHON3
#{}include <{}>
#else
#{}include <{}>
#endif
'''


def main():
    os.chdir(dirname(__file__))
    if exists('include'):
        shutil.rmtree('include')
    include_gen('contrib/python/numpy', ['numpy'])


def include_gen(root, subpaths):
    for path in list_subpaths(subpaths):
        out = join('include', path)
        py2 = join('py2', path)
        py3 = join('py3', path)
        if "/_core/" in out:
            out = out.replace("/_core/", "/core/")
            py2 = py2.replace("/_core/", "/core/")
        os.makedirs(dirname(out), exist_ok=True)
        with open(out, 'w') as f:
            f.write(template.format(
                '' if exists(py3) else 'error #',
                join(root, py3),
                '' if exists(py2) else 'error #',
                join(root, py2),
            ))


def is_header(s):
    return s.endswith(('.h', '.hpp'))


def list_subpaths(subpaths, roots=('py2', 'py3'), test=is_header):
    for root in roots:
        for subpath in subpaths:
            for dirpath, _, filenames in os.walk(join(root, subpath)):
                rootrel = relpath(dirpath, root)
                for filename in filenames:
                    if test(filename):
                        yield join(rootrel, filename)


if __name__ == '__main__':
    main()
