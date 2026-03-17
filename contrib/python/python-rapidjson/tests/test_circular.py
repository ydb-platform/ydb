# -*- coding: utf-8 -*-
# :Project:   python-rapidjson -- Circular references tests
# :Author:    John Anderson <sontek@gmail.com>
# :License:   MIT License
# :Copyright: © 2015 John Anderson
# :Copyright: © 2017, 2018, 2020, 2023, 2024 Lele Gaifax
#

import sys

import pytest


def test_circular_dict(dumps):
    dct = {}
    dct['a'] = dct

    with pytest.raises(RecursionError):
        dumps(dct)


def test_circular_list(dumps):
    lst = []
    lst.append(lst)

    with pytest.raises(RecursionError):
        dumps(lst)


def test_circular_composite(dumps):
    dct2 = {}
    dct2['a'] = []
    dct2['a'].append(dct2)

    with pytest.raises(RecursionError):
        dumps(dct2)


if sys.version_info < (3, 12):
    # In Python 3.12 and beyond builtin functions are not affected by the recursion limit.
    #
    # See the issue https://github.com/python/cpython/issues/107263 and the question
    # that originated it, then the NEWS introduced by this commit
    # https://github.com/python/cpython/commit/fa45958450aa3489607daf9855ca0474a2a20878:
    #
    #    The recursion limit [that is sys.setrecursionlimit() and sys.getrecursionlimit()]
    #    now applies only to Python code. Builtin functions do not use the recursion
    #    limit, but are protected by a different mechanism that prevents recursion from
    #    causing a virtual machine crash.

    def test_max_recursion_depth(dumps):
        root = child = {}
        for i in range(500):
            nephew = {'value': i}
            child['child'] = nephew
            child = nephew

        dumps(root)

        rl = sys.getrecursionlimit()

        if hasattr(sys, "pyston_version_info"):
            # In Pyston setrecursionlimit() sets a lower bound on the number of frames,
            # not an exact count. So set the limit lower:
            sys.setrecursionlimit(100)
        else:
            # Otherwise set it to the exact value:
            sys.setrecursionlimit(500)

        try:
            with pytest.raises(RecursionError):
                dumps(root)
        finally:
            sys.setrecursionlimit(rl)


def test_parse_respects_recursion_limit(loads):
    rl = sys.getrecursionlimit()

    deep_array = '[' * rl + ']' * rl
    deep_obj = '{}'
    for depth in range(rl-1):
        deep_obj = '{"obj": ' + deep_obj + '}'

    loads(deep_array)
    loads(deep_obj)

    sys.setrecursionlimit(rl // 2)

    try:
        with pytest.raises(RecursionError):
            loads(deep_array)
        with pytest.raises(RecursionError):
            loads(deep_obj)
    finally:
        sys.setrecursionlimit(rl)
