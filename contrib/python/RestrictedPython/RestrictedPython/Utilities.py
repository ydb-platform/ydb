##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

import math
import random
import string


utility_builtins = {}


class _AttributeDelegator:
    def __init__(self, mod, *excludes):
        """delegate attribute lookups outside *excludes* to module *mod*."""
        self.__mod = mod
        self.__excludes = excludes

    def __getattr__(self, attr):
        if attr in self.__excludes:
            raise NotImplementedError(
                f"{self.__mod.__name__}.{attr} is not safe")
        try:
            return getattr(self.__mod, attr)
        except AttributeError as e:
            e.obj = self
            raise


utility_builtins['string'] = _AttributeDelegator(string, "Formatter")
utility_builtins['math'] = math
utility_builtins['random'] = random
utility_builtins['whrandom'] = random
utility_builtins['set'] = set
utility_builtins['frozenset'] = frozenset

try:
    import DateTime
    utility_builtins['DateTime'] = DateTime.DateTime
except ImportError:
    pass


def same_type(arg1, *args):
    """Compares the class or type of two or more objects."""
    t = getattr(arg1, '__class__', type(arg1))
    for arg in args:
        if getattr(arg, '__class__', type(arg)) is not t:
            return False
    return True


utility_builtins['same_type'] = same_type


def test(*args):
    length = len(args)
    for i in range(1, length, 2):
        if args[i - 1]:
            return args[i]

    if length % 2:
        return args[-1]


utility_builtins['test'] = test


def reorder(s, with_=None, without=()):
    # s, with_, and without are sequences treated as sets.
    # The result is subtract(intersect(s, with_), without),
    # unless with_ is None, in which case it is subtract(s, without).
    if with_ is None:
        with_ = s
    orig = {}
    for item in s:
        if isinstance(item, tuple) and len(item) == 2:
            key, value = item
        else:
            key = value = item
        orig[key] = value

    result = []

    for item in without:
        if isinstance(item, tuple) and len(item) == 2:
            key, ignored = item
        else:
            key = item
        if key in orig:
            del orig[key]

    for item in with_:
        if isinstance(item, tuple) and len(item) == 2:
            key, ignored = item
        else:
            key = item
        if key in orig:
            result.append((key, orig[key]))
            del orig[key]

    return result


utility_builtins['reorder'] = reorder
