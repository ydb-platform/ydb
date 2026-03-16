# -*- coding: utf-8 -*-
"""
Compatibility layer for Python 2 and Python 3.

Probably could have used six...
This file will light up most linters...
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import functools
import sys

# These functions are used to make the doctests compatible between
# python2 and python3, and also provide uniform functionality between
# the two versions.  This code is pretty much lifted from the iPython
# project's py3compat.py file.  Credit to the iPython devs.

# Numeric form of version
PY_VERSION = float(sys.version[:3])
NEWPY = PY_VERSION >= 3.3

# Assume all strings are Unicode in Python 2
py23_str = str if PY_VERSION >= 3 else unicode

# Use the range iterator always
py23_range = range if PY_VERSION >= 3 else xrange

# Uniform base string type
py23_basestring = str if PY_VERSION >= 3 else basestring

# Iniform int type
py23_int = (int,) if PY_VERSION >= 3 else (int, long)

# unichr function
py23_unichr = chr if PY_VERSION >= 3 else unichr

# Proper lower-casing of letters.
py23_lower = py23_str.casefold if NEWPY else py23_str.lower


def _py23_cmp(a, b):
    return (a > b) - (a < b)


py23_cmp = _py23_cmp if PY_VERSION >= 3 else cmp

# zip as an iterator
if PY_VERSION >= 3:
    py23_zip = zip
    py23_map = map
    py23_filter = filter
else:
    import itertools

    py23_zip = itertools.izip
    py23_map = itertools.imap
    py23_filter = itertools.ifilter


# This function is intended to decorate other functions that will modify
# either a string directly, or a function's docstring.
def _modify_str_or_docstring(str_change_func):
    @functools.wraps(str_change_func)
    def wrapper(func_or_str):
        if isinstance(func_or_str, py23_basestring):
            func = None
            doc = func_or_str
        else:
            func = func_or_str
            doc = func.__doc__

        if doc is not None:
            doc = str_change_func(doc)

        if func:
            func.__doc__ = doc
            return func
        return doc

    return wrapper


# Properly modify a doctstring to either have the unicode literal or not.
if PY_VERSION >= 3:
    # Abstract u'abc' syntax:
    @_modify_str_or_docstring
    def u_format(s):
        """"{u}'abc'" --> "'abc'" (Python 3)

        Accepts a string or a function, so it can be used as a decorator."""
        return s.format(u="")


else:
    # Abstract u'abc' syntax:
    @_modify_str_or_docstring
    def u_format(s):
        """"{u}'abc'" --> "u'abc'" (Python 2)

        Accepts a string or a function, so it can be used as a decorator."""
        return s.format(u="u")
