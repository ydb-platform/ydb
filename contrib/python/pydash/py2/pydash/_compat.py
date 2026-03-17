# -*- coding: utf-8 -*-
# flake8: noqa
# pylint: skip-file
"""
Python 2/3 compatibility.

Some py2/py3 compatibility support based on a stripped down
version of six so we don't have to depend on a specific version
of it.

Borrowed from
https://github.com/mitsuhiko/flask/blob/master/flask/_compat.py
"""

from __future__ import absolute_import

import cgi
from decimal import Decimal
from functools import partial
import inspect
import sys


PY2 = sys.version_info[0] == 2


def _identity(x):
    return x


if PY2:
    from collections import Hashable, Iterable, Mapping, Sequence
    from itertools import izip
    from urllib import urlencode

    from HTMLParser import HTMLParser
    import __builtin__ as _builtins
    from urlparse import parse_qs, parse_qsl, urlsplit, urlunsplit

    text_type = unicode
    string_types = (str, unicode)
    integer_types = (int, long)
    number_types = (int, long, float, Decimal)
    getfullargspec = inspect.getargspec
    html_unescape = HTMLParser().unescape
    html_escape = partial(cgi.escape, quote=True)

    def iterkeys(d):
        return d.iterkeys()

    def itervalues(d):
        return d.itervalues()

    def iteritems(d):
        return d.iteritems()

    _range = xrange
    _cmp = cmp

    def implements_to_string(cls):
        cls.__unicode__ = cls.__str__
        cls.__str__ = lambda x: x.__unicode__().encode("utf-8")
        return cls


else:
    import builtins as _builtins
    from collections.abc import Hashable, Iterable, Mapping, Sequence
    import html
    from html.parser import HTMLParser
    from urllib.parse import parse_qs, parse_qsl, urlencode, urlsplit, urlunsplit

    text_type = str
    string_types = (str,)
    integer_types = (int,)
    number_types = (int, float, Decimal)
    builtins = _builtins.__dict__.values()
    getfullargspec = inspect.getfullargspec
    html_unescape = html.unescape
    html_escape = html.escape

    def iterkeys(d):
        return iter(d.keys())

    def itervalues(d):
        return iter(d.values())

    def iteritems(d):
        return iter(d.items())

    _range = range

    implements_to_string = _identity
    izip = zip

    def _cmp(a, b):
        if a is None and b is None:
            return 0
        elif a is None:
            return -1
        elif b is None:
            return 1
        return (a > b) - (a < b)


builtins = {
    value: key for key, value in iteritems(_builtins.__dict__) if isinstance(value, Hashable)
}

try:
    from functools import cmp_to_key
except ImportError:
    # This function is missing on PY26.
    def cmp_to_key(mycmp):
        """Convert a cmp= function into a key= function."""

        class K(object):
            __slots__ = ["obj"]

            def __init__(self, obj, *args):
                self.obj = obj

            def __lt__(self, other):
                return mycmp(self.obj, other.obj) < 0

            def __gt__(self, other):
                return mycmp(self.obj, other.obj) > 0

            def __eq__(self, other):
                return mycmp(self.obj, other.obj) == 0

            def __le__(self, other):
                return mycmp(self.obj, other.obj) <= 0

            def __ge__(self, other):
                return mycmp(self.obj, other.obj) >= 0

            def __ne__(self, other):
                return mycmp(self.obj, other.obj) != 0

            def __hash__(self):
                raise TypeError("hash not implemented")

        return K
