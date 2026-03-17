#
# Author: Satoru SATOH <ssato redhat.com>
# License: MIT
#
# pylint: disable=unused-import,import-error,invalid-name,wrong-import-position
"""
Compatiblity module
"""
from __future__ import absolute_import

import inspect
import itertools
import sys

try:
    import pathlib  # flake8: noqa
except ImportError:
    pathlib = None

try:
    from logging import NullHandler
except ImportError:  # python < 2.7 doesn't have it.
    import logging

    class NullHandler(logging.Handler):
        """
        Logging handler does nothing.
        """
        def emit(self, record):
            pass

from collections import OrderedDict  # noqa: F401


(_PY_MAJOR, _PY_MINOR) = sys.version_info[:2]
IS_PYTHON_3 = _PY_MAJOR == 3
IS_PYTHON_2_6 = _PY_MAJOR == 2 and _PY_MINOR == 6


# Borrowed from library doc, 9.7.1 Itertools functions:
def _from_iterable(iterables):
    """
    itertools.chain.from_iterable alternative.

    >>> list(_from_iterable([[1, 2], [3, 4]]))
    [1, 2, 3, 4]
    """
    for itr in iterables:
        for element in itr:
            yield element


def py3_iteritems(dic):
    """wrapper for dict.items() in python 3.x.

    >>> list(py3_iteritems({}))
    []
    >>> sorted(py3_iteritems(dict(a=1, b=2)))
    [('a', 1), ('b', 2)]
    """
    return dic.items()


# pylint: disable=redefined-builtin
if IS_PYTHON_3:
    import configparser  # flake8: noqa
    from io import StringIO  # flake8: noqa
    iteritems = py3_iteritems
    from_iterable = itertools.chain.from_iterable
    raw_input = input
    STR_TYPES = (str, )
    getargspec = inspect.getfullargspec  # flake8: noqa
else:
    import ConfigParser as configparser  # flake8: noqa
    try:
        from cStringIO import StringIO  # flake8: noqa
    except ImportError:
        from StringIO import StringIO  # flake8: noqa

    try:
        from_iterable = itertools.chain.from_iterable
    except AttributeError:
        from_iterable = _from_iterable

    assert configparser  # silence pyflakes
    assert StringIO  # ditto

    def py_iteritems(dic):
        """wrapper for dict.iteritems() in python < 3.x

        >>> list(py_iteritems({}))
        []
        >>> sorted(py_iteritems(dict(a=1, b=2)))
        [('a', 1), ('b', 2)]
        """
        return dic.iteritems()

    iteritems = py_iteritems
    raw_input = raw_input
    STR_TYPES = (str, unicode)  # noqa: F821
    getargspec = inspect.getargspec  # flake8: noqa

# vim:sw=4:ts=4:et:
