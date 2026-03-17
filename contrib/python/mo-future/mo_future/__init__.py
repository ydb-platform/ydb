# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import builtins as __builtin__
import json
import platform
import sys
from _thread import allocate_lock, get_ident, start_new_thread, interrupt_main
from builtins import input
from collections import OrderedDict, UserDict
from collections.abc import Callable, Iterable, Mapping, Set, MutableMapping
from configparser import ConfigParser
from datetime import datetime, timezone
from functools import cmp_to_key, reduce, update_wrapper
from html.parser import HTMLParser
from io import BytesIO
from io import StringIO
from itertools import zip_longest
from urllib.parse import urlparse

__all__ = [
    "__builtin__",
    "allocate_lock",
    "binary_type",
    "boolean_type",
    "BytesIO",
    "Callable",
    "ConfigParser",
    "decorate",
    "extend",
    "first",
    "flatten",
    "function_type",
    "generator_types",
    "get_function_arguments",
    "get_function_name",
    "get_ident",
    "HTMLParser",
    "input",
    "integer_types",
    "interrupt_main",
    "is_binary",
    "is_text",
    "is_windows",
    "items",
    "Iterable",
    "izip",
    "long",
    "Mapping",
    "mockable",
    "Mockable",
    "MutableMapping",
    "NEXT",
    "next",
    "none_type",
    "OrderedDict",
    "process_time",
    "reduce",
    "Set",
    "sort_using_key",
    "start_new_thread",
    "StringIO",
    "STDOUT",
    "STDERR",
    "text",
    "transpose",
    "urlparse",
    "UserDict",
    "zip_longest",
    "utcnow",
    "utcfromtimestamp",
]

PYPY = False
PY2 = False
PY3 = True

try:
    import __pypy__ as _

    PYPY = True
except Exception:
    PYPY = False


none_type = type(None)
boolean_type = type(True)

try:
    STDOUT = sys.stdout.buffer
except Exception as e:
    # WE HOPE WHATEVER REPLACED sys.stdout CAN HANDLE BYTES IN UTF8
    STDOUT = sys.stdout

try:
    STDERR = sys.stderr.buffer
except Exception as e:
    # WE HOPE WHATEVER REPLACED sys.stderr CAN HANDLE BYTES IN UTF8
    STDERR = sys.stderr

try:
    from time import process_time
except:
    from time import clock as process_time

if "windows" in platform.system().lower():
    is_windows = True
else:
    is_windows = False

izip = zip
text = str
string_types = str
binary_type = bytes
integer_types = (int,)
number_types = (int, float)
long = int
unichr = chr
round = round
xrange = range
POS_INF = float("+inf")


def _gen():
    yield


generator_types = (
    type(_gen()),
    type(filter(lambda x: True, [])),
    type({}.items()),
    type({}.values()),
    type(map(lambda: 0, iter([]))),
    type(reversed([])),
    type(range(1)),
)


def items(d):
    return list(d.items())


def iteritems(d):
    return d.items()


def transpose(*args):
    return list(zip(*args))


def get_function_name(func):
    return func.__name__


def get_function_arguments(func):
    return func.__code__.co_varnames[: func.__code__.co_argcount]


def get_function_code(func):
    return func.__code__


def get_function_defaults(func):
    return func.__defaults__


def sort_using_cmp(data, cmp):
    return sorted(data, key=cmp_to_key(cmp))


def sort_using_key(data, key):
    return sorted(data, key=key)


def first(values):
    try:
        return iter(values).__next__()
    except StopIteration:
        return None


def NEXT(_iter):
    """
    RETURN next() FUNCTION, DO NOT CALL
    """
    return _iter.__next__


def next(_iter):
    return _iter.__next__()


def is_text(t):
    return t.__class__ is str


def is_binary(b):
    return b.__class__ is bytes


utf8_json_encoder = (
    json
    .JSONEncoder(
        skipkeys=False,
        ensure_ascii=False,  # DIFF FROM DEFAULTS
        check_circular=True,
        allow_nan=True,
        indent=None,
        separators=(",", ":"),
        default=None,
        sort_keys=True,  # <-- IMPORTANT!  sort_keys==True
    )
    .encode
)


function_type = (lambda: None).__class__


class decorate:
    def __init__(self, func):
        self.func = func

    def __call__(self, caller):
        """
        :param caller: A METHOD THAT IS EXPECTED TO CALL func
        :return: caller, BUT WITH SIGNATURE OF  self.func
        """
        return update_wrapper(caller, self.func)


def extend(cls):
    """
    DECORATOR TO ADD METHODS TO CLASSES
    :param cls: THE CLASS TO ADD THE METHOD TO
    :return:
    """

    def extender(func):
        setattr(cls, get_function_name(func), func)
        return func

    return extender


def flatten(items):
    return (vv for v in items for vv in v)


if sys.version_info >= (3, 12):

    def utcnow():
        return datetime.now(timezone.utc)

    def utcfromtimestamp(timestamp):
        return datetime.fromtimestamp(timestamp, timezone.utc)


else:
    utcnow = datetime.utcnow

    def utcfromtimestamp(u):
        d = datetime.utcfromtimestamp(u)
        d = d.replace(tzinfo=timezone.utc)
        return d


def mockable(enable):
    """
    FUNCTION DECORATOR TO ALLOW MOCKING THIS FUNCTION
    :param enable:
    :return:
    """
    if isinstance(enable, bool):

        def decorator(func):
            if enable:
                return Mockable(func)
            else:
                return func

        return decorator

    # ALLOW FUNCTION TO BE CALLED AS A DECORATOR
    return Mockable(enable)


class Mockable:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)