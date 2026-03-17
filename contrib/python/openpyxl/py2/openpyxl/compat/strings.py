from __future__ import absolute_import
# Copyright (c) 2010-2019 openpyxl

from datetime import datetime
from math import isnan, isinf
import sys

VER = sys.version_info

from .numbers import NUMERIC_TYPES

if VER[0] >= 3:
    basestring = str
    unicode = str
    from io import BufferedReader
    file = BufferedReader
    from io import BufferedRandom
    tempfile = BufferedRandom
    bytes = bytes
else:
    basestring = basestring
    unicode = unicode
    file = file
    tempfile = file
    bytes = str


def safe_string(value):
    """Safely and consistently format numeric values"""
    if isinstance(value, NUMERIC_TYPES):
        if isnan(value) or isinf(value):
            value = ""
        else:
            value = "%.16g" % value
    elif value is None:
        value = "none"
    elif isinstance(value, datetime):
        value = value.isoformat()
    elif not isinstance(value, basestring):
        value = str(value)
    return value


def safe_repr(value):
    """
    Safely convert unicode to ASCII for Python 2
    """
    if VER[0] == 3:
        return value
    return value.encode("ascii", 'backslashreplace')
