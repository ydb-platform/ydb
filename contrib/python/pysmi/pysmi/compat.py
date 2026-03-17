#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import sys


def encode(s):
    if isinstance(s, str):
        s = s.encode("utf-8", "ignore")
    return s


def decode(s):
    if isinstance(s, bytes):
        s = s.decode("utf-8", "ignore")
    return s
