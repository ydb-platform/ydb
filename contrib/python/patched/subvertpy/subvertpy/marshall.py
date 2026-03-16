# Copyright (C) 2006-2007 Jelmer Vernooij <jelmer@jelmer.uk>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2.1 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA

"""Marshalling for the svn_ra protocol."""


class literal(object):
    """A protocol literal."""

    def __init__(self, txt):
        self.txt = txt

    def __str__(self):
        return self.txt

    def __repr__(self):
        return self.txt

    def __eq__(self, other):
        return (type(self) == type(other) and self.txt == other.txt)

# 1. Syntactic structure
# ----------------------
#
# The Subversion protocol is specified in terms of the following
# syntactic elements, specified using ABNF [RFC 2234]:
#
#   item   = word / number / string / list
#   word   = ALPHA *(ALPHA / DIGIT / "-") space
#   number = 1*DIGIT space
#   string = 1*DIGIT ":" *OCTET space
#          ; digits give the byte count of the *OCTET portion
#   list   = "(" space *item ")" space
#   space  = 1*(SP / LF)
#


class MarshallError(Exception):
    """A Marshall error."""


class NeedMoreData(MarshallError):
    """More data needed."""


def marshall(x):
    """Marshall a Python data item.

    :param x: Data item
    :return: encoded byte string
    """
    if isinstance(x, int):
        return ("%d " % x).encode("ascii")
    elif isinstance(x, (list, tuple)):
        return b"( " + bytes().join(map(marshall, x)) + b") "
    elif isinstance(x, literal):
        return ("%s " % x).encode("ascii")
    elif isinstance(x, bytes):
        return ("%d:" % len(x)).encode("ascii") + x + b" "
    elif isinstance(x, str):
        x = x.encode("utf-8")
        return ("%d:" % len(x)).encode("ascii") + x + b" "
    elif isinstance(x, bool):
        if x is True:
            return b"true "
        elif x is False:
            return b"false "
    raise MarshallError("Unable to marshall type %s" % x)


def unmarshall(x):
    """Unmarshall the next item from a buffer.

    :param x: Bytes to parse
    :return: tuple with unpacked item and remaining bytes
    """
    whitespace = frozenset(b'\n ')
    if len(x) == 0:
        raise NeedMoreData("Not enough data")
    if x[0:1] == b"(":  # list follows
        if len(x) <= 1:
            raise NeedMoreData("Missing whitespace")
        if x[1:2] != b" ":
            raise MarshallError("missing whitespace after list start")
        x = x[2:]
        ret = []
        try:
            while x[0:1] != b")":
                (x, n) = unmarshall(x)
                ret.append(n)
        except IndexError:
            raise NeedMoreData("List not terminated")

        if len(x) <= 1:
            raise NeedMoreData("Missing whitespace")

        if not x[1] in whitespace:
            raise MarshallError("Expected space, got '%c'" % x[1])

        return (x[2:], ret)
    elif x[0:1].isdigit():
        num = bytearray()
        # Check if this is a string or a number
        while x[:1].isdigit():
            num.append(x[0])
            x = x[1:]
        num = int(num)

        if x[0] in whitespace:
            return (x[1:], num)
        elif x[0:1] == b":":
            if len(x) < num:
                raise NeedMoreData("Expected string of length %r" % num)
            return (x[num+2:], x[1:num+1])
        elif not x:
            raise MarshallError("Expected whitespace, got end of string.")
        else:
            raise MarshallError("Expected whitespace or ':', got '%c'" % x[0])
    elif x[:1].isalpha():
        ret = bytearray()
        # Parse literal
        try:
            while x[:1].isalpha() or x[:1].isdigit() or x[0:1] == b'-':
                ret.append(x[0])
                x = x[1:]
        except IndexError:
            raise NeedMoreData("Expected literal")

        if not x:
            raise MarshallError("Expected whitespace, got end of string.")

        if not x[0] in whitespace:
            raise MarshallError("Expected whitespace, got '%c'" % x[0])

        return (x[1:], literal(ret.decode("ascii")))
    else:
        raise MarshallError("Unexpected character '%c'" % x[0])
