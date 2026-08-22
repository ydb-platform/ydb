# Copyright (C) Jean-Paul Calderone
# Copyright (C) Twisted Matrix Laboratories.
# See LICENSE for details.
"""
Helpers for the OpenSSL test suite, largely copied from
U{Twisted<http://twistedmatrix.com/>}.
"""

# This is the UTF-8 encoding of the SNOWMAN unicode code point.
NON_ASCII = b"\xe2\x98\x83".decode("utf-8")

# The type name expected in warnings about using the wrong string type.
WARNING_TYPE_EXPECTED = "str"
