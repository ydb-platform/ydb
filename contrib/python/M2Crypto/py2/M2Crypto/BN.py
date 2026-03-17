from __future__ import absolute_import

"""
M2Crypto wrapper for OpenSSL BN (BIGNUM) API.

Copyright (c) 2005 Open Source Applications Foundation. All rights reserved.
"""

from M2Crypto import m2
from typing import Optional  # noqa


def rand(bits, top=-1, bottom=0):
    # type: (int, int, int) -> Optional[int]
    """
    Generate cryptographically strong random number.

    :param bits:   Length of random number in bits.
    :param top:    If -1, the most significant bit can be 0. If 0, the most
                   significant bit is 1, and if 1, the two most significant
                   bits will be 1.
    :param bottom: If bottom is true, the number will be odd.
    """
    return m2.bn_rand(bits, top, bottom)


def rand_range(range):
    # type: (int) -> int
    """
    Generate a random number in a range.

    :param range: Upper limit for range.
    :return:      A random number in the range [0, range)
    """
    return m2.bn_rand_range(range)


def randfname(length):
    # type: (int) -> str
    """
    Return a random filename, which is simply a string where all
    the characters are from the set [a-zA-Z0-9].

    :param length: Length of filename to return.
    :return:       random filename string
    """
    import warnings
    warnings.warn(
        "Don't use BN.randfname(), use tempfile methods instead.",
        DeprecationWarning, stacklevel=2)
    letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890'
    lettersLen = len(letters)
    fname = []  # type: list
    for x in range(length):
        fname += [letters[m2.bn_rand_range(lettersLen)]]

    return ''.join(fname)
