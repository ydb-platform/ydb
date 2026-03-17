#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from base64 import standard_b64decode as stddecode
from base64 import urlsafe_b64encode as b64encode

"""
Numbers to bytes or strings and URLs coder/decoders.
"""

c2i = lambda c: c
i2c = lambda i: bytes([i])


def num_to_bin(num):
    """
    Convert a `num` integer or long to a binary string byte-ordered such that
    the least significant bytes are at the beginning of the string (aka. big
    endian).
    """
    # Zero is not encoded but returned as an empty value
    if num == 0:
        return b'\x00'

    return num.to_bytes((num.bit_length() + 7) // 8, 'big')


def bin_to_num(binstr):
    """
    Convert a big endian byte-ordered binary string to an integer or long.
    """
    return int.from_bytes(binstr, byteorder='big', signed=False)


def urlsafe_b64encode(s):
    """
    Encode a binary string to a url safe base64 encoding.
    """
    return b64encode(s)


def urlsafe_b64decode(b64):
    """
    Decode a url safe base64-encoded string.
    Note that we use stddecode to work around a bug in the standard library.
    """
    b = b64.replace(b'-', b'+').replace(b'_', b'/')
    return stddecode(b)


def urlsafe_b64encode_int(num):
    """
    Encode a number (int or long) in url safe base64.
    """
    return b64encode(num_to_bin(num))
