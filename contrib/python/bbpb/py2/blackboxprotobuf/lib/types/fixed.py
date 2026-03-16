"""Functions for encoding and decoding fixed size integers and floats"""

# Copyright (c) 2018-2024 NCC Group Plc
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import struct
import binascii
import six
from blackboxprotobuf.lib.exceptions import DecoderException, EncoderException

import six

if six.PY3:
    from typing import Any, Tuple


# Generic functions for encoding/decoding structs based on the "struct" format
def encode_struct(fmt, value):
    # type: (str, Any) -> bytes
    """Generic method for encoding arbitrary python "struct" values"""
    try:
        return struct.pack(fmt, value)
    except struct.error as exc:
        six.raise_from(
            EncoderException(
                "Error encoding value %r with format string %s" % (value, fmt)
            ),
            exc,
        )


def decode_struct(fmt, buf, pos):
    # type: (str, bytes, int) -> Tuple[Any, int]
    """Generic method for decoding arbitrary python "struct" values"""
    new_pos = pos + struct.calcsize(fmt)
    try:
        return struct.unpack(fmt, buf[pos:new_pos])[0], new_pos
    except struct.error as exc:
        six.raise_from(
            DecoderException(
                "Error deocding format string %s from bytes: %r"
                % (fmt, binascii.hexlify(buf[pos:new_pos]))
            ),
            exc,
        )


_fixed32_fmt = "<I"

# Note on types: We use Any here for decoded objects. While we could manually
# enforce individual types, we would have to do so via `typing.cast` anyway
# because of `struct`. The Message type also has `Any` anyways.


def encode_fixed32(value):
    # type: (Any) -> bytes
    """Encode a single 32 bit fixed-size value"""
    return encode_struct(_fixed32_fmt, value)


def decode_fixed32(buf, pos):
    # type: (bytes, int) -> Tuple[Any, int]
    """Decode a single 32 bit fixed-size value"""
    return decode_struct(_fixed32_fmt, buf, pos)


_sfixed32_fmt = "<i"


def encode_sfixed32(value):
    # type: (Any) -> bytes
    """Encode a single signed 32 bit fixed-size value"""
    return encode_struct(_sfixed32_fmt, value)


def decode_sfixed32(buf, pos):
    # type: (bytes, int) -> Tuple[Any, int]
    """Decode a single signed 32 bit fixed-size value"""
    return decode_struct(_sfixed32_fmt, buf, pos)


_float_fmt = "<f"


def encode_float(value):
    # type: (Any) -> bytes
    """Encode a single 32 bit floating point value"""
    return encode_struct(_float_fmt, value)


def decode_float(buf, pos):
    # type: (bytes, int) -> Tuple[Any, int]
    """Decode a single 32 bit floating point value"""
    return decode_struct(_float_fmt, buf, pos)


_fixed64_fmt = "<Q"


def encode_fixed64(value):
    # type: (Any) -> bytes
    """Encode a single 64 bit fixed-size value"""
    return encode_struct(_fixed64_fmt, value)


def decode_fixed64(buf, pos):
    # type: (bytes, int) -> Tuple[Any, int]
    """Decode a single 64 bit fixed-size value"""
    return decode_struct(_fixed64_fmt, buf, pos)


_sfixed64_fmt = "<q"


def encode_sfixed64(value):
    # type: (Any) -> bytes
    """Encode a single signed 64 bit fixed-size value"""
    return encode_struct(_sfixed64_fmt, value)


def decode_sfixed64(buf, pos):
    # type: (bytes, int) -> Tuple[Any, int]
    """Decode a single signed 64 bit fixed-size value"""
    return decode_struct(_sfixed64_fmt, buf, pos)


_double_fmt = "<d"


def encode_double(value):
    # type: (Any) -> bytes
    """Encode a single 64 bit floating point value"""
    return encode_struct(_double_fmt, value)


def decode_double(buf, pos):
    # type: (bytes, int) -> Tuple[Any, int]
    """Decode a single 64 bit floating point value"""
    return decode_struct(_double_fmt, buf, pos)
