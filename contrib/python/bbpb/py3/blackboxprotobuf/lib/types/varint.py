"""Classes for encoding and decoding varint types"""

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

import binascii
import struct
import six

from blackboxprotobuf.lib.exceptions import EncoderException, DecoderException

if six.PY3:
    from typing import Any, Tuple

# These are set in decoder.py
# In theory, uvarints and zigzag varints shouldn't have a max
# But this is enforced by protobuf
MAX_UVARINT = (1 << 64) - 1
MIN_UVARINT = 0
MAX_SVARINT = (1 << 63) - 1
MIN_SVARINT = -(1 << 63)


def encode_uvarint(value):
    # type: (Any) -> bytes
    """Encode a long or int into a bytearray."""
    if not isinstance(value, six.integer_types):
        raise EncoderException("Got non-int type for uvarint encoding: %s" % value)
    output = bytearray()
    if value < MIN_UVARINT:
        raise EncoderException(
            "Error encoding %d as uvarint. Value must be positive" % value
        )
    if value > MAX_UVARINT:
        raise EncoderException(
            "Error encoding %d as uvarint. Value must be %s or less"
            % (value, MAX_UVARINT)
        )

    if not value:
        output.append(value & 0x7F)
    else:
        while value:
            next_byte = value & 0x7F
            value >>= 7
            if value:
                next_byte |= 0x80
            output.append(next_byte)

    return output


def decode_uvarint(buf, pos):
    # type: (bytes, int) -> Tuple[int, int]
    """Decode bytearray into a long."""
    pos_start = pos
    # Convert buffer to string
    if six.PY2:
        buf = bytes(buf)

    try:
        value = 0
        shift = 0
        while six.indexbytes(buf, pos) & 0x80:
            value += (six.indexbytes(buf, pos) & 0x7F) << (shift * 7)
            pos += 1
            shift += 1
        value += (six.indexbytes(buf, pos) & 0x7F) << (shift * 7)
        pos += 1
    except IndexError:
        raise DecoderException(
            "Error decoding uvarint: read past the end of the buffer"
        )

    # Validate that this is a cononical encoding by re-encoding the value
    try:
        test_encode = encode_uvarint(value)
    except EncoderException as ex:
        raise DecoderException(
            "Error decoding uvarint: value (%s) was not able to be re-encoded: %s"
            % (value, ex)
        )
    if buf[pos_start:pos] != test_encode:
        raise DecoderException(
            "Error decoding uvarint: Encoding is not standard:\noriginal:  %r\nstandard: %r"
            % (buf[pos_start:pos], test_encode)
        )

    return (value, pos)


def encode_varint(value):
    # type: (Any) -> bytes
    """Encode a long or int into a bytearray."""
    if not isinstance(value, six.integer_types):
        raise EncoderException("Got non-int type for varint encoding: %s" % value)
    if value > MAX_SVARINT:
        raise EncoderException(
            "Error encoding %d as varint. Value must be <= %s" % (value, MAX_SVARINT)
        )
    if value < MIN_SVARINT:
        raise EncoderException(
            "Error encoding %d as varint. Value must be >= %s" % (value, MIN_SVARINT)
        )
    if value < 0:
        value += 1 << 64
    output = encode_uvarint(value)
    return output


def decode_varint(buf, pos):
    # type: (bytes, int) -> Tuple[int, int]
    """Decode bytearray into a long."""
    # Convert buffer to string
    pos_start = pos
    if six.PY2:
        buf = bytes(buf)

    value, pos = decode_uvarint(buf, pos)
    if value & (1 << 63):
        value -= 1 << 64

    # Validate that this is a cononical encoding by re-encoding the value
    try:
        test_encode = encode_varint(value)
    except EncoderException as ex:
        raise DecoderException(
            "Error decoding varint: value (%s) was not able to be re-encoded: %s"
            % (value, ex)
        )

    if buf[pos_start:pos] != test_encode:
        raise DecoderException(
            "Error decoding varint: Encoding is not standard:\noriginal:  %r\nstandard: %r"
            % (buf[pos_start:pos], test_encode)
        )
    return (value, pos)


def encode_zig_zag(value):
    # type: (int) -> int
    if value < 0:
        return (abs(value) << 1) - 1
    return value << 1


def decode_zig_zag(value):
    # type: (int) -> int
    if value & 0x1:
        # negative
        return -((value + 1) >> 1)
    return value >> 1


def encode_svarint(value):
    # type: (Any) -> bytes
    """Zigzag encode the potentially signed value prior to encoding"""
    if not isinstance(value, six.integer_types):
        raise EncoderException("Got non-int type for svarint encoding: %s" % value)
    # zigzag encode value
    if value > MAX_SVARINT:
        raise EncoderException(
            "Error encoding %d as svarint. Value must be <= %s" % (value, MAX_SVARINT)
        )
    if value < MIN_SVARINT:
        raise EncoderException(
            "Error encoding %d as svarint. Value must be >= %s" % (value, MIN_SVARINT)
        )
    return encode_uvarint(encode_zig_zag(value))


def decode_svarint(buf, pos):
    # type: (bytes, int) -> Tuple[int, int]
    """Decode bytearray into a long."""
    pos_start = pos

    output, pos = decode_uvarint(buf, pos)
    value = decode_zig_zag(output)

    # Validate that this is a cononical encoding by re-encoding the value
    test_encode = encode_svarint(value)
    if buf[pos_start:pos] != test_encode:
        raise DecoderException(
            "Error decoding svarint: Encoding is not standard:\noriginal:  %r\nstandard: %r"
            % (buf[pos_start:pos], test_encode)
        )

    return value, pos
