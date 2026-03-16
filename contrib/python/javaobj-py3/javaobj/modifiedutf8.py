#!/usr/bin/python
# -- Content-Encoding: utf-8 --
"""
Implements the support of the Java-specific kind of UTF-8 encoding.

This module is a modified version of ``py2jdbc.mutf8`` provided by
`@guywithface <https://github.com/guywithface>`_.

The project the original file comes from is available at:
https://github.com/swstephe/py2jdbc/

:authors: Scott Stephens (@swstephe), @guywithface
:license: Apache License 2.0
:version: 0.4.4
:status: Alpha
"""

from __future__ import unicode_literals

import sys


# Module version
__version_info__ = (0, 4, 4)
__version__ = ".".join(str(x) for x in __version_info__)

# Documentation strings format
__docformat__ = "restructuredtext en"

# Encoding name: not cesu-8, which uses a different zero-byte
NAME = "mutf8"

# ------------------------------------------------------------------------------

if sys.version_info[0] >= 3:
    unicode_char = chr  # pylint:disable=C0103

    def byte_to_int(data):
        # type: (bytes) -> int
        """
        Converts the first byte of the given data to an integer
        """
        if isinstance(data, int):
            return data

        if isinstance(data, bytes):
            return data[0]

        raise ValueError(
            "Expected byte or int as input, got: {0}".format(
                type(data).__name__
            )
        )


else:
    unicode_char = (
        unichr  # pylint:disable=C0103,undefined-variable  # noqa: F821
    )

    def byte_to_int(data):
        # type: (bytes) -> int
        """
        Converts the first byte of the given data to an integer
        """
        if isinstance(data, int):
            return data

        if isinstance(data, str):
            return ord(data[0])

        raise ValueError(
            "Expected byte or int as input, got: {0}".format(
                type(data).__name__
            )
        )


# ------------------------------------------------------------------------------


class DecodeMap(object):  # pylint:disable=R0205
    """
    A utility class which manages masking, comparing and mapping in bits.
    If the mask and compare fails, this will raise UnicodeDecodeError so
    encode and decode will correctly handle bad characters.
    """

    def __init__(self, count, mask, value, bits):
        """
        Initialize a DecodeMap, entry from a static dictionary for the module.
        It automatically calculates the mask for the bits for the value
        (always assumed to be at the bottom of the byte).

        :param count: The number of bytes in this entire sequence.
        :param mask: The mask to apply to the byte at this position.
        :param value: The value of masked bits, (without shifting).
        :param bits: The number of bits.
        """
        self.count = count
        self.mask = mask
        self.value = value
        self.bits = bits
        self.mask2 = (1 << bits) - 1

    def apply(self, byte, value, data, i, count):
        """
        Apply mask, compare to expected value, shift and return result.
        Eventually, this could become a ``reduce`` function.

        :param byte: The byte to compare
        :param value: The currently accumulated value.
        :param data: The data buffer, (array of bytes).
        :param i: The position within the data buffer.
        :param count: The position of this comparison.
        :return: A new value with the bits merged in.
        :raises UnicodeDecodeError: if marked bits don't match.
        """
        if byte & self.mask == self.value:
            value <<= self.bits
            value |= byte & self.mask2
        else:
            raise UnicodeDecodeError(
                NAME,
                data,
                i,
                i + count,
                "invalid {}-byte sequence".format(self.count),
            )
        return value

    def __repr__(self):
        return "DecodeMap({})".format(
            ", ".join(
                "{}=0x{:02x}".format(n, getattr(self, n))
                for n in ("count", "mask", "value", "bits", "mask2")
            )
        )


DECODER_MAP = {
    2: ((0xC0, 0x80, 6),),
    3: ((0xC0, 0x80, 6), (0xC0, 0x80, 6)),
    6: (
        (0xF0, 0xA0, 4),
        (0xC0, 0x80, 6),
        (0xFF, 0xED, 0),
        (0xF0, 0xB0, 4),
        (0xC0, 0x80, 6),
    ),
}

DECODE_MAP = dict(
    (k, tuple(DecodeMap(k, *vv) for vv in v)) for k, v in DECODER_MAP.items()
)


def decoder(data):
    """
    This generator processes a sequence of bytes in Modified UTF-8 encoding
    and produces a sequence of unicode string characters.

    It takes bits from the byte until it matches one of the known encoding
    sequences.
    It uses ``DecodeMap`` to mask, compare and generate values.

    :param data: a string of bytes in Modified UTF-8 encoding.
    :return: a generator producing a string of unicode characters
    :raises UnicodeDecodeError: unrecognised byte in sequence encountered.
    """

    def next_byte(_it, start, count):
        try:
            return next(_it)[1]
        except StopIteration:
            raise UnicodeDecodeError(
                NAME, data, start, start + count, "incomplete byte sequence"
            )

    it = iter(enumerate(data))
    for i, d in it:
        if d == 0x00:  # 00000000
            raise UnicodeDecodeError(
                NAME, data, i, i + 1, "embedded zero-byte not allowed"
            )

        if d & 0x80:  # 1xxxxxxx
            if d & 0x40:  # 11xxxxxx
                if d & 0x20:  # 111xxxxx
                    if d & 0x10:  # 1111xxxx
                        raise UnicodeDecodeError(
                            NAME, data, i, i + 1, "invalid encoding character"
                        )

                    if d == 0xED:
                        value = 0
                        for i1, dm in enumerate(DECODE_MAP[6]):
                            d1 = next_byte(it, i, i1 + 1)
                            value = dm.apply(d1, value, data, i, i1 + 1)
                    else:  # 1110xxxx
                        value = d & 0x0F
                        for i1, dm in enumerate(DECODE_MAP[3]):
                            d1 = next_byte(it, i, i1 + 1)
                            value = dm.apply(d1, value, data, i, i1 + 1)
                else:  # 110xxxxx
                    value = d & 0x1F
                    for i1, dm in enumerate(DECODE_MAP[2]):
                        d1 = next_byte(it, i, i1 + 1)
                        value = dm.apply(d1, value, data, i, i1 + 1)
            else:  # 10xxxxxx
                raise UnicodeDecodeError(
                    NAME, data, i, i + 1, "misplaced continuation character"
                )
        else:  # 0xxxxxxx
            value = d
        # noinspection PyCompatibility
        yield mutf8_unichr(value)


def decode_modified_utf8(data, errors="strict"):
    """
    Decodes a sequence of bytes to a unicode text and length using
    Modified UTF-8.
    This function is designed to be used with Python ``codecs`` module.

    :param data: a string of bytes in Modified UTF-8
    :param errors: handle decoding errors
    :return: unicode text and length
    :raises UnicodeDecodeError: sequence is invalid.
    """
    value, length = "", 0
    it = iter(decoder(byte_to_int(d) for d in data))
    while True:
        try:
            value += next(it)
            length += 1
        except StopIteration:
            break
        except UnicodeDecodeError as e:
            if errors == "strict":
                raise e

            if errors == "ignore":
                pass
            elif errors == "replace":
                value += "\uFFFD"
                length += 1
    return value, length


def mutf8_unichr(value):
    """
    Mimics Python 2 unichr() and Python 3 chr()
    """
    return unicode_char(value)
