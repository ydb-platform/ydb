# -*- coding: utf-8 -*-
"""Common code.

:copyright: 2014-2024 by PyVISA-sim Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import logging
from typing import Iterator, Optional

from pyvisa import logger

LOGGER = logging.LoggerAdapter(logger, {"backend": "py"})  # type: ignore


class NamedObject(object):
    """A class to construct named sentinels."""

    #: Name used to identify the sentinel
    name: str

    def __init__(self, name) -> None:
        self.name = name

    def __repr__(self) -> str:
        return "<%s>" % self.name

    __str__ = __repr__


def int_to_byte(val):
    return val.to_bytes(1, "big")


# TODO(anyone): This is copypasta from `pyvisa-sim` project - find a way to
#   reduce duplication, probably in that project instead of here.
def _create_bitmask(bits: int) -> int:
    """Create a bitmask for the given number of bits."""
    mask = (1 << bits) - 1
    return mask


# TODO(anyone): This is copypasta from `pyvisa-sim` project - find a way to
#   reduce duplication, probably in that project instead of here.
def iter_bytes(
    data: bytes, data_bits: Optional[int] = None, send_end: Optional[bool] = None
) -> Iterator[bytes]:
    """Clip values to the correct number of bits per byte.
    Serial communication may use from 5 to 8 bits.
    Parameters
    ----------
    data : The data to clip as a byte string.
    data_bits : How many bits per byte should be sent. Clip to this many bits.
        For example: data_bits=5: 0xff (0b1111_1111) --> 0x1f (0b0001_1111).
        Acceptable range is 5 to 8, inclusive. Values above 8 will be clipped to 8.
        This maps to the VISA attribute VI_ATTR_ASRL_DATA_BITS.
    send_end :
        If None (the default), apply the mask that is determined by data_bits.
        If False, apply the mask and set the highest (post-mask) bit to 0 for
        all bytes.
        If True, apply the mask and set the highest (post-mask) bit to 0 for
        all bytes except for the final byte, which has the highest bit set to 1.
    References
    ----------
    + https://www.ivifoundation.org/downloads/Architecture%20Specifications/vpp43_2022-05-19.pdf,
    + https://www.ni.com/docs/en-US/bundle/ni-visa/page/ni-visa/vi_attr_asrl_data_bits.html,
    + https://www.ni.com/docs/en-US/bundle/ni-visa/page/ni-visa/vi_attr_asrl_end_out.html

    """
    if send_end and data_bits is None:
        raise ValueError("'send_end' requires a valid 'data_bits' value.")

    if data_bits is None:
        for d in data:
            yield bytes([d])
    else:
        if data_bits <= 0:
            raise ValueError("'data_bits' cannot be zero or negative")
        if data_bits > 8:
            data_bits = 8

        if send_end is None:
            # only apply the mask
            mask = _create_bitmask(data_bits)
            for d in data:
                yield bytes([d & mask])
        elif bool(send_end) is False:
            # apply the mask and set highest bits to 0
            # This is effectively the same has reducing the mask by 1 bit.
            mask = _create_bitmask(data_bits - 1)
            for d in data:
                yield bytes([d & mask])
        elif bool(send_end) is True:
            # apply the mask and set highest bits to 0
            # This is effectively the same has reducing the mask by 1 bit.
            mask = _create_bitmask(data_bits - 1)
            for d in data[:-1]:
                yield bytes([d & mask])
            # except for the last byte which has it's highest bit set to 1.
            last_byte = data[-1]
            highest_bit = 1 << (data_bits - 1)
            yield bytes([(last_byte & mask) | highest_bit])
        else:
            raise ValueError(f"Unknown 'send_end' value '{send_end}'")
