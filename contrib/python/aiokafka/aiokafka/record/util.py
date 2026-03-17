from collections.abc import Callable, Iterable

from aiokafka.util import NO_EXTENSIONS

from ._crc32c import crc as crc32c_py


def encode_varint_py(value: int, write: Callable[[int], None]) -> int:
    """Encode an integer to a varint presentation. See
    https://developers.google.com/protocol-buffers/docs/encoding?csw=1#varints
    on how those can be produced.

        Arguments:
            value (int): Value to encode
            write (function): Called per byte that needs to be written

        Returns:
            int: Number of bytes written
    """
    value = (value << 1) ^ (value >> 63)

    if value <= 0x7F:  # 1 byte
        write(value)
        return 1
    if value <= 0x3FFF:  # 2 bytes
        write(0x80 | (value & 0x7F))
        write(value >> 7)
        return 2
    if value <= 0x1FFFFF:  # 3 bytes
        write(0x80 | (value & 0x7F))
        write(0x80 | ((value >> 7) & 0x7F))
        write(value >> 14)
        return 3
    if value <= 0xFFFFFFF:  # 4 bytes
        write(0x80 | (value & 0x7F))
        write(0x80 | ((value >> 7) & 0x7F))
        write(0x80 | ((value >> 14) & 0x7F))
        write(value >> 21)
        return 4
    if value <= 0x7FFFFFFFF:  # 5 bytes
        write(0x80 | (value & 0x7F))
        write(0x80 | ((value >> 7) & 0x7F))
        write(0x80 | ((value >> 14) & 0x7F))
        write(0x80 | ((value >> 21) & 0x7F))
        write(value >> 28)
        return 5
    else:
        # Return to general algorithm
        bits = value & 0x7F
        value >>= 7
        i = 0
        while value:
            write(0x80 | bits)
            bits = value & 0x7F
            value >>= 7
            i += 1
    write(bits)
    return i


def size_of_varint_py(value: int) -> int:
    """Number of bytes needed to encode an integer in variable-length format."""
    value = (value << 1) ^ (value >> 63)
    if value <= 0x7F:
        return 1
    if value <= 0x3FFF:
        return 2
    if value <= 0x1FFFFF:
        return 3
    if value <= 0xFFFFFFF:
        return 4
    if value <= 0x7FFFFFFFF:
        return 5
    if value <= 0x3FFFFFFFFFF:
        return 6
    if value <= 0x1FFFFFFFFFFFF:
        return 7
    if value <= 0xFFFFFFFFFFFFFF:
        return 8
    if value <= 0x7FFFFFFFFFFFFFFF:
        return 9
    return 10


def decode_varint_py(buffer: bytearray, pos: int = 0) -> tuple[int, int]:
    """Decode an integer from a varint presentation. See
    https://developers.google.com/protocol-buffers/docs/encoding?csw=1#varints
    on how those can be produced.

        Arguments:
            buffer (bytearry): buffer to read from.
            pos (int): optional position to read from

        Returns:
            (int, int): Decoded int value and next read position
    """
    result = buffer[pos]
    if not (result & 0x81):
        return (result >> 1), pos + 1
    if not (result & 0x80):
        return (result >> 1) ^ (~0), pos + 1

    result &= 0x7F
    pos += 1
    shift = 7
    while True:
        b = buffer[pos]
        result |= (b & 0x7F) << shift
        pos += 1
        if not (b & 0x80):
            return ((result >> 1) ^ -(result & 1), pos)
        shift += 7
        if shift >= 64:
            raise ValueError("Out of int64 range")


def calc_crc32c_py(memview: Iterable[int]) -> int:
    """Calculate CRC-32C (Castagnoli) checksum over a memoryview of data"""
    crc = crc32c_py(memview)
    return crc


calc_crc32c: Callable[[bytes | bytearray], int]
decode_varint: Callable[[bytearray, int], tuple[int, int]]
size_of_varint: Callable[[int], int]
encode_varint: Callable[[int, Callable[[int], None]], int]

if NO_EXTENSIONS:
    calc_crc32c = calc_crc32c_py
    decode_varint = decode_varint_py
    size_of_varint = size_of_varint_py
    encode_varint = encode_varint_py
else:
    try:
        from ._crecords import (
            crc32c_cython,
            decode_varint_cython,
            encode_varint_cython,
            size_of_varint_cython,
        )

        decode_varint = decode_varint_cython
        encode_varint = encode_varint_cython
        size_of_varint = size_of_varint_cython
        calc_crc32c = crc32c_cython
    except ImportError:  # pragma: no cover
        calc_crc32c = calc_crc32c_py
        decode_varint = decode_varint_py
        size_of_varint = size_of_varint_py
        encode_varint = encode_varint_py
