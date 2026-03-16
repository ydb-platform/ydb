import struct

from .varint import write_varint
from .util import compat


MAX_UINT64 = (1 << 64) - 1
MAX_INT64 = (1 << 63) - 1


if compat.PY3:
    def _byte(b):
        return bytes((b, ))
else:
    _byte = chr


def write_binary_str(text, buf):
    text = text.encode('utf-8')
    write_binary_bytes(text, buf)


def write_binary_bytes(text, buf):
    write_varint(len(text), buf)
    buf.write(text)


def write_binary_int(number, buf, fmt):
    """
    Writes int from buffer with provided format.
    """
    fmt = '<' + fmt
    buf.write(struct.pack(fmt, number))


def write_binary_int8(number, buf):
    write_binary_int(number, buf, 'b')


def write_binary_int16(number, buf):
    write_binary_int(number, buf, 'h')


def write_binary_int32(number, buf):
    write_binary_int(number, buf, 'i')


def write_binary_int64(number, buf):
    write_binary_int(number, buf, 'q')


def write_binary_uint8(number, buf):
    write_binary_int(number, buf, 'B')


def write_binary_uint16(number, buf):
    write_binary_int(number, buf, 'H')


def write_binary_uint32(number, buf):
    write_binary_int(number, buf, 'I')


def write_binary_uint64(number, buf):
    write_binary_int(number, buf, 'Q')


def write_binary_uint128(number, buf):
    fmt = '<QQ'
    packed = struct.pack(fmt, (number >> 64) & MAX_UINT64, number & MAX_UINT64)
    buf.write(packed)
