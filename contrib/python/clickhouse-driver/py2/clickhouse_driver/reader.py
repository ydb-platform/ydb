from struct import Struct

from .varint import read_varint


def read_binary_str(buf):
    length = read_varint(buf)
    return read_binary_str_fixed_len(buf, length)


def read_binary_bytes(buf):
    length = read_varint(buf)
    return read_binary_bytes_fixed_len(buf, length)


def read_binary_str_fixed_len(buf, length):
    return read_binary_bytes_fixed_len(buf, length).decode('utf-8')


def read_binary_bytes_fixed_len(buf, length):
    return buf.read(length)


def read_binary_int(buf, fmt):
    """
    Reads int from buffer with provided format.
    """
    # Little endian.
    s = Struct('<' + fmt)
    return s.unpack(buf.read(s.size))[0]


def read_binary_int8(buf):
    return read_binary_int(buf, 'b')


def read_binary_int16(buf):
    return read_binary_int(buf, 'h')


def read_binary_int32(buf):
    return read_binary_int(buf, 'i')


def read_binary_int64(buf):
    return read_binary_int(buf, 'q')


def read_binary_uint8(buf):
    return read_binary_int(buf, 'B')


def read_binary_uint16(buf):
    return read_binary_int(buf, 'H')


def read_binary_uint32(buf):
    return read_binary_int(buf, 'I')


def read_binary_uint64(buf):
    return read_binary_int(buf, 'Q')


def read_binary_uint128(buf):
    hi = read_binary_int(buf, 'Q')
    lo = read_binary_int(buf, 'Q')

    return (hi << 64) + lo
