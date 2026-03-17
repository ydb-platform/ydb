#cython: language_level=3
from aiokafka.errors import CorruptRecordException

# VarInt implementation

cdef inline int decode_varint64(
        char* buf, Py_ssize_t* read_pos, int64_t* out_value) except -1:
    cdef:
        int shift = 0
        char byte
        Py_ssize_t pos = read_pos[0]
        uint64_t value = 0

    while True:
        byte = buf[pos]
        pos += 1
        if byte & 0x80 != 0:
            value |= <uint64_t>(byte & 0x7f) << shift
            shift += 7
        else:
            value |= <uint64_t>byte << shift
            break
        if shift > 63:
            raise CorruptRecordException("Out of double range")
    # Normalize sign
    out_value[0] = <int64_t>(value >> 1) ^ -<int64_t>(value & 1)
    read_pos[0] = pos
    return 0


cdef inline Py_ssize_t size_of_varint64(int64_t value) except -1:
    cdef:
        Py_ssize_t bytes = 1
        uint64_t v

    v = <uint64_t> (value << 1) ^ <uint64_t> (value >> 63)
    while ((v & <uint64_t> 0xffffffffffffff80) != 0):
        bytes += 1
        v = v >> 7
    return bytes


cdef inline Py_ssize_t size_of_varint(Py_ssize_t value) except -1:
    return size_of_varint64(<int64_t> value)


cdef inline int encode_varint64(
        char* buf, Py_ssize_t* write_pos, int64_t value) except -1:
    cdef:
        uint64_t v
        Py_ssize_t pos

    pos = write_pos[0]
    v = <uint64_t> (value << 1) ^ <uint64_t> (value >> 63)
    while ((v & <uint64_t> 0xffffffffffffff80) != 0):
        buf[pos] = <char> ((v & 0x7f) | 0x80)
        pos += 1
        v = v >> 7
    buf[pos] = <char> (v & 0x7f)
    write_pos[0] = pos + 1
    return 0


cdef inline int encode_varint(
        char* buf, Py_ssize_t* write_pos, Py_ssize_t value) except -1:
    encode_varint64(buf, write_pos, <int64_t> value)
    return 0


def decode_varint_cython(buffer, pos=0):
    """ Decode an integer from a varint presentation. See
    https://developers.google.com/protocol-buffers/docs/encoding?csw=1#varints
    on how those can be produced.

        Arguments:
            buffer (bytearry): buffer to read from.
            pos (int): optional position to read from

        Returns:
            (int, int): Decoded int value and next read position
    """
    cdef:
        Py_buffer buf
        Py_ssize_t read_pos
        int64_t out_value = 0

    read_pos = pos

    PyObject_GetBuffer(buffer, &buf, PyBUF_SIMPLE)
    try:
        decode_varint64(<char*>buf.buf, &read_pos, &out_value)
    except CorruptRecordException:
        raise ValueError("Out of double range")
    finally:
        PyBuffer_Release(&buf)
    return out_value, read_pos


def encode_varint_cython(int64_t value, write):
    """ Encode an integer to a varint presentation. See
    https://developers.google.com/protocol-buffers/docs/encoding?csw=1#varints
    on how those can be produced.

        Arguments:
            value (int): Value to encode
            write (function): Called per byte that needs to be written

        Returns:
            int: Number of bytes written
    """
    cdef:
        Py_ssize_t pos = 0
        bytes x
        char* buf

    x = bytes(10)  # Max for 64 bits is 10 bytes encoded
    buf = PyBytes_AS_STRING(x)

    encode_varint64(buf, &pos, value)

    for i in range(pos):
        write(x[i])


def size_of_varint_cython(int64_t value):
    """ Number of bytes needed to encode an integer in variable-length format.
    """
    return size_of_varint64(value)

# END: VarInt implementation


# CRC32C C implementation

# Init on import
def _init_crc32c():
    crc32c_global_init()

_init_crc32c()


def crc32c_cython(data):
    cdef:
        Py_buffer buf
        uint32_t crc

    PyObject_GetBuffer(data, &buf, PyBUF_SIMPLE)

    calc_crc32c(
        0,
        <char*> buf.buf,
        <size_t> buf.len,
        &crc
    )

    PyBuffer_Release(&buf)
    return crc

# END: CRC32C C implementation
