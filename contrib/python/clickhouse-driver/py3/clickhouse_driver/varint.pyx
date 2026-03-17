from cpython cimport PyBytes_FromStringAndSize


def make_varint(unsigned long long number):
    """
    Writes integer of variable length using LEB128.
    """
    cdef unsigned char to_write, i = 0
    # unsigned PY_LONG_LONG checks integer on function call and
    # raises OverflowError if integer overflows unsigned PY_LONG_LONG.
    # Long enough for handling unsigned PY_LONG_LONG.
    cdef unsigned char num_buf[32]

    while True:
        to_write = number & 0x7f
        number >>= 7
        if number:
            num_buf[i] = to_write | 0x80
            i += 1
        else:
            num_buf[i] = to_write
            i += 1
            break

    return PyBytes_FromStringAndSize(<char *>num_buf, i)


def write_varint(unsigned long long number, buf):
    """
    Writes integer of variable length using LEB128.
    """
    cdef unsigned char to_write, i = 0
    # unsigned PY_LONG_LONG checks integer on function call and
    # raises OverflowError if integer overflows unsigned PY_LONG_LONG.
    # Long enough for handling unsigned PY_LONG_LONG.
    cdef unsigned char num_buf[32]

    while True:
        to_write = number & 0x7f
        number >>= 7
        if number:
            num_buf[i] = to_write | 0x80
            i += 1
        else:
            num_buf[i] = to_write
            i += 1
            break

    buf.write(PyBytes_FromStringAndSize(<char *>num_buf, i))


def read_varint(f):
    """
    Reads integer of variable length using LEB128.
    """
    cdef unsigned char shift = 0
    cdef unsigned long long i, result = 0

    read_one = f.read_one

    while True:
        i = read_one()
        result |= (i & 0x7f) << shift
        shift += 7
        if i < 0x80:
            break

    return result
