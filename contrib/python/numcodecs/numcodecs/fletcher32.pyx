# cython: language_level=3
# cython: overflowcheck=False
# cython: cdivision=True


from libc.stdint cimport uint8_t, uint16_t, uint32_t
from libc.string cimport memcpy

from cpython.bytes cimport PyBytes_FromStringAndSize

from ._utils cimport store_le32, load_le32

from numcodecs.abc import Codec
from numcodecs.compat import ensure_contiguous_ndarray


cdef extern from *:
    """
    const Py_ssize_t FOOTER_LENGTH = sizeof(uint32_t);
    """
    const Py_ssize_t FOOTER_LENGTH


cdef uint32_t _fletcher32(const uint8_t[::1] _data):
    # converted from
    # https://github.com/Unidata/netcdf-c/blob/main/plugins/H5checksum.c#L109
    cdef:
        const uint8_t *data = &_data[0]
        size_t _len = _data.shape[0]
        size_t len = _len / 2
        size_t tlen
        uint32_t sum1 = 0, sum2 = 0;


    while len:
        tlen = 360 if len > 360 else len
        len -= tlen
        while True:
            sum1 += <uint32_t>((<uint16_t>data[0]) << 8) | (<uint16_t>data[1])
            data += 2
            sum2 += sum1
            tlen -= 1
            if tlen < 1:
                break
        sum1 = (sum1 & 0xffff) + (sum1 >> 16)
        sum2 = (sum2 & 0xffff) + (sum2 >> 16)

    if _len % 2:
        sum1 += <uint32_t>((<uint16_t>(data[0])) << 8)
        sum2 += sum1
        sum1 = (sum1 & 0xffff) + (sum1 >> 16)
        sum2 = (sum2 & 0xffff) + (sum2 >> 16)

    sum1 = (sum1 & 0xffff) + (sum1 >> 16)
    sum2 = (sum2 & 0xffff) + (sum2 >> 16)

    return (sum2 << 16) | sum1


class Fletcher32(Codec):
    """The fletcher checksum with 16-bit words and 32-bit output

    This is the netCDF4/HED5 implementation, which is not equivalent
    to the one in wikipedia
    https://github.com/Unidata/netcdf-c/blob/main/plugins/H5checksum.c#L95

    With this codec, the checksum is concatenated on the end of the data
    bytes when encoded. At decode time, the checksum is performed on
    the data portion and compared with the four-byte checksum, raising
    RuntimeError if inconsistent.
    """

    codec_id = "fletcher32"

    def encode(self, buf):
        """Return buffer plus a footer with the fletcher checksum (4-bytes)"""
        buf = ensure_contiguous_ndarray(buf).ravel().view('uint8')
        cdef const uint8_t[::1] b_mv = buf
        cdef const uint8_t* b_ptr = &b_mv[0]
        cdef Py_ssize_t b_len = len(b_mv)

        cdef Py_ssize_t out_len = b_len + FOOTER_LENGTH
        cdef bytes out = PyBytes_FromStringAndSize(NULL, out_len)
        cdef uint8_t* out_ptr = <uint8_t*>out

        memcpy(out_ptr, b_ptr, b_len)
        store_le32(out_ptr + b_len, _fletcher32(b_mv))

        return out

    def decode(self, buf, out=None):
        """Check fletcher checksum, and return buffer without it"""
        b = ensure_contiguous_ndarray(buf).view('uint8')
        cdef const uint8_t[::1] b_mv = b
        cdef const uint8_t* b_ptr = &b_mv[0]
        cdef Py_ssize_t b_len = len(b_mv)

        val = _fletcher32(b_mv[:-FOOTER_LENGTH])
        found = load_le32(&b_mv[-FOOTER_LENGTH])
        if val != found:
            raise RuntimeError(
                f"The fletcher32 checksum of the data ({val}) did not"
                f" match the expected checksum ({found}).\n"
                "This could be a sign that the data has been corrupted."
            )

        cdef uint8_t[::1] out_mv
        cdef uint8_t* out_ptr
        if out is not None:
            out_mv = ensure_contiguous_ndarray(out).view("uint8")
            out_ptr = &out_mv[0]
            memcpy(out_ptr, b_ptr, b_len - FOOTER_LENGTH)
        else:
            out = b_mv[:-FOOTER_LENGTH]
        return out
