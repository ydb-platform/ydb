# cython: embedsignature=True
# cython: profile=False
# cython: linetrace=False
# cython: binding=False
# cython: language_level=3


cimport cython

from libc.stdint cimport uint8_t, uint32_t
from libc.string cimport memcpy

from cpython.bytearray cimport PyByteArray_FromStringAndSize
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.memoryview cimport PyMemoryView_GET_BUFFER
from cpython.unicode cimport PyUnicode_FromStringAndSize

from numpy cimport ndarray

from .compat_ext cimport ensure_continguous_memoryview
from ._utils cimport store_le32, load_le32

import numpy as np

from .abc import Codec
from .compat import ensure_contiguous_ndarray


# Define header size used to store number of items that follow.
cdef extern from *:
    """
    const Py_ssize_t HEADER_LENGTH = sizeof(uint32_t);
    """
    const Py_ssize_t HEADER_LENGTH


def check_out_param(out, n_items):
    if not isinstance(out, np.ndarray):
        raise TypeError('out must be 1-dimensional array')
    if out.dtype != object:
        raise ValueError('out must be object array')
    out = out.reshape(-1, order='A')
    if out.shape[0] < n_items:
        raise ValueError('out is too small')
    return out


class VLenUTF8(Codec):
    """Encode variable-length unicode string objects via UTF-8.

    Examples
    --------
    >>> import numcodecs
    >>> import numpy as np
    >>> x = np.array(['foo', 'bar', 'baz'], dtype='object')
    >>> codec = numcodecs.VLenUTF8()
    >>> codec.decode(codec.encode(x))
    array(['foo', 'bar', 'baz'], dtype=object)

    See Also
    --------
    numcodecs.pickles.Pickle, numcodecs.json.JSON, numcodecs.msgpacks.MsgPack

    Notes
    -----
    The encoded bytes values for each string are packed into a parquet-style byte array.

    """

    codec_id = 'vlen-utf8'

    def __init__(self):
        pass

    @cython.wraparound(False)
    @cython.boundscheck(False)
    def encode(self, buf):
        cdef:
            Py_ssize_t i, L, n_items, data_length
            ndarray[object, ndim=1] input_values
            object[:] encoded_values
            int[:] encoded_lengths
            bytes b
            bytearray out
            char* data
            object o
            unicode u

        # normalise input
        input_values = np.asarray(buf, dtype=object).reshape(-1, order='A')

        # determine number of items
        n_items = input_values.shape[0]

        # setup intermediates
        encoded_values = np.empty(n_items, dtype=object)
        encoded_lengths = np.empty(n_items, dtype=np.intc)

        # first iteration to convert to bytes
        data_length = HEADER_LENGTH
        for i in range(n_items):
            o = input_values[i]
            # replace missing value and coerce to typed data
            u = "" if o is None or o == 0 else o
            b = u.encode("utf-8")
            L = len(b)
            encoded_values[i] = b
            data_length += L + HEADER_LENGTH
            encoded_lengths[i] = L

        # setup output
        out = PyByteArray_FromStringAndSize(NULL, data_length)

        # write header
        data = out
        store_le32(<uint8_t*>data, n_items)

        # second iteration, store data
        data += HEADER_LENGTH
        for i in range(n_items):
            L = encoded_lengths[i]
            store_le32(<uint8_t*>data, L)
            data += HEADER_LENGTH
            b = encoded_values[i]
            memcpy(data, <const char*>b, L)
            data += L

        return out

    @cython.wraparound(False)
    @cython.boundscheck(False)
    def decode(self, buf, out=None):
        cdef:
            memoryview buf_mv
            const Py_buffer* buf_pb
            const char* data
            const char* data_end
            Py_ssize_t i, L, n_items, data_length

        # obtain memoryview
        buf = ensure_contiguous_ndarray(buf)
        buf_mv = ensure_continguous_memoryview(buf)
        buf_pb = PyMemoryView_GET_BUFFER(buf_mv)

        # sanity checks
        if buf_pb.len < HEADER_LENGTH:
            raise ValueError('corrupt buffer, missing or truncated header')

        # obtain input data pointer
        data = <const char*>buf_pb.buf
        data_end = data + buf_pb.len

        # load number of items
        n_items = load_le32(<uint8_t*>data)

        # setup output
        if out is not None:
            out = check_out_param(out, n_items)
        else:
            out = np.empty(n_items, dtype=object)

        # iterate and decode - N.B., do not try to cast `out` as object[:]
        # as this causes segfaults, possibly similar to
        # https://github.com/cython/cython/issues/1608
        data += HEADER_LENGTH
        for i in range(n_items):
            if data + HEADER_LENGTH > data_end:
                raise ValueError('corrupt buffer, data seem truncated')
            L = load_le32(<uint8_t*>data)
            data += HEADER_LENGTH
            if data + L > data_end:
                raise ValueError('corrupt buffer, data seem truncated')
            out[i] = PyUnicode_FromStringAndSize(data, L)
            data += L

        return out


class VLenBytes(Codec):
    """Encode variable-length byte string objects.

    Examples
    --------
    >>> import numcodecs
    >>> import numpy as np
    >>> x = np.array([b'foo', b'bar', b'baz'], dtype='object')
    >>> codec = numcodecs.VLenBytes()
    >>> codec.decode(codec.encode(x))
    array([b'foo', b'bar', b'baz'], dtype=object)

    See Also
    --------
    numcodecs.pickles.Pickle

    Notes
    -----
    The bytes values for each string are packed into a parquet-style byte array.

    """

    codec_id = 'vlen-bytes'

    def __init__(self):
        pass

    @cython.wraparound(False)
    @cython.boundscheck(False)
    def encode(self, buf):
        cdef:
            Py_ssize_t i, L, n_items, data_length
            object[:] values
            object[:] normed_values
            int[:] lengths
            object o
            bytes b
            bytearray out
            char* data

        # normalise input
        values = np.asarray(buf, dtype=object).reshape(-1, order='A')

        # determine number of items
        n_items = values.shape[0]

        # setup intermediates
        normed_values = np.empty(n_items, dtype=object)
        lengths = np.empty(n_items, dtype=np.intc)

        # first iteration to find lengths
        data_length = HEADER_LENGTH
        for i in range(n_items):
            o = values[i]
            # replace missing value and coerce to typed data
            b = b"" if o is None or o == 0 else o
            normed_values[i] = b
            L = len(b)
            data_length += HEADER_LENGTH + L
            lengths[i] = L

        # setup output
        out = PyByteArray_FromStringAndSize(NULL, data_length)

        # write header
        data = out
        store_le32(<uint8_t*>data, n_items)

        # second iteration, store data
        data += HEADER_LENGTH
        for i in range(n_items):
            L = lengths[i]
            store_le32(<uint8_t*>data, L)
            data += HEADER_LENGTH
            b = normed_values[i]
            memcpy(data, <const char*>b, L)
            data += L

        return out

    @cython.wraparound(False)
    @cython.boundscheck(False)
    def decode(self, buf, out=None):
        cdef:
            memoryview buf_mv
            const Py_buffer* buf_pb
            const char* data
            const char* data_end
            Py_ssize_t i, L, n_items, data_length

        # obtain memoryview
        buf = ensure_contiguous_ndarray(buf)
        buf_mv = ensure_continguous_memoryview(buf)
        buf_pb = PyMemoryView_GET_BUFFER(buf_mv)

        # sanity checks
        if buf_pb.len < HEADER_LENGTH:
            raise ValueError('corrupt buffer, missing or truncated header')

        # obtain input data pointer
        data = <const char*>buf_pb.buf
        data_end = data + buf_pb.len

        # load number of items
        n_items = load_le32(<uint8_t*>data)

        # setup output
        if out is not None:
            out = check_out_param(out, n_items)
        else:
            out = np.empty(n_items, dtype=object)

        # iterate and decode - N.B., do not try to cast `out` as object[:]
        # as this causes segfaults, possibly similar to
        # https://github.com/cython/cython/issues/1608
        data += HEADER_LENGTH
        for i in range(n_items):
            if data + HEADER_LENGTH > data_end:
                raise ValueError('corrupt buffer, data seem truncated')
            L = load_le32(<uint8_t*>data)
            data += HEADER_LENGTH
            if data + L > data_end:
                raise ValueError('corrupt buffer, data seem truncated')
            out[i] = PyBytes_FromStringAndSize(data, L)
            data += L

        return out


class VLenArray(Codec):
    """Encode variable-length 1-dimensional arrays via UTF-8.

    Examples
    --------
    >>> import numcodecs
    >>> import numpy as np
    >>> x1 = np.array([1, 3, 5], dtype=np.int16)
    >>> x2 = np.array([4], dtype=np.int16)
    >>> x3 = np.array([7, 9], dtype=np.int16)
    >>> x = np.array([x1, x2, x3], dtype='object')
    >>> codec = numcodecs.VLenArray('<i2')
    >>> codec.decode(codec.encode(x))
    array([array([1, 3, 5], dtype=int16), array([4], dtype=int16),
           array([7, 9], dtype=int16)], dtype=object)

    See Also
    --------
    numcodecs.pickles.Pickle, numcodecs.json.JSON, numcodecs.msgpacks.MsgPack

    Notes
    -----
    The binary data for each array are packed into a parquet-style byte array.

    """

    codec_id = 'vlen-array'

    def __init__(self, dtype):
        self.dtype = np.dtype(dtype)

    def get_config(self):
        config = dict()
        config['id'] = self.codec_id
        config['dtype'] = self.dtype.str
        return config

    def __repr__(self):
        return '%s(dtype=%r)' % (type(self).__name__, self.dtype.str,)

    @cython.wraparound(False)
    @cython.boundscheck(False)
    def encode(self, buf):
        cdef:
            Py_ssize_t i, L, n_items, data_length
            object[:] values
            object[:] normed_values
            int[:] lengths
            bytes b
            bytearray out
            char* data
            memoryview value_mv
            const Py_buffer* value_pb
            object o

        # normalise input
        values = np.asarray(buf, dtype=object).reshape(-1, order='A')

        # determine number of items
        n_items = values.shape[0]

        # setup intermediates
        normed_values = np.empty(n_items, dtype=object)
        lengths = np.empty(n_items, dtype=np.intc)

        # first iteration to convert to bytes
        data_length = HEADER_LENGTH
        for i in range(n_items):
            o = values[i]
            # replace missing value and coerce to typed data
            value_mv = ensure_continguous_memoryview(
                np.array([], dtype=self.dtype) if o is None
                else np.ascontiguousarray(o, self.dtype)
            )
            value_pb = PyMemoryView_GET_BUFFER(value_mv)
            if value_pb.ndim != 1:
                raise ValueError("only 1-dimensional arrays are supported")
            L = value_pb.len
            normed_values[i] = value_mv
            data_length += HEADER_LENGTH + L
            lengths[i] = L

        # setup output
        out = PyByteArray_FromStringAndSize(NULL, data_length)

        # write header
        data = out
        store_le32(<uint8_t*>data, n_items)

        # second iteration, store data
        data += HEADER_LENGTH
        for i in range(n_items):
            L = lengths[i]
            store_le32(<uint8_t*>data, L)
            data += HEADER_LENGTH

            value_mv = normed_values[i]
            value_pb = PyMemoryView_GET_BUFFER(value_mv)

            memcpy(data, value_pb.buf, L)
            data += L

        return out

    @cython.wraparound(False)
    @cython.boundscheck(False)
    def decode(self, buf, out=None):
        cdef:
            memoryview buf_mv
            const Py_buffer* buf_pb
            const char* data
            const char* data_end
            object v
            memoryview v_mv
            Py_buffer* v_pb
            Py_ssize_t i, L, n_items, data_length

        # obtain memoryview
        buf = ensure_contiguous_ndarray(buf)
        buf_mv = ensure_continguous_memoryview(buf)
        buf_pb = PyMemoryView_GET_BUFFER(buf_mv)

        # sanity checks
        if buf_pb.len < HEADER_LENGTH:
            raise ValueError('corrupt buffer, missing or truncated header')

        # obtain input data pointer
        data = <const char*>buf_pb.buf
        data_end = data + buf_pb.len

        # load number of items
        n_items = load_le32(<uint8_t*>data)

        # setup output
        if out is not None:
            out = check_out_param(out, n_items)
        else:
            out = np.empty(n_items, dtype=object)

        # iterate and decode - N.B., do not try to cast `out` as object[:]
        # as this causes segfaults, possibly similar to
        # https://github.com/cython/cython/issues/1608
        data += HEADER_LENGTH
        for i in range(n_items):
            if data + HEADER_LENGTH > data_end:
                raise ValueError('corrupt buffer, data seem truncated')
            L = load_le32(<uint8_t*>data)
            data += HEADER_LENGTH
            if data + L > data_end:
                raise ValueError('corrupt buffer, data seem truncated')

            # Create & fill array value
            v = np.empty((L,), dtype="uint8").view(self.dtype)
            v_mv = memoryview(v)
            v_pb = PyMemoryView_GET_BUFFER(v_mv)
            memcpy(v_pb.buf, data, L)

            out[i] = v
            data += L

        return out
