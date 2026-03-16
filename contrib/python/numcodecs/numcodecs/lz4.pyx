# cython: embedsignature=True
# cython: profile=False
# cython: linetrace=False
# cython: binding=False
# cython: language_level=3


from libc.stdint cimport uint8_t, uint32_t

from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_FromStringAndSize
from cpython.memoryview cimport PyMemoryView_GET_BUFFER

from .compat_ext cimport PyBytes_RESIZE, ensure_continguous_memoryview
from ._utils cimport store_le32, load_le32

from .compat import ensure_contiguous_ndarray
from .abc import Codec


cdef extern from "lz4.h":

    const char* LZ4_versionString() nogil

    int LZ4_compress_fast(const char* source,
                          char* dest,
                          int sourceSize,
                          int maxDestSize,
                          int acceleration) nogil

    int LZ4_decompress_safe(const char* source,
                            char* dest,
                            int compressedSize,
                            int maxDecompressedSize) nogil

    int LZ4_compressBound(int inputSize) nogil


VERSION_STRING = LZ4_versionString()
VERSION_STRING = str(VERSION_STRING, 'ascii')
__version__ = VERSION_STRING
DEFAULT_ACCELERATION = 1


def compress(source, int acceleration=DEFAULT_ACCELERATION):
    """Compress data.

    Parameters
    ----------
    source : bytes-like
        Data to be compressed. Can be any object supporting the buffer
        protocol.
    acceleration : int
        Acceleration level. The larger the acceleration value, the faster the algorithm, but also
        the lesser the compression.

    Returns
    -------
    dest : bytes
        Compressed data.

    Notes
    -----
    The compressed output includes a 4-byte header storing the original size of the decompressed
    data as a little-endian 32-bit integer.

    """

    cdef:
        memoryview source_mv
        const Py_buffer* source_pb
        const char* source_ptr
        bytes dest
        char* dest_ptr
        char* dest_start
        int source_size, dest_size, compressed_size

    # check level
    if acceleration <= 0:
        acceleration = DEFAULT_ACCELERATION

    # setup source buffer
    source_mv = ensure_continguous_memoryview(source)
    source_pb = PyMemoryView_GET_BUFFER(source_mv)

    # extract metadata
    source_ptr = <const char*>source_pb.buf
    source_size = source_pb.len

    try:

        # setup destination
        dest_size = LZ4_compressBound(source_size)
        dest = PyBytes_FromStringAndSize(NULL, dest_size + sizeof(uint32_t))
        dest_ptr = PyBytes_AS_STRING(dest)
        store_le32(<uint8_t*>dest_ptr, source_size)
        dest_start = dest_ptr + sizeof(uint32_t)

        # perform compression
        with nogil:
            compressed_size = LZ4_compress_fast(source_ptr, dest_start, source_size, dest_size,
                                                acceleration)

    finally:
        pass

    # check compression was successful
    if compressed_size <= 0:
        raise RuntimeError('LZ4 compression error: %s' % compressed_size)

    # resize after compression
    compressed_size += sizeof(uint32_t)
    PyBytes_RESIZE(dest, compressed_size)

    return dest


def decompress(source, dest=None):
    """Decompress data.

    Parameters
    ----------
    source : bytes-like
        Compressed data. Can be any object supporting the buffer protocol.
    dest : array-like, optional
        Object to decompress into.

    Returns
    -------
    dest : bytes
        Object containing decompressed data.

    """
    cdef:
        memoryview source_mv
        const Py_buffer* source_pb
        const char* source_ptr
        const char* source_start
        memoryview dest_mv
        Py_buffer* dest_pb
        char* dest_ptr
        int source_size, dest_size, decompressed_size

    # setup source buffer
    source_mv = ensure_continguous_memoryview(source)
    source_pb = PyMemoryView_GET_BUFFER(source_mv)

    # extract source metadata
    source_ptr = <const char*>source_pb.buf
    source_size = source_pb.len

    try:

        # determine uncompressed size
        if source_size < sizeof(uint32_t):
            raise ValueError('bad input data')
        dest_size = load_le32(<uint8_t*>source_ptr)
        if dest_size <= 0:
            raise RuntimeError('LZ4 decompression error: invalid input data')
        source_start = source_ptr + sizeof(uint32_t)
        source_size -= sizeof(uint32_t)

        # setup destination buffer
        if dest is None:
            # allocate memory
            dest_1d = dest = PyBytes_FromStringAndSize(NULL, dest_size)
        else:
            dest_1d = ensure_contiguous_ndarray(dest)

        # obtain dest memoryview
        dest_mv = memoryview(dest_1d)
        dest_pb = PyMemoryView_GET_BUFFER(dest_mv)
        dest_ptr = <char*>dest_pb.buf
        dest_nbytes = dest_pb.len

        if dest_nbytes < dest_size:
            raise ValueError('destination buffer too small; expected at least %s, '
                             'got %s' % (dest_size, dest_nbytes))

        # perform decompression
        with nogil:
            decompressed_size = LZ4_decompress_safe(source_start, dest_ptr, source_size, dest_size)

    finally:
        pass

    # check decompression was successful
    if decompressed_size <= 0:
        raise RuntimeError('LZ4 decompression error: %s' % decompressed_size)
    elif decompressed_size != dest_size:
        raise RuntimeError('LZ4 decompression error: expected to decompress %s, got %s' %
                           (dest_size, decompressed_size))

    return dest



class LZ4(Codec):
    """Codec providing compression using LZ4.

    Parameters
    ----------
    acceleration : int
        Acceleration level. The larger the acceleration value, the faster the algorithm, but also
        the lesser the compression.

    See Also
    --------
    numcodecs.zstd.Zstd, numcodecs.blosc.Blosc

    """

    codec_id = 'lz4'
    max_buffer_size = 0x7E000000

    def __init__(self, acceleration=DEFAULT_ACCELERATION):
        self.acceleration = acceleration

    def encode(self, buf):
        buf = ensure_contiguous_ndarray(buf, self.max_buffer_size)
        return compress(buf, self.acceleration)

    def decode(self, buf, out=None):
        buf = ensure_contiguous_ndarray(buf, self.max_buffer_size)
        return decompress(buf, out)

    def __repr__(self):
        r = '%s(acceleration=%r)' % \
            (type(self).__name__,
             self.acceleration)
        return r
