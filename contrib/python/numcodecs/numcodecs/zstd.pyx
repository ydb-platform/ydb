# cython: embedsignature=True
# cython: profile=False
# cython: linetrace=False
# cython: binding=False
# cython: language_level=3


from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_FromStringAndSize
from cpython.memoryview cimport PyMemoryView_GET_BUFFER

from .compat_ext cimport PyBytes_RESIZE, ensure_continguous_memoryview

from .compat import ensure_contiguous_ndarray
from .abc import Codec

from libc.stdlib cimport malloc, realloc, free

cdef extern from "stdint.h":
    cdef size_t SIZE_MAX

cdef extern from "zstd.h":

    unsigned ZSTD_versionNumber() nogil

    struct ZSTD_CCtx_s:
        pass
    ctypedef ZSTD_CCtx_s ZSTD_CCtx

    struct ZSTD_DStream_s:
        pass
    ctypedef ZSTD_DStream_s ZSTD_DStream

    struct ZSTD_inBuffer_s:
        const void* src
        size_t size
        size_t pos
    ctypedef ZSTD_inBuffer_s ZSTD_inBuffer

    struct ZSTD_outBuffer_s:
        void* dst
        size_t size
        size_t pos
    ctypedef ZSTD_outBuffer_s ZSTD_outBuffer

    cdef enum ZSTD_cParameter:
        ZSTD_c_compressionLevel=100
        ZSTD_c_checksumFlag=201

    ZSTD_CCtx* ZSTD_createCCtx() nogil
    size_t ZSTD_freeCCtx(ZSTD_CCtx* cctx) nogil
    size_t ZSTD_CCtx_setParameter(ZSTD_CCtx* cctx,
                                  ZSTD_cParameter param,
                                  int value) nogil

    size_t ZSTD_compress2(ZSTD_CCtx* cctx,
                          void* dst,
                          size_t dstCapacity,
                          const void* src,
                          size_t srcSize) nogil
    size_t ZSTD_decompress(void* dst,
                           size_t dstCapacity,
                           const void* src,
                           size_t compressedSize) nogil

    size_t ZSTD_decompressStream(ZSTD_DStream* zds,
                                 ZSTD_outBuffer* output,
                                 ZSTD_inBuffer* input) nogil

    size_t ZSTD_DStreamOutSize() nogil
    ZSTD_DStream* ZSTD_createDStream() nogil
    size_t ZSTD_freeDStream(ZSTD_DStream* zds) nogil
    size_t ZSTD_initDStream(ZSTD_DStream* zds) nogil

    cdef unsigned long long ZSTD_CONTENTSIZE_UNKNOWN
    cdef unsigned long long ZSTD_CONTENTSIZE_ERROR

    unsigned long long ZSTD_getFrameContentSize(const void* src,
                                                size_t srcSize) nogil
    size_t ZSTD_findFrameCompressedSize(const void* src, size_t srcSize) nogil

    int ZSTD_minCLevel() nogil
    int ZSTD_maxCLevel() nogil
    int ZSTD_defaultCLevel() nogil

    size_t ZSTD_compressBound(size_t srcSize) nogil

    unsigned ZSTD_isError(size_t code) nogil

    const char* ZSTD_getErrorName(size_t code) nogil


VERSION_NUMBER = ZSTD_versionNumber()
MAJOR_VERSION_NUMBER = VERSION_NUMBER // (100 * 100)
MINOR_VERSION_NUMBER = (VERSION_NUMBER - (MAJOR_VERSION_NUMBER * 100 * 100)) // 100
MICRO_VERSION_NUMBER = (
    VERSION_NUMBER -
    (MAJOR_VERSION_NUMBER * 100 * 100) -
    (MINOR_VERSION_NUMBER * 100)
)
__version__ = '%s.%s.%s' % (MAJOR_VERSION_NUMBER, MINOR_VERSION_NUMBER, MICRO_VERSION_NUMBER)
DEFAULT_CLEVEL = 0
MAX_CLEVEL = ZSTD_maxCLevel()


def compress(source, int level=DEFAULT_CLEVEL, bint checksum=False):
    """Compress data.

    Parameters
    ----------
    source : bytes-like
        Data to be compressed. Can be any object supporting the buffer
        protocol.
    level : int
        Compression level (-131072 to 22).
    checksum : bool
        Flag to enable checksums. The default is False.

    Returns
    -------
    dest : bytes
        Compressed data.
    """

    cdef:
        memoryview source_mv
        const Py_buffer* source_pb
        const char* source_ptr
        size_t source_size, dest_size, compressed_size
        bytes dest
        char* dest_ptr

    # check level
    if level > MAX_CLEVEL:
        level = MAX_CLEVEL

    # obtain source memoryview
    source_mv = ensure_continguous_memoryview(source)
    source_pb = PyMemoryView_GET_BUFFER(source_mv)

    # setup source buffer
    source_ptr = <const char*>source_pb.buf
    source_size = source_pb.len

    cctx = ZSTD_createCCtx()
    param_set_result = ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, level)

    if ZSTD_isError(param_set_result):
        error = ZSTD_getErrorName(param_set_result)
        raise RuntimeError('Could not set zstd compression level: %s' % error)

    param_set_result = ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 1 if checksum else 0)

    if ZSTD_isError(param_set_result):
        error = ZSTD_getErrorName(param_set_result)
        raise RuntimeError('Could not set zstd checksum flag: %s' % error)

    try:

        # setup destination
        dest_size = ZSTD_compressBound(source_size)
        dest = PyBytes_FromStringAndSize(NULL, dest_size)
        dest_ptr = PyBytes_AS_STRING(dest)

        # perform compression
        with nogil:
            compressed_size = ZSTD_compress2(cctx, dest_ptr, dest_size, source_ptr, source_size)

    finally:
        if cctx:
            ZSTD_freeCCtx(cctx)

    # check compression was successful
    if ZSTD_isError(compressed_size):
        error = ZSTD_getErrorName(compressed_size)
        raise RuntimeError('Zstd compression error: %s' % error)

    # resize after compression
    PyBytes_RESIZE(dest, compressed_size)

    return dest


def decompress(source, dest=None):
    """Decompress data.

    Parameters
    ----------
    source : bytes-like
        Compressed data. Can be any object supporting the buffer protocol.
    dest : array-like, optional
        Object to decompress into. If the content size is unknown, the
        length of dest must match the decompressed size. If the content size
        is unknown and dest is not provided, streaming decompression will be
        used.

    Returns
    -------
    dest : bytes
        Object containing decompressed data.

    """
    cdef:
        memoryview source_mv
        const Py_buffer* source_pb
        const char* source_ptr
        memoryview dest_mv
        Py_buffer* dest_pb
        char* dest_ptr
        size_t source_size, dest_size, decompressed_size
        size_t dest_nbytes
        unsigned long long content_size

    # obtain source memoryview
    source_mv = ensure_continguous_memoryview(source)
    source_pb = PyMemoryView_GET_BUFFER(source_mv)

    # get source pointer
    source_ptr = <const char*>source_pb.buf
    source_size = source_pb.len

    try:
        # determine uncompressed size using unsigned long long for full range
        try:
            content_size = findTotalContentSize(source_ptr, source_size)
        except RuntimeError:
            raise RuntimeError('Zstd decompression error: invalid input data')

        if content_size == ZSTD_CONTENTSIZE_UNKNOWN and dest is None:
            return stream_decompress(source_pb)
        elif content_size == ZSTD_CONTENTSIZE_UNKNOWN:
            # dest is not None
            # set dest_size based on dest
            pass
        elif content_size == ZSTD_CONTENTSIZE_ERROR or content_size == 0:
            raise RuntimeError('Zstd decompression error: invalid input data')
        elif content_size > (<unsigned long long>SIZE_MAX):
            raise RuntimeError('Zstd decompression error: content size too large for platform')

        dest_size = <size_t>content_size

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

        if content_size == ZSTD_CONTENTSIZE_UNKNOWN:
            dest_size = dest_nbytes

        # validate output buffer
        if dest_nbytes < dest_size:
            raise ValueError('destination buffer too small; expected at least %s, '
                             'got %s' % (dest_size, dest_nbytes))

        # perform decompression
        with nogil:
            decompressed_size = ZSTD_decompress(dest_ptr, dest_size, source_ptr, source_size)

    finally:
        pass

    # check decompression was successful
    if ZSTD_isError(decompressed_size):
        error = ZSTD_getErrorName(decompressed_size)
        raise RuntimeError('Zstd decompression error: %s' % error)
    elif decompressed_size != dest_size:
        raise RuntimeError('Zstd decompression error: expected to decompress %s, got %s' %
                           (dest_size, decompressed_size))

    return dest

cdef stream_decompress(const Py_buffer* source_pb):
    """Decompress data of unknown size

        Parameters
        ----------
        source : Py_buffer
            Compressed data buffer

        Returns
        -------
        dest : bytes
            Object containing decompressed data.
    """

    cdef:
        const char *source_ptr
        void *dest_ptr
        void *new_dst
        size_t source_size, dest_size, decompressed_size
        size_t DEST_GROWTH_SIZE, status
        ZSTD_inBuffer input
        ZSTD_outBuffer output
        ZSTD_DStream *zds

    # Recommended size for output buffer, guaranteed to flush at least
    # one completely block in all circumstances
    DEST_GROWTH_SIZE = ZSTD_DStreamOutSize();

    source_ptr = <const char*>source_pb.buf
    source_size = source_pb.len

    # unknown content size, guess it is twice the size as the source
    dest_size = source_size * 2

    if dest_size < DEST_GROWTH_SIZE:
        # minimum dest_size is DEST_GROWTH_SIZE
        dest_size = DEST_GROWTH_SIZE

    dest_ptr = <char *>malloc(dest_size)
    zds = ZSTD_createDStream()

    try:

        with nogil:

            status = ZSTD_initDStream(zds)
            if ZSTD_isError(status):
                error = ZSTD_getErrorName(status)
                ZSTD_freeDStream(zds);
                raise RuntimeError('Zstd stream decompression error on ZSTD_initDStream: %s' % error)

            input = ZSTD_inBuffer(source_ptr, source_size, 0)
            output = ZSTD_outBuffer(dest_ptr, dest_size, 0)

            # Initialize to 1 to force a loop iteration
            status = 1
            while(status > 0 or input.pos < input.size):
                # Possible returned values of ZSTD_decompressStream:
                # 0: frame is completely decoded and fully flushed
                # error (<0)
                # >0: suggested next input size
                status = ZSTD_decompressStream(zds, &output, &input)

                if ZSTD_isError(status):
                    error = ZSTD_getErrorName(status)
                    raise RuntimeError('Zstd stream decompression error on ZSTD_decompressStream: %s' % error)

                # There is more to decompress, grow the buffer
                if status > 0 and output.pos == output.size:
                    new_size = output.size + DEST_GROWTH_SIZE

                    if new_size < output.size or new_size < DEST_GROWTH_SIZE:
                        raise RuntimeError('Zstd stream decompression error: output buffer overflow')

                    new_dst = realloc(output.dst, new_size)

                    if new_dst == NULL:
                        # output.dst freed in finally block
                        raise RuntimeError('Zstd stream decompression error on realloc: could not expand output buffer')

                    output.dst = new_dst
                    output.size = new_size

        # Copy the output to a bytes object
        dest = PyBytes_FromStringAndSize(<char *>output.dst, output.pos)

    finally:
        ZSTD_freeDStream(zds)
        free(output.dst)

    return dest

cdef unsigned long long findTotalContentSize(const char* source_ptr, size_t source_size):
    """Find the total uncompressed content size of all frames in the source buffer

    Parameters
    ----------
    source_ptr : Pointer to the beginning of the buffer
    source_size : Size of the buffer containing the frame sizes to sum

    Returns
    -------
    total_content_size: Sum of the content size of all frames within the source buffer
        If any of the frame sizes is unknown, return ZSTD_CONTENTSIZE_UNKNOWN.
        If any of the frames causes ZSTD_getFrameContentSize to error, return ZSTD_CONTENTSIZE_ERROR.
    """
    cdef:
        unsigned long long frame_content_size = 0
        unsigned long long total_content_size = 0
        size_t frame_compressed_size = 0
        size_t offset = 0

    while offset < source_size:
        frame_compressed_size = ZSTD_findFrameCompressedSize(source_ptr + offset, source_size - offset);

        if ZSTD_isError(frame_compressed_size):
            error = ZSTD_getErrorName(frame_compressed_size)
            raise RuntimeError('Could not set determine zstd frame size: %s' % error)

        frame_content_size = ZSTD_getFrameContentSize(source_ptr + offset, frame_compressed_size);

        if frame_content_size == ZSTD_CONTENTSIZE_ERROR:
            return ZSTD_CONTENTSIZE_ERROR

        if frame_content_size == ZSTD_CONTENTSIZE_UNKNOWN:
            return ZSTD_CONTENTSIZE_UNKNOWN

        total_content_size += frame_content_size
        offset += frame_compressed_size

    return total_content_size

class Zstd(Codec):
    """Codec providing compression using Zstandard.

    Parameters
    ----------
    level : int
        Compression level (-131072 to 22).
    checksum : bool
        Flag to enable checksums. The default is False.

    See Also
    --------
    numcodecs.lz4.LZ4, numcodecs.blosc.Blosc

    """

    codec_id = 'zstd'

    # Note: unlike the LZ4 and Blosc codecs, there does not appear to be a (currently)
    # practical limit on the size of buffers that Zstd can process and so we don't
    # enforce a max_buffer_size option here.

    def __init__(self, level=DEFAULT_CLEVEL, checksum=False):
        self.level = level
        self.checksum = checksum

    def encode(self, buf):
        buf = ensure_contiguous_ndarray(buf)
        return compress(buf, self.level, self.checksum)

    def decode(self, buf, out=None):
        buf = ensure_contiguous_ndarray(buf)
        return decompress(buf, out)

    def __repr__(self):
        r = '%s(level=%r)' % \
            (type(self).__name__,
             self.level)
        return r

    @classmethod
    def default_level(cls):
        """Returns the default compression level of the underlying zstd library."""
        return ZSTD_defaultCLevel()

    @classmethod
    def min_level(cls):
        """Returns the minimum compression level of the underlying zstd library."""
        return ZSTD_minCLevel()

    @classmethod
    def max_level(cls):
        """Returns the maximum compression level of the underlying zstd library."""
        return ZSTD_maxCLevel()
