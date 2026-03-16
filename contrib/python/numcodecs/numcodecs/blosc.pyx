# cython: embedsignature=True
# cython: profile=False
# cython: linetrace=False
# cython: binding=False
# cython: language_level=3
import threading
import multiprocessing
import os


from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_FromStringAndSize
from cpython.memoryview cimport PyMemoryView_GET_BUFFER

from .compat_ext cimport PyBytes_RESIZE, ensure_continguous_memoryview

from .compat import ensure_contiguous_ndarray
from .abc import Codec


cdef extern from "blosc.h":
    cdef enum:
        BLOSC_MAX_OVERHEAD,
        BLOSC_VERSION_STRING,
        BLOSC_VERSION_DATE,
        BLOSC_NOSHUFFLE,
        BLOSC_SHUFFLE,
        BLOSC_BITSHUFFLE,
        BLOSC_MAX_BUFFERSIZE,
        BLOSC_MAX_THREADS,
        BLOSC_MAX_TYPESIZE,
        BLOSC_DOSHUFFLE,
        BLOSC_DOBITSHUFFLE,
        BLOSC_MEMCPYED

    void blosc_init()
    void blosc_destroy()
    int blosc_get_nthreads()
    int blosc_set_nthreads(int nthreads)
    int blosc_set_compressor(const char *compname)
    void blosc_set_blocksize(size_t blocksize)
    char* blosc_list_compressors()
    int blosc_compress(int clevel, int doshuffle, size_t typesize, size_t nbytes,
                       void* src, void* dest, size_t destsize) nogil
    int blosc_decompress(void *src, void *dest, size_t destsize) nogil
    int blosc_getitem(void* src, int start, int nitems, void* dest)
    int blosc_compress_ctx(int clevel, int doshuffle, size_t typesize, size_t nbytes,
                           const void* src, void* dest, size_t destsize,
                           const char* compressor, size_t blocksize,
                           int numinternalthreads) nogil
    int blosc_decompress_ctx(const void* src, void* dest, size_t destsize,
                             int numinternalthreads) nogil
    void blosc_cbuffer_sizes(const void* cbuffer, size_t* nbytes, size_t* cbytes,
                             size_t* blocksize)
    char* blosc_cbuffer_complib(const void* cbuffer)
    void blosc_cbuffer_metainfo(const void* cbuffer, size_t* typesize, int* flags)


MAX_OVERHEAD = BLOSC_MAX_OVERHEAD
MAX_BUFFERSIZE = BLOSC_MAX_BUFFERSIZE
MAX_THREADS = BLOSC_MAX_THREADS
MAX_TYPESIZE = BLOSC_MAX_TYPESIZE
VERSION_STRING = <char *> BLOSC_VERSION_STRING
VERSION_DATE = <char *> BLOSC_VERSION_DATE
VERSION_STRING = VERSION_STRING.decode()
VERSION_DATE = VERSION_DATE.decode()
__version__ = VERSION_STRING
NOSHUFFLE = BLOSC_NOSHUFFLE
SHUFFLE = BLOSC_SHUFFLE
BITSHUFFLE = BLOSC_BITSHUFFLE
# automatic shuffle
AUTOSHUFFLE = -1
# automatic block size - let blosc decide
AUTOBLOCKS = 0

# synchronization
_MUTEX = None
_MUTEX_IS_INIT = False

def get_mutex():
    global _MUTEX_IS_INIT, _MUTEX
    if not _MUTEX_IS_INIT:
        try:
            mutex = multiprocessing.Lock()
        except OSError:
            mutex = None
        except ImportError:
            mutex = None
        _MUTEX = mutex
        _MUTEX_IS_INIT = True
    return _MUTEX

# store ID of process that first loads the module, so we can detect a fork later
_importer_pid = os.getpid()


def _init():
    """Initialize the Blosc library environment."""
    blosc_init()


def _destroy():
    """Destroy the Blosc library environment."""
    blosc_destroy()


def list_compressors():
    """Get a list of compressors supported in the current build."""
    s = blosc_list_compressors()
    s = s.decode('ascii')
    return s.split(',')


def get_nthreads():
    """Get the number of threads that Blosc uses internally for compression and
    decompression."""
    return blosc_get_nthreads()


def set_nthreads(int nthreads):
    """Set the number of threads that Blosc uses internally for compression and
    decompression."""
    return blosc_set_nthreads(nthreads)


def _cbuffer_sizes(source):
    """Return information about a compressed buffer, namely the number of uncompressed
    bytes (`nbytes`) and compressed (`cbytes`).  It also returns the `blocksize` (which
    is used internally for doing the compression by blocks).

    Returns
    -------
    nbytes : int
    cbytes : int
    blocksize : int

    """
    cdef:
        memoryview source_mv
        const Py_buffer* source_pb
        size_t nbytes, cbytes, blocksize

    # obtain source memoryview
    source_mv = ensure_continguous_memoryview(source)
    source_pb = PyMemoryView_GET_BUFFER(source_mv)

    # determine buffer size
    blosc_cbuffer_sizes(source_pb.buf, &nbytes, &cbytes, &blocksize)

    return nbytes, cbytes, blocksize


def cbuffer_complib(source):
    """Return the name of the compression library used to compress `source`."""
    cdef:
        memoryview source_mv
        const Py_buffer* source_pb

    # obtain source memoryview
    source_mv = ensure_continguous_memoryview(source)
    source_pb = PyMemoryView_GET_BUFFER(source_mv)

    # determine buffer size
    complib = blosc_cbuffer_complib(source_pb.buf)

    complib = complib.decode('ascii')

    return complib


def _cbuffer_metainfo(source):
    """Return some meta-information about the compressed buffer in `source`, including
    the typesize, whether the shuffle or bit-shuffle filters were used, and the
    whether the buffer was memcpyed.

    Returns
    -------
    typesize
    shuffle
    memcpyed

    """
    cdef:
        memoryview source_mv
        const Py_buffer* source_pb
        size_t typesize
        int flags

    # obtain source memoryview
    source_mv = ensure_continguous_memoryview(source)
    source_pb = PyMemoryView_GET_BUFFER(source_mv)

    # determine buffer size
    blosc_cbuffer_metainfo(source_pb.buf, &typesize, &flags)

    # decompose flags
    if flags & BLOSC_DOSHUFFLE:
        shuffle = SHUFFLE
    elif flags & BLOSC_DOBITSHUFFLE:
        shuffle = BITSHUFFLE
    else:
        shuffle = NOSHUFFLE
    memcpyed = flags & BLOSC_MEMCPYED

    return typesize, shuffle, memcpyed

def _err_bad_cname(cname):
    raise ValueError('bad compressor or compressor not supported: %r; expected one of '
                     '%s' % (cname, list_compressors()))


def compress(source, char* cname, int clevel, int shuffle=SHUFFLE,
             int blocksize=AUTOBLOCKS, typesize=None):
    """Compress data.

    Parameters
    ----------
    source : bytes-like
        Data to be compressed. Can be any object supporting the buffer
        protocol.
    cname : bytes
        Name of compression library to use.
    clevel : int
        Compression level.
    shuffle : int
        Either NOSHUFFLE (0), SHUFFLE (1), BITSHUFFLE (2) or AUTOSHUFFLE (-1). If AUTOSHUFFLE,
        bit-shuffle will be used for buffers with itemsize 1, and byte-shuffle will
        be used otherwise. The default is `SHUFFLE`.
    blocksize : int
        The requested size of the compressed blocks.  If 0, an automatic blocksize will
        be used.

    Returns
    -------
    dest : bytes
        Compressed data.

    """

    cdef:
        memoryview source_mv
        const Py_buffer* source_pb
        const char* source_ptr
        size_t nbytes, itemsize
        int cbytes
        bytes dest
        char* dest_ptr

    # check valid cname early
    cname_str = cname.decode('ascii')
    if cname_str not in list_compressors():
        _err_bad_cname(cname_str)

    # obtain source memoryview
    source_mv = ensure_continguous_memoryview(source)
    source_pb = PyMemoryView_GET_BUFFER(source_mv)

    # extract metadata
    source_ptr = <const char*>source_pb.buf
    nbytes = source_pb.len

    # validate typesize
    if isinstance(typesize, int):
        if typesize < 1:
            raise ValueError(f"Cannot use typesize {typesize} less than 1.")
        itemsize = typesize
    else:
        itemsize = source_pb.itemsize

    # determine shuffle
    if shuffle == AUTOSHUFFLE:
        if itemsize == 1:
            shuffle = BITSHUFFLE
        else:
            shuffle = SHUFFLE
    elif shuffle not in [NOSHUFFLE, SHUFFLE, BITSHUFFLE]:
        raise ValueError('invalid shuffle argument; expected -1, 0, 1 or 2, found %r' %
                         shuffle)

    try:

        # setup destination
        dest = PyBytes_FromStringAndSize(NULL, nbytes + BLOSC_MAX_OVERHEAD)
        dest_ptr = PyBytes_AS_STRING(dest)

        # perform compression
        if _get_use_threads():
            # allow blosc to use threads internally

            # N.B., we are using blosc's global context, and so we need to use a lock
            # to ensure no-one else can modify the global context while we're setting it
            # up and using it.
            with get_mutex():

                # set compressor
                compressor_set = blosc_set_compressor(cname)
                if compressor_set < 0:
                    # shouldn't happen if we checked against list of compressors
                    # already, but just in case
                    _err_bad_cname(cname_str)

                # set blocksize
                blosc_set_blocksize(blocksize)

                # perform compression
                with nogil:
                    cbytes = blosc_compress(clevel, shuffle, itemsize, nbytes, source_ptr,
                                            dest_ptr, nbytes + BLOSC_MAX_OVERHEAD)

        else:
            with nogil:
                cbytes = blosc_compress_ctx(clevel, shuffle, itemsize, nbytes, source_ptr,
                                            dest_ptr, nbytes + BLOSC_MAX_OVERHEAD,
                                            cname, blocksize, 1)

    finally:
        pass

    # check compression was successful
    if cbytes <= 0:
        raise RuntimeError('error during blosc compression: %d' % cbytes)

    # resize after compression
    PyBytes_RESIZE(dest, cbytes)

    return dest


def decompress(source, dest=None):
    """Decompress data.

    Parameters
    ----------
    source : bytes-like
        Compressed data, including blosc header. Can be any object supporting the buffer
        protocol.
    dest : array-like, optional
        Object to decompress into.

    Returns
    -------
    dest : bytes
        Object containing decompressed data.

    """
    cdef:
        int ret
        memoryview source_mv
        const Py_buffer* source_pb
        const char* source_ptr
        memoryview dest_mv
        Py_buffer* dest_pb
        char* dest_ptr
        size_t nbytes, cbytes, blocksize

    # obtain source memoryview
    source_mv = ensure_continguous_memoryview(source)
    source_pb = PyMemoryView_GET_BUFFER(source_mv)

    # get source pointer
    source_ptr = <const char*>source_pb.buf

    # determine buffer size
    blosc_cbuffer_sizes(source_ptr, &nbytes, &cbytes, &blocksize)

    # setup destination buffer
    if dest is None:
        # allocate memory
        dest_1d = dest = PyBytes_FromStringAndSize(NULL, nbytes)
    else:
        dest_1d = ensure_contiguous_ndarray(dest)

    # obtain dest memoryview
    dest_mv = memoryview(dest_1d)
    dest_pb = PyMemoryView_GET_BUFFER(dest_mv)
    dest_ptr = <char*>dest_pb.buf
    dest_nbytes = dest_pb.len

    try:

        # guard condition
        if dest_nbytes < nbytes:
            raise ValueError('destination buffer too small; expected at least %s, '
                             'got %s' % (nbytes, dest_nbytes))

        # perform decompression
        if _get_use_threads():
            # allow blosc to use threads internally
            with nogil:
                ret = blosc_decompress(source_ptr, dest_ptr, nbytes)
        else:
            with nogil:
                ret = blosc_decompress_ctx(source_ptr, dest_ptr, nbytes, 1)

    finally:
        pass

    # handle errors
    if ret <= 0:
        raise RuntimeError('error during blosc decompression: %d' % ret)

    return dest


# set the value of this variable to True or False to override the
# default adaptive behaviour
use_threads = None


def _get_use_threads():
    global use_threads
    proc = multiprocessing.current_process()

    # check if locks are available, and if not no threads
    if not get_mutex():
        return False

    # check for fork
    if proc.pid != _importer_pid:
        # If this module has been imported in the parent process, and the current process
        # is a fork, attempting to use blosc in multi-threaded mode will cause a
        # program hang, so we force use of blosc ctx functions, i.e., no threads.
        return False

    if use_threads in [True, False]:
        # user has manually overridden the default behaviour
        _use_threads = use_threads

    else:
        # Adaptive behaviour: allow blosc to use threads if it is being called from the
        # main Python thread in the main Python process, inferring that it is being run
        # from within a single-threaded, single-process program; otherwise do not allow
        # blosc to use threads, inferring it is being run from within a multi-threaded
        # program or multi-process program

        if proc.name != 'MainProcess':
            _use_threads = False
        elif hasattr(threading, 'main_thread'):
            _use_threads = (threading.main_thread() == threading.current_thread())
        else:
            _use_threads = threading.current_thread().name == 'MainThread'

    return _use_threads


_shuffle_repr = ['AUTOSHUFFLE', 'NOSHUFFLE', 'SHUFFLE', 'BITSHUFFLE']


class Blosc(Codec):
    """Codec providing compression using the Blosc meta-compressor.

    Parameters
    ----------
    cname : string, optional
        A string naming one of the compression algorithms available within blosc, e.g.,
        'zstd', 'blosclz', 'lz4', 'lz4hc', 'zlib' or 'snappy'.
    clevel : integer, optional
        An integer between 0 and 9 specifying the compression level.
    shuffle : integer, optional
        Either NOSHUFFLE (0), SHUFFLE (1), BITSHUFFLE (2) or AUTOSHUFFLE (-1). If AUTOSHUFFLE,
        bit-shuffle will be used for buffers with itemsize 1, and byte-shuffle will
        be used otherwise. The default is `SHUFFLE`.
    blocksize : int
        The requested size of the compressed blocks.  If 0 (default), an automatic
        blocksize will be used.
    typesize : int, optional
        The size in bytes of uncompressed array elements.

    See Also
    --------
    numcodecs.zstd.Zstd, numcodecs.lz4.LZ4

    """

    codec_id = 'blosc'
    NOSHUFFLE = NOSHUFFLE
    SHUFFLE = SHUFFLE
    BITSHUFFLE = BITSHUFFLE
    AUTOSHUFFLE = AUTOSHUFFLE
    max_buffer_size = 2**31 - 1

    def __init__(self, cname='lz4', clevel=5, shuffle=SHUFFLE, blocksize=AUTOBLOCKS, typesize=None):
        if isinstance(typesize, int) and typesize < 1:
            raise ValueError(f"Cannot use typesize {typesize} less than 1.")
        self.cname = cname
        if isinstance(cname, str):
            self._cname_bytes = cname.encode('ascii')
        else:
            self._cname_bytes = cname
        self.clevel = clevel
        self.shuffle = shuffle
        self.blocksize = blocksize
        self._typesize = typesize

    def encode(self, buf):
        buf = ensure_contiguous_ndarray(buf, self.max_buffer_size)
        return compress(buf, self._cname_bytes, self.clevel, self.shuffle, self.blocksize, self._typesize)

    def decode(self, buf, out=None):
        buf = ensure_contiguous_ndarray(buf, self.max_buffer_size)
        return decompress(buf, out)

    def __repr__(self):
        r = '%s(cname=%r, clevel=%r, shuffle=%s, blocksize=%s)' % \
            (type(self).__name__,
             self.cname,
             self.clevel,
             _shuffle_repr[self.shuffle + 1],
             self.blocksize)
        return r
