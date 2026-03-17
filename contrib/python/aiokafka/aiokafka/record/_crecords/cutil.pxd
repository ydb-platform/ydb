#cython: language_level=3
from libc.stdint cimport int64_t, uint64_t, uint32_t
from libc.limits cimport UINT_MAX
from cpython cimport (
    PyObject_GetBuffer, PyBuffer_Release, Py_buffer, PyBUF_READ, PyBUF_SIMPLE,
    PyBytes_AS_STRING
)
cdef extern from "Python.h":
    object PyMemoryView_FromMemory(char *mem, ssize_t size, int flags)

# Time implementation ported from Python/pytime.c

DEF NS_TO_MS = (1000 * 1000)
DEF SEC_TO_NS = (1000 * 1000 * 1000)
DEF US_TO_NS = 1000

IF UNAME_SYSNAME == "Windows":
    cdef extern from "windows.h":
        # File types
        ctypedef unsigned long DWORD
        ctypedef unsigned long long ULONGLONG
        # Structs
        ctypedef struct FILETIME:
            DWORD dwLowDateTime
            DWORD dwHighDateTime
        ctypedef struct __inner_ulonglong:
            DWORD LowPart;
            DWORD HighPart;
        ctypedef union ULARGE_INTEGER:
            __inner_ulonglong u
            ULONGLONG QuadPart

        void GetSystemTimeAsFileTime(FILETIME *time)

ELSE:
    from posix.time cimport gettimeofday, timeval, timezone


IF UNAME_SYSNAME == "Windows":
    cdef inline int64_t get_time_as_unix_ms():
        cdef:
            FILETIME system_time
            ULARGE_INTEGER large


        GetSystemTimeAsFileTime(&system_time)
        large.u.LowPart = system_time.dwLowDateTime
        large.u.HighPart = system_time.dwHighDateTime
        # 11,644,473,600,000,000,000: number of nanoseconds between
        # the 1st january 1601 and the 1st january 1970 (369 years + 89 leap
        # days).
        large.QuadPart = <ULONGLONG>(
            (large.QuadPart / NS_TO_MS) * 100 - <ULONGLONG> 11644473600000)
        return <int64_t> (large.QuadPart)

ELSE:
    cdef inline int64_t get_time_as_unix_ms():
        cdef:
            timeval tv
            long long res
            int err

        err = gettimeofday(&tv, <timezone *>NULL) # Fixme: handle error?
        if err:
            return -1

        res = (<long long> tv.tv_sec) * SEC_TO_NS
        res += tv.tv_usec * US_TO_NS
        return <int64_t> (res / NS_TO_MS)
# END: Time implementation


# CRC32 function

IF UNAME_SYSNAME == "Windows":
    from zlib import crc32 as py_crc32
    cdef inline int calc_crc32(
            unsigned long crc, unsigned char *buf, size_t len,
            unsigned long *crc_out) except -1:
        cdef:
            object memview
        memview = PyMemoryView_FromMemory(
            <char *>buf, <Py_ssize_t> len, PyBUF_READ)
        crc_out[0] = py_crc32(memview)
        crc_out[0] = crc_out[0] & 0xffffffff
        return 0
ELSE:
    cdef extern from "zlib.h":
        unsigned long crc32(
            unsigned long crc, const unsigned char *buf, unsigned int len
        ) nogil

    cdef inline int calc_crc32(
            unsigned long crc, unsigned char *buf, size_t len,
            unsigned long *crc_out) except -1:
        """ Implementation taken from Python's zlib source.
        """
        cdef:
            unsigned long signed_val

        # Releasing the GIL for very small buffers is inefficient
        # and may lower performance
        if len > 1024*5:
            with nogil:
                # Avoid truncation of length for very large buffers. crc32()
                # takes length as an unsigned int, which may be narrower than
                # Py_ssize_t.
                while (<size_t>len > UINT_MAX):
                    crc = crc32(crc, buf, UINT_MAX)
                    buf += <size_t> UINT_MAX
                    len -= <size_t> UINT_MAX
                signed_val = crc32(crc, buf, <unsigned int> len)
        else:
            signed_val = crc32(crc, buf, <unsigned int> len)

        crc_out[0] = signed_val & 0xffffffff
        return 0

# END: CRC32 function


# VarInt implementation

cdef int decode_varint64(
        char* buf, Py_ssize_t* read_pos, int64_t* out_value) except -1

cdef int encode_varint64(
        char* buf, Py_ssize_t* write_pos, int64_t value) except -1
cdef int encode_varint(
        char* buf, Py_ssize_t* write_pos, Py_ssize_t value) except -1

cdef Py_ssize_t size_of_varint(Py_ssize_t value) except -1
cdef Py_ssize_t size_of_varint64(int64_t value) except -1

# END: VarInt implementation


# CRC32C C implementation

cdef extern from "crc32c.h":

    uint32_t crc32c(uint32_t crc, const void *buf, size_t len) nogil

    void crc32c_global_init() nogil

cdef inline int calc_crc32c(
        uint32_t crc, char *buf, size_t len,
        uint32_t *crc_out) except -1:
    cdef:
        uint32_t signed_val


    # Releasing the GIL for very small buffers is inefficient
    # and may lower performance
    if len > 1024*5:
        with nogil:
            # Avoid truncation of length for very large buffers. crc32()
            # takes length as an unsigned int, which may be narrower than
            # Py_ssize_t.
            while (len > <size_t> UINT_MAX):
                crc = crc32c(crc, buf, UINT_MAX)
                buf += <size_t> UINT_MAX
                len -= <size_t> UINT_MAX
            signed_val = crc32c(crc, buf, len)
    else:
        signed_val = crc32c(crc, buf, len)

    crc_out[0] = signed_val & 0xffffffff
    return 0

# END: CRC32C C implementation
