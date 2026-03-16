#cython: language_level=3
from libc.stdint cimport int64_t, uint32_t


cdef class LegacyRecordBatch:

    cdef:
        Py_buffer _buffer
        char _magic
        int _decompressed
        LegacyRecord _main_record

    @staticmethod
    cdef inline LegacyRecordBatch new(
        bytes buffer, Py_ssize_t pos, Py_ssize_t slice_end, char magic)

    cdef int _decompress(self, char compression_type) except -1
    cdef int64_t _read_last_offset(self) except -1
    cdef inline int _check_bounds(
            self, Py_ssize_t pos, Py_ssize_t size) except -1
    cdef LegacyRecord _read_record(self, Py_ssize_t* read_pos)


cdef class LegacyRecord:

    cdef:
        readonly int64_t offset
        int64_t timestamp
        char attributes
        readonly object key
        readonly object value
        uint32_t crc

    @staticmethod
    cdef inline LegacyRecord new(
        int64_t offset, int64_t timestamp, char attributes,
        object key, object value, uint32_t crc)
