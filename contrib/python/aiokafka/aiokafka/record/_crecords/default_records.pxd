#cython: language_level=3
from libc.stdint cimport int64_t, uint32_t, int32_t, int16_t


cdef class DefaultRecordBatch:

    cdef:
        Py_buffer _buffer
        int _decompressed
        Py_ssize_t _pos
        int32_t _next_record_index


        readonly int64_t base_offset
        int32_t length
        readonly char magic
        readonly uint32_t crc
        readonly int16_t attributes
        readonly int32_t last_offset_delta
        readonly int64_t first_timestamp
        readonly int64_t max_timestamp

        readonly int64_t producer_id
        readonly int16_t producer_epoch
        readonly int32_t base_sequence

        readonly char timestamp_type
        int32_t num_records

    @staticmethod
    cdef inline DefaultRecordBatch new(
        bytes buffer, Py_ssize_t pos, Py_ssize_t slice_end, char magic)

    cdef DefaultRecord _read_msg(self)

    cdef inline int _check_bounds(
            self, Py_ssize_t pos, Py_ssize_t size) except -1
    cdef inline _read_header(self)
    cdef _maybe_uncompress(self)


cdef class DefaultRecord:

    cdef:
        readonly int64_t offset
        readonly int64_t timestamp
        readonly char timestamp_type
        readonly object key
        readonly object value
        readonly object headers

    @staticmethod
    cdef inline DefaultRecord new(
        int64_t offset, int64_t timestamp, char timestamp_type,
        object key, object value, object headers)
