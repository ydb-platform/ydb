#cython: language_level=3
# This class takes advantage of the fact that all formats v0, v1 and v2 of
# messages storage has the same byte offsets for Length and Magic fields.
# Lets look closely at what leading bytes all versions have:
#
# V0 and V1 (Offset is MessageSet part, other bytes are Message ones):
#  Offset => Int64
#  BytesLength => Int32
#  CRC => Int32
#  Magic => Int8
#  ...
#
# V2:
#  BaseOffset => Int64
#  Length => Int32
#  PartitionLeaderEpoch => Int32
#  Magic => Int8
#  ...
#
# So we can iterate over batches just by knowing offsets of Length. Magic is
# used to construct the correct class for Batch itself.

from aiokafka.errors import CorruptRecordException

from .default_records cimport DefaultRecordBatch
from .legacy_records cimport LegacyRecordBatch
from . cimport hton
from cpython cimport PyBytes_GET_SIZE, PyBytes_AS_STRING, Py_buffer,\
    PyObject_GetBuffer, PyBuffer_Release, PyBUF_SIMPLE

cdef extern from "Python.h":
    object PyMemoryView_FromObject(object obj)
    Py_buffer* PyMemoryView_GET_BUFFER(object mview)


DEF LENGTH_OFFSET = 8
DEF LOG_OVERHEAD = 12
DEF MAGIC_OFFSET = 16
DEF RECORD_OVERHEAD_V0 = 14


cdef class MemoryRecords:

    cdef:
        bytes _buffer
        Py_ssize_t _pos

    def __init__(self, bytes bytes_data):
        self._buffer = bytes_data
        self._pos = 0

    def size_in_bytes(self):
        return PyBytes_GET_SIZE(self._buffer)

    cdef object _get_next(self):
        cdef:
            Py_ssize_t buffer_len
            char* buf
            Py_ssize_t length
            Py_ssize_t pos = self._pos
            Py_ssize_t slice_end
            char magic

        buffer_len = PyBytes_GET_SIZE(self._buffer)
        buf = PyBytes_AS_STRING(self._buffer)

        remaining = buffer_len - pos
        if remaining < LOG_OVERHEAD:
            return None

        length = <Py_ssize_t> hton.unpack_int32(&buf[pos + LENGTH_OFFSET])

        if length < RECORD_OVERHEAD_V0:
            raise CorruptRecordException(
                "Record size is less than the minimum record overhead "
                "({})".format(RECORD_OVERHEAD_V0))

        slice_end = pos + LOG_OVERHEAD + length
        if slice_end > buffer_len:
            return None

        self._pos = slice_end

        magic = buf[MAGIC_OFFSET]
        if magic < 2:
            return LegacyRecordBatch.new(self._buffer, pos, slice_end, magic)
        else:
            return DefaultRecordBatch.new(self._buffer, pos, slice_end, magic)

    def has_next(self):
        cdef:
            char* buf
            Py_ssize_t buffer_len
            Py_ssize_t length

        buffer_len = PyBytes_GET_SIZE(self._buffer)
        if buffer_len - self._pos < LOG_OVERHEAD:
            return False

        buf = PyBytes_AS_STRING(self._buffer)
        length = <Py_ssize_t> hton.unpack_int32(
            &buf[self._pos + LENGTH_OFFSET])
        if buffer_len - self._pos < LOG_OVERHEAD + length:
            return False

        return True

    def next_batch(self):
        return self._get_next()
