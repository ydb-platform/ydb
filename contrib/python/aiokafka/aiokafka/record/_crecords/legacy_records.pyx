#cython: language_level=3

import aiokafka.codec as codecs
from aiokafka.codec import (
    gzip_encode, snappy_encode, lz4_encode,
    gzip_decode, snappy_decode, lz4_decode,
)
from aiokafka.errors import CorruptRecordException, UnsupportedCodecError
from zlib import crc32 as py_crc32  # needed for windows macro

from cpython cimport PyObject_GetBuffer, PyBuffer_Release, PyBUF_WRITABLE, \
                     PyBUF_SIMPLE, PyBUF_READ, Py_buffer, \
                     PyBytes_FromStringAndSize
from libc.stdint cimport int32_t, int64_t, uint32_t
from libc.string cimport memcpy
cimport cython
cdef extern from "Python.h":
    ssize_t PyByteArray_GET_SIZE(object)
    char* PyByteArray_AS_STRING(bytearray ba)
    int PyByteArray_Resize(object, ssize_t) except -1

    object PyMemoryView_FromMemory(char *mem, ssize_t size, int flags)


# This should be before _cutil to generate include for `winsock2.h` before
# `windows.h`
from . cimport hton
from . cimport cutil

include "consts.pxi"
# Those are used for fast size calculations
DEF RECORD_OVERHEAD_V0_DEF = 14
DEF RECORD_OVERHEAD_V1_DEF = 22
DEF KEY_OFFSET_V0 = 18
DEF KEY_OFFSET_V1 = 26
DEF LOG_OVERHEAD = 12
DEF KEY_LENGTH = 4
DEF VALUE_LENGTH = 4

# Field offsets
DEF LENGTH_OFFSET = 8
DEF CRC_OFFSET = LENGTH_OFFSET + 4
DEF MAGIC_OFFSET = CRC_OFFSET + 4
DEF ATTRIBUTES_OFFSET = MAGIC_OFFSET + 1
DEF TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + 1


cdef _assert_has_codec(char compression_type):
    if compression_type == _ATTR_CODEC_GZIP:
        checker, name = codecs.has_gzip, "gzip"
    elif compression_type == _ATTR_CODEC_SNAPPY:
        checker, name = codecs.has_snappy, "snappy"
    elif compression_type == _ATTR_CODEC_LZ4:
        checker, name = codecs.has_lz4, "lz4"
    else:
        raise UnsupportedCodecError(
            f"Unknown compression codec {compression_type:#04x}")
    if not checker():
        raise UnsupportedCodecError(
            f"Libraries for {name} compression codec not found")


@cython.no_gc_clear
@cython.final
@cython.freelist(_LEGACY_RECORD_BATCH_FREELIST_SIZE)
cdef class LegacyRecordBatch:

    RECORD_OVERHEAD_V0 = RECORD_OVERHEAD_V0_DEF
    RECORD_OVERHEAD_V1 = RECORD_OVERHEAD_V1_DEF
    CODEC_MASK = _ATTR_CODEC_MASK
    CODEC_GZIP = _ATTR_CODEC_GZIP
    CODEC_SNAPPY = _ATTR_CODEC_SNAPPY
    CODEC_LZ4 = _ATTR_CODEC_LZ4

    is_control_batch = False
    is_transactional = False
    producer_id = None

    def __init__(self, object buffer, char magic):
        PyObject_GetBuffer(buffer, &self._buffer, PyBUF_SIMPLE)
        self._magic = magic
        self._decompressed = 0
        self._main_record = self._read_record(NULL)

    @staticmethod
    cdef inline LegacyRecordBatch new(
            bytes buffer, Py_ssize_t pos, Py_ssize_t slice_end, char magic):
        """ Fast constructor to initialize from C.
            NOTE: We take ownership of the Py_buffer object, so caller does not
                  need to call PyBuffer_Release.
        """
        cdef:
            LegacyRecordBatch batch
            char* buf
        batch = LegacyRecordBatch.__new__(LegacyRecordBatch)
        PyObject_GetBuffer(buffer, &batch._buffer, PyBUF_SIMPLE)
        buf = <char *>batch._buffer.buf
        # Change the buffer to include a proper slice
        batch._buffer.buf = <void *> &buf[pos]
        batch._buffer.len = slice_end - pos

        batch._magic = magic
        batch._decompressed = 0
        batch._main_record = batch._read_record(NULL)
        return batch

    def __dealloc__(self):
        PyBuffer_Release(&self._buffer)

    @property
    def next_offset(self):
        return self._main_record.offset + 1

    def validate_crc(self):
        cdef:
            unsigned long crc = 0
            char * buf

        buf = <char*> self._buffer.buf
        cutil.calc_crc32(
            0,
            <unsigned char*> &buf[MAGIC_OFFSET],
            <size_t> self._buffer.len - MAGIC_OFFSET,
            &crc
        )

        return self._main_record.crc == <uint32_t> crc

    cdef int _decompress(self, char compression_type) except -1:
        cdef:
            bytes value

        if self._main_record.value is None:
            raise CorruptRecordException("Value of compressed message is None")
        value = self._main_record.value

        _assert_has_codec(compression_type)
        if compression_type == _ATTR_CODEC_GZIP:
            uncompressed = gzip_decode(value)
        elif compression_type == _ATTR_CODEC_SNAPPY:
            uncompressed = snappy_decode(value)
        elif compression_type == _ATTR_CODEC_LZ4:
            if self._magic == 0:
                # https://issues.apache.org/jira/browse/KAFKA-3160
                raise UnsupportedCodecError(
                    "LZ4 is not supported for broker version 0.8/0.9"
                )
            else:
                uncompressed = lz4_decode(value)

        PyBuffer_Release(&self._buffer)
        PyObject_GetBuffer(uncompressed, &self._buffer, PyBUF_SIMPLE)
        return 0

    cdef int64_t _read_last_offset(self) except -1:
        cdef:
            Py_ssize_t buffer_len = self._buffer.len
            Py_ssize_t pos = 0
            Py_ssize_t length = 0
            char* buf
        buf = <char*> self._buffer.buf
        while pos < buffer_len:
            length = <Py_ssize_t> hton.unpack_int32(&buf[pos + LENGTH_OFFSET])
            pos += LOG_OVERHEAD + length
        if pos > buffer_len:
            raise CorruptRecordException("Corrupted compressed message")
        pos -= LOG_OVERHEAD + length
        return hton.unpack_int64(&buf[pos])

    cdef inline int _check_bounds(
            self, Py_ssize_t pos, Py_ssize_t size) except -1:
        """ Confirm that the slice is not outside buffer range
        """
        if pos + size > self._buffer.len:
            raise CorruptRecordException(
                "Can't read {} bytes from pos {}".format(size, pos))

    cdef LegacyRecord _read_record(self, Py_ssize_t* read_pos):
        """ Read records from position `read_pos`. After reading set `read_pos`
        to the offset of the next record. Can pass NULL to read from start.
        """

        cdef:
            Py_ssize_t pos
            char* buf
            Py_ssize_t read_size

            int64_t offset
            int64_t timestamp
            char attrs,
            uint32_t crc

        if read_pos == NULL:
            pos = 0
        else:
            pos = read_pos[0]

        # Minimum record size check
        self._check_bounds(pos, RECORD_OVERHEAD_V0_DEF + LOG_OVERHEAD)

        buf = <char*>self._buffer.buf
        offset = hton.unpack_int64(&buf[pos])
        crc = <uint32_t> hton.unpack_int32(&buf[pos + CRC_OFFSET])
        magic = buf[pos + MAGIC_OFFSET]
        attrs = buf[pos + ATTRIBUTES_OFFSET]
        if magic == 1:
            # Minimum record size for V1
            self._check_bounds(pos, RECORD_OVERHEAD_V1_DEF + LOG_OVERHEAD)
            timestamp = hton.unpack_int64(&buf[pos + TIMESTAMP_OFFSET])
            pos += KEY_OFFSET_V1
        else:
            timestamp = -1
            pos += KEY_OFFSET_V0

        # Read key
        read_size = <Py_ssize_t> hton.unpack_int32(&buf[pos])
        pos += KEY_LENGTH
        if read_size != -1:
            self._check_bounds(pos, read_size)
            key = PyBytes_FromStringAndSize(&buf[pos], read_size)
            pos += read_size
        else:
            key = None
        # Read value
        read_size = <Py_ssize_t> hton.unpack_int32(&buf[pos])
        pos += VALUE_LENGTH
        if read_size != -1:
            self._check_bounds(pos, read_size)
            value = PyBytes_FromStringAndSize(&buf[pos], read_size)
            pos += read_size
        else:
            value = None

        if read_pos != NULL:
            read_pos[0] = pos
        return LegacyRecord.new(
            offset, timestamp, attrs, key=key, value=value, crc=crc)

    def __iter__(self):
        cdef:
            char compression
            int64_t absolute_base_offset
            Py_ssize_t pos = 0
            LegacyRecord next_record
            char timestamp_type

        compression = self._main_record.attributes & _ATTR_CODEC_MASK
        if compression:
            # In case we will call iter again
            if not self._decompressed:
                self._decompress(compression)
                self._decompressed = True

            # If relative offset is used, we need to decompress the entire
            # message first to compute the absolute offset.
            if self._magic > 0:
                absolute_base_offset = (
                    self._main_record.offset - self._read_last_offset()
                )
            else:
                absolute_base_offset = -1
            timestamp_type = \
                self._main_record.attributes & _TIMESTAMP_TYPE_MASK

            while pos < self._buffer.len:
                next_record = self._read_record(&pos)
                # There should only ever be a single layer of compression
                assert not next_record.attributes & _ATTR_CODEC_MASK, (
                    'MessageSet at offset %d appears double-compressed. This '
                    'should not happen -- check your producers!'
                    % next_record.offset)

                # When magic value is greater than 0, the timestamp
                # of a compressed message depends on the
                # typestamp type of the wrapper message:
                if timestamp_type != 0:
                    next_record.timestamp = self._main_record.timestamp
                    next_record.attributes |= _TIMESTAMP_TYPE_MASK

                if absolute_base_offset >= 0:
                    next_record.offset += absolute_base_offset

                yield next_record
        else:
            yield self._main_record


@cython.no_gc_clear
@cython.final
@cython.freelist(_LEGACY_RECORD_FREELIST_SIZE)
cdef class LegacyRecord:

    def __init__(self, int64_t offset, int64_t timestamp, char attributes,
                  object key, object value, uint32_t crc):
        self.offset = offset
        self.timestamp = timestamp
        self.attributes = attributes
        self.key = key
        self.value = value
        self.crc = crc

    @staticmethod
    cdef inline LegacyRecord new(
            int64_t offset, int64_t timestamp, char attributes,
            object key, object value, uint32_t crc):
        """ Fast constructor to initialize from C.
        """
        cdef LegacyRecord record
        record = LegacyRecord.__new__(LegacyRecord)
        record.offset = offset
        record.timestamp = timestamp
        record.attributes = attributes
        record.key = key
        record.value = value
        record.crc = crc
        return record

    @property
    def headers(self):
        return []

    @property
    def timestamp(self):
        if self.timestamp != -1:
            return self.timestamp
        else:
            return None

    @property
    def timestamp_type(self):
        if self.timestamp != -1:
            if self.attributes & _TIMESTAMP_TYPE_MASK:
                return 1
            else:
                return 0
        else:
            return None

    @property
    def checksum(self):
        return self.crc

    def __repr__(self):
        return (
            "LegacyRecord(offset={!r}, timestamp={!r}, timestamp_type={!r},"
            " key={!r}, value={!r}, crc={!r})".format(
                self.offset, self.timestamp, self.timestamp_type,
                self.key, self.value, self.crc)
        )


cdef class LegacyRecordBatchBuilder:

    cdef:
        char _magic
        char _compression_type
        Py_ssize_t _batch_size
        bytearray _buffer

    CODEC_MASK = _ATTR_CODEC_MASK
    CODEC_GZIP = _ATTR_CODEC_GZIP
    CODEC_SNAPPY = _ATTR_CODEC_SNAPPY
    CODEC_LZ4 = _ATTR_CODEC_LZ4

    def __cinit__(self, char magic, char compression_type,
                  Py_ssize_t batch_size):
        self._magic = magic
        self._compression_type = compression_type
        self._batch_size = batch_size
        self._buffer = bytearray()

    def append(self, int64_t offset, timestamp, key, value, headers=None):
        """ Append message to batch.
        """
        cdef:
            Py_ssize_t pos
            Py_ssize_t size
            char *buf
            int64_t ts
            LegacyRecordMetadata metadata
            uint32_t crc

        if self._magic == 0:
            ts = -1
        elif timestamp is None:
            ts = cutil.get_time_as_unix_ms()
        else:
            ts = timestamp

        # Check if we have room for another message
        pos = PyByteArray_GET_SIZE(self._buffer)
        size = _size_in_bytes(self._magic, key, value)
        # We always allow at least one record to be appended
        if offset != 0 and pos + size >= self._batch_size:
            return None

        # Allocate proper buffer length
        PyByteArray_Resize(self._buffer, pos + size)

        # Encode message
        buf = PyByteArray_AS_STRING(self._buffer)
        _encode_msg(
            self._magic, pos, buf,
            offset, ts, key, value, 0, &crc)

        metadata = LegacyRecordMetadata.new(offset, crc, size, ts)
        return metadata

    def size(self):
        """ Return current size of data written to buffer
        """
        return PyByteArray_GET_SIZE(self._buffer)

    # Size calculations. Just copied Java's implementation

    def size_in_bytes(self, offset, timestamp, key, value):
        """ Actual size of message to add
        """
        return _size_in_bytes(self._magic, key, value)

    @staticmethod
    def record_overhead(char magic):
        if magic == 0:
            return RECORD_OVERHEAD_V0_DEF
        else:
            return RECORD_OVERHEAD_V1_DEF

    cdef int _maybe_compress(self) except -1:
        cdef:
            object compressed
            char *buf
            Py_ssize_t size
            uint32_t crc

        if self._compression_type != 0:
            _assert_has_codec(self._compression_type)
            if self._compression_type == _ATTR_CODEC_GZIP:
                compressed = gzip_encode(self._buffer)
            elif self._compression_type == _ATTR_CODEC_SNAPPY:
                compressed = snappy_encode(self._buffer)
            elif self._compression_type == _ATTR_CODEC_LZ4:
                if self._magic == 0:
                    # https://issues.apache.org/jira/browse/KAFKA-3160
                    raise UnsupportedCodecError(
                        "LZ4 is not supported for broker version 0.8/0.9"
                    )
                else:
                    compressed = lz4_encode(bytes(self._buffer))
            else:
                return 0
            size = _size_in_bytes(self._magic, key=None, value=compressed)
            # We will just write the result into the same memory space.
            PyByteArray_Resize(self._buffer, size)

            buf = PyByteArray_AS_STRING(self._buffer)
            _encode_msg(
                self._magic, start_pos=0, buf=buf,
                offset=0, timestamp=0, key=None, value=compressed,
                attributes=self._compression_type, crc_out=&crc)
            return 1
        return 0

    def build(self):
        """Compress batch to be ready for send"""
        self._maybe_compress()
        return self._buffer


cdef Py_ssize_t _size_in_bytes(char magic, object key, object value) except -1:
    """ Actual size of message to add
    """
    cdef:
        Py_buffer buf
        Py_ssize_t key_len
        Py_ssize_t value_len

    if key is None:
        key_len = 0
    else:
        PyObject_GetBuffer(key, &buf, PyBUF_SIMPLE)
        key_len = buf.len
        PyBuffer_Release(&buf)

    if value is None:
        value_len = 0
    else:
        PyObject_GetBuffer(value, &buf, PyBUF_SIMPLE)
        value_len = buf.len
        PyBuffer_Release(&buf)

    if magic == 0:
        return LOG_OVERHEAD + RECORD_OVERHEAD_V0_DEF + key_len + value_len
    else:
        return LOG_OVERHEAD + RECORD_OVERHEAD_V1_DEF + key_len + value_len


cdef int _encode_msg(
        char magic, Py_ssize_t start_pos, char *buf,
        int64_t offset, int64_t timestamp, object key, object value,
        char attributes, uint32_t* crc_out) except -1:
    """ Encode msg data into the `msg_buffer`, which should be allocated
        to at least the size of this message.
    """
    cdef:
        Py_buffer key_val_buf
        Py_ssize_t pos = start_pos
        int32_t length
        unsigned long crc = 0

    # Write key and value
    pos += KEY_OFFSET_V0 if magic == 0 else KEY_OFFSET_V1

    if key is None:
        hton.pack_int32(&buf[pos], -1)
        pos += KEY_LENGTH
    else:
        PyObject_GetBuffer(key, &key_val_buf, PyBUF_SIMPLE)
        hton.pack_int32(&buf[pos], <int32_t>key_val_buf.len)
        pos += KEY_LENGTH
        memcpy(&buf[pos], <char*>key_val_buf.buf, <size_t>key_val_buf.len)
        pos += key_val_buf.len
        PyBuffer_Release(&key_val_buf)

    if value is None:
        hton.pack_int32(&buf[pos], -1)
        pos += VALUE_LENGTH
    else:
        PyObject_GetBuffer(value, &key_val_buf, PyBUF_SIMPLE)
        hton.pack_int32(&buf[pos], <int32_t>key_val_buf.len)
        pos += VALUE_LENGTH
        memcpy(&buf[pos], <char*>key_val_buf.buf, <size_t>key_val_buf.len)
        pos += key_val_buf.len
        PyBuffer_Release(&key_val_buf)
    length = <int32_t> ((pos - start_pos) - LOG_OVERHEAD)

    # Write msg header. Note, that Crc should be updated last
    hton.pack_int64(&buf[start_pos], offset)
    hton.pack_int32(&buf[start_pos + LENGTH_OFFSET], length)
    buf[start_pos + MAGIC_OFFSET] = magic
    buf[start_pos + ATTRIBUTES_OFFSET] = attributes
    if magic == 1:
        hton.pack_int64(&buf[start_pos + TIMESTAMP_OFFSET], timestamp)

    cutil.calc_crc32(
        0,
        <unsigned char*> &buf[start_pos + MAGIC_OFFSET],
        <size_t> (pos - (start_pos + MAGIC_OFFSET)),
        &crc
    )
    hton.pack_int32(&buf[start_pos + CRC_OFFSET], <int32_t> crc)

    crc_out[0] = <uint32_t> crc
    return 0


@cython.no_gc_clear
@cython.final
@cython.freelist(_LEGACY_RECORD_METADATA_FREELIST_SIZE)
cdef class LegacyRecordMetadata:

    cdef:
        readonly int64_t offset
        readonly uint32_t crc
        readonly Py_ssize_t size
        readonly int64_t timestamp

    def __init__(self, int64_t offset, uint32_t crc, Py_ssize_t size,
                 int64_t timestamp):
        self.offset = offset
        self.crc = crc
        self.size = size
        self.timestamp = timestamp

    @staticmethod
    cdef inline LegacyRecordMetadata new(
            int64_t offset, uint32_t crc, Py_ssize_t size,
            int64_t timestamp):
        """ Fast constructor to initialize from C.
        """
        cdef LegacyRecordMetadata metadata
        metadata = LegacyRecordMetadata.__new__(LegacyRecordMetadata)
        metadata.offset = offset
        metadata.crc = crc
        metadata.size = size
        metadata.timestamp = timestamp
        return metadata

    def __repr__(self):
        return (
            "LegacyRecordMetadata(offset={!r}, crc={!r}, size={!r},"
            " timestamp={!r})".format(
                self.offset, self.crc, self.size, self.timestamp)
        )
