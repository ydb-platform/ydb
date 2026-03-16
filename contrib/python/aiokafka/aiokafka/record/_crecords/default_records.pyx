#cython: language_level=3
# See:
# https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/\
#    apache/kafka/common/record/DefaultRecordBatch.java
# https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/\
#    apache/kafka/common/record/DefaultRecord.java

# RecordBatch and Record implementation for magic 2 and above.
# The schema is given below:

# RecordBatch =>
#  BaseOffset => Int64
#  Length => Int32
#  PartitionLeaderEpoch => Int32
#  Magic => Int8
#  CRC => Uint32
#  Attributes => Int16
#  LastOffsetDelta => Int32 // also serves as LastSequenceDelta
#  FirstTimestamp => Int64
#  MaxTimestamp => Int64
#  ProducerId => Int64
#  ProducerEpoch => Int16
#  BaseSequence => Int32
#  Records => [Record]

# Record =>
#   Length => Varint
#   Attributes => Int8
#   TimestampDelta => Varlong
#   OffsetDelta => Varint
#   Key => Bytes
#   Value => Bytes
#   Headers => [HeaderKey HeaderValue]
#     HeaderKey => String
#     HeaderValue => Bytes

# Note that when compression is enabled (see attributes below), the compressed
# record data is serialized directly following the count of the number of
# records. (ie Records => [Record], but without length bytes)

# The CRC covers the data from the attributes to the end of the batch (i.e. all
# the bytes that follow the CRC). It is located after the magic byte, which
# means that clients must parse the magic byte before deciding how to interpret
# the bytes between the batch length and the magic byte. The partition leader
# epoch field is not included in the CRC computation to avoid the need to
# recompute the CRC when this field is assigned for every batch that is
# received by the broker. The CRC-32C (Castagnoli) polynomial is used for the
# computation.

# The current RecordBatch attributes are given below:
#
# * Unused (6-15)
# * Control (5)
# * Transactional (4)
# * Timestamp Type (3)
# * Compression Type (0-2)

import aiokafka.codec as codecs
from aiokafka.codec import (
    gzip_encode, snappy_encode, lz4_encode, zstd_encode,
    gzip_decode, snappy_decode, lz4_decode, zstd_decode
)
from aiokafka.errors import CorruptRecordException, UnsupportedCodecError

from cpython cimport PyObject_GetBuffer, PyBuffer_Release, PyBUF_WRITABLE, \
                     PyBUF_SIMPLE, PyBUF_READ, Py_buffer, \
                     PyBytes_FromStringAndSize
from libc.stdint cimport int32_t, int64_t, uint32_t, int16_t
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

# Header structure offsets
DEF BASE_OFFSET_OFFSET = 0
DEF LENGTH_OFFSET = BASE_OFFSET_OFFSET + 8
DEF PARTITION_LEADER_EPOCH_OFFSET = LENGTH_OFFSET + 4
DEF MAGIC_OFFSET = PARTITION_LEADER_EPOCH_OFFSET + 4
DEF CRC_OFFSET = MAGIC_OFFSET + 1
DEF ATTRIBUTES_OFFSET = CRC_OFFSET + 4
DEF LAST_OFFSET_DELTA_OFFSET = ATTRIBUTES_OFFSET + 2
DEF FIRST_TIMESTAMP_OFFSET = LAST_OFFSET_DELTA_OFFSET + 4
DEF MAX_TIMESTAMP_OFFSET = FIRST_TIMESTAMP_OFFSET + 8
DEF PRODUCER_ID_OFFSET = MAX_TIMESTAMP_OFFSET + 8
DEF PRODUCER_EPOCH_OFFSET = PRODUCER_ID_OFFSET + 8
DEF BASE_SEQUENCE_OFFSET = PRODUCER_EPOCH_OFFSET + 2
DEF RECORD_COUNT_OFFSET = BASE_SEQUENCE_OFFSET + 4
DEF FIRST_RECORD_OFFSET = RECORD_COUNT_OFFSET + 4

# excluding key, value and headers:
# 5 bytes length + 10 bytes timestamp + 5 bytes offset + 1 byte attributes
DEF MAX_RECORD_OVERHEAD = 21

DEF NO_PARTITION_LEADER_EPOCH = -1


# Initialize CRC32 on import
cutil.crc32c_global_init()


cdef _assert_has_codec(char compression_type):
    if compression_type == _ATTR_CODEC_GZIP:
        checker, name = codecs.has_gzip, "gzip"
    elif compression_type == _ATTR_CODEC_SNAPPY:
        checker, name = codecs.has_snappy, "snappy"
    elif compression_type == _ATTR_CODEC_LZ4:
        checker, name = codecs.has_lz4, "lz4"
    elif compression_type == _ATTR_CODEC_ZSTD:
        checker, name = codecs.has_zstd, "zstd"
    else:
        raise UnsupportedCodecError(
            f"Unknown compression codec {compression_type:#04x}")
    if not checker():
        raise UnsupportedCodecError(
            f"Libraries for {name} compression codec not found")


@cython.no_gc_clear
@cython.final
@cython.freelist(_DEFAULT_RECORD_BATCH_FREELIST_SIZE)
cdef class DefaultRecordBatch:

    CODEC_NONE = _ATTR_CODEC_NONE
    CODEC_MASK = _ATTR_CODEC_MASK
    CODEC_GZIP = _ATTR_CODEC_GZIP
    CODEC_SNAPPY = _ATTR_CODEC_SNAPPY
    CODEC_LZ4 = _ATTR_CODEC_LZ4
    CODEC_ZSTD = _ATTR_CODEC_ZSTD

    def __init__(self, object buffer):
        PyObject_GetBuffer(buffer, &self._buffer, PyBUF_SIMPLE)
        self._decompressed = 0
        self._read_header()
        self._pos = FIRST_RECORD_OFFSET
        self._next_record_index = 0

    @staticmethod
    cdef inline DefaultRecordBatch new(
            bytes buffer, Py_ssize_t pos, Py_ssize_t slice_end, char magic):
        """ Fast constructor to initialize from C.
            NOTE: We take ownership of the Py_buffer object, so caller does not
                  need to call PyBuffer_Release.
        """
        cdef:
            DefaultRecordBatch batch
            char* buf
        batch = DefaultRecordBatch.__new__(DefaultRecordBatch)
        PyObject_GetBuffer(buffer, &batch._buffer, PyBUF_SIMPLE)
        buf = <char *>batch._buffer.buf
        # Change the buffer to include a proper slice
        batch._buffer.buf = <void *> &buf[pos]
        batch._buffer.len = slice_end - pos

        batch._decompressed = 0
        batch._read_header()
        batch._pos = FIRST_RECORD_OFFSET
        batch._next_record_index = 0
        return batch


    def __dealloc__(self):
        PyBuffer_Release(&self._buffer)

    @property
    def compression_type(self):
        return self.attributes & _ATTR_CODEC_MASK

    @property
    def is_transactional(self):
        if self.attributes & _TRANSACTIONAL_MASK:
            return True
        else:
            return False

    @property
    def is_control_batch(self):
        if self.attributes & _CONTROL_MASK:
            return True
        else:
            return False

    @property
    def next_offset(self):
        return self.base_offset + self.last_offset_delta + 1

    cdef inline _read_header(self):
        # NOTE: We move the data to it's slot in the structure because we will
        #       lose the header part after decompression.

        cdef:
            char* buf

        buf = <char*> self._buffer.buf
        self.base_offset = hton.unpack_int64(&buf[BASE_OFFSET_OFFSET])
        self.length = hton.unpack_int32(&buf[LENGTH_OFFSET])
        self.magic = buf[MAGIC_OFFSET]
        self.crc = <uint32_t> hton.unpack_int32(&buf[CRC_OFFSET])
        self.attributes = hton.unpack_int16(&buf[ATTRIBUTES_OFFSET])
        self.last_offset_delta = \
            hton.unpack_int32(&buf[LAST_OFFSET_DELTA_OFFSET])
        self.first_timestamp = \
            hton.unpack_int64(&buf[FIRST_TIMESTAMP_OFFSET])
        self.max_timestamp = \
            hton.unpack_int64(&buf[MAX_TIMESTAMP_OFFSET])
        self.num_records = hton.unpack_int32(&buf[RECORD_COUNT_OFFSET])

        self.producer_id = hton.unpack_int64(&buf[PRODUCER_ID_OFFSET])
        self.producer_epoch = hton.unpack_int16(&buf[PRODUCER_EPOCH_OFFSET])
        self.base_sequence = hton.unpack_int32(&buf[BASE_SEQUENCE_OFFSET])

        if self.attributes & _TIMESTAMP_TYPE_MASK:
            self.timestamp_type = 1
        else:
            self.timestamp_type = 0

    cdef _maybe_uncompress(self):
        cdef:
            char compression_type
            char* buf
        if not self._decompressed:
            compression_type = <char> self.attributes & _ATTR_CODEC_MASK
            if compression_type != _ATTR_CODEC_NONE:
                _assert_has_codec(compression_type)
                buf = <char *> self._buffer.buf
                data = PyMemoryView_FromMemory(
                    &buf[self._pos],
                    self._buffer.len - self._pos,
                    PyBUF_READ)
                if compression_type == _ATTR_CODEC_GZIP:
                    uncompressed = gzip_decode(data)
                elif compression_type == _ATTR_CODEC_SNAPPY:
                    uncompressed = snappy_decode(data.tobytes())
                elif compression_type == _ATTR_CODEC_LZ4:
                    uncompressed = lz4_decode(data.tobytes())
                elif compression_type == _ATTR_CODEC_ZSTD:
                    uncompressed = zstd_decode(data.tobytes())

                PyBuffer_Release(&self._buffer)
                PyObject_GetBuffer(uncompressed, &self._buffer, PyBUF_SIMPLE)
                self._pos = 0
        self._decompressed = 1

    cdef inline int _check_bounds(
            self, Py_ssize_t pos, Py_ssize_t size) except -1:
        """ Confirm that the slice is not outside buffer range
        """
        if pos + size > self._buffer.len:
            raise CorruptRecordException(
                "Can't read {} bytes from pos {}".format(size, pos))

    cdef DefaultRecord _read_msg(self):
        # Record =>
        #   Length => Varint
        #   Attributes => Int8
        #   TimestampDelta => Varlong
        #   OffsetDelta => Varint
        #   Key => Bytes
        #   Value => Bytes
        #   Headers => [HeaderKey HeaderValue]
        #     HeaderKey => String
        #     HeaderValue => Bytes

        cdef:
            Py_ssize_t pos = self._pos
            char* buf

            int64_t length
            int64_t attrs
            int64_t ts_delta
            int64_t offset_delta
            int64_t key_len
            int64_t value_len
            int64_t header_count
            Py_ssize_t start_pos

            int64_t offset
            int64_t timestamp
            object key
            object value
            list headers
            bytes h_key
            object h_value

        # Minimum record size check

        buf = <char*> self._buffer.buf
        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &length)
        start_pos = pos
        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &attrs)

        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &ts_delta)
        if self.attributes & _TIMESTAMP_TYPE_MASK:  # LOG_APPEND_TIME
            timestamp = self.max_timestamp
        else:
            timestamp = self.first_timestamp + ts_delta

        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &offset_delta)
        offset = self.base_offset + offset_delta

        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &key_len)
        if key_len >= 0:
            self._check_bounds(pos, <Py_ssize_t> key_len)
            key = PyBytes_FromStringAndSize(
                &buf[pos], <Py_ssize_t> key_len)
            pos += <Py_ssize_t> key_len
        else:
            key = None

        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &value_len)
        if value_len >= 0:
            self._check_bounds(pos, <Py_ssize_t> value_len)
            value = PyBytes_FromStringAndSize(
                &buf[pos], <Py_ssize_t> value_len)
            pos += <Py_ssize_t> value_len
        else:
            value = None

        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &header_count)
        if header_count < 0:
            raise CorruptRecordException("Found invalid number of record "
                                         "headers {}".format(header_count))
        headers = []
        while header_count > 0:
            # Header key is of type String, that can't be None
            cutil.decode_varint64(buf, &pos, &key_len)
            if key_len < 0:
                raise CorruptRecordException(
                    "Invalid negative header key size %d" % (key_len, ))
            self._check_bounds(pos, <Py_ssize_t> key_len)
            h_key = PyBytes_FromStringAndSize(
                &buf[pos], <Py_ssize_t> key_len)
            pos += <Py_ssize_t> key_len

            # Value is of type NULLABLE_BYTES, so it can be None
            cutil.decode_varint64(buf, &pos, &value_len)
            if value_len >= 0:
                self._check_bounds(pos, <Py_ssize_t> value_len)
                h_value = PyBytes_FromStringAndSize(
                    &buf[pos], <Py_ssize_t> value_len)
                pos += <Py_ssize_t> value_len
            else:
                h_value = None

            headers.append((h_key.decode("utf-8"), h_value))
            header_count -= 1

        # validate whether we have read all bytes in the current record
        if pos - start_pos != <Py_ssize_t> length:
            raise CorruptRecordException(
                "Invalid record size: expected to read {} bytes in record "
                "payload, but instead read {}".format(length, pos - start_pos))
        self._pos = pos

        return DefaultRecord.new(
            offset, timestamp, self.timestamp_type, key, value, headers)

    def __iter__(self):
        assert self._next_record_index == 0
        self._maybe_uncompress()
        return self

    def __next__(self):
        if self._next_record_index >= self.num_records:
            if self._pos != self._buffer.len:
                raise CorruptRecordException(
                    "{} unconsumed bytes after all records consumed".format(
                        self._buffer.len - self._pos))
            self._next_record_index = 0
            raise StopIteration

        msg = self._read_msg()
        self._next_record_index += 1
        return msg

    # NOTE from CYTHON docs:
    # ```
    #    Do NOT explicitly give your type a next() method, or bad things
    #    could happen.
    # ```

    def validate_crc(self):
        assert self._decompressed == 0, \
            "Validate should be called before iteration"

        cdef:
            uint32_t verify_crc = 0
            char* buf

        crc = self.crc
        buf = <char*> self._buffer.buf
        cutil.calc_crc32c(
            0,
            &buf[ATTRIBUTES_OFFSET],
            <size_t> self._buffer.len - ATTRIBUTES_OFFSET,
            &verify_crc
        )
        return crc == verify_crc


@cython.no_gc_clear
@cython.final
@cython.freelist(_DEFAULT_RECORD_FREELIST_SIZE)
cdef class DefaultRecord:

    # V2 does not include a record checksum
    checksum = None

    def __init__(self, int64_t offset, int64_t timestamp, char timestamp_type,
                 object key, object value, object headers):
        self.offset = offset
        self.timestamp = timestamp
        self.timestamp_type = timestamp_type
        self.key = key
        self.value = value
        self.headers = headers

    @staticmethod
    cdef inline DefaultRecord new(
            int64_t offset, int64_t timestamp, char timestamp_type,
            object key, object value, object headers):
        """ Fast constructor to initialize from C.
        """
        cdef DefaultRecord record
        record = DefaultRecord.__new__(DefaultRecord)
        record.offset = offset
        record.timestamp = timestamp
        record.timestamp_type = timestamp_type
        record.key = key
        record.value = value
        record.headers = headers
        return record

    def __repr__(self):
        return (
            "DefaultRecord(offset={!r}, timestamp={!r}, timestamp_type={!r},"
            " key={!r}, value={!r}, headers={!r})".format(
                self.offset, self.timestamp, self.timestamp_type,
                self.key, self.value, self.headers)
        )


cdef class DefaultRecordBatchBuilder:

    cdef:
        char _magic
        char _compression_type
        Py_ssize_t _batch_size
        Py_ssize_t _pos
        bytearray _buffer

        char _is_transactional
        readonly int64_t producer_id
        readonly int16_t producer_epoch
        readonly int32_t base_sequence

        int64_t _first_timestamp
        int64_t _max_timestamp
        int32_t _last_offset
        int32_t _num_records



    def __cinit__(
            self, char magic, char compression_type, char is_transactional,
            int64_t producer_id, int16_t producer_epoch,
            int32_t base_sequence, Py_ssize_t batch_size):
        assert magic >= 2
        self._magic = magic
        self._compression_type = compression_type & _ATTR_CODEC_MASK
        self._batch_size = batch_size
        self._is_transactional = is_transactional
        # KIP-98 fields for EOS
        self.producer_id = producer_id
        self.producer_epoch = producer_epoch
        self.base_sequence = base_sequence

        self._first_timestamp = -1
        self._max_timestamp = -1
        self._last_offset = 0
        self._num_records = 0

        self._buffer = bytearray(FIRST_RECORD_OFFSET)
        self._pos = FIRST_RECORD_OFFSET

    def set_producer_state(
            self, int64_t producer_id, int16_t producer_epoch,
            int32_t base_sequence):
        self.producer_id = producer_id
        self.producer_epoch = producer_epoch
        self.base_sequence = base_sequence

    cdef int16_t _get_attributes(self, int include_compression_type):
        cdef:
            int16_t attrs
        attrs = 0
        if include_compression_type:
            attrs = <int16_t> (attrs | <int16_t> self._compression_type)
        # Timestamp Type is set by Broker
        if self._is_transactional:
            attrs |= _TRANSACTIONAL_MASK
        # Control batches are only created by Broker
        return attrs

    def append(self, int64_t offset, timestamp, key, value, list headers):
        """ Write message to messageset buffer with MsgVersion 2
        """
        cdef:
            Py_ssize_t pos
            Py_ssize_t size
            Py_ssize_t msg_size
            char *buf
            int64_t ts

        # Check types
        if timestamp is None:
            ts = cutil.get_time_as_unix_ms()
        else:
            ts = timestamp

        # Check if we have room for another message
        pos = self._pos
        msg_size = self._size_of_body(offset, ts, key, value, headers)
        size = msg_size + cutil.size_of_varint(msg_size)
        # We always allow at least one record to be appended
        if offset != 0 and pos + size >= self._batch_size:
            return None

        # Allocate proper buffer length
        PyByteArray_Resize(self._buffer, pos + size)

        # Encode message
        buf = PyByteArray_AS_STRING(self._buffer)
        self._encode_msg(pos, buf, offset, ts, msg_size, key, value, headers)
        self._pos = pos + size

        return DefaultRecordMetadata.new(offset, size, ts)

    cdef int _encode_msg(
            self, Py_ssize_t pos, char* buf, int64_t offset, int64_t timestamp,
            Py_ssize_t msg_size, object key, object value, list headers
            ) except -1:
        cdef:
            int64_t timestamp_delta
            char first_message
            Py_buffer tmp_buf
            Py_ssize_t header_count
            tuple header
            str h_key
            bytes h_value

        # We will always add the first message, so those will be set
        if self._first_timestamp == -1:
            self._first_timestamp = timestamp
            self._max_timestamp = timestamp
            timestamp_delta = 0
            first_message = 1
        else:
            timestamp_delta = timestamp - self._first_timestamp
            first_message = 0

        cutil.encode_varint(buf, &pos, msg_size)  #   Length => Varint

        buf[pos] = 0  #   Attributes => Int8
        pos += 1

        cutil.encode_varint64(buf, &pos, timestamp_delta)
        # Base offset is always 0 on Produce
        cutil.encode_varint64(buf, &pos, offset)

        if key is None:
            cutil.encode_varint64(buf, &pos, -1)
        else:
            PyObject_GetBuffer(key, &tmp_buf, PyBUF_SIMPLE)
            cutil.encode_varint(buf, &pos, tmp_buf.len)
            memcpy(&buf[pos], <char*>tmp_buf.buf, <size_t>tmp_buf.len)
            pos += tmp_buf.len
            PyBuffer_Release(&tmp_buf)

        if value is None:
            cutil.encode_varint64(buf, &pos, -1)
        else:
            PyObject_GetBuffer(value, &tmp_buf, PyBUF_SIMPLE)
            cutil.encode_varint(buf, &pos, tmp_buf.len)
            memcpy(&buf[pos], <char*>tmp_buf.buf, <size_t>tmp_buf.len)
            pos += tmp_buf.len
            PyBuffer_Release(&tmp_buf)

        header_count = len(headers)
        cutil.encode_varint(buf, &pos, header_count)
        for header in headers:
            h_key, h_value = header

            PyObject_GetBuffer(h_key.encode("utf-8"), &tmp_buf, PyBUF_SIMPLE)
            cutil.encode_varint(buf, &pos, tmp_buf.len)
            memcpy(&buf[pos], <char*>tmp_buf.buf, <size_t>tmp_buf.len)
            pos += tmp_buf.len
            PyBuffer_Release(&tmp_buf)

            if h_value is None:
                cutil.encode_varint64(buf, &pos, -1)
            else:
                PyObject_GetBuffer(h_value, &tmp_buf, PyBUF_SIMPLE)
                cutil.encode_varint(buf, &pos, tmp_buf.len)
                memcpy(&buf[pos], <char*>tmp_buf.buf, <size_t>tmp_buf.len)
                pos += tmp_buf.len
                PyBuffer_Release(&tmp_buf)

        # Those should be updated after the length check
        if self._max_timestamp < timestamp:
            self._max_timestamp = timestamp
        self._num_records += 1
        # Offset is counted from 0, so no loss should be here
        self._last_offset = <int32_t> offset
        return 0

    cdef _write_header(self, int use_compression_type):
        cdef:
            char *buf
            uint32_t crc = 0

        buf = PyByteArray_AS_STRING(self._buffer)
        # Proper BaseOffset will be set by broker
        hton.pack_int64(&buf[BASE_OFFSET_OFFSET], 0)
        # Size from here to end
        hton.pack_int32(
            &buf[LENGTH_OFFSET],
            <int32_t> self._pos - PARTITION_LEADER_EPOCH_OFFSET)
        # PartitionLeaderEpoch, set by broker
        hton.pack_int32(
            &buf[PARTITION_LEADER_EPOCH_OFFSET], NO_PARTITION_LEADER_EPOCH)
        buf[MAGIC_OFFSET] = self._magic
        # CRC will be set below
        hton.pack_int16(
            &buf[ATTRIBUTES_OFFSET],
            self._get_attributes(use_compression_type))
        hton.pack_int32(&buf[LAST_OFFSET_DELTA_OFFSET], self._last_offset)
        hton.pack_int64(&buf[FIRST_TIMESTAMP_OFFSET], self._first_timestamp)
        hton.pack_int64(&buf[MAX_TIMESTAMP_OFFSET], self._max_timestamp)
        hton.pack_int64(&buf[PRODUCER_ID_OFFSET], self.producer_id)
        hton.pack_int16(&buf[PRODUCER_EPOCH_OFFSET], self.producer_epoch)
        hton.pack_int32(&buf[BASE_SEQUENCE_OFFSET], self.base_sequence)
        hton.pack_int32(&buf[RECORD_COUNT_OFFSET], self._num_records)

        cutil.calc_crc32c(
            0,
            <char *> &buf[ATTRIBUTES_OFFSET],
            <size_t> (self._pos - ATTRIBUTES_OFFSET),
            &crc
        )
        hton.pack_int32(&buf[CRC_OFFSET], <int32_t> crc)

    cdef int _maybe_compress(self) except -1:
        cdef:
            object compressed
            char *buf
            Py_ssize_t size
            Py_ssize_t header_size
            bytes data


        if self._compression_type != _ATTR_CODEC_NONE:
            _assert_has_codec(self._compression_type)
            data = bytes(self._buffer[FIRST_RECORD_OFFSET:self._pos])
            if self._compression_type == _ATTR_CODEC_GZIP:
                compressed = gzip_encode(data)
            elif self._compression_type == _ATTR_CODEC_SNAPPY:
                compressed = snappy_encode(data)
            elif self._compression_type == _ATTR_CODEC_LZ4:
                compressed = lz4_encode(data)
            elif self._compression_type == _ATTR_CODEC_ZSTD:
                compressed = zstd_encode(data)
            size = (<Py_ssize_t> len(compressed)) + FIRST_RECORD_OFFSET
            # We will just write the result into the same memory space.
            PyByteArray_Resize(self._buffer, size)

            self._buffer[FIRST_RECORD_OFFSET:size] = compressed
            self._pos = size
            return 1
        return 0

    def build(self):
        cdef:
            int send_compressed
        send_compressed = self._maybe_compress()
        self._write_header(send_compressed)
        PyByteArray_Resize(self._buffer, self._pos)
        return self._buffer

    def size(self):
        """ Return current size of data written to buffer
        """
        return self._pos

    cdef Py_ssize_t _size_of_body(
            self, int64_t offset, int64_t timestamp, object key, object value,
            list headers
            ) except -1:
        cdef:
            Py_ssize_t size_of_body
            int64_t timestamp_delta

        if self._first_timestamp != -1:
            timestamp_delta = timestamp - self._first_timestamp
        else:
            timestamp_delta = 0
        size_of_body = (
            1 +  # Attrs
            cutil.size_of_varint64(offset) +
            cutil.size_of_varint64(timestamp_delta) +
            _size_of(key, value, headers)
        )
        return size_of_body

    def size_in_bytes(self, offset, timestamp, key, value, headers):
        cdef:
            Py_ssize_t size
        size = self._size_of_body(offset, timestamp, key, value, headers)
        return size + cutil.size_of_varint(size)

    @classmethod
    def size_of(cls, key, value, headers):
        return _size_of(key, value, headers)

    @classmethod
    def estimate_size_in_bytes(cls, key, value, headers):
        """ Get the upper bound estimate on the size of record
        """
        return (
            FIRST_RECORD_OFFSET + MAX_RECORD_OVERHEAD +
            _size_of(key, value, headers)
        )


cdef inline Py_ssize_t _bytelike_len(object obj) except -2:
    cdef:
        Py_buffer buf
        Py_ssize_t obj_len

    if obj is None:
        return -1
    else:
        PyObject_GetBuffer(obj, &buf, PyBUF_SIMPLE)
        obj_len = buf.len
        PyBuffer_Release(&buf)
    return obj_len


cdef Py_ssize_t _size_of(object key, object value, list headers) except -1:
    cdef:
        Py_ssize_t key_len
        Py_ssize_t value_len
        Py_ssize_t size = 0
        Py_ssize_t header_count
        Py_ssize_t i
        tuple header
        str h_key
        bytes h_value

    if key is None:
        size += 1
    else:
        key_len = _bytelike_len(key)
        size += cutil.size_of_varint(key_len) + key_len
    # Value size
    if value is None:
        size += 1
    else:
        value_len = _bytelike_len(value)
        size += cutil.size_of_varint(value_len) + value_len
    # Header size
    header_count = len(headers)
    size += cutil.size_of_varint(header_count)
    for header in headers:
        h_key, h_value = header
        key_len = _bytelike_len(h_key.encode("utf-8"))
        size += cutil.size_of_varint(key_len) + key_len

        if h_value is None:
            size += 1
        else:
            value_len = _bytelike_len(h_value)
            size += cutil.size_of_varint(value_len) + value_len
    return size


@cython.no_gc_clear
@cython.final
@cython.freelist(_DEFAULT_RECORD_METADATA_FREELIST_SIZE)
cdef class DefaultRecordMetadata:

    cdef:
        readonly int64_t offset
        readonly Py_ssize_t size
        readonly int64_t timestamp

    crc = None

    def __init__(self, int64_t offset, Py_ssize_t size, int64_t timestamp):
        self.offset = offset
        self.size = size
        self.timestamp = timestamp

    @staticmethod
    cdef inline DefaultRecordMetadata new(
            int64_t offset, Py_ssize_t size, int64_t timestamp):
        """ Fast constructor to initialize from C.
        """
        cdef DefaultRecordMetadata metadata
        metadata = DefaultRecordMetadata.__new__(DefaultRecordMetadata)
        metadata.offset = offset
        metadata.size = size
        metadata.timestamp = timestamp
        return metadata

    def __repr__(self):
        return (
            "DefaultRecordMetadata(offset={!r}, size={!r}, timestamp={!r})"
            .format(self.offset, self.size, self.timestamp)
        )
