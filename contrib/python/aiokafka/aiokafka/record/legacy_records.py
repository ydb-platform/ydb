from __future__ import annotations

import struct
import time
from binascii import crc32
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, Literal, final

from typing_extensions import Never

import aiokafka.codec as codecs
from aiokafka.codec import (
    gzip_decode,
    gzip_encode,
    lz4_decode,
    lz4_encode,
    snappy_decode,
    snappy_encode,
)
from aiokafka.errors import CorruptRecordException, UnsupportedCodecError
from aiokafka.util import NO_EXTENSIONS

from ._protocols import (
    LegacyRecordBatchBuilderProtocol,
    LegacyRecordBatchProtocol,
    LegacyRecordMetadataProtocol,
    LegacyRecordProtocol,
)


class LegacyRecordBase:
    __slots__ = ()

    HEADER_STRUCT_V0 = struct.Struct(
        ">q"  # BaseOffset => Int64
        "i"  # Length => Int32
        "I"  # CRC => Int32
        "b"  # Magic => Int8
        "b"  # Attributes => Int8
    )
    HEADER_STRUCT_V1 = struct.Struct(
        ">q"  # BaseOffset => Int64
        "i"  # Length => Int32
        "I"  # CRC => Int32
        "b"  # Magic => Int8
        "b"  # Attributes => Int8
        "q"  # timestamp => Int64
    )

    LOG_OVERHEAD = CRC_OFFSET = struct.calcsize(
        ">q"  # Offset
        "i"  # Size
    )
    MAGIC_OFFSET = LOG_OVERHEAD + struct.calcsize(">I")  # CRC
    # Those are used for fast size calculations
    RECORD_OVERHEAD_V0 = struct.calcsize(
        ">I"  # CRC
        "b"  # magic
        "b"  # attributes
        "i"  # Key length
        "i"  # Value length
    )
    RECORD_OVERHEAD_V1 = struct.calcsize(
        ">I"  # CRC
        "b"  # magic
        "b"  # attributes
        "q"  # timestamp
        "i"  # Key length
        "i"  # Value length
    )
    RECORD_OVERHEAD = {
        0: RECORD_OVERHEAD_V0,
        1: RECORD_OVERHEAD_V1,
    }

    KEY_OFFSET_V0 = HEADER_STRUCT_V0.size
    KEY_OFFSET_V1 = HEADER_STRUCT_V1.size
    KEY_LENGTH = VALUE_LENGTH = struct.calcsize(">i")  # Bytes length is Int32

    CODEC_MASK = 0x07
    CODEC_GZIP = 0x01
    CODEC_SNAPPY = 0x02
    CODEC_LZ4 = 0x03
    TIMESTAMP_TYPE_MASK = 0x08

    LOG_APPEND_TIME = 1
    CREATE_TIME = 0

    def _assert_has_codec(self, compression_type: int) -> bool:
        if compression_type == self.CODEC_GZIP:
            checker, name = codecs.has_gzip, "gzip"
        elif compression_type == self.CODEC_SNAPPY:
            checker, name = codecs.has_snappy, "snappy"
        elif compression_type == self.CODEC_LZ4:
            checker, name = codecs.has_lz4, "lz4"
        else:
            raise UnsupportedCodecError(
                f"Unknown compression codec {compression_type:#04x}"
            )
        if not checker():
            raise UnsupportedCodecError(
                f"Libraries for {name} compression codec not found"
            )
        return True


@final
class _LegacyRecordBatchPy(LegacyRecordBase, LegacyRecordBatchProtocol):
    is_control_batch: bool = False
    is_transactional: bool = False
    producer_id: int | None = None

    def __init__(self, buffer: bytes | bytearray | memoryview, magic: int):
        self._buffer = memoryview(buffer)
        self._magic = magic

        offset, length, crc, magic_, attrs, timestamp = self._read_header(0)
        assert length == len(buffer) - self.LOG_OVERHEAD
        assert magic == magic_

        self._offset = offset
        self._crc = crc
        self._timestamp = timestamp
        self._attributes = attrs
        self._decompressed = False

    @property
    def _timestamp_type(self) -> Literal[0, 1] | None:
        """0 for CreateTime; 1 for LogAppendTime; None if unsupported.

        Value is determined by broker; produced messages should always set to 0
        Requires Kafka >= 0.10 / message version >= 1
        """
        if self._magic == 0:
            return None
        elif self._attributes & self.TIMESTAMP_TYPE_MASK:
            return 1
        else:
            return 0

    @property
    def _compression_type(self) -> int:
        return self._attributes & self.CODEC_MASK

    @property
    def next_offset(self) -> int:
        return self._offset + 1

    def validate_crc(self) -> bool:
        crc = crc32(self._buffer[self.MAGIC_OFFSET :])
        return self._crc == crc

    def _decompress(self, key_offset: int) -> bytes:
        # Copy of `_read_key_value`, but uses memoryview
        pos = key_offset
        key_size = struct.unpack_from(">i", self._buffer, pos)[0]
        pos += self.KEY_LENGTH
        if key_size != -1:
            pos += key_size
        value_size = struct.unpack_from(">i", self._buffer, pos)[0]
        pos += self.VALUE_LENGTH
        if value_size == -1:
            raise CorruptRecordException("Value of compressed message is None")
        else:
            data = self._buffer[pos : pos + value_size]

        compression_type = self._compression_type
        assert self._assert_has_codec(compression_type)
        if compression_type == self.CODEC_GZIP:
            uncompressed = gzip_decode(data)
        elif compression_type == self.CODEC_SNAPPY:
            uncompressed = snappy_decode(data.tobytes())
        elif compression_type == self.CODEC_LZ4:
            if self._magic == 0:
                # https://issues.apache.org/jira/browse/KAFKA-3160
                raise UnsupportedCodecError(
                    "LZ4 is not supported for broker version 0.8/0.9"
                )
            else:
                uncompressed = lz4_decode(data.tobytes())
        else:
            # Must not be possible
            raise RuntimeError(f"Invalid compression codec {compression_type:#04x}")
        return uncompressed

    def _read_header(self, pos: int) -> tuple[int, int, int, int, int, int | None]:
        if self._magic == 0:
            offset, length, crc, magic_read, attrs = self.HEADER_STRUCT_V0.unpack_from(
                self._buffer, pos
            )
            timestamp = None
        else:
            (
                offset,
                length,
                crc,
                magic_read,
                attrs,
                timestamp,
            ) = self.HEADER_STRUCT_V1.unpack_from(self._buffer, pos)
        return offset, length, crc, magic_read, attrs, timestamp

    def _read_all_headers(
        self,
    ) -> list[tuple[tuple[int, int, int, int, int, int | None], int]]:
        pos = 0
        msgs: list[tuple[tuple[int, int, int, int, int, int | None], int]] = []
        buffer_len = len(self._buffer)
        while pos < buffer_len:
            header = self._read_header(pos)
            msgs.append((header, pos))
            pos += self.LOG_OVERHEAD + header[1]  # length
        return msgs

    def _read_key_value(self, pos: int) -> tuple[bytes | None, bytes | None]:
        key_size: int = struct.unpack_from(">i", self._buffer, pos)[0]
        pos += self.KEY_LENGTH
        if key_size == -1:
            key = None
        else:
            key = self._buffer[pos : pos + key_size].tobytes()
            pos += key_size

        value_size: int = struct.unpack_from(">i", self._buffer, pos)[0]
        pos += self.VALUE_LENGTH
        if value_size == -1:
            value = None
        else:
            value = self._buffer[pos : pos + value_size].tobytes()
        return key, value

    def __iter__(self) -> Generator[_LegacyRecordPy, None, None]:
        if self._magic == 1:
            key_offset = self.KEY_OFFSET_V1
        else:
            key_offset = self.KEY_OFFSET_V0
        timestamp_type = self._timestamp_type

        if self._compression_type:
            # In case we will call iter again
            if not self._decompressed:
                self._buffer = memoryview(self._decompress(key_offset))
                self._decompressed = True

            # If relative offset is used, we need to decompress the entire
            # message first to compute the absolute offset.
            headers = self._read_all_headers()
            if self._magic > 0:
                msg_header, _ = headers[-1]
                absolute_base_offset = self._offset - msg_header[0]
            else:
                absolute_base_offset = -1

            for header, msg_pos in headers:
                offset, _, crc, _, attrs, timestamp = header
                # There should only ever be a single layer of compression
                assert not attrs & self.CODEC_MASK, (
                    f"MessageSet at offset {offset} appears double-compressed. This "
                    "should not happen -- check your producers!"
                )

                # When magic value is greater than 0, the timestamp
                # of a compressed message depends on the
                # typestamp type of the wrapper message:
                if timestamp_type == self.LOG_APPEND_TIME:
                    timestamp = self._timestamp

                if absolute_base_offset >= 0:
                    offset += absolute_base_offset

                key, value = self._read_key_value(msg_pos + key_offset)
                yield _LegacyRecordPy(
                    offset, timestamp, timestamp_type, key, value, crc
                )
        else:
            key, value = self._read_key_value(key_offset)
            yield _LegacyRecordPy(
                self._offset, self._timestamp, timestamp_type, key, value, self._crc
            )


@final
@dataclass(frozen=True)
class _LegacyRecordPy(LegacyRecordProtocol):
    __slots__ = ("crc", "key", "offset", "timestamp", "timestamp_type", "value")

    offset: int
    timestamp: int | None
    timestamp_type: Literal[0, 1] | None
    key: bytes | None
    value: bytes | None
    crc: int

    @property
    def headers(self) -> list[Never]:
        return []

    @property
    def checksum(self) -> int:
        return self.crc

    def __repr__(self) -> str:
        return (
            f"LegacyRecord(offset={self.offset!r}, timestamp={self.timestamp!r},"
            f" timestamp_type={self.timestamp_type!r},"
            f" key={self.key!r}, value={self.value!r}, crc={self.crc!r})"
        )


@final
class _LegacyRecordBatchBuilderPy(LegacyRecordBase, LegacyRecordBatchBuilderProtocol):
    _buffer: bytearray | None = None

    def __init__(
        self,
        magic: Literal[0, 1],
        compression_type: int,
        batch_size: int,
    ) -> None:
        assert magic in [0, 1]
        self._magic = magic
        self._compression_type = compression_type
        self._batch_size = batch_size
        self._msg_buffers: list[bytearray] = []
        self._pos = 0

    def append(
        self,
        offset: int,
        timestamp: int | None,
        key: bytes | None,
        value: bytes | None,
        headers: Any = None,
    ) -> _LegacyRecordMetadataPy | None:
        """Append message to batch."""
        if self._magic == 0:
            timestamp = -1
        elif timestamp is None:
            timestamp = int(time.time() * 1000)

        # calculating length is not cheap; only do it once
        key_size = len(key) if key is not None else 0
        value_size = len(value) if value is not None else 0

        pos = self._pos
        size = self._size_in_bytes(key_size, value_size)

        # always allow at least one record to be appended
        if offset != 0 and pos + size >= self._batch_size:
            return None

        msg_buf = bytearray(size)
        try:
            crc = self._encode_msg(
                msg_buf, offset, timestamp, key_size, key, value_size, value
            )
            self._msg_buffers.append(msg_buf)
            self._pos += size
            return _LegacyRecordMetadataPy(offset, crc, size, timestamp)

        except struct.error as exc:
            # perform expensive type checking only to translate struct errors
            # to human-readable messages
            if not isinstance(offset, int):
                raise TypeError(offset) from exc
            if not isinstance(timestamp, int):
                raise TypeError(timestamp) from exc
            if not isinstance(key, bytes | None):
                raise TypeError(f"Unsupported type for key: {type(key)}") from exc
            if not isinstance(value, bytes | None):
                raise TypeError(f"Unsupported type for value: {type(value)}") from exc
            raise

    def _encode_msg(
        self,
        buf: bytearray,
        offset: int,
        timestamp: int,
        key_size: int,
        key: bytes | None,
        value_size: int,
        value: bytes | None,
        attributes: int = 0,
    ) -> int:
        """Encode msg data into the `msg_buffer`, which should be allocated
        to at least the size of this message.
        """
        magic = self._magic
        length = (
            self.KEY_LENGTH
            + key_size
            + self.VALUE_LENGTH
            + value_size
            - self.LOG_OVERHEAD
        )

        if magic == 0:
            length += self.KEY_OFFSET_V0
            struct.pack_into(
                ">q"  # BaseOffset => Int64
                "i"  # Length => Int32
                "I"  # CRC => Int32
                "b"  # Magic => Int8
                "b"  # Attributes => Int8
                "i"  # key length => Int32
                f"{key_size:d}s"  # key => bytes
                "i"  # value length => Int32
                f"{value_size:d}s",  # value => bytes
                buf,
                0,
                offset,
                length,
                0,
                magic,
                attributes,
                key_size if key is not None else -1,
                key or b"",
                value_size if value is not None else -1,
                value or b"",
            )
        else:
            length += self.KEY_OFFSET_V1
            struct.pack_into(
                ">q"  # BaseOffset => Int64
                "i"  # Length => Int32
                "I"  # CRC => Int32
                "b"  # Magic => Int8
                "b"  # Attributes => Int8
                "q"  # timestamp => Int64
                "i"  # key length => Int32
                f"{key_size:d}s"  # key => bytes
                "i"  # value length => Int32
                f"{value_size:d}s",  # value => bytes
                buf,
                0,
                offset,
                length,
                0,
                magic,
                attributes,
                timestamp,
                key_size if key is not None else -1,
                key or b"",
                value_size if value is not None else -1,
                value or b"",
            )

        crc = crc32(memoryview(buf)[self.MAGIC_OFFSET :])
        struct.pack_into(">I", buf, self.CRC_OFFSET, crc)
        return crc

    def _maybe_compress(self) -> bool:
        if self._compression_type:
            assert self._buffer is not None
            assert self._assert_has_codec(self._compression_type)
            buf = self._buffer
            if self._compression_type == self.CODEC_GZIP:
                compressed = gzip_encode(buf)
            elif self._compression_type == self.CODEC_SNAPPY:
                compressed = snappy_encode(buf)
            elif self._compression_type == self.CODEC_LZ4:
                if self._magic == 0:
                    # https://issues.apache.org/jira/browse/KAFKA-3160
                    raise UnsupportedCodecError(
                        "LZ4 is not supported for broker version 0.8/0.9"
                    )
                else:
                    compressed = lz4_encode(bytes(buf))

            else:
                # Must not be possible
                raise RuntimeError(
                    f"Invalid compression codec {self._compression_type:#04x}"
                )
            compressed_size = len(compressed)
            size = self._size_in_bytes(key_size=0, value_size=compressed_size)
            if size > len(self._buffer):
                self._buffer = bytearray(size)
            else:
                del self._buffer[size:]
            self._encode_msg(
                self._buffer,
                offset=0,
                timestamp=0,
                key_size=0,
                key=None,
                value_size=compressed_size,
                value=compressed,
                attributes=self._compression_type,
            )
            self._pos = size
            return True
        return False

    def build(self) -> bytearray:
        """Compress batch to be ready for send"""
        self._buffer = bytearray().join(self._msg_buffers)
        self._maybe_compress()
        return self._buffer

    def size(self) -> int:
        """Return current size of data written to buffer"""
        return self._pos

    def size_in_bytes(
        self,
        offset: Any,
        timestamp: Any,
        key: bytes | None,
        value: bytes | None,
    ) -> int:
        """Actual size of message to add"""
        key_size = len(key) if key is not None else 0
        value_size = len(value) if value is not None else 0
        return self._size_in_bytes(key_size, value_size)

    def _size_in_bytes(self, key_size: int, value_size: int) -> int:
        return (
            self.LOG_OVERHEAD
            + self.RECORD_OVERHEAD[self._magic]
            + key_size
            + value_size
        )

    @classmethod
    def record_overhead(cls, magic: int) -> int:
        try:
            return cls.RECORD_OVERHEAD[magic]
        except KeyError:
            raise ValueError(f"Unsupported magic: {magic}") from None


@final
class _LegacyRecordMetadataPy(LegacyRecordMetadataProtocol):
    __slots__ = ("_crc", "_offset", "_size", "_timestamp")

    def __init__(self, offset: int, crc: int, size: int, timestamp: int) -> None:
        self._offset = offset
        self._crc = crc
        self._size = size
        self._timestamp = timestamp

    @property
    def offset(self) -> int:
        return self._offset

    @property
    def crc(self) -> int:
        return self._crc

    @property
    def size(self) -> int:
        return self._size

    @property
    def timestamp(self) -> int:
        return self._timestamp

    def __repr__(self) -> str:
        return (
            f"LegacyRecordMetadata(offset={self._offset!r},"
            f" crc={self._crc!r}, size={self._size!r},"
            f" timestamp={self._timestamp!r})"
        )


LegacyRecordBatchBuilder: type[LegacyRecordBatchBuilderProtocol]
LegacyRecordMetadata: type[LegacyRecordMetadataProtocol]
LegacyRecordBatch: type[LegacyRecordBatchProtocol]
LegacyRecord: type[LegacyRecordProtocol]

if NO_EXTENSIONS:
    LegacyRecordBatchBuilder = _LegacyRecordBatchBuilderPy
    LegacyRecordMetadata = _LegacyRecordMetadataPy
    LegacyRecordBatch = _LegacyRecordBatchPy
    LegacyRecord = _LegacyRecordPy
else:
    try:
        from ._crecords import LegacyRecord as _LegacyRecordCython
        from ._crecords import LegacyRecordBatch as _LegacyRecordBatchCython
        from ._crecords import (
            LegacyRecordBatchBuilder as _LegacyRecordBatchBuilderCython,
        )
        from ._crecords import LegacyRecordMetadata as _LegacyRecordMetadataCython

        LegacyRecordBatchBuilder = _LegacyRecordBatchBuilderCython
        LegacyRecordMetadata = _LegacyRecordMetadataCython
        LegacyRecordBatch = _LegacyRecordBatchCython
        LegacyRecord = _LegacyRecordCython
    except ImportError:  # pragma: no cover
        LegacyRecordBatchBuilder = _LegacyRecordBatchBuilderPy
        LegacyRecordMetadata = _LegacyRecordMetadataPy
        LegacyRecordBatch = _LegacyRecordBatchPy
        LegacyRecord = _LegacyRecordPy
