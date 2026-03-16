import io
import time
from binascii import crc32
from collections.abc import Iterable
from typing import Literal, cast, overload

from typing_extensions import Self

from aiokafka.codec import (
    gzip_decode,
    has_gzip,
    has_lz4,
    has_snappy,
    has_zstd,
    lz4_decode,
    snappy_decode,
    zstd_decode,
)

from .struct import Struct
from .types import Bytes, Int8, Int32, Int64, Schema, UInt32


class Message(Struct):
    # FIXME: override __eq__/__repr__ methods from Struct

    BASE_FIELDS = (
        ("crc", UInt32),
        ("magic", Int8),
        ("attributes", Int8),
    )
    MAGIC0_FIELDS = (
        ("key", Bytes),
        ("value", Bytes),
    )
    MAGIC1_FIELDS = (
        ("timestamp", Int64),
        ("key", Bytes),
        ("value", Bytes),
    )
    SCHEMAS = [
        Schema(
            *BASE_FIELDS,
            *MAGIC0_FIELDS,
        ),
        Schema(
            *BASE_FIELDS,
            *MAGIC1_FIELDS,
        ),
    ]
    SCHEMA = SCHEMAS[1]
    CODEC_MASK = 0x07
    CODEC_GZIP = 0x01
    CODEC_SNAPPY = 0x02
    CODEC_LZ4 = 0x03
    CODEC_ZSTD = 0x04
    TIMESTAMP_TYPE_MASK = 0x08
    HEADER_SIZE = (
        22  # crc(4), magic(1), attributes(1), timestamp(8), key+value size(4*2)
    )

    @overload
    def __init__(
        self,
        *,
        value: bytes | None,
        key: bytes | None,
        magic: Literal[0],
        attributes: int,
        crc: int,
    ) -> None: ...

    @overload
    def __init__(
        self,
        *,
        value: bytes | None,
        key: bytes | None,
        magic: Literal[1],
        attributes: int,
        crc: int,
        timestamp: int,
    ) -> None: ...

    def __init__(
        self,
        *,
        value: bytes | None,
        key: bytes | None,
        magic: Literal[0, 1],
        attributes: int,
        crc: int,
        timestamp: int | None = None,
    ) -> None:
        assert value is None or isinstance(value, bytes), "value must be bytes"
        assert key is None or isinstance(key, bytes), "key must be bytes"
        assert magic > 0 or timestamp is None, "timestamp not supported in v0"

        # Default timestamp to now for v1 messages
        if magic > 0 and timestamp is None:
            timestamp = int(time.time() * 1000)
        self.timestamp = timestamp
        self.crc = crc
        self._validated_crc: int | None = None
        self.magic = magic
        self.attributes = attributes
        self.key = key
        self.value = value

    @property
    def timestamp_type(self) -> Literal[0, 1] | None:
        """0 for CreateTime; 1 for LogAppendTime; None if unsupported.

        Value is determined by broker; produced messages should always set to 0
        Requires Kafka >= 0.10 / message version >= 1
        """
        if self.magic == 0:
            return None
        elif self.attributes & self.TIMESTAMP_TYPE_MASK:
            return 1
        else:
            return 0

    def encode(self, recalc_crc: bool = True) -> bytes:
        version = self.magic
        if version == 1:
            message = Message.SCHEMAS[version].encode(
                (
                    self.crc,
                    self.magic,
                    self.attributes,
                    self.timestamp,
                    self.key,
                    self.value,
                )
            )
        elif version == 0:
            message = Message.SCHEMAS[version].encode(
                (self.crc, self.magic, self.attributes, self.key, self.value)
            )
        else:
            raise ValueError(f"Unrecognized message version: {version}")
        if not recalc_crc:
            return message
        self.crc = crc32(message[4:])
        crc_field = self.BASE_FIELDS[0][1]
        return crc_field.encode(self.crc) + message[4:]

    @classmethod
    def decode(cls, data: io.BytesIO | bytes) -> Self:
        _validated_crc: int | None = None
        if isinstance(data, bytes):
            _validated_crc = crc32(data[4:])
            data = io.BytesIO(data)
        # Partial decode required to determine message version
        crc, magic, attributes = (
            cls.BASE_FIELDS[0][1].decode(data),
            cls.BASE_FIELDS[1][1].decode(data),
            cls.BASE_FIELDS[2][1].decode(data),
        )
        if magic == 1:
            magic = cast(Literal[1], magic)
            timestamp, key, value = (
                cls.MAGIC1_FIELDS[0][1].decode(data),
                cls.MAGIC1_FIELDS[1][1].decode(data),
                cls.MAGIC1_FIELDS[2][1].decode(data),
            )
            msg = cls(
                value=value,
                key=key,
                magic=magic,
                attributes=attributes,
                crc=crc,
                timestamp=timestamp,
            )
        elif magic == 0:
            magic = cast(Literal[0], magic)
            key, value = (
                cls.MAGIC0_FIELDS[0][1].decode(data),
                cls.MAGIC0_FIELDS[1][1].decode(data),
            )
            msg = cls(
                value=value,
                key=key,
                magic=magic,
                attributes=attributes,
                crc=crc,
            )
        else:
            raise ValueError(f"Unrecognized message version: {magic}")

        msg._validated_crc = _validated_crc
        return msg

    def validate_crc(self) -> bool:
        if self._validated_crc is None:
            raw_msg = self.encode(recalc_crc=False)
            self._validated_crc = crc32(raw_msg[4:])
        return self.crc == self._validated_crc

    def is_compressed(self) -> bool:
        return self.attributes & self.CODEC_MASK != 0

    def decompress(
        self,
    ) -> list[tuple[int, int, "Message"] | tuple[None, None, "PartialMessage"]]:
        assert self.value is not None
        codec = self.attributes & self.CODEC_MASK
        assert codec in (
            self.CODEC_GZIP,
            self.CODEC_SNAPPY,
            self.CODEC_LZ4,
            self.CODEC_ZSTD,
        )
        if codec == self.CODEC_GZIP:
            assert has_gzip(), "Gzip decompression unsupported"
            raw_bytes = gzip_decode(self.value)
        elif codec == self.CODEC_SNAPPY:
            assert has_snappy(), "Snappy decompression unsupported"
            raw_bytes = snappy_decode(self.value)
        elif codec == self.CODEC_LZ4:
            assert has_lz4(), "LZ4 decompression unsupported"
            raw_bytes = lz4_decode(self.value)
        elif codec == self.CODEC_ZSTD:
            assert has_zstd(), "ZSTD decompression unsupported"
            raw_bytes = zstd_decode(self.value)
        else:
            raise AssertionError("This should be impossible")

        return MessageSet.decode(raw_bytes, bytes_to_read=len(raw_bytes))


class PartialMessage(bytes):
    def __repr__(self) -> str:
        return f"PartialMessage({self!r})"


class MessageSet:
    ITEM = Schema(("offset", Int64), ("message", Bytes))
    HEADER_SIZE = 12  # offset + message_size

    @classmethod
    def encode(
        cls,
        items: io.BytesIO | Iterable[tuple[int, bytes]],
        prepend_size: bool = True,
    ) -> bytes:
        # RecordAccumulator encodes messagesets internally
        if isinstance(items, io.BytesIO):
            size = Int32.decode(items)
            if prepend_size:
                # rewind and return all the bytes
                items.seek(items.tell() - 4)
                size += 4
            return items.read(size)

        encoded_values: list[bytes] = []
        for offset, message in items:
            encoded_values.append(Int64.encode(offset))
            encoded_values.append(Bytes.encode(message))
        encoded = b"".join(encoded_values)
        if prepend_size:
            return Bytes.encode(encoded)
        else:
            return encoded

    @classmethod
    def decode(
        cls, data: io.BytesIO | bytes, bytes_to_read: int | None = None
    ) -> list[tuple[int, int, Message] | tuple[None, None, PartialMessage]]:
        """Compressed messages should pass in bytes_to_read (via message size)
        otherwise, we decode from data as Int32
        """
        if isinstance(data, bytes):
            data = io.BytesIO(data)
        if bytes_to_read is None:
            bytes_to_read = Int32.decode(data)

        # if FetchRequest max_bytes is smaller than the available message set
        # the server returns partial data for the final message
        # So create an internal buffer to avoid over-reading
        raw = io.BytesIO(data.read(bytes_to_read))

        items: list[tuple[int, int, Message] | tuple[None, None, PartialMessage]] = []
        try:
            while bytes_to_read:
                offset = Int64.decode(raw)
                msg_bytes = Bytes.decode(raw)
                assert msg_bytes is not None
                bytes_to_read -= 8 + 4 + len(msg_bytes)
                items.append(
                    (offset, len(msg_bytes), Message.decode(msg_bytes)),
                )
        except ValueError:
            # PartialMessage to signal that max_bytes may be too small
            items.append(
                (None, None, PartialMessage()),
            )
        return items

    @classmethod
    def repr(
        cls,
        messages: io.BytesIO
        | list[tuple[int, int, Message] | tuple[None, None, PartialMessage]],
    ) -> str:
        if isinstance(messages, io.BytesIO):
            offset = messages.tell()
            decoded = cls.decode(messages)
            messages.seek(offset)
            decoded_messages = decoded
        else:
            decoded_messages = messages
        return str([cls.ITEM.repr(m) for m in decoded_messages])
