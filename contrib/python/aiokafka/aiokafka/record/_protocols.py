from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import (
    Any,
    ClassVar,
    Literal,
    Protocol,
    runtime_checkable,
)

from typing_extensions import Never


class DefaultRecordBatchBuilderProtocol(Protocol):
    def __init__(
        self,
        magic: int,
        compression_type: int,
        is_transactional: int,
        producer_id: int,
        producer_epoch: int,
        base_sequence: int,
        batch_size: int,
    ): ...
    def append(
        self,
        offset: int,
        timestamp: int | None,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
    ) -> DefaultRecordMetadataProtocol | None: ...
    def build(self) -> bytearray: ...
    def size(self) -> int: ...
    def size_in_bytes(
        self,
        offset: int,
        timestamp: int,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
    ) -> int: ...
    @classmethod
    def size_of(
        cls,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
    ) -> int: ...
    @classmethod
    def estimate_size_in_bytes(
        cls,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
    ) -> int: ...
    def set_producer_state(
        self, producer_id: int, producer_epoch: int, base_sequence: int
    ) -> None: ...
    @property
    def producer_id(self) -> int: ...
    @property
    def producer_epoch(self) -> int: ...
    @property
    def base_sequence(self) -> int: ...


class DefaultRecordMetadataProtocol(Protocol):
    def __init__(self, offset: int, size: int, timestamp: int) -> None: ...
    @property
    def offset(self) -> int: ...
    @property
    def crc(self) -> None: ...
    @property
    def size(self) -> int: ...
    @property
    def timestamp(self) -> int: ...


class DefaultRecordBatchProtocol(Iterator["DefaultRecordProtocol"], Protocol):
    CODEC_MASK: ClassVar[int]
    CODEC_NONE: ClassVar[int]
    CODEC_GZIP: ClassVar[int]
    CODEC_SNAPPY: ClassVar[int]
    CODEC_LZ4: ClassVar[int]
    CODEC_ZSTD: ClassVar[int]

    def __init__(self, buffer: bytes | bytearray | memoryview) -> None: ...
    @property
    def base_offset(self) -> int: ...
    @property
    def magic(self) -> int: ...
    @property
    def crc(self) -> int: ...
    @property
    def attributes(self) -> int: ...
    @property
    def compression_type(self) -> int: ...
    @property
    def timestamp_type(self) -> int: ...
    @property
    def is_transactional(self) -> bool: ...
    @property
    def is_control_batch(self) -> bool: ...
    @property
    def last_offset_delta(self) -> int: ...
    @property
    def first_timestamp(self) -> int: ...
    @property
    def max_timestamp(self) -> int: ...
    @property
    def producer_id(self) -> int: ...
    @property
    def producer_epoch(self) -> int: ...
    @property
    def base_sequence(self) -> int: ...
    @property
    def next_offset(self) -> int: ...
    def validate_crc(self) -> bool: ...


@runtime_checkable
class DefaultRecordProtocol(Protocol):
    def __init__(
        self,
        offset: int,
        timestamp: int,
        timestamp_type: int,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
    ) -> None: ...
    @property
    def offset(self) -> int: ...
    @property
    def timestamp(self) -> int:
        """Epoch milliseconds"""

    @property
    def timestamp_type(self) -> int:
        """CREATE_TIME(0) or APPEND_TIME(1)"""

    @property
    def key(self) -> bytes | None:
        """Bytes key or None"""

    @property
    def value(self) -> bytes | None:
        """Bytes value or None"""

    @property
    def headers(self) -> list[tuple[str, bytes | None]]: ...
    @property
    def checksum(self) -> None: ...


class LegacyRecordBatchBuilderProtocol(Protocol):
    def __init__(
        self,
        magic: Literal[0, 1],
        compression_type: int,
        batch_size: int,
    ) -> None: ...
    def append(
        self,
        offset: int,
        timestamp: int | None,
        key: bytes | None,
        value: bytes | None,
        headers: Any = None,
    ) -> LegacyRecordMetadataProtocol | None: ...
    def build(self) -> bytearray:
        """Compress batch to be ready for send"""

    def size(self) -> int:
        """Return current size of data written to buffer"""

    def size_in_bytes(
        self,
        offset: int,
        timestamp: int,
        key: bytes | None,
        value: bytes | None,
    ) -> int:
        """Actual size of message to add"""

    @classmethod
    def record_overhead(cls, magic: int) -> int: ...


class LegacyRecordMetadataProtocol(Protocol):
    def __init__(self, offset: int, crc: int, size: int, timestamp: int) -> None: ...
    @property
    def offset(self) -> int: ...
    @property
    def crc(self) -> int: ...
    @property
    def size(self) -> int: ...
    @property
    def timestamp(self) -> int: ...


class LegacyRecordBatchProtocol(Iterable["LegacyRecordProtocol"], Protocol):
    CODEC_MASK: ClassVar[int]
    CODEC_GZIP: ClassVar[int]
    CODEC_SNAPPY: ClassVar[int]
    CODEC_LZ4: ClassVar[int]

    is_control_batch: bool
    is_transactional: bool
    producer_id: int | None

    def __init__(self, buffer: bytes | bytearray | memoryview, magic: int): ...
    @property
    def next_offset(self) -> int: ...
    def validate_crc(self) -> bool: ...


@runtime_checkable
class LegacyRecordProtocol(Protocol):
    def __init__(
        self,
        offset: int,
        timestamp: int | None,
        timestamp_type: Literal[0, 1] | None,
        key: bytes | None,
        value: bytes | None,
        crc: int,
    ) -> None: ...
    @property
    def offset(self) -> int: ...
    @property
    def timestamp(self) -> int | None:
        """Epoch milliseconds"""

    @property
    def timestamp_type(self) -> Literal[0, 1] | None:
        """CREATE_TIME(0) or APPEND_TIME(1)"""

    @property
    def key(self) -> bytes | None:
        """Bytes key or None"""

    @property
    def value(self) -> bytes | None:
        """Bytes value or None"""

    @property
    def headers(self) -> list[Never]: ...
    @property
    def checksum(self) -> int: ...


class MemoryRecordsProtocol(Protocol):
    def __init__(self, bytes_data: bytes) -> None: ...
    def size_in_bytes(self) -> int: ...
    def has_next(self) -> bool: ...
    def next_batch(
        self,
    ) -> DefaultRecordBatchProtocol | LegacyRecordBatchProtocol | None: ...
