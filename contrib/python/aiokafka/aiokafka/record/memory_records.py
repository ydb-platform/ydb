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

import struct
from typing import final

from aiokafka.errors import CorruptRecordException
from aiokafka.util import NO_EXTENSIONS

from ._protocols import (
    DefaultRecordBatchProtocol,
    LegacyRecordBatchProtocol,
    MemoryRecordsProtocol,
)
from .default_records import DefaultRecordBatch
from .legacy_records import LegacyRecordBatch, _LegacyRecordBatchPy


@final
class _MemoryRecordsPy(MemoryRecordsProtocol):
    LENGTH_OFFSET = struct.calcsize(">q")
    LOG_OVERHEAD = struct.calcsize(">qi")
    MAGIC_OFFSET = struct.calcsize(">qii")

    # Minimum space requirements for Record V0
    MIN_SLICE = LOG_OVERHEAD + _LegacyRecordBatchPy.RECORD_OVERHEAD_V0

    def __init__(self, bytes_data: bytes) -> None:
        self._buffer = bytes_data
        self._pos: int = 0
        # We keep one slice ahead so `has_next` will return very fast
        self._next_slice: memoryview | None = None
        self._remaining_bytes = 0
        self._cache_next()

    def size_in_bytes(self) -> int:
        return len(self._buffer)

    # NOTE: we cache offsets here as kwargs for a bit more speed, as cPython
    # will use LOAD_FAST opcode in this case
    def _cache_next(
        self, len_offset: int = LENGTH_OFFSET, log_overhead: int = LOG_OVERHEAD
    ) -> None:
        buffer = self._buffer
        buffer_len = len(buffer)
        pos = self._pos
        remaining = buffer_len - pos
        if remaining < log_overhead:
            # Will be re-checked in Fetcher for remaining bytes.
            self._remaining_bytes = remaining
            self._next_slice = None
            return

        length: int = struct.unpack_from(">i", buffer, pos + len_offset)[0]

        slice_end = pos + log_overhead + length
        if slice_end > buffer_len:
            # Will be re-checked in Fetcher for remaining bytes
            self._remaining_bytes = remaining
            self._next_slice = None
            return

        self._next_slice = memoryview(buffer)[pos:slice_end]
        self._pos = slice_end

    def has_next(self) -> bool:
        return self._next_slice is not None

    # NOTE: same cache for LOAD_FAST as above
    def next_batch(
        self, _min_slice: int = MIN_SLICE, _magic_offset: int = MAGIC_OFFSET
    ) -> DefaultRecordBatchProtocol | LegacyRecordBatchProtocol | None:
        next_slice = self._next_slice
        if next_slice is None:
            return None
        if len(next_slice) < _min_slice:
            raise CorruptRecordException(
                "Record size is less than the minimum record overhead "
                f"({_min_slice - self.LOG_OVERHEAD})"
            )
        self._cache_next()
        magic = next_slice[_magic_offset]
        if magic >= 2:  # pragma: no cover
            return DefaultRecordBatch(next_slice)
        else:
            return LegacyRecordBatch(next_slice, magic)


MemoryRecords: type[MemoryRecordsProtocol]

if NO_EXTENSIONS:
    MemoryRecords = _MemoryRecordsPy
else:
    try:
        from ._crecords import MemoryRecords as _MemoryRecordsCython

        MemoryRecords = _MemoryRecordsCython
    except ImportError:  # pragma: no cover
        MemoryRecords = _MemoryRecordsPy
