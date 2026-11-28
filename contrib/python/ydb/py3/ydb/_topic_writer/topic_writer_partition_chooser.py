import abc
import bisect
from typing import Callable, List, Tuple

from .topic_writer import PublicMessage, TopicWriterError
from .._grpc.grpcwrapper.ydb_topic_public_types import PublicDescribeTopicResult

_MASK32 = 0xFFFFFFFF
_MASK64 = 0xFFFFFFFFFFFFFFFF

# Metadata key the server uses to validate bound-based partition selection.
PARTITION_KEY_METADATA_KEY = "__partition_key"

PartitionInfo = PublicDescribeTopicResult.PartitionInfo


def murmur2_32(data: bytes, seed: int = 0) -> int:
    """MurmurHash2, 32-bit, little-endian."""
    m = 0x5BD1E995
    r = 24
    n = len(data)
    h = (seed ^ n) & _MASK32

    body = n - (n % 4)
    for i in range(0, body, 4):
        k = int.from_bytes(data[i : i + 4], "little")
        k = (k * m) & _MASK32
        k ^= k >> r
        k = (k * m) & _MASK32
        h = (h * m) & _MASK32
        h ^= k

    rem = n % 4
    if rem:
        tail = data[body:]
        if rem >= 3:
            h ^= tail[2] << 16
        if rem >= 2:
            h ^= tail[1] << 8
        h ^= tail[0]
        h = (h * m) & _MASK32

    h ^= h >> 13
    h = (h * m) & _MASK32
    h ^= h >> 15
    return h & _MASK32


def murmur64a(data: bytes, seed: int = 0) -> int:
    """MurmurHash64A, 64-bit, little-endian. Matches the YDB Go SDK
    ``xhash.Murmur2Hash64A`` and the C++ default partitioning key hasher."""
    m = 0xC6A4A7935BD1E995
    r = 47
    n = len(data)
    h = (seed ^ ((n * m) & _MASK64)) & _MASK64

    body = n - (n % 8)
    for i in range(0, body, 8):
        k = int.from_bytes(data[i : i + 8], "little")
        k = (k * m) & _MASK64
        k ^= k >> r
        k = (k * m) & _MASK64
        h ^= k
        h = (h * m) & _MASK64

    rem = n % 8
    if rem:
        tail = data[body:]
        for j in range(rem - 1, -1, -1):
            h ^= tail[j] << (8 * j)
        h = (h * m) & _MASK64

    h ^= h >> r
    h = (h * m) & _MASK64
    h ^= h >> r
    return h & _MASK64


def default_bound_key_hasher(key: str) -> bytes:
    """Hash a routing key the way the YDB server expects for bound-based
    partition selection: MurmurHash64A(seed=0) as 8 big-endian bytes."""
    return murmur64a(key.encode("utf-8"), 0).to_bytes(8, "big")


class PublicPartitionChooser(abc.ABC):
    """Maps a message to a target partition id and tracks the live partition set."""

    @abc.abstractmethod
    def choose_partition(self, message: PublicMessage) -> int: ...

    @abc.abstractmethod
    def add_partitions(self, partitions: List[PartitionInfo]) -> None: ...

    @abc.abstractmethod
    def remove_partition(self, partition_id: int) -> None: ...


class PublicPartitionByKeyKafka(PublicPartitionChooser):
    """Kafka-compatible routing for topics with a fixed partition count.

    Hashes UTF-8 keys with 32-bit MurmurHash2 (seed ``0x9747b28c``), clears the
    sign bit and indexes partition IDs sorted in ascending order.
    """

    def __init__(self):
        self._partitions: List[int] = []

    def add_partitions(self, partitions: List[PartitionInfo]) -> None:
        for p in partitions:
            # Any key range at all means the topic is auto-partitioned, including the fully open
            # one a single partition reports before its first split. Accepting that would work
            # until the split, then break: the children come back bounded, and modulo routing
            # cannot place a key by bounds.
            if p.key_range is not None:
                raise ValueError("PublicPartitionByKeyKafka does not support partition key ranges")
            self._partitions.append(p.partition_id)
        self._partitions.sort()

    def remove_partition(self, partition_id: int) -> None:
        self._partitions = [p for p in self._partitions if p != partition_id]

    def choose_partition(self, message: PublicMessage) -> int:
        if not self._partitions:
            raise ValueError("no partitions configured for partition chooser")
        # Apache Kafka's DefaultPartitioner applies toPositive() (mask the sign bit) to the
        # murmur2 hash before the modulo; match it so the same key lands on the same partition.
        if message.key is None:
            raise TopicWriterError("A partition key is required for write-by-key routing")
        h = murmur2_32(message.key.encode("utf-8"), 0x9747B28C) & 0x7FFFFFFF
        return self._partitions[h % len(self._partitions)]


class PublicPartitionByKeyBound(PublicPartitionChooser):
    """Server-accurate routing: hashes the key and selects the partition whose
    ``[from_bound, to_bound)`` key range owns it. Mirrors YDB auto-partitioning,
    so the same key lands where the server expects it.
    """

    def __init__(self, key_hasher: Callable[[str], bytes] = default_bound_key_hasher):
        self._key_hasher = key_hasher
        # (from_bound, to_bound, partition_id) sorted by from_bound. An empty from_bound is the
        # start of the key space, an empty to_bound is its end.
        self._partitions: List[Tuple[bytes, bytes, int]] = []

    def add_partitions(self, partitions: List[PartitionInfo]) -> None:
        # Normalise and sort before validating: the caller may pass partitions in any order
        # (DescribeTopic does not promise one), and only the partition that sorts leftmost may
        # carry an open lower bound. Validating in argument order rejects a valid set that
        # happens to arrive reversed.
        added: List[Tuple[bytes, bytes, int]] = []
        for p in partitions:
            from_bound = p.key_range.from_bound if p.key_range is not None else b""
            to_bound = p.key_range.to_bound if p.key_range is not None else b""
            added.append((from_bound, to_bound, p.partition_id))
        added.sort(key=lambda x: x[0])

        for i, (from_bound, _to_bound, partition_id) in enumerate(added):
            if i > 0 and not from_bound:
                raise ValueError(
                    "partition %d has no from_bound key range, but only the leftmost partition may"
                    " have an open lower bound" % partition_id
                )

        self._partitions.extend(added)
        self._partitions.sort(key=lambda x: x[0])

    def remove_partition(self, partition_id: int) -> None:
        self._partitions = [p for p in self._partitions if p[-1] != partition_id]

    def choose_partition(self, message: PublicMessage) -> int:
        if not self._partitions:
            raise ValueError("no partitions configured for partition chooser")
        if message.key is None:
            raise TopicWriterError("A partition key is required for write-by-key routing")
        hashed = self._key_hasher(message.key)

        if message.metadata_items is None:
            message.metadata_items = {}
        message.metadata_items[PARTITION_KEY_METADATA_KEY] = hashed

        # First partition whose from_bound is strictly greater than the hashed key; the owning
        # partition is the previous one. Searched in place with a key rather than by building a
        # list of bounds, which would allocate per message on the hot path.
        idx = bisect.bisect_right(self._partitions, hashed, key=lambda entry: entry[0])
        if idx == 0:
            raise RuntimeError("inconsistent partition bounds: lower-bound search returned 0")

        from_bound, to_bound, partition_id = self._partitions[idx - 1]
        # A key range is [from_bound, to_bound); an empty to_bound means the range runs to the
        # end of the key space. Landing past to_bound means the known partitions have a hole --
        # e.g. only one child of a split is visible yet -- and the greatest-lower-bound search
        # silently points at the *sibling* that owns the range to the left. Refuse instead:
        # routing a key into a sibling branch is exactly what breaks one-key-one-lineage.
        if to_bound and hashed >= to_bound:
            raise RuntimeError(
                "key is not covered by any known partition: it is above partition %d to_bound;"
                " the partition set is incomplete" % partition_id
            )
        return partition_id
