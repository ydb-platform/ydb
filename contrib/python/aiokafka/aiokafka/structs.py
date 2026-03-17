from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Generic, NamedTuple, TypeVar

from aiokafka.errors import KafkaError

__all__ = [
    "BrokerMetadata",
    "ConsumerRecord",
    "OffsetAndMetadata",
    "PartitionMetadata",
    "RecordMetadata",
    "TopicPartition",
]


class TopicPartition(NamedTuple):
    """A topic and partition tuple"""

    topic: str
    "A topic name"

    partition: int
    "A partition id"


class BrokerMetadata(NamedTuple):
    """A Kafka broker metadata used by admin tools"""

    # FIXME: consider updating implementation (https://github.com/aio-libs/aiokafka/issues/1050)
    nodeId: int | str
    "The Kafka broker id"

    host: str
    "The Kafka broker hostname"

    port: int
    "The Kafka broker port"

    rack: str | None
    """The rack of the broker, which is used to in rack aware partition
    assignment for fault tolerance.
    Examples: `RACK1`, `us-east-1d`. Default: None
    """


class PartitionMetadata(NamedTuple):
    """A topic partition metadata describing the state in the MetadataResponse"""

    topic: str
    "The topic name of the partition this metadata relates to"

    partition: int
    "The id of the partition this metadata relates to"

    leader: int
    "The id of the broker that is the leader for the partition"

    replicas: list[int]
    "The ids of all brokers that contain replicas of the partition"
    isr: list[int]
    "The ids of all brokers that contain in-sync replicas of the partition"

    error: KafkaError | None
    "A KafkaError object associated with the request for this partition metadata"


class OffsetAndMetadata(NamedTuple):
    """The Kafka offset commit API

    The Kafka offset commit API allows users to provide additional metadata
    (in the form of a string) when an offset is committed. This can be useful
    (for example) to store information about which node made the commit,
    what time the commit was made, etc.
    """

    offset: int
    "The offset to be committed"

    metadata: str
    "Non-null metadata"

    # TODO add leaderEpoch:


class RecordMetadata(NamedTuple):
    """Returned when a :class:`~.AIOKafkaProducer` sends a message"""

    topic: str
    "The topic name"

    partition: int
    "The partition number"

    topic_partition: TopicPartition
    ""

    offset: int
    """The unique offset of the message in this partition.

    See :ref:`Offsets and Consumer Position <offset_and_position>` for more
    details on offsets.
    """

    timestamp: int | None
    "Timestamp in millis, None for older Brokers"

    timestamp_type: int
    """The timestamp type of this record.

    If the broker respected the timestamp passed to
    :meth:`.AIOKafkaProducer.send`, ``0`` will be returned (``CreateTime``).

    If the broker set it's own timestamp, ``1`` will be returned (``LogAppendTime``).
    """

    log_start_offset: int | None
    ""


KT = TypeVar("KT")
VT = TypeVar("VT")


@dataclass
class ConsumerRecord(Generic[KT, VT]):
    topic: str
    "The topic this record is received from"

    partition: int
    "The partition from which this record is received"

    offset: int
    "The position of this record in the corresponding Kafka partition."

    timestamp: int
    "The timestamp of this record"

    timestamp_type: int
    "The timestamp type of this record"

    key: KT | None
    "The key (or `None` if no key is specified)"

    value: VT | None
    "The value"

    checksum: int | None
    "Deprecated"

    serialized_key_size: int
    "The size of the serialized, uncompressed key in bytes."

    serialized_value_size: int
    "The size of the serialized, uncompressed value in bytes."

    headers: Sequence[tuple[str, bytes]]
    "The headers"


class OffsetAndTimestamp(NamedTuple):
    offset: int
    timestamp: int | None  # Only None if used with old broker version
