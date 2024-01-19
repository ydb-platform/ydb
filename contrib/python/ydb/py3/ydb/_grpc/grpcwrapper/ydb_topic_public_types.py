import datetime
import typing
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional, List, Union, Dict

try:
    from ydb.public.api.protos import ydb_topic_pb2
except ImportError:
    from contrib.ydb.public.api.protos import ydb_topic_pb2

from .common_utils import IToProto
from ...scheme import SchemeEntry


@dataclass
# need similar struct to PublicDescribeTopicResult
class CreateTopicRequestParams:
    path: str
    min_active_partitions: Optional[int]
    partition_count_limit: Optional[int]
    retention_period: Optional[datetime.timedelta]
    retention_storage_mb: Optional[int]
    supported_codecs: Optional[List[Union["PublicCodec", int]]]
    partition_write_speed_bytes_per_second: Optional[int]
    partition_write_burst_bytes: Optional[int]
    attributes: Optional[Dict[str, str]]
    consumers: Optional[List[Union["PublicConsumer", str]]]
    metering_mode: Optional["PublicMeteringMode"]


class PublicCodec(int):
    """
    Codec value may contain any int number.

    Values below is only well-known predefined values,
    but protocol support custom codecs.
    """

    UNSPECIFIED = 0
    RAW = 1
    GZIP = 2
    LZOP = 3  # Has not supported codec in standard library
    ZSTD = 4  # Has not supported codec in standard library


class PublicMeteringMode(IntEnum):
    UNSPECIFIED = 0
    RESERVED_CAPACITY = 1
    REQUEST_UNITS = 2


@dataclass
class PublicConsumer:
    name: str
    important: bool = False
    """
    Consumer may be marked as 'important'. It means messages for this consumer will never expire due to retention.
    User should take care that such consumer never stalls, to prevent running out of disk space.
    """

    read_from: Optional[datetime.datetime] = None
    "All messages with smaller server written_at timestamp will be skipped."

    supported_codecs: List[PublicCodec] = field(default_factory=lambda: list())
    """
    List of supported codecs by this consumer.
    supported_codecs on topic must be contained inside this list.
    """

    attributes: Dict[str, str] = field(default_factory=lambda: dict())
    "Attributes of consumer"


@dataclass
class DropTopicRequestParams(IToProto):
    path: str

    def to_proto(self) -> ydb_topic_pb2.DropTopicRequest:
        return ydb_topic_pb2.DropTopicRequest(path=self.path)


@dataclass
class DescribeTopicRequestParams(IToProto):
    path: str
    include_stats: bool

    def to_proto(self) -> ydb_topic_pb2.DescribeTopicRequest:
        return ydb_topic_pb2.DescribeTopicRequest(path=self.path, include_stats=self.include_stats)


@dataclass
# Need similar struct to CreateTopicRequestParams
class PublicDescribeTopicResult:
    self: SchemeEntry
    "Description of scheme object"

    min_active_partitions: int
    "Minimum partition count auto merge would stop working at"

    partition_count_limit: int
    "Limit for total partition count, including active (open for write) and read-only partitions"

    partitions: List["PublicDescribeTopicResult.PartitionInfo"]
    "Partitions description"

    retention_period: datetime.timedelta
    "How long data in partition should be stored"

    retention_storage_mb: int
    "How much data in partition should be stored. Zero value means infinite limit"

    supported_codecs: List[PublicCodec]
    "List of allowed codecs for writers"

    partition_write_speed_bytes_per_second: int
    "Partition write speed in bytes per second"

    partition_write_burst_bytes: int
    "Burst size for write in partition, in bytes"

    attributes: Dict[str, str]
    """User and server attributes of topic. Server attributes starts from "_" and will be validated by server."""

    consumers: List[PublicConsumer]
    """List of consumers for this topic"""

    metering_mode: PublicMeteringMode
    "Metering settings"

    topic_stats: "PublicDescribeTopicResult.TopicStats"
    "Statistics of topic"

    @dataclass
    class PartitionInfo:
        partition_id: int
        "Partition identifier"

        active: bool
        "Is partition open for write"

        child_partition_ids: List[int]
        "Ids of partitions which was formed when this partition was split or merged"

        parent_partition_ids: List[int]
        "Ids of partitions from which this partition was formed by split or merge"

        partition_stats: Optional["PublicPartitionStats"]
        "Stats for partition, filled only when include_stats in request is true"

    @dataclass
    class TopicStats:
        store_size_bytes: int
        "Approximate size of topic"

        min_last_write_time: datetime.datetime
        "Minimum of timestamps of last write among all partitions."

        max_write_time_lag: datetime.timedelta
        """
        Maximum of differences between write timestamp and create timestamp for all messages,
        written during last minute.
        """

        bytes_written: "PublicMultipleWindowsStat"
        "How much bytes were written statistics."


@dataclass
class PublicPartitionStats:
    partition_start: int
    "first message offset in the partition"

    partition_end: int
    "offset after last stored message offset in the partition (last offset + 1)"

    store_size_bytes: int
    "Approximate size of partition"

    last_write_time: datetime.datetime
    "Timestamp of last write"

    max_write_time_lag: datetime.timedelta
    "Maximum of differences between write timestamp and create timestamp for all messages, written during last minute."

    bytes_written: "PublicMultipleWindowsStat"
    "How much bytes were written during several windows in this partition."

    partition_node_id: int
    "Host where tablet for this partition works. Useful for debugging purposes."


@dataclass
class PublicMultipleWindowsStat:
    per_minute: int
    per_hour: int
    per_day: int
