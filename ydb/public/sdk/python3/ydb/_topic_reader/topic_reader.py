import concurrent.futures
import enum
import datetime
from dataclasses import dataclass
from typing import (
    Union,
    Optional,
    List,
    Mapping,
    Callable,
)

from ..table import RetrySettings
from .._grpc.grpcwrapper.ydb_topic import StreamReadMessage, OffsetsRange


@dataclass
class PublicTopicSelector:
    path: str
    partitions: Optional[Union[int, List[int]]] = None
    read_from: Optional[datetime.datetime] = None
    max_lag: Optional[datetime.timedelta] = None

    def _to_topic_read_settings(self) -> StreamReadMessage.InitRequest.TopicReadSettings:
        partitions = self.partitions
        if partitions is None:
            partitions = []

        elif not isinstance(partitions, list):
            partitions = [partitions]

        return StreamReadMessage.InitRequest.TopicReadSettings(
            path=self.path,
            partition_ids=partitions,
            max_lag=self.max_lag,
            read_from=self.read_from,
        )


TopicSelectorTypes = Union[str, PublicTopicSelector, List[Union[str, PublicTopicSelector]]]


@dataclass
class PublicReaderSettings:
    consumer: str
    topic: TopicSelectorTypes
    buffer_size_bytes: int = 50 * 1024 * 1024

    decoders: Union[Mapping[int, Callable[[bytes], bytes]], None] = None
    """decoders: map[codec_code] func(encoded_bytes)->decoded_bytes"""

    # decoder_executor, must be set for handle non raw messages
    decoder_executor: Optional[concurrent.futures.Executor] = None
    update_token_interval: Union[int, float] = 3600

    def __post_init__(self):
        # check possible create init message
        _ = self._init_message()

    def _init_message(self) -> StreamReadMessage.InitRequest:
        if not isinstance(self.consumer, str):
            raise TypeError("Unsupported type for customer field: '%s'" % type(self.consumer))

        if isinstance(self.topic, list):
            selectors = self.topic
        else:
            selectors = [self.topic]

        for index, selector in enumerate(selectors):
            if isinstance(selector, str):
                selectors[index] = PublicTopicSelector(path=selector)
            elif isinstance(selector, PublicTopicSelector):
                pass
            else:
                raise TypeError("Unsupported type for topic field: '%s'" % type(selector))

        return StreamReadMessage.InitRequest(
            topics_read_settings=list(map(PublicTopicSelector._to_topic_read_settings, selectors)),  # type: ignore
            consumer=self.consumer,
        )

    def _retry_settings(self) -> RetrySettings:
        return RetrySettings(idempotent=True)


class Events:
    class OnCommit:
        topic: str
        offset: int

    class OnPartitionGetStartOffsetRequest:
        topic: str
        partition_id: int

    class OnPartitionGetStartOffsetResponse:
        start_offset: int

    class OnInitPartition:
        pass

    class OnShutdownPatition:
        pass


class RetryPolicy:
    connection_timeout_sec: float
    overload_timeout_sec: float
    retry_access_denied: bool = False


class CommitResult:
    topic: str
    partition: int
    offset: int
    state: "CommitResult.State"
    details: str  # for humans only, content messages may be change in any time

    class State(enum.Enum):
        UNSENT = 1  # commit didn't send to the server
        SENT = 2  # commit was sent to server, but ack hasn't received
        ACKED = 3  # ack from server is received


class SessionStat:
    path: str
    partition_id: str
    partition_offsets: OffsetsRange
    committed_offset: int
    write_time_high_watermark: datetime.datetime
    write_time_high_watermark_timestamp_nano: int


class StubEvent:
    pass
