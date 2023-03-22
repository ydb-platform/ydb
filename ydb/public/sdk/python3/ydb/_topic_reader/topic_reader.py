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


class Selector:
    path: str
    partitions: Union[None, int, List[int]]
    read_from_timestamp_ms: Optional[int]
    max_time_lag_ms: Optional[int]

    def __init__(self, path, *, partitions: Union[None, int, List[int]] = None):
        self.path = path
        self.partitions = partitions


@dataclass
class PublicReaderSettings:
    consumer: str
    topic: str
    buffer_size_bytes: int = 50 * 1024 * 1024

    decoders: Union[Mapping[int, Callable[[bytes], bytes]], None] = None
    """decoders: map[codec_code] func(encoded_bytes)->decoded_bytes"""

    # decoder_executor, must be set for handle non raw messages
    decoder_executor: Optional[concurrent.futures.Executor] = None
    update_token_interval: Union[int, float] = 3600

    def _init_message(self) -> StreamReadMessage.InitRequest:
        return StreamReadMessage.InitRequest(
            topics_read_settings=[
                StreamReadMessage.InitRequest.TopicReadSettings(
                    path=self.topic,
                )
            ],
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
