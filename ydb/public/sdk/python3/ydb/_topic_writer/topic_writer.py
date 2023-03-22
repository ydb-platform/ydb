import concurrent.futures
import datetime
import enum
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import List, Union, Optional, Any, Dict

import typing

import ydb.aio
from .._grpc.grpcwrapper.ydb_topic import StreamWriteMessage
from .._grpc.grpcwrapper.common_utils import IToProto
from .._grpc.grpcwrapper.ydb_topic_public_types import PublicCodec

Message = typing.Union["PublicMessage", "PublicMessage.SimpleMessageSourceType"]


@dataclass
class PublicWriterSettings:
    """
    Settings for topic writer.

    order of fields IS NOT stable, use keywords only
    """

    topic: str
    producer_id: Optional[str] = None
    session_metadata: Optional[Dict[str, str]] = None
    partition_id: Optional[int] = None
    auto_seqno: bool = True
    auto_created_at: bool = True
    codec: Optional[PublicCodec] = None  # default mean auto-select
    encoder_executor: Optional[
        concurrent.futures.Executor
    ] = None  # default shared client executor pool
    encoders: Optional[
        typing.Mapping[PublicCodec, typing.Callable[[bytes], bytes]]
    ] = None
    # get_last_seqno: bool = False
    # serializer: Union[Callable[[Any], bytes], None] = None
    # send_buffer_count: Optional[int] = 10000
    # send_buffer_bytes: Optional[int] = 100 * 1024 * 1024
    # codec: Optional[int] = None
    # codec_autoselect: bool = True
    # retry_policy: Optional["RetryPolicy"] = None
    update_token_interval: Union[int, float] = 3600

    def __post_init__(self):
        if self.producer_id is None:
            self.producer_id = uuid.uuid4().hex


@dataclass
class PublicWriteResult:
    @dataclass(eq=True)
    class Written:
        __slots__ = "offset"
        offset: int

    @dataclass(eq=True)
    class Skipped:
        pass


PublicWriteResultTypes = Union[PublicWriteResult.Written, PublicWriteResult.Skipped]


class WriterSettings(PublicWriterSettings):
    def __init__(self, settings: PublicWriterSettings):
        self.__dict__ = settings.__dict__.copy()

    def create_init_request(self) -> StreamWriteMessage.InitRequest:
        return StreamWriteMessage.InitRequest(
            path=self.topic,
            producer_id=self.producer_id,
            write_session_meta=self.session_metadata,
            partitioning=self.get_partitioning(),
            get_last_seq_no=True,
        )

    def get_partitioning(self) -> StreamWriteMessage.PartitioningType:
        if self.partition_id is not None:
            return StreamWriteMessage.PartitioningPartitionID(self.partition_id)
        return StreamWriteMessage.PartitioningMessageGroupID(self.producer_id)


class SendMode(Enum):
    ASYNC = 1
    SYNC = 2


@dataclass
class PublicWriterInitInfo:
    __slots__ = ("last_seqno", "supported_codecs")
    last_seqno: Optional[int]
    supported_codecs: List[PublicCodec]


class PublicMessage:
    seqno: Optional[int]
    created_at: Optional[datetime.datetime]
    data: "PublicMessage.SimpleMessageSourceType"

    SimpleMessageSourceType = Union[str, bytes]  # Will be extend

    def __init__(
        self,
        data: SimpleMessageSourceType,
        *,
        seqno: Optional[int] = None,
        created_at: Optional[datetime.datetime] = None,
    ):
        self.seqno = seqno
        self.created_at = created_at
        self.data = data

    @staticmethod
    def _create_message(data: Message) -> "PublicMessage":
        if isinstance(data, PublicMessage):
            return data
        return PublicMessage(data=data)


class InternalMessage(StreamWriteMessage.WriteRequest.MessageData, IToProto):
    codec: PublicCodec

    def __init__(self, mess: PublicMessage):
        super().__init__(
            seq_no=mess.seqno,
            created_at=mess.created_at,
            data=mess.data,
            uncompressed_size=len(mess.data),
            partitioning=None,
        )
        self.codec = PublicCodec.RAW

    def get_bytes(self) -> bytes:
        if self.data is None:
            return bytes()
        if isinstance(self.data, bytes):
            return self.data
        if isinstance(self.data, str):
            return self.data.encode("utf-8")
        raise ValueError("Bad data type")

    def to_message_data(self) -> StreamWriteMessage.WriteRequest.MessageData:
        data = self.get_bytes()
        return StreamWriteMessage.WriteRequest.MessageData(
            seq_no=self.seq_no,
            created_at=self.created_at,
            data=data,
            uncompressed_size=len(data),
            partitioning=None,  # unsupported by server now
        )


class MessageSendResult:
    offset: Optional[int]
    write_status: "MessageWriteStatus"


class MessageWriteStatus(enum.Enum):
    Written = 1
    AlreadyWritten = 2


class RetryPolicy:
    connection_timeout_sec: float
    overload_timeout_sec: float
    retry_access_denied: bool = False


class TopicWriterError(ydb.Error):
    def __init__(self, message: str):
        super(TopicWriterError, self).__init__(message)


class TopicWriterClosedError(ydb.Error):
    def __init__(self):
        super().__init__("Topic writer already closed")


class TopicWriterRepeatableError(TopicWriterError):
    pass


class TopicWriterStopped(TopicWriterError):
    def __init__(self):
        super(TopicWriterStopped, self).__init__(
            "topic writer was stopped by call close"
        )


def default_serializer_message_content(data: Any) -> bytes:
    if data is None:
        return bytes()
    if isinstance(data, bytes):
        return data
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, str):
        return data.encode(encoding="utf-8")
    raise ValueError("can't serialize type %s to bytes" % type(data))


def messages_to_proto_requests(
    messages: List[InternalMessage],
) -> List[StreamWriteMessage.FromClient]:
    # todo split by proto message size and codec
    res = []
    for msg in messages:
        req = StreamWriteMessage.FromClient(
            StreamWriteMessage.WriteRequest(
                messages=[msg.to_message_data()],
                codec=msg.codec,
            )
        )
        res.append(req)
    return res
