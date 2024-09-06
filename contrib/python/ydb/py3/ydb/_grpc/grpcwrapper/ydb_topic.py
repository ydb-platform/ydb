from __future__ import annotations

import datetime
import enum
import typing
from dataclasses import dataclass, field
from typing import List, Union, Dict, Optional

from google.protobuf.message import Message

from . import ydb_topic_public_types
from ... import scheme
from ... import issues

try:
    from ydb.public.api.protos import ydb_scheme_pb2, ydb_topic_pb2
except ImportError:
    from contrib.ydb.public.api.protos import ydb_scheme_pb2, ydb_topic_pb2

from .common_utils import (
    IFromProto,
    IFromProtoWithProtoType,
    IToProto,
    IToPublic,
    IFromPublic,
    ServerStatus,
    UnknownGrpcMessageError,
    proto_duration_from_timedelta,
    proto_timestamp_from_datetime,
    datetime_from_proto_timestamp,
    timedelta_from_proto_duration,
)


class Codec(int, IToPublic, IFromPublic):
    CODEC_UNSPECIFIED = 0
    CODEC_RAW = 1
    CODEC_GZIP = 2
    CODEC_LZOP = 3
    CODEC_ZSTD = 4

    @staticmethod
    def from_proto_iterable(codecs: typing.Iterable[int]) -> List["Codec"]:
        return [Codec(int(codec)) for codec in codecs]

    def to_public(self) -> ydb_topic_public_types.PublicCodec:
        return ydb_topic_public_types.PublicCodec(int(self))

    @staticmethod
    def from_public(codec: Union[ydb_topic_public_types.PublicCodec, int]) -> "Codec":
        return Codec(int(codec))


@dataclass
class SupportedCodecs(IToProto, IFromProto, IToPublic, IFromPublic):
    codecs: List[Codec]

    def to_proto(self) -> ydb_topic_pb2.SupportedCodecs:
        return ydb_topic_pb2.SupportedCodecs(
            codecs=self.codecs,
        )

    @staticmethod
    def from_proto(msg: Optional[ydb_topic_pb2.SupportedCodecs]) -> "SupportedCodecs":
        if msg is None:
            return SupportedCodecs(codecs=[])

        return SupportedCodecs(
            codecs=Codec.from_proto_iterable(msg.codecs),
        )

    def to_public(self) -> List[ydb_topic_public_types.PublicCodec]:
        return list(map(Codec.to_public, self.codecs))

    @staticmethod
    def from_public(
        codecs: Optional[List[Union[ydb_topic_public_types.PublicCodec, int]]]
    ) -> Optional["SupportedCodecs"]:
        if codecs is None:
            return None

        return SupportedCodecs(codecs=[Codec.from_public(codec) for codec in codecs])


@dataclass(order=True)
class OffsetsRange(IFromProto, IToProto):
    """
    half-opened interval, include [start, end) offsets
    """

    __slots__ = ("start", "end")

    start: int  # first offset
    end: int  # offset after last, included to range

    def __post_init__(self):
        if self.end < self.start:
            raise ValueError("offset end must be not less then start. Got start=%s end=%s" % (self.start, self.end))

    @staticmethod
    def from_proto(msg: ydb_topic_pb2.OffsetsRange) -> "OffsetsRange":
        return OffsetsRange(
            start=msg.start,
            end=msg.end,
        )

    def to_proto(self) -> ydb_topic_pb2.OffsetsRange:
        return ydb_topic_pb2.OffsetsRange(
            start=self.start,
            end=self.end,
        )

    def is_intersected_with(self, other: "OffsetsRange") -> bool:
        return (
            self.start <= other.start < self.end
            or self.start < other.end <= self.end
            or other.start <= self.start < other.end
            or other.start < self.end <= other.end
        )


@dataclass
class UpdateTokenRequest(IToProto):
    token: str

    def to_proto(self) -> Message:
        res = ydb_topic_pb2.UpdateTokenRequest()
        res.token = self.token
        return res


@dataclass
class UpdateTokenResponse(IFromProto):
    @staticmethod
    def from_proto(msg: ydb_topic_pb2.UpdateTokenResponse) -> typing.Any:
        return UpdateTokenResponse()


########################################################################################################################
#  StreamWrite
########################################################################################################################


class StreamWriteMessage:
    @dataclass()
    class InitRequest(IToProto):
        path: str
        producer_id: str
        write_session_meta: typing.Dict[str, str]
        partitioning: "StreamWriteMessage.PartitioningType"
        get_last_seq_no: bool

        def to_proto(self) -> ydb_topic_pb2.StreamWriteMessage.InitRequest:
            proto = ydb_topic_pb2.StreamWriteMessage.InitRequest()
            proto.path = self.path
            proto.producer_id = self.producer_id

            if self.partitioning is None:
                pass
            elif isinstance(self.partitioning, StreamWriteMessage.PartitioningMessageGroupID):
                proto.message_group_id = self.partitioning.message_group_id
            elif isinstance(self.partitioning, StreamWriteMessage.PartitioningPartitionID):
                proto.partition_id = self.partitioning.partition_id
            else:
                raise Exception("Bad partitioning type at StreamWriteMessage.InitRequest")

            if self.write_session_meta:
                for key in self.write_session_meta:
                    proto.write_session_meta[key] = self.write_session_meta[key]

            proto.get_last_seq_no = self.get_last_seq_no
            return proto

    @dataclass
    class InitResponse(IFromProto):
        last_seq_no: Union[int, None]
        session_id: str
        partition_id: int
        supported_codecs: typing.List[int]
        status: ServerStatus = None

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.StreamWriteMessage.InitResponse,
        ) -> "StreamWriteMessage.InitResponse":
            codecs = []  # type: typing.List[int]
            if msg.supported_codecs:
                for codec in msg.supported_codecs.codecs:
                    codecs.append(codec)

            return StreamWriteMessage.InitResponse(
                last_seq_no=msg.last_seq_no,
                session_id=msg.session_id,
                partition_id=msg.partition_id,
                supported_codecs=codecs,
            )

    @dataclass
    class WriteRequest(IToProto):
        messages: typing.List["StreamWriteMessage.WriteRequest.MessageData"]
        codec: int

        @dataclass
        class MessageData(IToProto):
            seq_no: int
            created_at: datetime.datetime
            data: bytes
            uncompressed_size: int
            partitioning: "StreamWriteMessage.PartitioningType"

            def to_proto(
                self,
            ) -> ydb_topic_pb2.StreamWriteMessage.WriteRequest.MessageData:
                proto = ydb_topic_pb2.StreamWriteMessage.WriteRequest.MessageData()
                proto.seq_no = self.seq_no
                proto.created_at.FromDatetime(self.created_at)
                proto.data = self.data
                proto.uncompressed_size = self.uncompressed_size

                if self.partitioning is None:
                    pass
                elif isinstance(self.partitioning, StreamWriteMessage.PartitioningPartitionID):
                    proto.partition_id = self.partitioning.partition_id
                elif isinstance(self.partitioning, StreamWriteMessage.PartitioningMessageGroupID):
                    proto.message_group_id = self.partitioning.message_group_id
                else:
                    raise Exception("Bad partition at StreamWriteMessage.WriteRequest.MessageData")

                return proto

        def to_proto(self) -> ydb_topic_pb2.StreamWriteMessage.WriteRequest:
            proto = ydb_topic_pb2.StreamWriteMessage.WriteRequest()
            proto.codec = self.codec

            for message in self.messages:
                proto_mess = proto.messages.add()
                proto_mess.CopyFrom(message.to_proto())

            return proto

    @dataclass
    class WriteResponse(IFromProto):
        partition_id: int
        acks: typing.List["StreamWriteMessage.WriteResponse.WriteAck"]
        write_statistics: "StreamWriteMessage.WriteResponse.WriteStatistics"
        status: Optional[ServerStatus] = field(default=None)

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.StreamWriteMessage.WriteResponse,
        ) -> "StreamWriteMessage.WriteResponse":
            acks = []
            for proto_ack in msg.acks:
                ack = StreamWriteMessage.WriteResponse.WriteAck.from_proto(proto_ack)
                acks.append(ack)
            write_statistics = StreamWriteMessage.WriteResponse.WriteStatistics(
                persisting_time=msg.write_statistics.persisting_time.ToTimedelta(),
                min_queue_wait_time=msg.write_statistics.min_queue_wait_time.ToTimedelta(),
                max_queue_wait_time=msg.write_statistics.max_queue_wait_time.ToTimedelta(),
                partition_quota_wait_time=msg.write_statistics.partition_quota_wait_time.ToTimedelta(),
                topic_quota_wait_time=msg.write_statistics.topic_quota_wait_time.ToTimedelta(),
            )
            return StreamWriteMessage.WriteResponse(
                partition_id=msg.partition_id,
                acks=acks,
                write_statistics=write_statistics,
                status=None,
            )

        @dataclass
        class WriteAck(IFromProto):
            seq_no: int
            message_write_status: Union[
                "StreamWriteMessage.WriteResponse.WriteAck.StatusWritten",
                "StreamWriteMessage.WriteResponse.WriteAck.StatusSkipped",
                int,
            ]

            @classmethod
            def from_proto(cls, proto_ack: ydb_topic_pb2.StreamWriteMessage.WriteResponse.WriteAck):
                if proto_ack.HasField("written"):
                    message_write_status = StreamWriteMessage.WriteResponse.WriteAck.StatusWritten(
                        proto_ack.written.offset
                    )
                elif proto_ack.HasField("skipped"):
                    reason = proto_ack.skipped.reason
                    try:
                        message_write_status = StreamWriteMessage.WriteResponse.WriteAck.StatusSkipped(
                            reason=StreamWriteMessage.WriteResponse.WriteAck.StatusSkipped.Reason.from_protobuf_code(
                                reason
                            )
                        )
                    except ValueError:
                        message_write_status = reason
                else:
                    raise NotImplementedError("unexpected ack status")

                return StreamWriteMessage.WriteResponse.WriteAck(
                    seq_no=proto_ack.seq_no,
                    message_write_status=message_write_status,
                )

            @dataclass
            class StatusWritten:
                offset: int

            @dataclass
            class StatusSkipped:
                reason: "StreamWriteMessage.WriteResponse.WriteAck.StatusSkipped.Reason"

                class Reason(enum.Enum):
                    UNSPECIFIED = 0
                    ALREADY_WRITTEN = 1

                    @classmethod
                    def from_protobuf_code(
                        cls, code: int
                    ) -> Union["StreamWriteMessage.WriteResponse.WriteAck.StatusSkipped.Reason", int]:
                        try:
                            return StreamWriteMessage.WriteResponse.WriteAck.StatusSkipped.Reason(code)
                        except ValueError:
                            return code

        @dataclass
        class WriteStatistics:
            persisting_time: datetime.timedelta
            min_queue_wait_time: datetime.timedelta
            max_queue_wait_time: datetime.timedelta
            partition_quota_wait_time: datetime.timedelta
            topic_quota_wait_time: datetime.timedelta

    @dataclass
    class PartitioningMessageGroupID:
        message_group_id: str

    @dataclass
    class PartitioningPartitionID:
        partition_id: int

    PartitioningType = Union[PartitioningMessageGroupID, PartitioningPartitionID, None]

    @dataclass
    class FromClient(IToProto):
        value: "WriterMessagesFromClientToServer"

        def __init__(self, value: "WriterMessagesFromClientToServer"):
            self.value = value

        def to_proto(self) -> Message:
            res = ydb_topic_pb2.StreamWriteMessage.FromClient()
            value = self.value
            if isinstance(value, StreamWriteMessage.WriteRequest):
                res.write_request.CopyFrom(value.to_proto())
            elif isinstance(value, StreamWriteMessage.InitRequest):
                res.init_request.CopyFrom(value.to_proto())
            elif isinstance(value, UpdateTokenRequest):
                res.update_token_request.CopyFrom(value.to_proto())
            else:
                raise Exception("Unknown outcoming grpc message: %s" % value)
            return res

    class FromServer(IFromProto):
        @staticmethod
        def from_proto(msg: ydb_topic_pb2.StreamWriteMessage.FromServer) -> typing.Any:
            message_type = msg.WhichOneof("server_message")
            if message_type == "write_response":
                res = StreamWriteMessage.WriteResponse.from_proto(msg.write_response)
            elif message_type == "init_response":
                res = StreamWriteMessage.InitResponse.from_proto(msg.init_response)
            elif message_type == "update_token_response":
                res = UpdateTokenResponse.from_proto(msg.update_token_response)
            else:
                # todo log instead of exception - for allow add messages in the future
                raise UnknownGrpcMessageError("Unexpected proto message: %s" % msg)

            res.status = ServerStatus(msg.status, msg.issues)
            return res


WriterMessagesFromClientToServer = Union[
    StreamWriteMessage.InitRequest, StreamWriteMessage.WriteRequest, UpdateTokenRequest
]
WriterMessagesFromServerToClient = Union[
    StreamWriteMessage.InitResponse,
    StreamWriteMessage.WriteResponse,
    UpdateTokenResponse,
]


########################################################################################################################
#  StreamRead
########################################################################################################################


class StreamReadMessage:
    @dataclass
    class PartitionSession(IFromProto):
        partition_session_id: int
        path: str
        partition_id: int

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.StreamReadMessage.PartitionSession,
        ) -> "StreamReadMessage.PartitionSession":
            return StreamReadMessage.PartitionSession(
                partition_session_id=msg.partition_session_id,
                path=msg.path,
                partition_id=msg.partition_id,
            )

    @dataclass
    class InitRequest(IToProto):
        topics_read_settings: List["StreamReadMessage.InitRequest.TopicReadSettings"]
        consumer: str

        def to_proto(self) -> ydb_topic_pb2.StreamReadMessage.InitRequest:
            res = ydb_topic_pb2.StreamReadMessage.InitRequest()
            res.consumer = self.consumer
            for settings in self.topics_read_settings:
                res.topics_read_settings.append(settings.to_proto())
            return res

        @dataclass
        class TopicReadSettings(IToProto):
            path: str
            partition_ids: List[int] = field(default_factory=list)
            max_lag: Optional[datetime.timedelta] = None
            read_from: Optional[datetime.datetime] = None

            def to_proto(
                self,
            ) -> ydb_topic_pb2.StreamReadMessage.InitRequest.TopicReadSettings:
                res = ydb_topic_pb2.StreamReadMessage.InitRequest.TopicReadSettings()
                res.path = self.path
                res.partition_ids.extend(self.partition_ids)
                max_lag = proto_duration_from_timedelta(self.max_lag)
                if max_lag is not None:
                    res.max_lag = max_lag

                read_from = proto_timestamp_from_datetime(self.read_from)
                if read_from is not None:
                    res.read_from = read_from

                return res

    @dataclass
    class InitResponse(IFromProto):
        session_id: str

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.StreamReadMessage.InitResponse,
        ) -> "StreamReadMessage.InitResponse":
            return StreamReadMessage.InitResponse(session_id=msg.session_id)

    @dataclass
    class ReadRequest(IToProto):
        bytes_size: int

        def to_proto(self) -> ydb_topic_pb2.StreamReadMessage.ReadRequest:
            res = ydb_topic_pb2.StreamReadMessage.ReadRequest()
            res.bytes_size = self.bytes_size
            return res

    @dataclass
    class ReadResponse(IFromProto):
        partition_data: List["StreamReadMessage.ReadResponse.PartitionData"]
        bytes_size: int

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.StreamReadMessage.ReadResponse,
        ) -> "StreamReadMessage.ReadResponse":
            partition_data = []
            for proto_partition_data in msg.partition_data:
                partition_data.append(StreamReadMessage.ReadResponse.PartitionData.from_proto(proto_partition_data))
            return StreamReadMessage.ReadResponse(
                partition_data=partition_data,
                bytes_size=msg.bytes_size,
            )

        @dataclass
        class MessageData(IFromProto):
            offset: int
            seq_no: int
            created_at: datetime.datetime
            data: bytes
            uncompresed_size: int
            message_group_id: str

            @staticmethod
            def from_proto(
                msg: ydb_topic_pb2.StreamReadMessage.ReadResponse.MessageData,
            ) -> "StreamReadMessage.ReadResponse.MessageData":
                return StreamReadMessage.ReadResponse.MessageData(
                    offset=msg.offset,
                    seq_no=msg.seq_no,
                    created_at=msg.created_at.ToDatetime(),
                    data=msg.data,
                    uncompresed_size=msg.uncompressed_size,
                    message_group_id=msg.message_group_id,
                )

        @dataclass
        class Batch(IFromProto):
            message_data: List["StreamReadMessage.ReadResponse.MessageData"]
            producer_id: str
            write_session_meta: Dict[str, str]
            codec: int
            written_at: datetime.datetime

            @staticmethod
            def from_proto(
                msg: ydb_topic_pb2.StreamReadMessage.ReadResponse.Batch,
            ) -> "StreamReadMessage.ReadResponse.Batch":
                message_data = []
                for message in msg.message_data:
                    message_data.append(StreamReadMessage.ReadResponse.MessageData.from_proto(message))
                return StreamReadMessage.ReadResponse.Batch(
                    message_data=message_data,
                    producer_id=msg.producer_id,
                    write_session_meta=dict(msg.write_session_meta),
                    codec=msg.codec,
                    written_at=msg.written_at.ToDatetime(),
                )

        @dataclass
        class PartitionData(IFromProto):
            partition_session_id: int
            batches: List["StreamReadMessage.ReadResponse.Batch"]

            @staticmethod
            def from_proto(
                msg: ydb_topic_pb2.StreamReadMessage.ReadResponse.PartitionData,
            ) -> "StreamReadMessage.ReadResponse.PartitionData":
                batches = []
                for proto_batch in msg.batches:
                    batches.append(StreamReadMessage.ReadResponse.Batch.from_proto(proto_batch))
                return StreamReadMessage.ReadResponse.PartitionData(
                    partition_session_id=msg.partition_session_id,
                    batches=batches,
                )

    @dataclass
    class CommitOffsetRequest(IToProto):
        commit_offsets: List["PartitionCommitOffset"]

        def to_proto(self) -> ydb_topic_pb2.StreamReadMessage.CommitOffsetRequest:
            res = ydb_topic_pb2.StreamReadMessage.CommitOffsetRequest(
                commit_offsets=list(
                    map(
                        StreamReadMessage.CommitOffsetRequest.PartitionCommitOffset.to_proto,
                        self.commit_offsets,
                    )
                ),
            )
            return res

        @dataclass
        class PartitionCommitOffset(IToProto):
            partition_session_id: int
            offsets: List["OffsetsRange"]

            def to_proto(
                self,
            ) -> ydb_topic_pb2.StreamReadMessage.CommitOffsetRequest.PartitionCommitOffset:
                res = ydb_topic_pb2.StreamReadMessage.CommitOffsetRequest.PartitionCommitOffset(
                    partition_session_id=self.partition_session_id,
                    offsets=list(map(OffsetsRange.to_proto, self.offsets)),
                )
                return res

    @dataclass
    class CommitOffsetResponse(IFromProto):
        partitions_committed_offsets: List["StreamReadMessage.CommitOffsetResponse.PartitionCommittedOffset"]

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.StreamReadMessage.CommitOffsetResponse,
        ) -> "StreamReadMessage.CommitOffsetResponse":
            return StreamReadMessage.CommitOffsetResponse(
                partitions_committed_offsets=list(
                    map(
                        StreamReadMessage.CommitOffsetResponse.PartitionCommittedOffset.from_proto,
                        msg.partitions_committed_offsets,
                    )
                )
            )

        @dataclass
        class PartitionCommittedOffset(IFromProto):
            partition_session_id: int
            committed_offset: int

            @staticmethod
            def from_proto(
                msg: ydb_topic_pb2.StreamReadMessage.CommitOffsetResponse.PartitionCommittedOffset,
            ) -> "StreamReadMessage.CommitOffsetResponse.PartitionCommittedOffset":
                return StreamReadMessage.CommitOffsetResponse.PartitionCommittedOffset(
                    partition_session_id=msg.partition_session_id,
                    committed_offset=msg.committed_offset,
                )

    @dataclass
    class PartitionSessionStatusRequest(IToProto):
        partition_session_id: int

        def to_proto(self) -> ydb_topic_pb2.StreamReadMessage.PartitionSessionStatusRequest:
            return ydb_topic_pb2.StreamReadMessage.PartitionSessionStatusRequest(
                partition_session_id=self.partition_session_id
            )

    @dataclass
    class PartitionSessionStatusResponse(IFromProto):
        partition_session_id: int
        partition_offsets: "OffsetsRange"
        committed_offset: int
        write_time_high_watermark: float

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.StreamReadMessage.PartitionSessionStatusResponse,
        ) -> "StreamReadMessage.PartitionSessionStatusResponse":
            return StreamReadMessage.PartitionSessionStatusResponse(
                partition_session_id=msg.partition_session_id,
                partition_offsets=OffsetsRange.from_proto(msg.partition_offsets),
                committed_offset=msg.committed_offset,
                write_time_high_watermark=msg.write_time_high_watermark,
            )

    @dataclass
    class StartPartitionSessionRequest(IFromProto):
        partition_session: "StreamReadMessage.PartitionSession"
        committed_offset: int
        partition_offsets: "OffsetsRange"

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.StreamReadMessage.StartPartitionSessionRequest,
        ) -> "StreamReadMessage.StartPartitionSessionRequest":
            return StreamReadMessage.StartPartitionSessionRequest(
                partition_session=StreamReadMessage.PartitionSession.from_proto(msg.partition_session),
                committed_offset=msg.committed_offset,
                partition_offsets=OffsetsRange.from_proto(msg.partition_offsets),
            )

    @dataclass
    class StartPartitionSessionResponse(IToProto):
        partition_session_id: int
        read_offset: Optional[int]
        commit_offset: Optional[int]

        def to_proto(
            self,
        ) -> ydb_topic_pb2.StreamReadMessage.StartPartitionSessionResponse:
            res = ydb_topic_pb2.StreamReadMessage.StartPartitionSessionResponse()
            res.partition_session_id = self.partition_session_id
            if self.read_offset is not None:
                res.read_offset = self.read_offset
            if self.commit_offset is not None:
                res.commit_offset = self.commit_offset
            return res

    @dataclass
    class StopPartitionSessionRequest(IFromProto):
        partition_session_id: int
        graceful: bool
        committed_offset: int

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.StreamReadMessage.StopPartitionSessionRequest,
        ) -> StreamReadMessage.StopPartitionSessionRequest:
            return StreamReadMessage.StopPartitionSessionRequest(
                partition_session_id=msg.partition_session_id,
                graceful=msg.graceful,
                committed_offset=msg.committed_offset,
            )

    @dataclass
    class StopPartitionSessionResponse(IToProto):
        partition_session_id: int

        def to_proto(self) -> ydb_topic_pb2.StreamReadMessage.StopPartitionSessionResponse:
            return ydb_topic_pb2.StreamReadMessage.StopPartitionSessionResponse(
                partition_session_id=self.partition_session_id,
            )

    @dataclass
    class FromClient(IToProto):
        client_message: "ReaderMessagesFromClientToServer"

        def __init__(self, client_message: "ReaderMessagesFromClientToServer"):
            self.client_message = client_message

        def to_proto(self) -> ydb_topic_pb2.StreamReadMessage.FromClient:
            res = ydb_topic_pb2.StreamReadMessage.FromClient()
            if isinstance(self.client_message, StreamReadMessage.ReadRequest):
                res.read_request.CopyFrom(self.client_message.to_proto())
            elif isinstance(self.client_message, StreamReadMessage.CommitOffsetRequest):
                res.commit_offset_request.CopyFrom(self.client_message.to_proto())
            elif isinstance(self.client_message, StreamReadMessage.InitRequest):
                res.init_request.CopyFrom(self.client_message.to_proto())
            elif isinstance(self.client_message, UpdateTokenRequest):
                res.update_token_request.CopyFrom(self.client_message.to_proto())
            elif isinstance(self.client_message, StreamReadMessage.StartPartitionSessionResponse):
                res.start_partition_session_response.CopyFrom(self.client_message.to_proto())
            elif isinstance(self.client_message, StreamReadMessage.StopPartitionSessionResponse):
                res.stop_partition_session_response.CopyFrom(self.client_message.to_proto())
            elif isinstance(self.client_message, StreamReadMessage.PartitionSessionStatusRequest):
                res.start_partition_session_response.CopyFrom(self.client_message.to_proto())
            else:
                raise NotImplementedError("Unknown message type: %s" % type(self.client_message))
            return res

    @dataclass
    class FromServer(IFromProto):
        server_message: "ReaderMessagesFromServerToClient"
        server_status: ServerStatus

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.StreamReadMessage.FromServer,
        ) -> "StreamReadMessage.FromServer":
            mess_type = msg.WhichOneof("server_message")
            server_status = ServerStatus.from_proto(msg)
            if mess_type == "read_response":
                return StreamReadMessage.FromServer(
                    server_status=server_status,
                    server_message=StreamReadMessage.ReadResponse.from_proto(msg.read_response),
                )
            elif mess_type == "commit_offset_response":
                return StreamReadMessage.FromServer(
                    server_status=server_status,
                    server_message=StreamReadMessage.CommitOffsetResponse.from_proto(msg.commit_offset_response),
                )
            elif mess_type == "init_response":
                return StreamReadMessage.FromServer(
                    server_status=server_status,
                    server_message=StreamReadMessage.InitResponse.from_proto(msg.init_response),
                )
            elif mess_type == "start_partition_session_request":
                return StreamReadMessage.FromServer(
                    server_status=server_status,
                    server_message=StreamReadMessage.StartPartitionSessionRequest.from_proto(
                        msg.start_partition_session_request,
                    ),
                )
            elif mess_type == "stop_partition_session_request":
                return StreamReadMessage.FromServer(
                    server_status=server_status,
                    server_message=StreamReadMessage.StopPartitionSessionRequest.from_proto(
                        msg.stop_partition_session_request
                    ),
                )
            elif mess_type == "update_token_response":
                return StreamReadMessage.FromServer(
                    server_status=server_status,
                    server_message=UpdateTokenResponse.from_proto(msg.update_token_response),
                )
            elif mess_type == "partition_session_status_response":
                return StreamReadMessage.FromServer(
                    server_status=server_status,
                    server_message=StreamReadMessage.PartitionSessionStatusResponse.from_proto(
                        msg.partition_session_status_response
                    ),
                )
            else:
                raise issues.UnexpectedGrpcMessage(
                    "Unexpected message while parse ReaderMessagesFromServerToClient: '%s'" % mess_type
                )


ReaderMessagesFromClientToServer = Union[
    StreamReadMessage.InitRequest,
    StreamReadMessage.ReadRequest,
    StreamReadMessage.CommitOffsetRequest,
    StreamReadMessage.PartitionSessionStatusRequest,
    UpdateTokenRequest,
    StreamReadMessage.StartPartitionSessionResponse,
    StreamReadMessage.StopPartitionSessionResponse,
]

ReaderMessagesFromServerToClient = Union[
    StreamReadMessage.InitResponse,
    StreamReadMessage.ReadResponse,
    StreamReadMessage.CommitOffsetResponse,
    StreamReadMessage.PartitionSessionStatusResponse,
    UpdateTokenResponse,
    StreamReadMessage.StartPartitionSessionRequest,
    StreamReadMessage.StopPartitionSessionRequest,
]


@dataclass
class MultipleWindowsStat(IFromProto, IToPublic):
    per_minute: int
    per_hour: int
    per_day: int

    @staticmethod
    def from_proto(
        msg: Optional[ydb_topic_pb2.MultipleWindowsStat],
    ) -> Optional["MultipleWindowsStat"]:
        if msg is None:
            return None
        return MultipleWindowsStat(
            per_minute=msg.per_minute,
            per_hour=msg.per_hour,
            per_day=msg.per_day,
        )

    def to_public(self) -> ydb_topic_public_types.PublicMultipleWindowsStat:
        return ydb_topic_public_types.PublicMultipleWindowsStat(
            per_minute=self.per_minute,
            per_hour=self.per_hour,
            per_day=self.per_day,
        )


@dataclass
class Consumer(IToProto, IFromProto, IFromPublic, IToPublic):
    name: str
    important: bool
    read_from: typing.Optional[datetime.datetime]
    supported_codecs: SupportedCodecs
    attributes: Dict[str, str]
    consumer_stats: typing.Optional["Consumer.ConsumerStats"]

    def to_proto(self) -> ydb_topic_pb2.Consumer:
        return ydb_topic_pb2.Consumer(
            name=self.name,
            important=self.important,
            read_from=proto_timestamp_from_datetime(self.read_from),
            supported_codecs=self.supported_codecs.to_proto(),
            attributes=self.attributes,
            # consumer_stats - readonly field
        )

    @staticmethod
    def from_proto(msg: Optional[ydb_topic_pb2.Consumer]) -> Optional["Consumer"]:
        return Consumer(
            name=msg.name,
            important=msg.important,
            read_from=datetime_from_proto_timestamp(msg.read_from),
            supported_codecs=SupportedCodecs.from_proto(msg.supported_codecs),
            attributes=dict(msg.attributes),
            consumer_stats=Consumer.ConsumerStats.from_proto(msg.consumer_stats),
        )

    @staticmethod
    def from_public(consumer: ydb_topic_public_types.PublicConsumer):
        if consumer is None:
            return None

        supported_codecs = []
        if consumer.supported_codecs is not None:
            supported_codecs = consumer.supported_codecs

        return Consumer(
            name=consumer.name,
            important=consumer.important,
            read_from=consumer.read_from,
            supported_codecs=SupportedCodecs(codecs=supported_codecs),
            attributes=consumer.attributes,
            consumer_stats=None,
        )

    def to_public(self) -> ydb_topic_public_types.PublicConsumer:
        return ydb_topic_public_types.PublicConsumer(
            name=self.name,
            important=self.important,
            read_from=self.read_from,
            supported_codecs=self.supported_codecs.to_public(),
            attributes=self.attributes,
        )

    @dataclass
    class ConsumerStats(IFromProto):
        min_partitions_last_read_time: datetime.datetime
        max_read_time_lag: datetime.timedelta
        max_write_time_lag: datetime.timedelta
        bytes_read: MultipleWindowsStat

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.Consumer.ConsumerStats,
        ) -> "Consumer.ConsumerStats":
            return Consumer.ConsumerStats(
                min_partitions_last_read_time=datetime_from_proto_timestamp(msg.min_partitions_last_read_time),
                max_read_time_lag=timedelta_from_proto_duration(msg.max_read_time_lag),
                max_write_time_lag=timedelta_from_proto_duration(msg.max_write_time_lag),
                bytes_read=MultipleWindowsStat.from_proto(msg.bytes_read),
            )


@dataclass
class AlterConsumer(IToProto, IFromPublic):
    name: str
    set_important: Optional[bool]
    set_read_from: Optional[datetime.datetime]
    set_supported_codecs: Optional[SupportedCodecs]
    alter_attributes: Optional[Dict[str, str]]

    def to_proto(self) -> ydb_topic_pb2.AlterConsumer:
        supported_codecs = None
        if self.set_supported_codecs is not None:
            supported_codecs = self.set_supported_codecs.to_proto()

        return ydb_topic_pb2.AlterConsumer(
            name=self.name,
            set_important=self.set_important,
            set_read_from=proto_timestamp_from_datetime(self.set_read_from),
            set_supported_codecs=supported_codecs,
            alter_attributes=self.alter_attributes,
        )

    @staticmethod
    def from_public(alter_consumer: ydb_topic_public_types.PublicAlterConsumer) -> AlterConsumer:
        if not alter_consumer:
            return None

        return AlterConsumer(
            name=alter_consumer.name,
            set_important=alter_consumer.set_important,
            set_read_from=alter_consumer.set_read_from,
            set_supported_codecs=SupportedCodecs.from_public(alter_consumer.set_supported_codecs),
            alter_attributes=alter_consumer.alter_attributes,
        )


@dataclass
class PartitioningSettings(IToProto, IFromProto):
    min_active_partitions: int
    partition_count_limit: int

    @staticmethod
    def from_proto(msg: ydb_topic_pb2.PartitioningSettings) -> "PartitioningSettings":
        return PartitioningSettings(
            min_active_partitions=msg.min_active_partitions,
            partition_count_limit=msg.partition_count_limit,
        )

    def to_proto(self) -> ydb_topic_pb2.PartitioningSettings:
        return ydb_topic_pb2.PartitioningSettings(
            min_active_partitions=self.min_active_partitions,
            partition_count_limit=self.partition_count_limit,
        )


@dataclass
class AlterPartitioningSettings(IToProto):
    set_min_active_partitions: Optional[int]
    set_partition_count_limit: Optional[int]

    def to_proto(self) -> ydb_topic_pb2.AlterPartitioningSettings:
        return ydb_topic_pb2.AlterPartitioningSettings(
            set_min_active_partitions=self.set_min_active_partitions,
            set_partition_count_limit=self.set_partition_count_limit,
        )


class MeteringMode(int, IFromProto, IFromPublic, IToPublic):
    UNSPECIFIED = 0
    RESERVED_CAPACITY = 1
    REQUEST_UNITS = 2

    @staticmethod
    def from_public(
        m: Optional[ydb_topic_public_types.PublicMeteringMode],
    ) -> Optional["MeteringMode"]:
        if m is None:
            return None

        return MeteringMode(m)

    @staticmethod
    def from_proto(code: Optional[int]) -> Optional["MeteringMode"]:
        if code is None:
            return None

        return MeteringMode(code)

    def to_public(self) -> ydb_topic_public_types.PublicMeteringMode:
        try:
            ydb_topic_public_types.PublicMeteringMode(int(self))
        except KeyError:
            return ydb_topic_public_types.PublicMeteringMode.UNSPECIFIED


@dataclass
class CreateTopicRequest(IToProto, IFromPublic):
    path: str
    partitioning_settings: "PartitioningSettings"
    retention_period: typing.Optional[datetime.timedelta]
    retention_storage_mb: typing.Optional[int]
    supported_codecs: "SupportedCodecs"
    partition_write_speed_bytes_per_second: typing.Optional[int]
    partition_write_burst_bytes: typing.Optional[int]
    attributes: Dict[str, str]
    consumers: List["Consumer"]
    metering_mode: "MeteringMode"

    def to_proto(self) -> ydb_topic_pb2.CreateTopicRequest:
        return ydb_topic_pb2.CreateTopicRequest(
            path=self.path,
            partitioning_settings=self.partitioning_settings.to_proto(),
            retention_period=proto_duration_from_timedelta(self.retention_period),
            retention_storage_mb=self.retention_storage_mb,
            supported_codecs=self.supported_codecs.to_proto(),
            partition_write_speed_bytes_per_second=self.partition_write_speed_bytes_per_second,
            partition_write_burst_bytes=self.partition_write_burst_bytes,
            attributes=self.attributes,
            consumers=[consumer.to_proto() for consumer in self.consumers],
            metering_mode=self.metering_mode,
        )

    @staticmethod
    def from_public(req: ydb_topic_public_types.CreateTopicRequestParams):
        supported_codecs = []

        if req.supported_codecs is not None:
            supported_codecs = req.supported_codecs

        consumers = []
        if req.consumers is not None:
            for consumer in req.consumers:
                if isinstance(consumer, str):
                    consumer = ydb_topic_public_types.PublicConsumer(name=consumer)
                consumers.append(Consumer.from_public(consumer))

        return CreateTopicRequest(
            path=req.path,
            partitioning_settings=PartitioningSettings(
                min_active_partitions=req.min_active_partitions,
                partition_count_limit=req.partition_count_limit,
            ),
            retention_period=req.retention_period,
            retention_storage_mb=req.retention_storage_mb,
            supported_codecs=SupportedCodecs(
                codecs=supported_codecs,
            ),
            partition_write_speed_bytes_per_second=req.partition_write_speed_bytes_per_second,
            partition_write_burst_bytes=req.partition_write_burst_bytes,
            attributes=req.attributes,
            consumers=consumers,
            metering_mode=MeteringMode.from_public(req.metering_mode),
        )


@dataclass
class CreateTopicResult:
    pass


@dataclass
class AlterTopicRequest(IToProto, IFromPublic):
    path: str
    add_consumers: Optional[List["Consumer"]]
    alter_partitioning_settings: Optional[AlterPartitioningSettings]
    set_retention_period: Optional[datetime.timedelta]
    set_retention_storage_mb: Optional[int]
    set_supported_codecs: Optional[SupportedCodecs]
    set_partition_write_burst_bytes: Optional[int]
    set_partition_write_speed_bytes_per_second: Optional[int]
    alter_attributes: Optional[Dict[str, str]]
    alter_consumers: Optional[List[AlterConsumer]]
    drop_consumers: Optional[List[str]]
    set_metering_mode: Optional["MeteringMode"]

    def to_proto(self) -> ydb_topic_pb2.AlterTopicRequest:
        supported_codecs = None
        if self.set_supported_codecs is not None:
            supported_codecs = self.set_supported_codecs.to_proto()

        return ydb_topic_pb2.AlterTopicRequest(
            path=self.path,
            add_consumers=[consumer.to_proto() for consumer in self.add_consumers],
            alter_partitioning_settings=self.alter_partitioning_settings.to_proto(),
            set_retention_period=proto_duration_from_timedelta(self.set_retention_period),
            set_retention_storage_mb=self.set_retention_storage_mb,
            set_supported_codecs=supported_codecs,
            set_partition_write_burst_bytes=self.set_partition_write_burst_bytes,
            set_partition_write_speed_bytes_per_second=self.set_partition_write_speed_bytes_per_second,
            alter_attributes=self.alter_attributes,
            alter_consumers=[consumer.to_proto() for consumer in self.alter_consumers],
            drop_consumers=list(self.drop_consumers),
            set_metering_mode=self.set_metering_mode,
        )

    @staticmethod
    def from_public(req: ydb_topic_public_types.AlterTopicRequestParams) -> AlterTopicRequest:
        add_consumers = []
        if req.add_consumers:
            for consumer in req.add_consumers:
                if isinstance(consumer, str):
                    consumer = ydb_topic_public_types.PublicConsumer(name=consumer)
                add_consumers.append(Consumer.from_public(consumer))

        alter_consumers = []
        if req.alter_consumers:
            for consumer in req.alter_consumers:
                if isinstance(consumer, str):
                    consumer = ydb_topic_public_types.PublicAlterConsumer(name=consumer)
                alter_consumers.append(AlterConsumer.from_public(consumer))

        drop_consumers = req.drop_consumers if req.drop_consumers else []

        return AlterTopicRequest(
            path=req.path,
            alter_partitioning_settings=AlterPartitioningSettings(
                set_min_active_partitions=req.set_min_active_partitions,
                set_partition_count_limit=req.set_partition_count_limit,
            ),
            add_consumers=add_consumers,
            set_retention_period=req.set_retention_period,
            set_retention_storage_mb=req.set_retention_storage_mb,
            set_supported_codecs=SupportedCodecs.from_public(req.set_supported_codecs),
            set_partition_write_burst_bytes=req.set_partition_write_burst_bytes,
            set_partition_write_speed_bytes_per_second=req.set_partition_write_speed_bytes_per_second,
            alter_attributes=req.alter_attributes,
            alter_consumers=alter_consumers,
            drop_consumers=drop_consumers,
            set_metering_mode=MeteringMode.from_public(req.set_metering_mode),
        )


@dataclass
class DescribeTopicRequest:
    path: str
    include_stats: bool


@dataclass
class DescribeTopicResult(IFromProtoWithProtoType, IToPublic):
    self_proto: ydb_scheme_pb2.Entry
    partitioning_settings: PartitioningSettings
    partitions: List["DescribeTopicResult.PartitionInfo"]
    retention_period: datetime.timedelta
    retention_storage_mb: int
    supported_codecs: SupportedCodecs
    partition_write_speed_bytes_per_second: int
    partition_write_burst_bytes: int
    attributes: Dict[str, str]
    consumers: List["Consumer"]
    metering_mode: MeteringMode
    topic_stats: "DescribeTopicResult.TopicStats"

    @staticmethod
    def from_proto(msg: ydb_topic_pb2.DescribeTopicResult) -> "DescribeTopicResult":
        return DescribeTopicResult(
            self_proto=msg.self,
            partitioning_settings=PartitioningSettings.from_proto(msg.partitioning_settings),
            partitions=list(map(DescribeTopicResult.PartitionInfo.from_proto, msg.partitions)),
            retention_period=msg.retention_period,
            retention_storage_mb=msg.retention_storage_mb,
            supported_codecs=SupportedCodecs.from_proto(msg.supported_codecs),
            partition_write_speed_bytes_per_second=msg.partition_write_speed_bytes_per_second,
            partition_write_burst_bytes=msg.partition_write_burst_bytes,
            attributes=dict(msg.attributes),
            consumers=list(map(Consumer.from_proto, msg.consumers)),
            metering_mode=MeteringMode.from_proto(msg.metering_mode),
            topic_stats=DescribeTopicResult.TopicStats.from_proto(msg.topic_stats),
        )

    @staticmethod
    def empty_proto_message() -> ydb_topic_pb2.DescribeTopicResult:
        return ydb_topic_pb2.DescribeTopicResult()

    def to_public(self) -> ydb_topic_public_types.PublicDescribeTopicResult:
        return ydb_topic_public_types.PublicDescribeTopicResult(
            self=scheme._wrap_scheme_entry(self.self_proto),
            min_active_partitions=self.partitioning_settings.min_active_partitions,
            partition_count_limit=self.partitioning_settings.partition_count_limit,
            partitions=list(map(DescribeTopicResult.PartitionInfo.to_public, self.partitions)),
            retention_period=self.retention_period,
            retention_storage_mb=self.retention_storage_mb,
            supported_codecs=self.supported_codecs.to_public(),
            partition_write_speed_bytes_per_second=self.partition_write_speed_bytes_per_second,
            partition_write_burst_bytes=self.partition_write_burst_bytes,
            attributes=self.attributes,
            consumers=list(map(Consumer.to_public, self.consumers)),
            metering_mode=self.metering_mode.to_public(),
            topic_stats=self.topic_stats.to_public(),
        )

    @dataclass
    class PartitionInfo(IFromProto, IToPublic):
        partition_id: int
        active: bool
        child_partition_ids: List[int]
        parent_partition_ids: List[int]
        partition_stats: "PartitionStats"

        @staticmethod
        def from_proto(
            msg: Optional[ydb_topic_pb2.DescribeTopicResult.PartitionInfo],
        ) -> Optional["DescribeTopicResult.PartitionInfo"]:
            if msg is None:
                return None

            return DescribeTopicResult.PartitionInfo(
                partition_id=msg.partition_id,
                active=msg.active,
                child_partition_ids=list(msg.child_partition_ids),
                parent_partition_ids=list(msg.parent_partition_ids),
                partition_stats=PartitionStats.from_proto(msg.partition_stats),
            )

        def to_public(
            self,
        ) -> ydb_topic_public_types.PublicDescribeTopicResult.PartitionInfo:
            partition_stats = None
            if self.partition_stats is not None:
                partition_stats = self.partition_stats.to_public()
            return ydb_topic_public_types.PublicDescribeTopicResult.PartitionInfo(
                partition_id=self.partition_id,
                active=self.active,
                child_partition_ids=self.child_partition_ids,
                parent_partition_ids=self.parent_partition_ids,
                partition_stats=partition_stats,
            )

    @dataclass
    class TopicStats(IFromProto, IToPublic):
        store_size_bytes: int
        min_last_write_time: datetime.datetime
        max_write_time_lag: datetime.timedelta
        bytes_written: "MultipleWindowsStat"

        @staticmethod
        def from_proto(
            msg: Optional[ydb_topic_pb2.DescribeTopicResult.TopicStats],
        ) -> Optional["DescribeTopicResult.TopicStats"]:
            if msg is None:
                return None

            return DescribeTopicResult.TopicStats(
                store_size_bytes=msg.store_size_bytes,
                min_last_write_time=datetime_from_proto_timestamp(msg.min_last_write_time),
                max_write_time_lag=timedelta_from_proto_duration(msg.max_write_time_lag),
                bytes_written=MultipleWindowsStat.from_proto(msg.bytes_written),
            )

        def to_public(
            self,
        ) -> ydb_topic_public_types.PublicDescribeTopicResult.TopicStats:
            return ydb_topic_public_types.PublicDescribeTopicResult.TopicStats(
                store_size_bytes=self.store_size_bytes,
                min_last_write_time=self.min_last_write_time,
                max_write_time_lag=self.max_write_time_lag,
                bytes_written=self.bytes_written.to_public(),
            )


@dataclass
class PartitionStats(IFromProto, IToPublic):
    partition_offsets: OffsetsRange
    store_size_bytes: int
    last_write_time: datetime.datetime
    max_write_time_lag: datetime.timedelta
    bytes_written: "MultipleWindowsStat"
    partition_node_id: int

    @staticmethod
    def from_proto(
        msg: Optional[ydb_topic_pb2.PartitionStats],
    ) -> Optional["PartitionStats"]:
        if msg is None:
            return None
        return PartitionStats(
            partition_offsets=OffsetsRange.from_proto(msg.partition_offsets),
            store_size_bytes=msg.store_size_bytes,
            last_write_time=datetime_from_proto_timestamp(msg.last_write_time),
            max_write_time_lag=timedelta_from_proto_duration(msg.max_write_time_lag),
            bytes_written=MultipleWindowsStat.from_proto(msg.bytes_written),
            partition_node_id=msg.partition_node_id,
        )

    def to_public(self) -> ydb_topic_public_types.PublicPartitionStats:
        return ydb_topic_public_types.PublicPartitionStats(
            partition_start=self.partition_offsets.start,
            partition_end=self.partition_offsets.end,
            store_size_bytes=self.store_size_bytes,
            last_write_time=self.last_write_time,
            max_write_time_lag=self.max_write_time_lag,
            bytes_written=self.bytes_written.to_public(),
            partition_node_id=self.partition_node_id,
        )
