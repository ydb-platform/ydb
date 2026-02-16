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
class SupportedCodecs(
    IToProto,
    IFromProto[Optional["ydb_topic_pb2.SupportedCodecs"], "SupportedCodecs"],
    IToPublic,
    IFromPublic,
):
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
class OffsetsRange(IFromProto["ydb_topic_pb2.OffsetsRange", "OffsetsRange"], IToProto):
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

    def to_proto(self) -> ydb_topic_pb2.UpdateTokenRequest:
        res = ydb_topic_pb2.UpdateTokenRequest()
        res.token = self.token
        return res


@dataclass
class UpdateTokenResponse(IFromProto["ydb_topic_pb2.UpdateTokenResponse", "UpdateTokenResponse"]):
    status: Optional[ServerStatus] = None

    @staticmethod
    def from_proto(msg: ydb_topic_pb2.UpdateTokenResponse) -> "UpdateTokenResponse":
        return UpdateTokenResponse()


@dataclass
class CommitOffsetRequest(IToProto):
    path: str
    consumer: str
    partition_id: int
    offset: int
    read_session_id: Optional[str]

    def to_proto(self) -> ydb_topic_pb2.CommitOffsetRequest:
        return ydb_topic_pb2.CommitOffsetRequest(
            path=self.path,
            consumer=self.consumer,
            partition_id=self.partition_id,
            offset=self.offset,
            read_session_id=self.read_session_id,
        )


########################################################################################################################
#  StreamWrite
########################################################################################################################


@dataclass
class TransactionIdentity(IToProto):
    tx_id: str
    session_id: str

    def to_proto(self) -> ydb_topic_pb2.TransactionIdentity:
        return ydb_topic_pb2.TransactionIdentity(
            id=self.tx_id,
            session=self.session_id,
        )


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
    class InitResponse(IFromProto["ydb_topic_pb2.StreamWriteMessage.InitResponse", "StreamWriteMessage.InitResponse"]):
        last_seq_no: Union[int, None]
        session_id: str
        partition_id: int
        supported_codecs: typing.List[int]
        status: Optional[ServerStatus] = None

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
        tx_identity: Optional[TransactionIdentity]

        @dataclass
        class MessageData(IToProto):
            seq_no: int
            created_at: datetime.datetime
            data: bytes
            uncompressed_size: int
            partitioning: "StreamWriteMessage.PartitioningType"
            metadata_items: Dict[str, bytes]

            def to_proto(
                self,
            ) -> ydb_topic_pb2.StreamWriteMessage.WriteRequest.MessageData:
                proto = ydb_topic_pb2.StreamWriteMessage.WriteRequest.MessageData()
                proto.seq_no = self.seq_no
                proto.created_at.FromDatetime(self.created_at)
                proto.data = self.data
                proto.uncompressed_size = self.uncompressed_size

                for key, value in self.metadata_items.items():
                    item = ydb_topic_pb2.MetadataItem(key=key, value=value)
                    proto.metadata_items.append(item)

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

            if self.tx_identity is not None:
                proto.tx.CopyFrom(self.tx_identity.to_proto())

            for message in self.messages:
                proto_mess = proto.messages.add()
                proto_mess.CopyFrom(message.to_proto())

            return proto

    @dataclass
    class WriteResponse(
        IFromProto["ydb_topic_pb2.StreamWriteMessage.WriteResponse", "StreamWriteMessage.WriteResponse"]
    ):
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
        class WriteAck(
            IFromProto[
                "ydb_topic_pb2.StreamWriteMessage.WriteResponse.WriteAck",
                "StreamWriteMessage.WriteResponse.WriteAck",
            ]
        ):
            seq_no: int
            message_write_status: Union[
                "StreamWriteMessage.WriteResponse.WriteAck.StatusWritten",
                "StreamWriteMessage.WriteResponse.WriteAck.StatusSkipped",
                "StreamWriteMessage.WriteResponse.WriteAck.StatusWrittenInTx",
                int,
            ]

            @classmethod
            def from_proto(cls, proto_ack: ydb_topic_pb2.StreamWriteMessage.WriteResponse.WriteAck):
                message_write_status: Union[
                    StreamWriteMessage.WriteResponse.WriteAck.StatusWritten,
                    StreamWriteMessage.WriteResponse.WriteAck.StatusSkipped,
                    StreamWriteMessage.WriteResponse.WriteAck.StatusWrittenInTx,
                    int,
                ]
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
                elif proto_ack.HasField("written_in_tx"):
                    message_write_status = StreamWriteMessage.WriteResponse.WriteAck.StatusWrittenInTx()
                else:
                    raise NotImplementedError("unexpected ack status")

                return StreamWriteMessage.WriteResponse.WriteAck(
                    seq_no=proto_ack.seq_no,
                    message_write_status=message_write_status,
                )

            @dataclass
            class StatusWritten:
                offset: int

            class StatusWrittenInTx:
                pass

            @dataclass
            class StatusSkipped:
                reason: Union[
                    "StreamWriteMessage.WriteResponse.WriteAck.StatusSkipped.Reason",
                    int,
                ]

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

    class FromServer(IFromProto["ydb_topic_pb2.StreamWriteMessage.FromServer", "WriterMessagesFromServerToClient"]):
        @staticmethod
        def from_proto(msg: ydb_topic_pb2.StreamWriteMessage.FromServer) -> "WriterMessagesFromServerToClient":
            message_type = msg.WhichOneof("server_message")
            res: WriterMessagesFromServerToClient
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
    class PartitionSession(
        IFromProto["ydb_topic_pb2.StreamReadMessage.PartitionSession", "StreamReadMessage.PartitionSession"]
    ):
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
        consumer: Optional[str]
        auto_partitioning_support: bool

        def to_proto(self) -> ydb_topic_pb2.StreamReadMessage.InitRequest:
            res = ydb_topic_pb2.StreamReadMessage.InitRequest()
            if self.consumer is not None:
                res.consumer = self.consumer
            for settings in self.topics_read_settings:
                res.topics_read_settings.append(settings.to_proto())
            res.auto_partitioning_support = self.auto_partitioning_support
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
                return ydb_topic_pb2.StreamReadMessage.InitRequest.TopicReadSettings(
                    path=self.path,
                    partition_ids=self.partition_ids,
                    max_lag=proto_duration_from_timedelta(self.max_lag),
                    read_from=proto_timestamp_from_datetime(self.read_from),
                )

    @dataclass
    class InitResponse(IFromProto["ydb_topic_pb2.StreamReadMessage.InitResponse", "StreamReadMessage.InitResponse"]):
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
    class ReadResponse(IFromProto["ydb_topic_pb2.StreamReadMessage.ReadResponse", "StreamReadMessage.ReadResponse"]):
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
        class MessageData(
            IFromProto[
                "ydb_topic_pb2.StreamReadMessage.ReadResponse.MessageData",
                "StreamReadMessage.ReadResponse.MessageData",
            ]
        ):
            offset: int
            seq_no: int
            created_at: datetime.datetime
            data: bytes
            uncompresed_size: int
            message_group_id: str
            metadata_items: Dict[str, bytes]

            @staticmethod
            def from_proto(
                msg: ydb_topic_pb2.StreamReadMessage.ReadResponse.MessageData,
            ) -> "StreamReadMessage.ReadResponse.MessageData":
                metadata_items = {meta.key: meta.value for meta in msg.metadata_items}
                return StreamReadMessage.ReadResponse.MessageData(
                    offset=msg.offset,
                    seq_no=msg.seq_no,
                    created_at=msg.created_at.ToDatetime(),
                    data=msg.data,
                    metadata_items=metadata_items,
                    uncompresed_size=msg.uncompressed_size,
                    message_group_id=msg.message_group_id,
                )

        @dataclass
        class Batch(
            IFromProto[
                "ydb_topic_pb2.StreamReadMessage.ReadResponse.Batch",
                "StreamReadMessage.ReadResponse.Batch",
            ]
        ):
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
        class PartitionData(
            IFromProto[
                "ydb_topic_pb2.StreamReadMessage.ReadResponse.PartitionData",
                "StreamReadMessage.ReadResponse.PartitionData",
            ]
        ):
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
    class CommitOffsetResponse(
        IFromProto[
            "ydb_topic_pb2.StreamReadMessage.CommitOffsetResponse",
            "StreamReadMessage.CommitOffsetResponse",
        ]
    ):
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
        class PartitionCommittedOffset(
            IFromProto[
                "ydb_topic_pb2.StreamReadMessage.CommitOffsetResponse.PartitionCommittedOffset",
                "StreamReadMessage.CommitOffsetResponse.PartitionCommittedOffset",
            ]
        ):
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
    class PartitionSessionStatusResponse(
        IFromProto[
            "ydb_topic_pb2.StreamReadMessage.PartitionSessionStatusResponse",
            "StreamReadMessage.PartitionSessionStatusResponse",
        ]
    ):
        partition_session_id: int
        partition_offsets: "OffsetsRange"
        committed_offset: int
        write_time_high_watermark: Optional[datetime.datetime]

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.StreamReadMessage.PartitionSessionStatusResponse,
        ) -> "StreamReadMessage.PartitionSessionStatusResponse":
            return StreamReadMessage.PartitionSessionStatusResponse(
                partition_session_id=msg.partition_session_id,
                partition_offsets=OffsetsRange.from_proto(msg.partition_offsets),
                committed_offset=msg.committed_offset,
                write_time_high_watermark=datetime_from_proto_timestamp(msg.write_time_high_watermark),
            )

    @dataclass
    class StartPartitionSessionRequest(
        IFromProto[
            "ydb_topic_pb2.StreamReadMessage.StartPartitionSessionRequest",
            "StreamReadMessage.StartPartitionSessionRequest",
        ]
    ):
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
    class StopPartitionSessionRequest(
        IFromProto[
            "ydb_topic_pb2.StreamReadMessage.StopPartitionSessionRequest",
            "StreamReadMessage.StopPartitionSessionRequest",
        ]
    ):
        partition_session_id: int
        graceful: bool
        committed_offset: int

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.StreamReadMessage.StopPartitionSessionRequest,
        ) -> "StreamReadMessage.StopPartitionSessionRequest":
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
    class EndPartitionSession(
        IFromProto[
            "ydb_topic_pb2.StreamReadMessage.EndPartitionSession",
            "StreamReadMessage.EndPartitionSession",
        ]
    ):
        partition_session_id: int
        adjacent_partition_ids: List[int]
        child_partition_ids: List[int]

        @staticmethod
        def from_proto(
            msg: ydb_topic_pb2.StreamReadMessage.EndPartitionSession,
        ) -> "StreamReadMessage.EndPartitionSession":
            return StreamReadMessage.EndPartitionSession(
                partition_session_id=msg.partition_session_id,
                adjacent_partition_ids=list(msg.adjacent_partition_ids),
                child_partition_ids=list(msg.child_partition_ids),
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
                res.partition_session_status_request.CopyFrom(self.client_message.to_proto())
            else:
                raise NotImplementedError("Unknown message type: %s" % type(self.client_message))
            return res

    @dataclass
    class FromServer(IFromProto["ydb_topic_pb2.StreamReadMessage.FromServer", "StreamReadMessage.FromServer"]):
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
            elif mess_type == "end_partition_session":
                return StreamReadMessage.FromServer(
                    server_status=server_status,
                    server_message=StreamReadMessage.EndPartitionSession.from_proto(
                        msg.end_partition_session,
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
    StreamReadMessage.EndPartitionSession,
]


@dataclass
class MultipleWindowsStat(
    IFromProto[Optional["ydb_topic_pb2.MultipleWindowsStat"], Optional["MultipleWindowsStat"]],
    IToPublic,
):
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
class Consumer(
    IToProto,
    IFromProto[Optional["ydb_topic_pb2.Consumer"], Optional["Consumer"]],
    IFromPublic,
    IToPublic,
):
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
        if msg is None:
            return None
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

        codecs: List[Codec] = []
        if consumer.supported_codecs is not None:
            codecs = [Codec.from_public(c) for c in consumer.supported_codecs]

        return Consumer(
            name=consumer.name,
            important=consumer.important,
            read_from=consumer.read_from,
            supported_codecs=SupportedCodecs(codecs=codecs),
            attributes=consumer.attributes,
            consumer_stats=None,
        )

    def to_public(self) -> ydb_topic_public_types.PublicConsumer:
        consumer_stats = self.consumer_stats.to_public() if self.consumer_stats is not None else None
        return ydb_topic_public_types.PublicConsumer(
            name=self.name,
            important=self.important,
            read_from=self.read_from,
            supported_codecs=self.supported_codecs.to_public(),
            attributes=self.attributes,
            consumer_stats=consumer_stats,
        )

    @dataclass
    class ConsumerStats(
        IFromProto["ydb_topic_pb2.Consumer.ConsumerStats", "Consumer.ConsumerStats"],
        IToPublic,
    ):
        min_partitions_last_read_time: Optional[datetime.datetime]
        max_read_time_lag: Optional[datetime.timedelta]
        max_write_time_lag: Optional[datetime.timedelta]
        bytes_read: Optional[MultipleWindowsStat]

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

        def to_public(self) -> ydb_topic_public_types.PublicConsumer.ConsumerStats:
            bytes_read = self.bytes_read.to_public() if self.bytes_read is not None else None
            return ydb_topic_public_types.PublicConsumer.ConsumerStats(
                min_partitions_last_read_time=self.min_partitions_last_read_time,
                max_read_time_lag=self.max_read_time_lag,
                max_write_time_lag=self.max_write_time_lag,
                bytes_read=bytes_read,
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

        proto = ydb_topic_pb2.AlterConsumer(
            name=self.name,
            set_read_from=proto_timestamp_from_datetime(self.set_read_from),
            set_supported_codecs=supported_codecs,
            alter_attributes=self.alter_attributes,
        )
        if self.set_important is not None:
            proto.set_important = self.set_important
        return proto

    @staticmethod
    def from_public(alter_consumer: ydb_topic_public_types.PublicAlterConsumer) -> Optional["AlterConsumer"]:
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
class PartitioningSettings(
    IToProto,
    IFromProto["ydb_topic_pb2.PartitioningSettings", "PartitioningSettings"],
):
    min_active_partitions: Optional[int]
    partition_count_limit: Optional[int]
    max_active_partitions: Optional[int]
    auto_partitioning_settings: Optional[AutoPartitioningSettings]

    @staticmethod
    def from_proto(msg: ydb_topic_pb2.PartitioningSettings) -> "PartitioningSettings":
        return PartitioningSettings(
            min_active_partitions=msg.min_active_partitions,
            partition_count_limit=msg.partition_count_limit,
            max_active_partitions=msg.max_active_partitions,
            auto_partitioning_settings=AutoPartitioningSettings.from_proto(msg.auto_partitioning_settings),
        )

    def to_proto(self) -> ydb_topic_pb2.PartitioningSettings:
        auto_partitioning_settings = None
        if self.auto_partitioning_settings is not None:
            auto_partitioning_settings = self.auto_partitioning_settings.to_proto()

        return ydb_topic_pb2.PartitioningSettings(
            min_active_partitions=self.min_active_partitions or 0,
            partition_count_limit=self.partition_count_limit or 0,
            max_active_partitions=self.max_active_partitions or 0,
            auto_partitioning_settings=auto_partitioning_settings,
        )


class AutoPartitioningStrategy(
    int,
    IFromProto[Optional[int], Optional["AutoPartitioningStrategy"]],
    IFromPublic,
    IToPublic,
):
    UNSPECIFIED = 0
    DISABLED = 1
    SCALE_UP = 2
    SCALE_UP_AND_DOWN = 3
    PAUSED = 4

    @staticmethod
    def from_public(
        strategy: Optional[ydb_topic_public_types.PublicAutoPartitioningStrategy],
    ) -> Optional["AutoPartitioningStrategy"]:
        if strategy is None:
            return None

        return AutoPartitioningStrategy(strategy)

    @staticmethod
    def from_proto(code: Optional[int]) -> Optional["AutoPartitioningStrategy"]:
        if code is None:
            return None

        return AutoPartitioningStrategy(code)

    def to_public(self) -> ydb_topic_public_types.PublicAutoPartitioningStrategy:
        try:
            return ydb_topic_public_types.PublicAutoPartitioningStrategy(int(self))
        except KeyError:
            return ydb_topic_public_types.PublicAutoPartitioningStrategy.UNSPECIFIED


@dataclass
class AutoPartitioningSettings(
    IToProto,
    IFromProto[
        Optional["ydb_topic_pb2.AutoPartitioningSettings"],
        Optional["AutoPartitioningSettings"],
    ],
    IFromPublic,
    IToPublic,
):
    strategy: Optional[AutoPartitioningStrategy]
    partition_write_speed: Optional[AutoPartitioningWriteSpeedStrategy]

    @staticmethod
    def from_public(
        settings: Optional[ydb_topic_public_types.PublicAutoPartitioningSettings],
    ) -> Optional["AutoPartitioningSettings"]:
        if not settings:
            return None

        strategy = AutoPartitioningStrategy.from_public(settings.strategy)
        return AutoPartitioningSettings(
            strategy=strategy,
            partition_write_speed=AutoPartitioningWriteSpeedStrategy(
                stabilization_window=settings.stabilization_window,
                up_utilization_percent=settings.up_utilization_percent,
                down_utilization_percent=settings.down_utilization_percent,
            ),
        )

    @staticmethod
    def from_proto(
        msg: Optional[ydb_topic_pb2.AutoPartitioningSettings],
    ) -> Optional["AutoPartitioningSettings"]:
        if msg is None:
            return None

        return AutoPartitioningSettings(
            strategy=AutoPartitioningStrategy.from_proto(msg.strategy),
            partition_write_speed=AutoPartitioningWriteSpeedStrategy.from_proto(msg.partition_write_speed),
        )

    def to_proto(self) -> ydb_topic_pb2.AutoPartitioningSettings:
        partition_write_speed = self.partition_write_speed.to_proto() if self.partition_write_speed else None
        strategy = self.strategy if self.strategy is not None else AutoPartitioningStrategy.UNSPECIFIED
        return ydb_topic_pb2.AutoPartitioningSettings(
            strategy=strategy,  # type: ignore[arg-type]
            partition_write_speed=partition_write_speed,
        )

    def to_public(self) -> ydb_topic_public_types.PublicAutoPartitioningSettings:
        strategy = self.strategy.to_public() if self.strategy else None
        stabilization_window = self.partition_write_speed.stabilization_window if self.partition_write_speed else None
        up_utilization_percent = (
            self.partition_write_speed.up_utilization_percent if self.partition_write_speed else None
        )
        down_utilization_percent = (
            self.partition_write_speed.down_utilization_percent if self.partition_write_speed else None
        )
        return ydb_topic_public_types.PublicAutoPartitioningSettings(
            strategy=strategy,
            stabilization_window=stabilization_window,
            up_utilization_percent=up_utilization_percent,
            down_utilization_percent=down_utilization_percent,
        )


@dataclass
class AutoPartitioningWriteSpeedStrategy(
    IToProto,
    IFromProto[
        Optional["ydb_topic_pb2.AutoPartitioningWriteSpeedStrategy"],
        Optional["AutoPartitioningWriteSpeedStrategy"],
    ],
):
    stabilization_window: Optional[datetime.timedelta]
    up_utilization_percent: Optional[int]
    down_utilization_percent: Optional[int]

    def to_proto(self):
        return ydb_topic_pb2.AutoPartitioningWriteSpeedStrategy(
            stabilization_window=proto_duration_from_timedelta(self.stabilization_window),
            up_utilization_percent=self.up_utilization_percent,
            down_utilization_percent=self.down_utilization_percent,
        )

    @staticmethod
    def from_proto(
        msg: Optional[ydb_topic_pb2.AutoPartitioningWriteSpeedStrategy],
    ) -> Optional["AutoPartitioningWriteSpeedStrategy"]:
        if msg is None:
            return None

        return AutoPartitioningWriteSpeedStrategy(
            stabilization_window=timedelta_from_proto_duration(msg.stabilization_window),
            up_utilization_percent=msg.up_utilization_percent,
            down_utilization_percent=msg.down_utilization_percent,
        )


@dataclass
class AlterPartitioningSettings(IToProto):
    set_min_active_partitions: Optional[int]
    set_partition_count_limit: Optional[int]
    set_max_active_partitions: Optional[int]
    alter_auto_partitioning_settings: Optional[AlterAutoPartitioningSettings]

    def to_proto(self) -> ydb_topic_pb2.AlterPartitioningSettings:
        alter_auto_partitioning_settings = None
        if self.alter_auto_partitioning_settings is not None:
            alter_auto_partitioning_settings = self.alter_auto_partitioning_settings.to_proto()

        return ydb_topic_pb2.AlterPartitioningSettings(
            set_min_active_partitions=self.set_min_active_partitions,
            set_partition_count_limit=self.set_partition_count_limit,
            set_max_active_partitions=self.set_max_active_partitions,
            alter_auto_partitioning_settings=alter_auto_partitioning_settings,
        )


@dataclass
class AlterAutoPartitioningSettings(IToProto, IFromPublic):
    set_strategy: Optional[AutoPartitioningStrategy]
    set_partition_write_speed: Optional[AlterAutoPartitioningWriteSpeedStrategy]

    @staticmethod
    def from_public(
        settings: Optional[ydb_topic_public_types.PublicAlterAutoPartitioningSettings],
    ) -> Optional[AlterAutoPartitioningSettings]:
        if not settings:
            return None

        set_strategy = AutoPartitioningStrategy.from_public(settings.set_strategy)
        return AlterAutoPartitioningSettings(
            set_strategy=set_strategy,
            set_partition_write_speed=AlterAutoPartitioningWriteSpeedStrategy(
                set_stabilization_window=settings.set_stabilization_window,
                set_up_utilization_percent=settings.set_up_utilization_percent,
                set_down_utilization_percent=settings.set_down_utilization_percent,
            ),
        )

    def to_proto(self) -> ydb_topic_pb2.AlterAutoPartitioningSettings:
        set_partition_write_speed = None
        if self.set_partition_write_speed:
            set_partition_write_speed = self.set_partition_write_speed.to_proto()

        return ydb_topic_pb2.AlterAutoPartitioningSettings(
            set_strategy=self.set_strategy,  # type: ignore[arg-type]
            set_partition_write_speed=set_partition_write_speed,
        )


@dataclass
class AlterAutoPartitioningWriteSpeedStrategy(IToProto):
    set_stabilization_window: Optional[datetime.timedelta]
    set_up_utilization_percent: Optional[int]
    set_down_utilization_percent: Optional[int]

    def to_proto(self) -> ydb_topic_pb2.AlterAutoPartitioningWriteSpeedStrategy:
        return ydb_topic_pb2.AlterAutoPartitioningWriteSpeedStrategy(
            set_stabilization_window=proto_duration_from_timedelta(self.set_stabilization_window),
            set_up_utilization_percent=self.set_up_utilization_percent,
            set_down_utilization_percent=self.set_down_utilization_percent,
        )


class MeteringMode(
    int,
    IFromProto[Optional[int], Optional["MeteringMode"]],
    IFromPublic,
    IToPublic,
):
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
            return ydb_topic_public_types.PublicMeteringMode(int(self))
        except KeyError:
            return ydb_topic_public_types.PublicMeteringMode.UNSPECIFIED


@dataclass
class UpdateOffsetsInTransactionRequest(IToProto):
    tx: TransactionIdentity
    topics: List[UpdateOffsetsInTransactionRequest.TopicOffsets]
    consumer: str

    def to_proto(self):
        return ydb_topic_pb2.UpdateOffsetsInTransactionRequest(
            tx=self.tx.to_proto(),
            consumer=self.consumer,
            topics=list(
                map(
                    UpdateOffsetsInTransactionRequest.TopicOffsets.to_proto,
                    self.topics,
                )
            ),
        )

    @dataclass
    class TopicOffsets(IToProto):
        path: str
        partitions: List[UpdateOffsetsInTransactionRequest.TopicOffsets.PartitionOffsets]

        def to_proto(self):
            return ydb_topic_pb2.UpdateOffsetsInTransactionRequest.TopicOffsets(
                path=self.path,
                partitions=list(
                    map(
                        UpdateOffsetsInTransactionRequest.TopicOffsets.PartitionOffsets.to_proto,
                        self.partitions,
                    )
                ),
            )

        @dataclass
        class PartitionOffsets(IToProto):
            partition_id: int
            partition_offsets: List[OffsetsRange]

            def to_proto(self) -> ydb_topic_pb2.UpdateOffsetsInTransactionRequest.TopicOffsets.PartitionOffsets:
                return ydb_topic_pb2.UpdateOffsetsInTransactionRequest.TopicOffsets.PartitionOffsets(
                    partition_id=self.partition_id,
                    partition_offsets=list(map(OffsetsRange.to_proto, self.partition_offsets)),
                )


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
    metering_mode: Optional["MeteringMode"]

    def to_proto(self) -> ydb_topic_pb2.CreateTopicRequest:
        partitioning_settings = None
        if self.partitioning_settings is not None:
            partitioning_settings = self.partitioning_settings.to_proto()

        return ydb_topic_pb2.CreateTopicRequest(
            path=self.path,
            partitioning_settings=partitioning_settings,
            retention_period=proto_duration_from_timedelta(self.retention_period),
            retention_storage_mb=self.retention_storage_mb,
            supported_codecs=self.supported_codecs.to_proto(),
            partition_write_speed_bytes_per_second=self.partition_write_speed_bytes_per_second,
            partition_write_burst_bytes=self.partition_write_burst_bytes,
            attributes=self.attributes or {},
            consumers=[consumer.to_proto() for consumer in self.consumers],
            metering_mode=self.metering_mode,  # type: ignore[arg-type]
        )

    @staticmethod
    def from_public(req: ydb_topic_public_types.CreateTopicRequestParams):
        codecs: List[Codec] = []
        if req.supported_codecs is not None:
            codecs = [Codec.from_public(c) for c in req.supported_codecs]

        consumers: List[Consumer] = []
        if req.consumers is not None:
            for consumer in req.consumers:
                if isinstance(consumer, str):
                    consumer = ydb_topic_public_types.PublicConsumer(name=consumer)
                converted = Consumer.from_public(consumer)
                if converted is not None:
                    consumers.append(converted)

        auto_partitioning_settings = None
        if req.auto_partitioning_settings is not None:
            auto_partitioning_settings = AutoPartitioningSettings.from_public(req.auto_partitioning_settings)

        metering_mode = MeteringMode.from_public(req.metering_mode)

        return CreateTopicRequest(
            path=req.path,
            partitioning_settings=PartitioningSettings(
                min_active_partitions=req.min_active_partitions,
                partition_count_limit=req.partition_count_limit,
                max_active_partitions=req.max_active_partitions,
                auto_partitioning_settings=auto_partitioning_settings,
            ),
            retention_period=req.retention_period,
            retention_storage_mb=req.retention_storage_mb,
            supported_codecs=SupportedCodecs(codecs=codecs),
            partition_write_speed_bytes_per_second=req.partition_write_speed_bytes_per_second,
            partition_write_burst_bytes=req.partition_write_burst_bytes,
            attributes=req.attributes or {},
            consumers=consumers,
            metering_mode=metering_mode,
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

        add_consumers = [c.to_proto() for c in self.add_consumers] if self.add_consumers else []
        alter_partitioning_settings = (
            self.alter_partitioning_settings.to_proto() if self.alter_partitioning_settings else None
        )
        alter_consumers = [c.to_proto() for c in self.alter_consumers] if self.alter_consumers else []
        drop_consumers = list(self.drop_consumers) if self.drop_consumers else []

        return ydb_topic_pb2.AlterTopicRequest(
            path=self.path,
            add_consumers=add_consumers,
            alter_partitioning_settings=alter_partitioning_settings,
            set_retention_period=proto_duration_from_timedelta(self.set_retention_period),
            set_retention_storage_mb=self.set_retention_storage_mb,
            set_supported_codecs=supported_codecs,
            set_partition_write_burst_bytes=self.set_partition_write_burst_bytes,
            set_partition_write_speed_bytes_per_second=self.set_partition_write_speed_bytes_per_second,
            alter_attributes=self.alter_attributes,
            alter_consumers=alter_consumers,
            drop_consumers=drop_consumers,
            set_metering_mode=self.set_metering_mode,  # type: ignore[arg-type]
        )

    @staticmethod
    def from_public(req: ydb_topic_public_types.AlterTopicRequestParams) -> AlterTopicRequest:
        add_consumers: List[Consumer] = []
        if req.add_consumers:
            for c in req.add_consumers:
                pub_consumer: ydb_topic_public_types.PublicConsumer
                if isinstance(c, str):
                    pub_consumer = ydb_topic_public_types.PublicConsumer(name=c)
                else:
                    pub_consumer = c
                converted = Consumer.from_public(pub_consumer)
                if converted is not None:
                    add_consumers.append(converted)

        alter_consumers: List[AlterConsumer] = []
        if req.alter_consumers:
            for ac in req.alter_consumers:
                pub_alter: ydb_topic_public_types.PublicAlterConsumer
                if isinstance(ac, str):
                    pub_alter = ydb_topic_public_types.PublicAlterConsumer(name=ac)
                else:
                    pub_alter = ac
                converted_alter = AlterConsumer.from_public(pub_alter)
                if converted_alter is not None:
                    alter_consumers.append(converted_alter)

        alter_auto_partitioning_settings = None
        if req.alter_auto_partitioning_settings is not None:
            alter_auto_partitioning_settings = AlterAutoPartitioningSettings.from_public(
                req.alter_auto_partitioning_settings
            )

        drop_consumers = req.drop_consumers if req.drop_consumers else []

        return AlterTopicRequest(
            path=req.path,
            alter_partitioning_settings=AlterPartitioningSettings(
                set_min_active_partitions=req.set_min_active_partitions,
                set_partition_count_limit=req.set_partition_count_limit,
                set_max_active_partitions=req.set_max_active_partitions,
                alter_auto_partitioning_settings=alter_auto_partitioning_settings,
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
class DescribeTopicResult(
    IFromProtoWithProtoType["ydb_topic_pb2.DescribeTopicResult", "DescribeTopicResult"],
    IToPublic,
):
    self_proto: ydb_scheme_pb2.Entry
    partitioning_settings: PartitioningSettings
    partitions: List[Optional["DescribeTopicResult.PartitionInfo"]]
    retention_period: Optional[datetime.timedelta]
    retention_storage_mb: int
    supported_codecs: SupportedCodecs
    partition_write_speed_bytes_per_second: int
    partition_write_burst_bytes: int
    attributes: Dict[str, str]
    consumers: List[Optional["Consumer"]]
    metering_mode: Optional[MeteringMode]
    topic_stats: Optional["DescribeTopicResult.TopicStats"]

    @staticmethod
    def from_proto(msg: ydb_topic_pb2.DescribeTopicResult) -> "DescribeTopicResult":
        return DescribeTopicResult(
            self_proto=msg.self,
            partitioning_settings=PartitioningSettings.from_proto(msg.partitioning_settings),
            partitions=[DescribeTopicResult.PartitionInfo.from_proto(p) for p in msg.partitions],
            retention_period=timedelta_from_proto_duration(msg.retention_period),
            retention_storage_mb=msg.retention_storage_mb,
            supported_codecs=SupportedCodecs.from_proto(msg.supported_codecs),
            partition_write_speed_bytes_per_second=msg.partition_write_speed_bytes_per_second,
            partition_write_burst_bytes=msg.partition_write_burst_bytes,
            attributes=dict(msg.attributes),
            consumers=[Consumer.from_proto(c) for c in msg.consumers],
            metering_mode=MeteringMode.from_proto(msg.metering_mode),
            topic_stats=DescribeTopicResult.TopicStats.from_proto(msg.topic_stats),
        )

    @staticmethod
    def empty_proto_message() -> ydb_topic_pb2.DescribeTopicResult:
        return ydb_topic_pb2.DescribeTopicResult()

    def to_public(self) -> ydb_topic_public_types.PublicDescribeTopicResult:
        partitions = [p.to_public() for p in self.partitions if p is not None]
        consumers = [c.to_public() for c in self.consumers if c is not None]
        metering_mode = self.metering_mode.to_public() if self.metering_mode else None
        topic_stats = self.topic_stats.to_public() if self.topic_stats else None
        auto_partitioning_settings = (
            self.partitioning_settings.auto_partitioning_settings.to_public()
            if self.partitioning_settings.auto_partitioning_settings
            else None
        )

        return ydb_topic_public_types.PublicDescribeTopicResult(
            self=scheme._wrap_scheme_entry(self.self_proto),
            min_active_partitions=self.partitioning_settings.min_active_partitions,
            max_active_partitions=self.partitioning_settings.max_active_partitions,
            auto_partitioning_settings=auto_partitioning_settings,
            partition_count_limit=self.partitioning_settings.partition_count_limit,
            partitions=partitions,
            retention_period=self.retention_period,
            retention_storage_mb=self.retention_storage_mb,
            supported_codecs=self.supported_codecs.to_public(),
            partition_write_speed_bytes_per_second=self.partition_write_speed_bytes_per_second,
            partition_write_burst_bytes=self.partition_write_burst_bytes,
            attributes=self.attributes,
            consumers=consumers,
            metering_mode=metering_mode,
            topic_stats=topic_stats,
        )

    @dataclass
    class PartitionInfo(
        IFromProto[
            Optional["ydb_topic_pb2.DescribeTopicResult.PartitionInfo"],
            Optional["DescribeTopicResult.PartitionInfo"],
        ],
        IToPublic,
    ):
        partition_id: int
        active: bool
        child_partition_ids: List[int]
        parent_partition_ids: List[int]
        partition_stats: Optional["PartitionStats"]

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
    class TopicStats(
        IFromProto[
            Optional["ydb_topic_pb2.DescribeTopicResult.TopicStats"],
            Optional["DescribeTopicResult.TopicStats"],
        ],
        IToPublic,
    ):
        store_size_bytes: int
        min_last_write_time: Optional[datetime.datetime]
        max_write_time_lag: Optional[datetime.timedelta]
        bytes_written: Optional["MultipleWindowsStat"]

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
            bytes_written = self.bytes_written.to_public() if self.bytes_written else None
            return ydb_topic_public_types.PublicDescribeTopicResult.TopicStats(
                store_size_bytes=self.store_size_bytes,
                min_last_write_time=self.min_last_write_time,
                max_write_time_lag=self.max_write_time_lag,
                bytes_written=bytes_written,
            )


@dataclass
class PartitionStats(
    IFromProto[Optional["ydb_topic_pb2.PartitionStats"], Optional["PartitionStats"]],
    IToPublic,
):
    partition_offsets: OffsetsRange
    store_size_bytes: int
    last_write_time: Optional[datetime.datetime]
    max_write_time_lag: Optional[datetime.timedelta]
    bytes_written: Optional["MultipleWindowsStat"]
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
        bytes_written = self.bytes_written.to_public() if self.bytes_written else None
        return ydb_topic_public_types.PublicPartitionStats(
            partition_start=self.partition_offsets.start,
            partition_end=self.partition_offsets.end,
            store_size_bytes=self.store_size_bytes,
            last_write_time=self.last_write_time,
            max_write_time_lag=self.max_write_time_lag,
            bytes_written=bytes_written,
            partition_node_id=self.partition_node_id,
        )


@dataclass
class PartitionLocation(IFromProto, IToPublic):
    node_id: int
    generation: int

    @staticmethod
    def from_proto(
        msg: Optional[ydb_topic_pb2.PartitionLocation],
    ) -> Optional["PartitionLocation"]:
        if msg is None:
            return None
        return PartitionLocation(
            node_id=msg.node_id,
            generation=msg.generation,
        )

    def to_public(self) -> ydb_topic_public_types.PublicPartitionLocation:
        return ydb_topic_public_types.PublicPartitionLocation(
            node_id=self.node_id,
            generation=self.generation,
        )


@dataclass
class PartitionConsumerStats(IFromProto, IToPublic):
    last_read_offset: int
    committed_offset: int
    read_session_id: str
    partition_read_session_create_time: Optional[datetime.datetime]
    last_read_time: Optional[datetime.datetime]
    max_read_time_lag: Optional[datetime.timedelta]
    max_write_time_lag: Optional[datetime.timedelta]
    max_committed_time_lag: Optional[datetime.timedelta]
    bytes_read: Optional["MultipleWindowsStat"]
    reader_name: str
    connection_node_id: int

    @staticmethod
    def from_proto(
        msg: Optional[ydb_topic_pb2.DescribeConsumerResult.PartitionConsumerStats],
    ) -> Optional["PartitionConsumerStats"]:
        if msg is None:
            return None
        return PartitionConsumerStats(
            last_read_offset=msg.last_read_offset,
            committed_offset=msg.committed_offset,
            read_session_id=msg.read_session_id,
            partition_read_session_create_time=(
                datetime_from_proto_timestamp(msg.partition_read_session_create_time)
                if msg.HasField("partition_read_session_create_time")
                else None
            ),
            last_read_time=(
                datetime_from_proto_timestamp(msg.last_read_time) if msg.HasField("last_read_time") else None
            ),
            max_read_time_lag=(
                timedelta_from_proto_duration(msg.max_read_time_lag) if msg.HasField("max_read_time_lag") else None
            ),
            max_write_time_lag=(
                timedelta_from_proto_duration(msg.max_write_time_lag) if msg.HasField("max_write_time_lag") else None
            ),
            max_committed_time_lag=(
                timedelta_from_proto_duration(msg.max_committed_time_lag)
                if msg.HasField("max_committed_time_lag")
                else None
            ),
            bytes_read=MultipleWindowsStat.from_proto(msg.bytes_read) if msg.HasField("bytes_read") else None,
            reader_name=msg.reader_name,
            connection_node_id=msg.connection_node_id,
        )

    def to_public(self) -> ydb_topic_public_types.PublicPartitionConsumerStats:
        bytes_read = None
        if self.bytes_read is not None:
            bytes_read = self.bytes_read.to_public()
        return ydb_topic_public_types.PublicPartitionConsumerStats(
            last_read_offset=self.last_read_offset,
            committed_offset=self.committed_offset,
            read_session_id=self.read_session_id,
            partition_read_session_create_time=self.partition_read_session_create_time,
            last_read_time=self.last_read_time,
            max_read_time_lag=self.max_read_time_lag,
            max_write_time_lag=self.max_write_time_lag,
            max_committed_time_lag=self.max_committed_time_lag,
            bytes_read=bytes_read,
            reader_name=self.reader_name,
            connection_node_id=self.connection_node_id,
        )


@dataclass
class DescribeConsumerResult(IFromProtoWithProtoType, IToPublic):
    self_proto: ydb_scheme_pb2.Entry
    consumer: Consumer
    partitions: List["DescribeConsumerResult.PartitionInfo"]

    @staticmethod
    def from_proto(msg: ydb_topic_pb2.DescribeConsumerResult) -> "DescribeConsumerResult":
        consumer = Consumer.from_proto(msg.consumer)
        if consumer is None:
            raise TypeError("DescribeConsumerResult.consumer must not be None")

        partitions = [DescribeConsumerResult.PartitionInfo.from_proto(p) for p in msg.partitions if p is not None]
        return DescribeConsumerResult(
            self_proto=msg.self,
            consumer=consumer,
            partitions=[p for p in partitions if p is not None],
        )

    @staticmethod
    def empty_proto_message() -> ydb_topic_pb2.DescribeConsumerResult:
        return ydb_topic_pb2.DescribeConsumerResult()

    def to_public(self) -> ydb_topic_public_types.PublicDescribeConsumerResult:
        return ydb_topic_public_types.PublicDescribeConsumerResult(
            self=scheme._wrap_scheme_entry(self.self_proto),
            consumer=self.consumer.to_public(),
            partitions=list(map(DescribeConsumerResult.PartitionInfo.to_public, self.partitions)),
        )

    @dataclass
    class PartitionInfo(IFromProto, IToPublic):
        partition_id: int
        active: bool
        child_partition_ids: List[int]
        parent_partition_ids: List[int]
        partition_stats: Optional["PartitionStats"]
        partition_consumer_stats: Optional["PartitionConsumerStats"]
        partition_location: Optional["PartitionLocation"]

        @staticmethod
        def from_proto(
            msg: Optional[ydb_topic_pb2.DescribeConsumerResult.PartitionInfo],
        ) -> Optional["DescribeConsumerResult.PartitionInfo"]:
            if msg is None:
                return None

            return DescribeConsumerResult.PartitionInfo(
                partition_id=msg.partition_id,
                active=msg.active,
                child_partition_ids=list(msg.child_partition_ids),
                parent_partition_ids=list(msg.parent_partition_ids),
                partition_stats=(
                    PartitionStats.from_proto(msg.partition_stats) if msg.HasField("partition_stats") else None
                ),
                partition_consumer_stats=(
                    PartitionConsumerStats.from_proto(msg.partition_consumer_stats)
                    if msg.HasField("partition_consumer_stats")
                    else None
                ),
                partition_location=(
                    PartitionLocation.from_proto(msg.partition_location) if msg.HasField("partition_location") else None
                ),
            )

        def to_public(
            self,
        ) -> ydb_topic_public_types.PublicDescribeConsumerResult.PartitionInfo:
            partition_stats = None
            if self.partition_stats is not None:
                partition_stats = self.partition_stats.to_public()
            partition_consumer_stats = None
            if self.partition_consumer_stats is not None:
                partition_consumer_stats = self.partition_consumer_stats.to_public()
            partition_location = None
            if self.partition_location is not None:
                partition_location = self.partition_location.to_public()
            return ydb_topic_public_types.PublicDescribeConsumerResult.PartitionInfo(
                partition_id=self.partition_id,
                active=self.active,
                child_partition_ids=self.child_partition_ids,
                parent_partition_ids=self.parent_partition_ids,
                partition_stats=partition_stats,
                partition_consumer_stats=partition_consumer_stats,
                partition_location=partition_location,
            )
