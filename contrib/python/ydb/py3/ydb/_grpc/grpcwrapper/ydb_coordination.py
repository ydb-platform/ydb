import typing
from dataclasses import dataclass

from .ydb_coordination_public_types import NodeConfig

try:
    from ydb.public.api.protos import ydb_coordination_pb2
except ImportError:
    from contrib.ydb.public.api.protos import ydb_coordination_pb2

from .common_utils import IToProto


@dataclass
class CreateNodeRequest(IToProto):
    path: str
    config: typing.Optional[NodeConfig]

    def to_proto(self) -> "ydb_coordination_pb2.CreateNodeRequest":
        cfg_proto = self.config.to_proto() if self.config else None
        return ydb_coordination_pb2.CreateNodeRequest(
            path=self.path,
            config=cfg_proto,
        )


@dataclass
class AlterNodeRequest(IToProto):
    path: str
    config: NodeConfig

    def to_proto(self) -> "ydb_coordination_pb2.AlterNodeRequest":
        cfg_proto = self.config.to_proto() if self.config else None
        return ydb_coordination_pb2.AlterNodeRequest(
            path=self.path,
            config=cfg_proto,
        )


@dataclass
class DescribeNodeRequest(IToProto):
    path: str

    def to_proto(self) -> "ydb_coordination_pb2.DescribeNodeRequest":
        return ydb_coordination_pb2.DescribeNodeRequest(
            path=self.path,
        )


@dataclass
class DropNodeRequest(IToProto):
    path: str

    def to_proto(self) -> "ydb_coordination_pb2.DropNodeRequest":
        return ydb_coordination_pb2.DropNodeRequest(
            path=self.path,
        )


@dataclass
class SessionStart(IToProto):
    path: str
    timeout_millis: int
    description: str = ""
    session_id: int = 0
    seq_no: int = 0
    protection_key: bytes = b""

    def to_proto(self) -> "ydb_coordination_pb2.SessionRequest":
        return ydb_coordination_pb2.SessionRequest(
            session_start=ydb_coordination_pb2.SessionRequest.SessionStart(
                path=self.path,
                session_id=self.session_id,
                timeout_millis=self.timeout_millis,
                description=self.description,
                seq_no=self.seq_no,
                protection_key=self.protection_key,
            )
        )


@dataclass
class SessionStop(IToProto):
    def to_proto(self) -> "ydb_coordination_pb2.SessionRequest":
        return ydb_coordination_pb2.SessionRequest(session_stop=ydb_coordination_pb2.SessionRequest.SessionStop())


@dataclass
class Ping(IToProto):
    opaque: int = 0

    def to_proto(self) -> "ydb_coordination_pb2.SessionRequest":
        return ydb_coordination_pb2.SessionRequest(
            ping=ydb_coordination_pb2.SessionRequest.PingPong(opaque=self.opaque)
        )


@dataclass
class CreateSemaphore(IToProto):
    name: str
    req_id: int
    limit: int
    data: bytes = b""

    def to_proto(self) -> "ydb_coordination_pb2.SessionRequest":
        return ydb_coordination_pb2.SessionRequest(
            create_semaphore=ydb_coordination_pb2.SessionRequest.CreateSemaphore(
                req_id=self.req_id, name=self.name, limit=self.limit, data=self.data
            )
        )


@dataclass
class AcquireSemaphore(IToProto):
    name: str
    req_id: int
    count: int = 1
    timeout_millis: int = 0
    data: bytes = b""
    ephemeral: bool = False

    def to_proto(self) -> "ydb_coordination_pb2.SessionRequest":
        return ydb_coordination_pb2.SessionRequest(
            acquire_semaphore=ydb_coordination_pb2.SessionRequest.AcquireSemaphore(
                req_id=self.req_id,
                name=self.name,
                timeout_millis=self.timeout_millis,
                count=self.count,
                data=self.data,
                ephemeral=self.ephemeral,
            )
        )


@dataclass
class ReleaseSemaphore(IToProto):
    name: str
    req_id: int

    def to_proto(self) -> "ydb_coordination_pb2.SessionRequest":
        return ydb_coordination_pb2.SessionRequest(
            release_semaphore=ydb_coordination_pb2.SessionRequest.ReleaseSemaphore(req_id=self.req_id, name=self.name)
        )


@dataclass
class DescribeSemaphore(IToProto):
    include_owners: bool
    include_waiters: bool
    name: str
    req_id: int
    watch_data: bool
    watch_owners: bool

    def to_proto(self) -> "ydb_coordination_pb2.SessionRequest":
        return ydb_coordination_pb2.SessionRequest(
            describe_semaphore=ydb_coordination_pb2.SessionRequest.DescribeSemaphore(
                include_owners=self.include_owners,
                include_waiters=self.include_waiters,
                name=self.name,
                req_id=self.req_id,
                watch_data=self.watch_data,
                watch_owners=self.watch_owners,
            )
        )


@dataclass
class UpdateSemaphore(IToProto):
    name: str
    req_id: int
    data: bytes

    def to_proto(self) -> "ydb_coordination_pb2.SessionRequest":
        return ydb_coordination_pb2.SessionRequest(
            update_semaphore=ydb_coordination_pb2.SessionRequest.UpdateSemaphore(
                req_id=self.req_id, name=self.name, data=self.data
            )
        )


@dataclass
class DeleteSemaphore(IToProto):
    name: str
    req_id: int
    force: bool = False

    def to_proto(self) -> "ydb_coordination_pb2.SessionRequest":
        return ydb_coordination_pb2.SessionRequest(
            delete_semaphore=ydb_coordination_pb2.SessionRequest.DeleteSemaphore(
                req_id=self.req_id, name=self.name, force=self.force
            )
        )


@dataclass
class FromServer:
    raw: "ydb_coordination_pb2.SessionResponse"

    @staticmethod
    def from_proto(resp: "ydb_coordination_pb2.SessionResponse") -> "FromServer":
        return FromServer(raw=resp)

    def __getattr__(self, name: str):
        return getattr(self.raw, name)

    @property
    def session_started(self) -> typing.Optional["ydb_coordination_pb2.SessionResponse.SessionStarted"]:
        s = self.raw.session_started
        return s if s.session_id else None

    @property
    def opaque(self) -> typing.Optional[int]:
        if self.raw.HasField("ping"):
            return self.raw.ping.opaque
        return None

    @property
    def acquire_semaphore_result(self):
        return self.raw.acquire_semaphore_result if self.raw.HasField("acquire_semaphore_result") else None

    @property
    def create_semaphore_result(self):
        return self.raw.create_semaphore_result if self.raw.HasField("create_semaphore_result") else None
