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

    def to_proto(self) -> ydb_coordination_pb2.CreateNodeRequest:
        cfg_proto = self.config.to_proto() if self.config else None
        return ydb_coordination_pb2.CreateNodeRequest(
            path=self.path,
            config=cfg_proto,
        )


@dataclass
class AlterNodeRequest(IToProto):
    path: str
    config: NodeConfig

    def to_proto(self) -> ydb_coordination_pb2.AlterNodeRequest:
        cfg_proto = self.config.to_proto() if self.config else None
        return ydb_coordination_pb2.AlterNodeRequest(
            path=self.path,
            config=cfg_proto,
        )


@dataclass
class DescribeNodeRequest(IToProto):
    path: str

    def to_proto(self) -> ydb_coordination_pb2.DescribeNodeRequest:
        return ydb_coordination_pb2.DescribeNodeRequest(
            path=self.path,
        )


@dataclass
class DropNodeRequest(IToProto):
    path: str

    def to_proto(self) -> ydb_coordination_pb2.DropNodeRequest:
        return ydb_coordination_pb2.DropNodeRequest(
            path=self.path,
        )
