from typing import Optional

from .._grpc.grpcwrapper.ydb_coordination import (
    CreateNodeRequest,
    DescribeNodeRequest,
    AlterNodeRequest,
    DropNodeRequest,
)
from .._grpc.grpcwrapper.ydb_coordination_public_types import NodeConfig
from .base import BaseCoordinationClient


class CoordinationClient(BaseCoordinationClient):
    def create_node(self, path: str, config: Optional[NodeConfig], settings=None):
        return self._call_create(
            CreateNodeRequest(path=path, config=config).to_proto(),
            settings=settings,
        )

    def describe_node(self, path: str, settings=None) -> NodeConfig:
        return self._call_describe(
            DescribeNodeRequest(path=path).to_proto(),
            settings=settings,
        )

    def alter_node(self, path: str, new_config: NodeConfig, settings=None):
        return self._call_alter(
            AlterNodeRequest(path=path, config=new_config).to_proto(),
            settings=settings,
        )

    def delete_node(self, path: str, settings=None):
        return self._call_delete(
            DropNodeRequest(path=path).to_proto(),
            settings=settings,
        )

    def lock(self):
        raise NotImplementedError("Will be implemented in future release")
