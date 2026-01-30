import logging
from typing import Optional

from .._grpc.grpcwrapper.ydb_coordination import (
    CreateNodeRequest,
    DescribeNodeRequest,
    AlterNodeRequest,
    DropNodeRequest,
)
from .._grpc.grpcwrapper.ydb_coordination_public_types import NodeConfig
from .base import BaseCoordinationClient
from .session import CoordinationSession

logger = logging.getLogger(__name__)


class CoordinationClient(BaseCoordinationClient):
    def create_node(self, path: str, config: Optional[NodeConfig] = None, settings=None):
        self._log_experimental_api()

        return self._call_create(
            CreateNodeRequest(path=path, config=config).to_proto(),
            settings=settings,
        )

    def describe_node(self, path: str, settings=None) -> NodeConfig:
        self._log_experimental_api()

        return self._call_describe(
            DescribeNodeRequest(path=path).to_proto(),
            settings=settings,
        )

    def alter_node(self, path: str, new_config: NodeConfig, settings=None):
        self._log_experimental_api()

        return self._call_alter(
            AlterNodeRequest(path=path, config=new_config).to_proto(),
            settings=settings,
        )

    def delete_node(self, path: str, settings=None):
        self._log_experimental_api()

        return self._call_delete(
            DropNodeRequest(path=path).to_proto(),
            settings=settings,
        )

    def session(self, path: str) -> CoordinationSession:
        self._log_experimental_api()

        return CoordinationSession(self, path)
