import logging
from typing import Generic

from .. import _apis, issues
from .._grpc.grpcwrapper.ydb_coordination_public_types import NodeConfig, DescribeResult
from .._typing import DriverT

logger = logging.getLogger(__name__)


def wrapper_create_node(rpc_state, response_pb):
    issues._process_response(response_pb.operation)


def wrapper_describe_node(rpc_state, response_pb) -> NodeConfig:
    issues._process_response(response_pb.operation)
    return DescribeResult.from_proto(response_pb)


def wrapper_delete_node(rpc_state, response_pb):
    issues._process_response(response_pb.operation)


def wrapper_alter_node(rpc_state, response_pb):
    issues._process_response(response_pb.operation)


class BaseCoordinationClient(Generic[DriverT]):
    _driver: DriverT

    def __init__(self, driver: DriverT) -> None:
        self._driver = driver
        self._user_warned = False

    def _call_create(self, request, settings=None):
        return self._driver(
            request,
            _apis.CoordinationService.Stub,
            _apis.CoordinationService.CreateNode,
            wrap_result=wrapper_create_node,
            settings=settings,
        )

    def _call_describe(self, request, settings=None):
        return self._driver(
            request,
            _apis.CoordinationService.Stub,
            _apis.CoordinationService.DescribeNode,
            wrap_result=wrapper_describe_node,
            settings=settings,
        )

    def _call_alter(self, request, settings=None):
        return self._driver(
            request,
            _apis.CoordinationService.Stub,
            _apis.CoordinationService.AlterNode,
            wrap_result=wrapper_alter_node,
            settings=settings,
        )

    def _call_delete(self, request, settings=None):
        return self._driver(
            request,
            _apis.CoordinationService.Stub,
            _apis.CoordinationService.DropNode,
            wrap_result=wrapper_delete_node,
            settings=settings,
        )

    def _log_experimental_api(self):
        if not self._user_warned:
            logger.warning(
                "Coordination Service API is experimental, may contain bugs and may change in future releases."
            )
            self._user_warned = True
