from typing import TypeAlias

from aiokafka.errors import IncompatibleBrokerVersion

from .api import Request, RequestStruct, Response
from .types import Int8, Int16, Int32, Schema, String


class FindCoordinatorResponse_v0(Response):
    API_KEY = 10
    API_VERSION = 0
    SCHEMA = Schema(
        ("error_code", Int16),
        ("coordinator_id", Int32),
        ("host", String("utf-8")),
        ("port", Int32),
    )


class FindCoordinatorResponse_v1(Response):
    API_KEY = 10
    API_VERSION = 1
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("error_code", Int16),
        ("error_message", String("utf-8")),
        ("coordinator_id", Int32),
        ("host", String("utf-8")),
        ("port", Int32),
    )


class FindCoordinatorRequest_v0(RequestStruct):
    API_KEY = 10
    API_VERSION = 0
    RESPONSE_TYPE = FindCoordinatorResponse_v0
    SCHEMA = Schema(("consumer_group", String("utf-8")))


class FindCoordinatorRequest_v1(RequestStruct):
    API_KEY = 10
    API_VERSION = 1
    RESPONSE_TYPE = FindCoordinatorResponse_v1
    SCHEMA = Schema(("coordinator_key", String("utf-8")), ("coordinator_type", Int8))


FindCoordinatorRequestStruct: TypeAlias = (
    FindCoordinatorRequest_v0 | FindCoordinatorRequest_v1
)


class FindCoordinatorRequest(Request[FindCoordinatorRequestStruct]):
    API_KEY = 10

    def __init__(self, coordinator_key: str, coordinator_type: int):
        self._coordinator_key = coordinator_key
        self._coordinator_type = coordinator_type

    def build(
        self, request_struct_class: type[FindCoordinatorRequestStruct]
    ) -> FindCoordinatorRequestStruct:
        if request_struct_class.API_VERSION < 1:
            if self._coordinator_type:
                raise IncompatibleBrokerVersion(
                    "coordinator_type requires FindCoordinatorRequest >= v1"
                )

            return request_struct_class(self._coordinator_key)
        return request_struct_class(self._coordinator_key, self._coordinator_type)
