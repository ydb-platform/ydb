from typing import TypeAlias

from .api import Request, RequestStruct, Response
from .struct import Struct
from .types import Array, Bytes, Int16, Int32, Schema, String


class JoinGroupResponse_v0(Response):
    API_KEY = 11
    API_VERSION = 0
    SCHEMA = Schema(
        ("error_code", Int16),
        ("generation_id", Int32),
        ("group_protocol", String("utf-8")),
        ("leader_id", String("utf-8")),
        ("member_id", String("utf-8")),
        ("members", Array(("member_id", String("utf-8")), ("member_metadata", Bytes))),
    )


class JoinGroupResponse_v1(Response):
    API_KEY = 11
    API_VERSION = 1
    SCHEMA = JoinGroupResponse_v0.SCHEMA


class JoinGroupResponse_v2(Response):
    API_KEY = 11
    API_VERSION = 2
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("error_code", Int16),
        ("generation_id", Int32),
        ("group_protocol", String("utf-8")),
        ("leader_id", String("utf-8")),
        ("member_id", String("utf-8")),
        ("members", Array(("member_id", String("utf-8")), ("member_metadata", Bytes))),
    )


class JoinGroupResponse_v5(Response):
    API_KEY = 11
    API_VERSION = 5
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("error_code", Int16),
        ("generation_id", Int32),
        ("group_protocol", String("utf-8")),
        ("leader_id", String("utf-8")),
        ("member_id", String("utf-8")),
        (
            "members",
            Array(
                ("member_id", String("utf-8")),
                ("group_instance_id", String("utf-8")),
                ("member_metadata", Bytes),
            ),
        ),
    )


class JoinGroupRequest_v0(RequestStruct):
    API_KEY = 11
    API_VERSION = 0
    RESPONSE_TYPE = JoinGroupResponse_v0
    SCHEMA = Schema(
        ("group", String("utf-8")),
        ("session_timeout", Int32),
        ("member_id", String("utf-8")),
        ("protocol_type", String("utf-8")),
        (
            "group_protocols",
            Array(("protocol_name", String("utf-8")), ("protocol_metadata", Bytes)),
        ),
    )


class JoinGroupRequest_v1(RequestStruct):
    API_KEY = 11
    API_VERSION = 1
    RESPONSE_TYPE = JoinGroupResponse_v1
    SCHEMA = Schema(
        ("group", String("utf-8")),
        ("session_timeout", Int32),
        ("rebalance_timeout", Int32),
        ("member_id", String("utf-8")),
        ("protocol_type", String("utf-8")),
        (
            "group_protocols",
            Array(("protocol_name", String("utf-8")), ("protocol_metadata", Bytes)),
        ),
    )


class JoinGroupRequest_v2(RequestStruct):
    API_KEY = 11
    API_VERSION = 2
    RESPONSE_TYPE = JoinGroupResponse_v2
    SCHEMA = JoinGroupRequest_v1.SCHEMA


class JoinGroupRequest_v5(RequestStruct):
    API_KEY = 11
    API_VERSION = 5
    RESPONSE_TYPE = JoinGroupResponse_v5
    SCHEMA = Schema(
        ("group", String("utf-8")),
        ("session_timeout", Int32),
        ("rebalance_timeout", Int32),
        ("member_id", String("utf-8")),
        ("group_instance_id", String("utf-8")),
        ("protocol_type", String("utf-8")),
        (
            "group_protocols",
            Array(("protocol_name", String("utf-8")), ("protocol_metadata", Bytes)),
        ),
    )


JoinGroupRequestStruct: TypeAlias = (
    JoinGroupRequest_v0
    | JoinGroupRequest_v1
    | JoinGroupRequest_v2
    | JoinGroupRequest_v5
)


class JoinGroupRequest(Request[JoinGroupRequestStruct]):
    API_KEY = 11
    UNKNOWN_MEMBER_ID = ""

    def __init__(
        self,
        group: str,
        session_timeout: int,
        rebalance_timeout: int,
        member_id: str,
        group_instance_id: str,
        protocol_type: str,
        group_protocols: list[tuple[str, bytes]],
    ):
        self._group = group
        self._session_timeout = session_timeout
        self._rebalance_timeout = rebalance_timeout
        self._member_id = member_id
        self._group_instance_id = group_instance_id
        self._protocol_type = protocol_type
        self._group_protocols = group_protocols

    def build(
        self, request_struct_class: type[JoinGroupRequestStruct]
    ) -> JoinGroupRequestStruct:
        if request_struct_class.API_VERSION == 0:
            return request_struct_class(
                self._group,
                self._session_timeout,
                self._member_id,
                self._protocol_type,
                self._group_protocols,
            )
        if request_struct_class.API_VERSION <= 2:
            return request_struct_class(
                self._group,
                self._session_timeout,
                self._rebalance_timeout,
                self._member_id,
                self._protocol_type,
                self._group_protocols,
            )
        return request_struct_class(
            self._group,
            self._session_timeout,
            self._rebalance_timeout,
            self._member_id,
            self._group_instance_id,
            self._protocol_type,
            self._group_protocols,
        )


class ProtocolMetadata(Struct):
    SCHEMA = Schema(
        ("version", Int16),
        ("subscription", Array(String("utf-8"))),  # topics list
        ("user_data", Bytes),
    )


class SyncGroupResponse_v0(Response):
    API_KEY = 14
    API_VERSION = 0
    SCHEMA = Schema(("error_code", Int16), ("member_assignment", Bytes))


class SyncGroupResponse_v1(Response):
    API_KEY = 14
    API_VERSION = 1
    SCHEMA = Schema(
        ("throttle_time_ms", Int32), ("error_code", Int16), ("member_assignment", Bytes)
    )


class SyncGroupResponse_v3(Response):
    API_KEY = 14
    API_VERSION = 3
    SCHEMA = SyncGroupResponse_v1.SCHEMA


class SyncGroupRequest_v0(RequestStruct):
    API_KEY = 14
    API_VERSION = 0
    RESPONSE_TYPE = SyncGroupResponse_v0
    SCHEMA = Schema(
        ("group", String("utf-8")),
        ("generation_id", Int32),
        ("member_id", String("utf-8")),
        (
            "group_assignment",
            Array(("member_id", String("utf-8")), ("member_metadata", Bytes)),
        ),
    )


class SyncGroupRequest_v1(RequestStruct):
    API_KEY = 14
    API_VERSION = 1
    RESPONSE_TYPE = SyncGroupResponse_v1
    SCHEMA = SyncGroupRequest_v0.SCHEMA


class SyncGroupRequest_v3(RequestStruct):
    API_KEY = 14
    API_VERSION = 3
    RESPONSE_TYPE = SyncGroupResponse_v3
    SCHEMA = Schema(
        ("group", String("utf-8")),
        ("generation_id", Int32),
        ("member_id", String("utf-8")),
        ("group_instance_id", String("utf-8")),
        (
            "group_assignment",
            Array(("member_id", String("utf-8")), ("member_metadata", Bytes)),
        ),
    )


SyncGroupRequestStruct: TypeAlias = (
    SyncGroupRequest_v0 | SyncGroupRequest_v1 | SyncGroupRequest_v3
)


class SyncGroupRequest(Request[SyncGroupRequestStruct]):
    API_KEY = 14

    def __init__(
        self,
        group: str,
        generation_id: int,
        member_id: str,
        group_instance_id: str,
        group_assignment: list[tuple[str, bytes]],
    ):
        self._group = group
        self._generation_id = generation_id
        self._member_id = member_id
        self._group_instance_id = group_instance_id
        self._group_assignment = group_assignment

    def build(
        self, request_struct_class: type[SyncGroupRequestStruct]
    ) -> SyncGroupRequestStruct:
        if request_struct_class.API_VERSION < 3:
            return request_struct_class(
                self._group,
                self._generation_id,
                self._member_id,
                self._group_assignment,
            )
        return request_struct_class(
            self._group,
            self._generation_id,
            self._member_id,
            self._group_instance_id,
            self._group_assignment,
        )


class MemberAssignment(Struct):
    SCHEMA = Schema(
        ("version", Int16),
        ("assignment", Array(("topic", String("utf-8")), ("partitions", Array(Int32)))),
        ("user_data", Bytes),
    )


class HeartbeatResponse_v0(Response):
    API_KEY = 12
    API_VERSION = 0
    SCHEMA = Schema(("error_code", Int16))


class HeartbeatResponse_v1(Response):
    API_KEY = 12
    API_VERSION = 1
    SCHEMA = Schema(("throttle_time_ms", Int32), ("error_code", Int16))


class HeartbeatRequest_v0(RequestStruct):
    API_KEY = 12
    API_VERSION = 0
    RESPONSE_TYPE = HeartbeatResponse_v0
    SCHEMA = Schema(
        ("group", String("utf-8")),
        ("generation_id", Int32),
        ("member_id", String("utf-8")),
    )


class HeartbeatRequest_v1(RequestStruct):
    API_KEY = 12
    API_VERSION = 1
    RESPONSE_TYPE = HeartbeatResponse_v1
    SCHEMA = HeartbeatRequest_v0.SCHEMA


HeartbeatRequestStruct: TypeAlias = HeartbeatRequest_v0 | HeartbeatRequest_v1


class HeartbeatRequest(Request[HeartbeatRequestStruct]):
    API_KEY = 12

    def __init__(self, group: str, generation_id: int, member_id: str):
        self._group = group
        self._generation_id = generation_id
        self._member_id = member_id

    def build(
        self, request_struct_class: type[HeartbeatRequestStruct]
    ) -> HeartbeatRequestStruct:
        return request_struct_class(
            self._group,
            self._generation_id,
            self._member_id,
        )


class LeaveGroupResponse_v0(Response):
    API_KEY = 13
    API_VERSION = 0
    SCHEMA = Schema(("error_code", Int16))


class LeaveGroupResponse_v1(Response):
    API_KEY = 13
    API_VERSION = 1
    SCHEMA = Schema(("throttle_time_ms", Int32), ("error_code", Int16))


class LeaveGroupRequest_v0(RequestStruct):
    API_KEY = 13
    API_VERSION = 0
    RESPONSE_TYPE = LeaveGroupResponse_v0
    SCHEMA = Schema(("group", String("utf-8")), ("member_id", String("utf-8")))


class LeaveGroupRequest_v1(RequestStruct):
    API_KEY = 13
    API_VERSION = 1
    RESPONSE_TYPE = LeaveGroupResponse_v1
    SCHEMA = LeaveGroupRequest_v0.SCHEMA


LeaveGroupRequestStruct: TypeAlias = LeaveGroupRequest_v0 | LeaveGroupRequest_v1


class LeaveGroupRequest(Request[LeaveGroupRequestStruct]):
    API_KEY = 13

    def __init__(self, group: str, member_id: str):
        self._group = group
        self._member_id = member_id

    def build(
        self, request_struct_class: type[LeaveGroupRequestStruct]
    ) -> LeaveGroupRequestStruct:
        return request_struct_class(
            self._group,
            self._member_id,
        )
