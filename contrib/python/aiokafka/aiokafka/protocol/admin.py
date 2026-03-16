from collections.abc import Iterable
from typing import Any, TypeAlias

from aiokafka.errors import IncompatibleBrokerVersion

from .api import Request, RequestStruct, Response
from .types import (
    Array,
    Boolean,
    Bytes,
    CompactArray,
    CompactString,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Schema,
    String,
    TaggedFields,
)


class ApiVersionResponse_v0(Response):
    API_KEY = 18
    API_VERSION = 0
    SCHEMA = Schema(
        ("error_code", Int16),
        (
            "api_versions",
            Array(("api_key", Int16), ("min_version", Int16), ("max_version", Int16)),
        ),
    )


class ApiVersionResponse_v1(Response):
    API_KEY = 18
    API_VERSION = 1
    SCHEMA = Schema(
        ("error_code", Int16),
        (
            "api_versions",
            Array(("api_key", Int16), ("min_version", Int16), ("max_version", Int16)),
        ),
        ("throttle_time_ms", Int32),
    )


class ApiVersionResponse_v2(Response):
    API_KEY = 18
    API_VERSION = 2
    SCHEMA = ApiVersionResponse_v1.SCHEMA


class ApiVersionRequest_v0(RequestStruct):
    API_KEY = 18
    API_VERSION = 0
    RESPONSE_TYPE = ApiVersionResponse_v0
    SCHEMA = Schema()


class ApiVersionRequest_v1(RequestStruct):
    API_KEY = 18
    API_VERSION = 1
    RESPONSE_TYPE = ApiVersionResponse_v1
    SCHEMA = ApiVersionRequest_v0.SCHEMA


class ApiVersionRequest_v2(RequestStruct):
    API_KEY = 18
    API_VERSION = 2
    RESPONSE_TYPE = ApiVersionResponse_v1
    SCHEMA = ApiVersionRequest_v0.SCHEMA


ApiVersionRequestStruct: TypeAlias = (
    ApiVersionRequest_v0 | ApiVersionRequest_v1 | ApiVersionRequest_v2
)


class ApiVersionRequest(Request[ApiVersionRequestStruct]):
    API_KEY = 18
    ALLOW_UNKNOWN_API_VERSION = True

    def build(
        self, request_struct_class: type[ApiVersionRequestStruct]
    ) -> ApiVersionRequestStruct:
        return request_struct_class()


class CreateTopicsResponse_v0(Response):
    API_KEY = 19
    API_VERSION = 0
    SCHEMA = Schema(
        ("topic_errors", Array(("topic", String("utf-8")), ("error_code", Int16)))
    )


class CreateTopicsResponse_v1(Response):
    API_KEY = 19
    API_VERSION = 1
    SCHEMA = Schema(
        (
            "topic_errors",
            Array(
                ("topic", String("utf-8")),
                ("error_code", Int16),
                ("error_message", String("utf-8")),
            ),
        )
    )


class CreateTopicsResponse_v2(Response):
    API_KEY = 19
    API_VERSION = 2
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "topic_errors",
            Array(
                ("topic", String("utf-8")),
                ("error_code", Int16),
                ("error_message", String("utf-8")),
            ),
        ),
    )


class CreateTopicsResponse_v3(Response):
    API_KEY = 19
    API_VERSION = 3
    SCHEMA = CreateTopicsResponse_v2.SCHEMA


class CreateTopicsRequest_v0(RequestStruct):
    API_KEY = 19
    API_VERSION = 0
    RESPONSE_TYPE = CreateTopicsResponse_v0
    SCHEMA = Schema(
        (
            "create_topic_requests",
            Array(
                ("topic", String("utf-8")),
                ("num_partitions", Int32),
                ("replication_factor", Int16),
                (
                    "replica_assignment",
                    Array(("partition_id", Int32), ("replicas", Array(Int32))),
                ),
                (
                    "configs",
                    Array(
                        ("config_key", String("utf-8")),
                        ("config_value", String("utf-8")),
                    ),
                ),
            ),
        ),
        ("timeout", Int32),
    )


class CreateTopicsRequest_v1(RequestStruct):
    API_KEY = 19
    API_VERSION = 1
    RESPONSE_TYPE = CreateTopicsResponse_v1
    SCHEMA = Schema(
        (
            "create_topic_requests",
            Array(
                ("topic", String("utf-8")),
                ("num_partitions", Int32),
                ("replication_factor", Int16),
                (
                    "replica_assignment",
                    Array(("partition_id", Int32), ("replicas", Array(Int32))),
                ),
                (
                    "configs",
                    Array(
                        ("config_key", String("utf-8")),
                        ("config_value", String("utf-8")),
                    ),
                ),
            ),
        ),
        ("timeout", Int32),
        ("validate_only", Boolean),
    )


class CreateTopicsRequest_v2(RequestStruct):
    API_KEY = 19
    API_VERSION = 2
    RESPONSE_TYPE = CreateTopicsResponse_v2
    SCHEMA = CreateTopicsRequest_v1.SCHEMA


class CreateTopicsRequest_v3(RequestStruct):
    API_KEY = 19
    API_VERSION = 3
    RESPONSE_TYPE = CreateTopicsResponse_v3
    SCHEMA = CreateTopicsRequest_v1.SCHEMA


CreateTopicsRequestStruct: TypeAlias = (
    CreateTopicsRequest_v0
    | CreateTopicsRequest_v1
    | CreateTopicsRequest_v2
    | CreateTopicsRequest_v3
)


class CreateTopicsRequest(Request[CreateTopicsRequestStruct]):
    API_KEY = 19

    def __init__(
        self,
        create_topic_requests: list[tuple[Any]],
        timeout: int | None,
        validate_only: bool,
    ):
        self.create_topic_requests = create_topic_requests
        self.timeout = timeout
        self.validate_only = validate_only

    def build(
        self, request_struct_class: type[CreateTopicsRequestStruct]
    ) -> CreateTopicsRequestStruct:
        if request_struct_class.API_VERSION == 0:
            if self.validate_only:
                raise IncompatibleBrokerVersion(
                    "validate_only requires CreateTopicsRequest >= v1"
                )
            return request_struct_class(
                create_topic_requests=self.create_topic_requests,
                timeout=self.timeout,
            )
        return request_struct_class(
            create_topic_requests=self.create_topic_requests,
            timeout=self.timeout,
            validate_only=self.validate_only,
        )


class DeleteTopicsResponse_v0(Response):
    API_KEY = 20
    API_VERSION = 0
    SCHEMA = Schema(
        ("topic_error_codes", Array(("topic", String("utf-8")), ("error_code", Int16)))
    )


class DeleteTopicsResponse_v1(Response):
    API_KEY = 20
    API_VERSION = 1
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("topic_error_codes", Array(("topic", String("utf-8")), ("error_code", Int16))),
    )


class DeleteTopicsResponse_v2(Response):
    API_KEY = 20
    API_VERSION = 2
    SCHEMA = DeleteTopicsResponse_v1.SCHEMA


class DeleteTopicsResponse_v3(Response):
    API_KEY = 20
    API_VERSION = 3
    SCHEMA = DeleteTopicsResponse_v1.SCHEMA


class DeleteTopicsRequest_v0(RequestStruct):
    API_KEY = 20
    API_VERSION = 0
    RESPONSE_TYPE = DeleteTopicsResponse_v0
    SCHEMA = Schema(("topics", Array(String("utf-8"))), ("timeout", Int32))


class DeleteTopicsRequest_v1(RequestStruct):
    API_KEY = 20
    API_VERSION = 1
    RESPONSE_TYPE = DeleteTopicsResponse_v1
    SCHEMA = DeleteTopicsRequest_v0.SCHEMA


class DeleteTopicsRequest_v2(RequestStruct):
    API_KEY = 20
    API_VERSION = 2
    RESPONSE_TYPE = DeleteTopicsResponse_v2
    SCHEMA = DeleteTopicsRequest_v0.SCHEMA


class DeleteTopicsRequest_v3(RequestStruct):
    API_KEY = 20
    API_VERSION = 3
    RESPONSE_TYPE = DeleteTopicsResponse_v3
    SCHEMA = DeleteTopicsRequest_v0.SCHEMA


DeleteTopicsRequestStruct: TypeAlias = (
    DeleteTopicsRequest_v0
    | DeleteTopicsRequest_v1
    | DeleteTopicsRequest_v2
    | DeleteTopicsRequest_v3
)


class DeleteTopicsRequest(Request[DeleteTopicsRequestStruct]):
    API_KEY = 20

    def __init__(self, topics: list[str], timeout: int):
        self._topics = topics
        self._timeout = timeout

    def build(
        self, request_struct_class: type[DeleteTopicsRequestStruct]
    ) -> DeleteTopicsRequestStruct:
        return request_struct_class(self._topics, self._timeout)


class ListGroupsResponse_v0(Response):
    API_KEY = 16
    API_VERSION = 0
    SCHEMA = Schema(
        ("error_code", Int16),
        (
            "groups",
            Array(("group", String("utf-8")), ("protocol_type", String("utf-8"))),
        ),
    )


class ListGroupsResponse_v1(Response):
    API_KEY = 16
    API_VERSION = 1
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("error_code", Int16),
        (
            "groups",
            Array(("group", String("utf-8")), ("protocol_type", String("utf-8"))),
        ),
    )


class ListGroupsResponse_v2(Response):
    API_KEY = 16
    API_VERSION = 2
    SCHEMA = ListGroupsResponse_v1.SCHEMA


class ListGroupsRequest_v0(RequestStruct):
    API_KEY = 16
    API_VERSION = 0
    RESPONSE_TYPE = ListGroupsResponse_v0
    SCHEMA = Schema()


class ListGroupsRequest_v1(RequestStruct):
    API_KEY = 16
    API_VERSION = 1
    RESPONSE_TYPE = ListGroupsResponse_v1
    SCHEMA = ListGroupsRequest_v0.SCHEMA


class ListGroupsRequest_v2(RequestStruct):
    API_KEY = 16
    API_VERSION = 1
    RESPONSE_TYPE = ListGroupsResponse_v2
    SCHEMA = ListGroupsRequest_v0.SCHEMA


ListGroupsRequestStruct: TypeAlias = (
    ListGroupsRequest_v0 | ListGroupsRequest_v1 | ListGroupsRequest_v2
)


class ListGroupsRequest(Request[ListGroupsRequestStruct]):
    API_KEY = 16

    def build(
        self, request_struct_class: type[ListGroupsRequestStruct]
    ) -> ListGroupsRequestStruct:
        return request_struct_class()


class DescribeGroupsResponse_v0(Response):
    API_KEY = 15
    API_VERSION = 0
    SCHEMA = Schema(
        (
            "groups",
            Array(
                ("error_code", Int16),
                ("group", String("utf-8")),
                ("state", String("utf-8")),
                ("protocol_type", String("utf-8")),
                ("protocol", String("utf-8")),
                (
                    "members",
                    Array(
                        ("member_id", String("utf-8")),
                        ("client_id", String("utf-8")),
                        ("client_host", String("utf-8")),
                        ("member_metadata", Bytes),
                        ("member_assignment", Bytes),
                    ),
                ),
            ),
        )
    )


class DescribeGroupsResponse_v1(Response):
    API_KEY = 15
    API_VERSION = 1
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "groups",
            Array(
                ("error_code", Int16),
                ("group", String("utf-8")),
                ("state", String("utf-8")),
                ("protocol_type", String("utf-8")),
                ("protocol", String("utf-8")),
                (
                    "members",
                    Array(
                        ("member_id", String("utf-8")),
                        ("client_id", String("utf-8")),
                        ("client_host", String("utf-8")),
                        ("member_metadata", Bytes),
                        ("member_assignment", Bytes),
                    ),
                ),
            ),
        ),
    )


class DescribeGroupsResponse_v2(Response):
    API_KEY = 15
    API_VERSION = 2
    SCHEMA = DescribeGroupsResponse_v1.SCHEMA


class DescribeGroupsResponse_v3(Response):
    API_KEY = 15
    API_VERSION = 3
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "groups",
            Array(
                ("error_code", Int16),
                ("group", String("utf-8")),
                ("state", String("utf-8")),
                ("protocol_type", String("utf-8")),
                ("protocol", String("utf-8")),
                (
                    "members",
                    Array(
                        ("member_id", String("utf-8")),
                        ("client_id", String("utf-8")),
                        ("client_host", String("utf-8")),
                        ("member_metadata", Bytes),
                        ("member_assignment", Bytes),
                    ),
                ),
                ("authorized_operations", Int32),
            ),
        ),
    )


class DescribeGroupsRequest_v0(RequestStruct):
    API_KEY = 15
    API_VERSION = 0
    RESPONSE_TYPE = DescribeGroupsResponse_v0
    SCHEMA = Schema(("groups", Array(String("utf-8"))))


class DescribeGroupsRequest_v1(RequestStruct):
    API_KEY = 15
    API_VERSION = 1
    RESPONSE_TYPE = DescribeGroupsResponse_v1
    SCHEMA = DescribeGroupsRequest_v0.SCHEMA


class DescribeGroupsRequest_v2(RequestStruct):
    API_KEY = 15
    API_VERSION = 2
    RESPONSE_TYPE = DescribeGroupsResponse_v2
    SCHEMA = DescribeGroupsRequest_v0.SCHEMA


class DescribeGroupsRequest_v3(RequestStruct):
    API_KEY = 15
    API_VERSION = 3
    RESPONSE_TYPE = DescribeGroupsResponse_v2
    SCHEMA = Schema(
        ("groups", Array(String("utf-8"))), ("include_authorized_operations", Boolean)
    )


DescribeGroupsRequestStruct: TypeAlias = (
    DescribeGroupsRequest_v0
    | DescribeGroupsRequest_v1
    | DescribeGroupsRequest_v2
    | DescribeGroupsRequest_v3
)


class DescribeGroupsRequest(Request[DescribeGroupsRequestStruct]):
    API_KEY = 15

    def __init__(self, groups: list[str], include_authorized_operations: bool = False):
        self._groups = groups
        self._include_authorized_operations = include_authorized_operations

    def build(
        self, request_struct_class: type[DescribeGroupsRequestStruct]
    ) -> DescribeGroupsRequestStruct:
        if request_struct_class.API_VERSION < 3:
            if self._include_authorized_operations:
                raise IncompatibleBrokerVersion(
                    "include_authorized_operations requires DescribeGroupsRequest >= v3"
                )
            return request_struct_class(self._groups)
        return request_struct_class(self._groups, self._include_authorized_operations)


class SaslHandShakeResponse_v0(Response):
    API_KEY = 17
    API_VERSION = 0
    SCHEMA = Schema(
        ("error_code", Int16), ("enabled_mechanisms", Array(String("utf-8")))
    )


class SaslHandShakeResponse_v1(Response):
    API_KEY = 17
    API_VERSION = 1
    SCHEMA = SaslHandShakeResponse_v0.SCHEMA


class SaslHandShakeRequest_v0(RequestStruct):
    API_KEY = 17
    API_VERSION = 0
    RESPONSE_TYPE = SaslHandShakeResponse_v0
    SCHEMA = Schema(("mechanism", String("utf-8")))


class SaslHandShakeRequest_v1(RequestStruct):
    API_KEY = 17
    API_VERSION = 1
    RESPONSE_TYPE = SaslHandShakeResponse_v1
    SCHEMA = SaslHandShakeRequest_v0.SCHEMA


SaslHandShakeRequestStruct: TypeAlias = (
    SaslHandShakeRequest_v0 | SaslHandShakeRequest_v1
)


class SaslHandShakeRequest(Request[SaslHandShakeRequestStruct]):
    API_KEY = 17

    def __init__(self, mechanism: str):
        self._mechanism = mechanism

    def build(
        self, request_struct_class: type[SaslHandShakeRequestStruct]
    ) -> SaslHandShakeRequestStruct:
        return request_struct_class(self._mechanism)


class DescribeAclsResponse_v0(Response):
    API_KEY = 29
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("error_code", Int16),
        ("error_message", String("utf-8")),
        (
            "resources",
            Array(
                ("resource_type", Int8),
                ("resource_name", String("utf-8")),
                (
                    "acls",
                    Array(
                        ("principal", String("utf-8")),
                        ("host", String("utf-8")),
                        ("operation", Int8),
                        ("permission_type", Int8),
                    ),
                ),
            ),
        ),
    )


class DescribeAclsResponse_v1(Response):
    API_KEY = 29
    API_VERSION = 1
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("error_code", Int16),
        ("error_message", String("utf-8")),
        (
            "resources",
            Array(
                ("resource_type", Int8),
                ("resource_name", String("utf-8")),
                ("resource_pattern_type", Int8),
                (
                    "acls",
                    Array(
                        ("principal", String("utf-8")),
                        ("host", String("utf-8")),
                        ("operation", Int8),
                        ("permission_type", Int8),
                    ),
                ),
            ),
        ),
    )


class DescribeAclsResponse_v2(Response):
    API_KEY = 29
    API_VERSION = 2
    SCHEMA = DescribeAclsResponse_v1.SCHEMA


class DescribeAclsRequest_v0(RequestStruct):
    API_KEY = 29
    API_VERSION = 0
    RESPONSE_TYPE = DescribeAclsResponse_v0
    SCHEMA = Schema(
        ("resource_type", Int8),
        ("resource_name", String("utf-8")),
        ("principal", String("utf-8")),
        ("host", String("utf-8")),
        ("operation", Int8),
        ("permission_type", Int8),
    )


class DescribeAclsRequest_v1(RequestStruct):
    API_KEY = 29
    API_VERSION = 1
    RESPONSE_TYPE = DescribeAclsResponse_v1
    SCHEMA = Schema(
        ("resource_type", Int8),
        ("resource_name", String("utf-8")),
        ("resource_pattern_type_filter", Int8),
        ("principal", String("utf-8")),
        ("host", String("utf-8")),
        ("operation", Int8),
        ("permission_type", Int8),
    )


class DescribeAclsRequest_v2(RequestStruct):
    """
    Enable flexible version
    """

    API_KEY = 29
    API_VERSION = 2
    RESPONSE_TYPE = DescribeAclsResponse_v2
    SCHEMA = DescribeAclsRequest_v1.SCHEMA


DescribeAclsRequestStruct: TypeAlias = DescribeAclsRequest_v0 | DescribeAclsRequest_v1


class DescribeAclsRequest(Request[DescribeAclsRequestStruct]):
    API_KEY = 29

    def __init__(
        self,
        resource_type: int,
        resource_name: str,
        resource_pattern_type_filter: int,
        principal: str,
        host: str,
        operation: int,
        permission_type: int,
    ):
        self._resource_type = resource_type
        self._resource_name = resource_name
        self._resource_pattern_type_filter = resource_pattern_type_filter
        self._principal = principal
        self._host = host
        self._operation = operation
        self._permission_type = permission_type

    def build(
        self, request_struct_class: type[DescribeAclsRequestStruct]
    ) -> DescribeAclsRequestStruct:
        if request_struct_class.API_VERSION < 1:
            return request_struct_class(
                self._resource_type,
                self._resource_name,
                self._principal,
                self._host,
                self._operation,
                self._permission_type,
            )
        return request_struct_class(
            self._resource_type,
            self._resource_name,
            self._resource_pattern_type_filter,
            self._principal,
            self._host,
            self._operation,
            self._permission_type,
        )


class CreateAclsResponse_v0(Response):
    API_KEY = 30
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "creation_responses",
            Array(("error_code", Int16), ("error_message", String("utf-8"))),
        ),
    )


class CreateAclsResponse_v1(Response):
    API_KEY = 30
    API_VERSION = 1
    SCHEMA = CreateAclsResponse_v0.SCHEMA


class CreateAclsRequest_v0(RequestStruct):
    API_KEY = 30
    API_VERSION = 0
    RESPONSE_TYPE = CreateAclsResponse_v0
    SCHEMA = Schema(
        (
            "creations",
            Array(
                ("resource_type", Int8),
                ("resource_name", String("utf-8")),
                ("principal", String("utf-8")),
                ("host", String("utf-8")),
                ("operation", Int8),
                ("permission_type", Int8),
            ),
        )
    )


class CreateAclsRequest_v1(RequestStruct):
    API_KEY = 30
    API_VERSION = 1
    RESPONSE_TYPE = CreateAclsResponse_v1
    SCHEMA = Schema(
        (
            "creations",
            Array(
                ("resource_type", Int8),
                ("resource_name", String("utf-8")),
                ("resource_pattern_type", Int8),
                ("principal", String("utf-8")),
                ("host", String("utf-8")),
                ("operation", Int8),
                ("permission_type", Int8),
            ),
        )
    )


CreateAclsRequestStruct: TypeAlias = CreateAclsRequest_v0 | CreateAclsRequest_v1


class CreateAclsRequest(Request[CreateAclsRequestStruct]):
    API_KEY = 30

    def __init__(
        self,
        resource_type: int,
        resource_name: str,
        resource_pattern_type_filter: int,
        principal: str,
        host: str,
        operation: int,
        permission_type: int,
    ):
        self._resource_type = resource_type
        self._resource_name = resource_name
        self._resource_pattern_type_filter = resource_pattern_type_filter
        self._principal = principal
        self._host = host
        self._operation = operation
        self._permission_type = permission_type

    def build(
        self, request_struct_class: type[CreateAclsRequestStruct]
    ) -> CreateAclsRequestStruct:
        if request_struct_class.API_VERSION < 1:
            return request_struct_class(
                [
                    (
                        self._resource_type,
                        self._resource_name,
                        self._principal,
                        self._host,
                        self._operation,
                        self._permission_type,
                    )
                ]
            )
        return request_struct_class(
            [
                (
                    self._resource_type,
                    self._resource_name,
                    self._resource_pattern_type_filter,
                    self._principal,
                    self._host,
                    self._operation,
                    self._permission_type,
                )
            ]
        )


class DeleteAclsResponse_v0(Response):
    API_KEY = 31
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "filter_responses",
            Array(
                ("error_code", Int16),
                ("error_message", String("utf-8")),
                (
                    "matching_acls",
                    Array(
                        ("error_code", Int16),
                        ("error_message", String("utf-8")),
                        ("resource_type", Int8),
                        ("resource_name", String("utf-8")),
                        ("principal", String("utf-8")),
                        ("host", String("utf-8")),
                        ("operation", Int8),
                        ("permission_type", Int8),
                    ),
                ),
            ),
        ),
    )


class DeleteAclsResponse_v1(Response):
    API_KEY = 31
    API_VERSION = 1
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "filter_responses",
            Array(
                ("error_code", Int16),
                ("error_message", String("utf-8")),
                (
                    "matching_acls",
                    Array(
                        ("error_code", Int16),
                        ("error_message", String("utf-8")),
                        ("resource_type", Int8),
                        ("resource_name", String("utf-8")),
                        ("resource_pattern_type", Int8),
                        ("principal", String("utf-8")),
                        ("host", String("utf-8")),
                        ("operation", Int8),
                        ("permission_type", Int8),
                    ),
                ),
            ),
        ),
    )


class DeleteAclsRequest_v0(RequestStruct):
    API_KEY = 31
    API_VERSION = 0
    RESPONSE_TYPE = DeleteAclsResponse_v0
    SCHEMA = Schema(
        (
            "filters",
            Array(
                ("resource_type", Int8),
                ("resource_name", String("utf-8")),
                ("principal", String("utf-8")),
                ("host", String("utf-8")),
                ("operation", Int8),
                ("permission_type", Int8),
            ),
        )
    )


class DeleteAclsRequest_v1(RequestStruct):
    API_KEY = 31
    API_VERSION = 1
    RESPONSE_TYPE = DeleteAclsResponse_v1
    SCHEMA = Schema(
        (
            "filters",
            Array(
                ("resource_type", Int8),
                ("resource_name", String("utf-8")),
                ("resource_pattern_type_filter", Int8),
                ("principal", String("utf-8")),
                ("host", String("utf-8")),
                ("operation", Int8),
                ("permission_type", Int8),
            ),
        )
    )


DeleteAclsRequestStruct: TypeAlias = DeleteAclsRequest_v0 | DeleteAclsRequest_v1


class DeleteAclsRequest(Request[DeleteAclsRequestStruct]):
    API_KEY = 31

    def __init__(
        self,
        resource_type: int,
        resource_name: str,
        resource_pattern_type_filter: int,
        principal: str,
        host: str,
        operation: int,
        permission_type: int,
    ):
        self._resource_type = resource_type
        self._resource_name = resource_name
        self._resource_pattern_type_filter = resource_pattern_type_filter
        self._principal = principal
        self._host = host
        self._operation = operation
        self._permission_type = permission_type

    def build(
        self, request_struct_class: type[DeleteAclsRequestStruct]
    ) -> DeleteAclsRequestStruct:
        if request_struct_class.API_VERSION < 1:
            return request_struct_class(
                [
                    (
                        self._resource_type,
                        self._resource_name,
                        self._principal,
                        self._host,
                        self._operation,
                        self._permission_type,
                    )
                ]
            )
        return request_struct_class(
            [
                (
                    self._resource_type,
                    self._resource_name,
                    self._resource_pattern_type_filter,
                    self._principal,
                    self._host,
                    self._operation,
                    self._permission_type,
                )
            ]
        )


class AlterConfigsResponse_v0(Response):
    API_KEY = 33
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "resources",
            Array(
                ("error_code", Int16),
                ("error_message", String("utf-8")),
                ("resource_type", Int8),
                ("resource_name", String("utf-8")),
            ),
        ),
    )


class AlterConfigsResponse_v1(Response):
    API_KEY = 33
    API_VERSION = 1
    SCHEMA = AlterConfigsResponse_v0.SCHEMA


class AlterConfigsRequest_v0(RequestStruct):
    API_KEY = 33
    API_VERSION = 0
    RESPONSE_TYPE = AlterConfigsResponse_v0
    SCHEMA = Schema(
        (
            "resources",
            Array(
                ("resource_type", Int8),
                ("resource_name", String("utf-8")),
                (
                    "config_entries",
                    Array(
                        ("config_name", String("utf-8")),
                        ("config_value", String("utf-8")),
                    ),
                ),
            ),
        ),
        ("validate_only", Boolean),
    )


class AlterConfigsRequest_v1(RequestStruct):
    API_KEY = 33
    API_VERSION = 1
    RESPONSE_TYPE = AlterConfigsResponse_v1
    SCHEMA = AlterConfigsRequest_v0.SCHEMA


AlterConfigsRequestStruct: TypeAlias = AlterConfigsRequest_v0 | AlterConfigsRequest_v1


class AlterConfigsRequest(Request[AlterConfigsRequestStruct]):
    API_KEY = 33

    def __init__(
        self, resources: dict[int, Any] | list[Any], validate_only: bool = False
    ):
        self._resources = resources
        self._validate_only = validate_only

    def build(
        self, request_struct_class: type[AlterConfigsRequestStruct]
    ) -> AlterConfigsRequestStruct:
        return request_struct_class(self._resources, self._validate_only)


class DescribeConfigsResponse_v0(Response):
    API_KEY = 32
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "resources",
            Array(
                ("error_code", Int16),
                ("error_message", String("utf-8")),
                ("resource_type", Int8),
                ("resource_name", String("utf-8")),
                (
                    "config_entries",
                    Array(
                        ("config_names", String("utf-8")),
                        ("config_value", String("utf-8")),
                        ("read_only", Boolean),
                        ("is_default", Boolean),
                        ("is_sensitive", Boolean),
                    ),
                ),
            ),
        ),
    )


class DescribeConfigsResponse_v1(Response):
    API_KEY = 32
    API_VERSION = 1
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "resources",
            Array(
                ("error_code", Int16),
                ("error_message", String("utf-8")),
                ("resource_type", Int8),
                ("resource_name", String("utf-8")),
                (
                    "config_entries",
                    Array(
                        ("config_names", String("utf-8")),
                        ("config_value", String("utf-8")),
                        ("read_only", Boolean),
                        ("config_source", Int8),
                        ("is_sensitive", Boolean),
                        (
                            "config_synonyms",
                            Array(
                                ("config_name", String("utf-8")),
                                ("config_value", String("utf-8")),
                                ("config_source", Int8),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )


class DescribeConfigsResponse_v2(Response):
    API_KEY = 32
    API_VERSION = 2
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "resources",
            Array(
                ("error_code", Int16),
                ("error_message", String("utf-8")),
                ("resource_type", Int8),
                ("resource_name", String("utf-8")),
                (
                    "config_entries",
                    Array(
                        ("config_names", String("utf-8")),
                        ("config_value", String("utf-8")),
                        ("read_only", Boolean),
                        ("config_source", Int8),
                        ("is_sensitive", Boolean),
                        (
                            "config_synonyms",
                            Array(
                                ("config_name", String("utf-8")),
                                ("config_value", String("utf-8")),
                                ("config_source", Int8),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )


class DescribeConfigsRequest_v0(RequestStruct):
    API_KEY = 32
    API_VERSION = 0
    RESPONSE_TYPE = DescribeConfigsResponse_v0
    SCHEMA = Schema(
        (
            "resources",
            Array(
                ("resource_type", Int8),
                ("resource_name", String("utf-8")),
                ("config_names", Array(String("utf-8"))),
            ),
        )
    )


class DescribeConfigsRequest_v1(RequestStruct):
    API_KEY = 32
    API_VERSION = 1
    RESPONSE_TYPE = DescribeConfigsResponse_v1
    SCHEMA = Schema(
        (
            "resources",
            Array(
                ("resource_type", Int8),
                ("resource_name", String("utf-8")),
                ("config_names", Array(String("utf-8"))),
            ),
        ),
        ("include_synonyms", Boolean),
    )


class DescribeConfigsRequest_v2(RequestStruct):
    API_KEY = 32
    API_VERSION = 2
    RESPONSE_TYPE = DescribeConfigsResponse_v2
    SCHEMA = DescribeConfigsRequest_v1.SCHEMA


DescribeConfigsRequestStruct: TypeAlias = (
    DescribeConfigsRequest_v0 | DescribeConfigsRequest_v1 | DescribeConfigsRequest_v2
)


class DescribeConfigsRequest(Request[DescribeConfigsRequestStruct]):
    API_KEY = 32

    def __init__(
        self, resources: dict[int, Any] | list[Any], include_synonyms: bool = False
    ):
        self._resources = resources
        self._include_synonyms = include_synonyms

    def build(
        self, request_struct_class: type[DescribeConfigsRequestStruct]
    ) -> DescribeConfigsRequestStruct:
        if request_struct_class.API_VERSION < 1:
            if self._include_synonyms:
                raise IncompatibleBrokerVersion(
                    "include_synonyms requires DescribeConfigsRequest >= v1"
                )
            return request_struct_class(self._resources)
        return request_struct_class(self._resources, self._include_synonyms)


class SaslAuthenticateResponse_v0(Response):
    API_KEY = 36
    API_VERSION = 0
    SCHEMA = Schema(
        ("error_code", Int16),
        ("error_message", String("utf-8")),
        ("sasl_auth_bytes", Bytes),
    )


class SaslAuthenticateResponse_v1(Response):
    API_KEY = 36
    API_VERSION = 1
    SCHEMA = Schema(
        ("error_code", Int16),
        ("error_message", String("utf-8")),
        ("sasl_auth_bytes", Bytes),
        ("session_lifetime_ms", Int64),
    )


class SaslAuthenticateRequest_v0(RequestStruct):
    API_KEY = 36
    API_VERSION = 0
    RESPONSE_TYPE = SaslAuthenticateResponse_v0
    SCHEMA = Schema(("sasl_auth_bytes", Bytes))


class SaslAuthenticateRequest_v1(RequestStruct):
    API_KEY = 36
    API_VERSION = 1
    RESPONSE_TYPE = SaslAuthenticateResponse_v1
    SCHEMA = SaslAuthenticateRequest_v0.SCHEMA


SaslAuthenticateRequestStruct: TypeAlias = (
    SaslAuthenticateRequest_v0 | SaslAuthenticateRequest_v1
)


class SaslAuthenticateRequest(Request[SaslAuthenticateRequestStruct]):
    API_KEY = 36

    def __init__(self, payload: Any):
        self._payload = payload

    def build(
        self, request_struct_class: type[SaslAuthenticateRequestStruct]
    ) -> SaslAuthenticateRequestStruct:
        return request_struct_class(self._payload)


class CreatePartitionsResponse_v0(Response):
    API_KEY = 37
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "topic_errors",
            Array(
                ("topic", String("utf-8")),
                ("error_code", Int16),
                ("error_message", String("utf-8")),
            ),
        ),
    )


class CreatePartitionsResponse_v1(Response):
    API_KEY = 37
    API_VERSION = 1
    SCHEMA = CreatePartitionsResponse_v0.SCHEMA


class CreatePartitionsRequest_v0(RequestStruct):
    API_KEY = 37
    API_VERSION = 0
    RESPONSE_TYPE = CreatePartitionsResponse_v0
    SCHEMA = Schema(
        (
            "topic_partitions",
            Array(
                ("topic", String("utf-8")),
                (
                    "new_partitions",
                    Schema(("count", Int32), ("assignment", Array(Array(Int32)))),
                ),
            ),
        ),
        ("timeout", Int32),
        ("validate_only", Boolean),
    )


class CreatePartitionsRequest_v1(RequestStruct):
    API_KEY = 37
    API_VERSION = 1
    SCHEMA = CreatePartitionsRequest_v0.SCHEMA
    RESPONSE_TYPE = CreatePartitionsResponse_v1


CreatePartitionsRequestStruct: TypeAlias = (
    CreatePartitionsRequest_v0 | CreatePartitionsRequest_v1
)


class CreatePartitionsRequest(Request[CreatePartitionsRequestStruct]):
    API_KEY = 37

    def __init__(
        self,
        topic_partitions: list[tuple[str, tuple[int, list[int]]]],
        timeout: int,
        validate_only: bool,
    ):
        self._topic_partitions = topic_partitions
        self._timeout = timeout
        self._validate_only = validate_only

    def build(
        self, request_struct_class: type[CreatePartitionsRequestStruct]
    ) -> CreatePartitionsRequestStruct:
        return request_struct_class(
            self._topic_partitions, self._timeout, self._validate_only
        )


class DeleteGroupsResponse_v0(Response):
    API_KEY = 42
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("results", Array(("group_id", String("utf-8")), ("error_code", Int16))),
    )


class DeleteGroupsResponse_v1(Response):
    API_KEY = 42
    API_VERSION = 1
    SCHEMA = DeleteGroupsResponse_v0.SCHEMA


class DeleteGroupsRequest_v0(RequestStruct):
    API_KEY = 42
    API_VERSION = 0
    RESPONSE_TYPE = DeleteGroupsResponse_v0
    SCHEMA = Schema(("groups_names", Array(String("utf-8"))))


class DeleteGroupsRequest_v1(RequestStruct):
    API_KEY = 42
    API_VERSION = 1
    RESPONSE_TYPE = DeleteGroupsResponse_v1
    SCHEMA = DeleteGroupsRequest_v0.SCHEMA


DeleteGroupsRequestStruct: TypeAlias = DeleteGroupsRequest_v0 | DeleteGroupsRequest_v1


class DeleteGroupsRequest(Request[DeleteGroupsRequestStruct]):
    API_KEY = 42

    def __init__(self, group_names: list[str]):
        self._group_names = group_names

    def build(
        self, request_struct_class: type[DeleteGroupsRequestStruct]
    ) -> DeleteGroupsRequestStruct:
        return request_struct_class(self._group_names)


class DescribeClientQuotasResponse_v0(Response):
    API_KEY = 48
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("error_code", Int16),
        ("error_message", String("utf-8")),
        (
            "entries",
            Array(
                (
                    "entity",
                    Array(
                        ("entity_type", String("utf-8")),
                        ("entity_name", String("utf-8")),
                    ),
                ),
                ("values", Array(("name", String("utf-8")), ("value", Float64))),
            ),
        ),
    )


class DescribeClientQuotasRequest_v0(RequestStruct):
    API_KEY = 48
    API_VERSION = 0
    RESPONSE_TYPE = DescribeClientQuotasResponse_v0
    SCHEMA = Schema(
        (
            "components",
            Array(
                ("entity_type", String("utf-8")),
                ("match_type", Int8),
                ("match", String("utf-8")),
            ),
        ),
        ("strict", Boolean),
    )


DescribeClientQuotasRequestStruct: TypeAlias = DescribeClientQuotasRequest_v0


class DescribeClientQuotasRequest(Request[DescribeClientQuotasRequestStruct]):
    API_KEY = 48

    def __init__(self, components: list[tuple[str, int, str]], strict: bool):
        self._components = components
        self._strict = strict

    def build(
        self, request_struct_class: type[DescribeClientQuotasRequestStruct]
    ) -> DescribeClientQuotasRequestStruct:
        return request_struct_class(self._components, self._strict)


class AlterPartitionReassignmentsResponse_v0(Response):
    API_KEY = 45
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("error_code", Int16),
        ("error_message", CompactString("utf-8")),
        (
            "responses",
            CompactArray(
                ("name", CompactString("utf-8")),
                (
                    "partitions",
                    CompactArray(
                        ("partition_index", Int32),
                        ("error_code", Int16),
                        ("error_message", CompactString("utf-8")),
                        ("tags", TaggedFields),
                    ),
                ),
                ("tags", TaggedFields),
            ),
        ),
        ("tags", TaggedFields),
    )


class AlterPartitionReassignmentsRequest_v0(RequestStruct):
    FLEXIBLE_VERSION = True
    API_KEY = 45
    API_VERSION = 0
    RESPONSE_TYPE = AlterPartitionReassignmentsResponse_v0
    SCHEMA = Schema(
        ("timeout_ms", Int32),
        (
            "topics",
            CompactArray(
                ("name", CompactString("utf-8")),
                (
                    "partitions",
                    CompactArray(
                        ("partition_index", Int32),
                        ("replicas", CompactArray(Int32)),
                        ("tags", TaggedFields),
                    ),
                ),
                ("tags", TaggedFields),
            ),
        ),
        ("tags", TaggedFields),
    )


AlterPartitionReassignmentsRequestStruct: TypeAlias = (
    AlterPartitionReassignmentsRequest_v0
)


class AlterPartitionReassignmentsRequest(
    Request[AlterPartitionReassignmentsRequestStruct]
):
    API_KEY = 45

    def __init__(
        self,
        timeout_ms: int,
        topics: list[tuple[str, tuple[int, list[int], TaggedFields], TaggedFields]],
        tags: TaggedFields,
    ):
        self._timeout_ms = timeout_ms
        self._topics = topics
        self._tags = tags

    def build(
        self, request_struct_class: type[AlterPartitionReassignmentsRequestStruct]
    ) -> AlterPartitionReassignmentsRequestStruct:
        return request_struct_class(self._timeout_ms, self._topics, self._tags)


class ListPartitionReassignmentsResponse_v0(Response):
    API_KEY = 46
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("error_code", Int16),
        ("error_message", CompactString("utf-8")),
        (
            "topics",
            CompactArray(
                ("name", CompactString("utf-8")),
                (
                    "partitions",
                    CompactArray(
                        ("partition_index", Int32),
                        ("replicas", CompactArray(Int32)),
                        ("adding_replicas", CompactArray(Int32)),
                        ("removing_replicas", CompactArray(Int32)),
                        ("tags", TaggedFields),
                    ),
                ),
                ("tags", TaggedFields),
            ),
        ),
        ("tags", TaggedFields),
    )


class ListPartitionReassignmentsRequest_v0(RequestStruct):
    FLEXIBLE_VERSION = True
    API_KEY = 46
    API_VERSION = 0
    RESPONSE_TYPE = ListPartitionReassignmentsResponse_v0
    SCHEMA = Schema(
        ("timeout_ms", Int32),
        (
            "topics",
            CompactArray(
                ("name", CompactString("utf-8")),
                ("partition_index", CompactArray(Int32)),
                ("tags", TaggedFields),
            ),
        ),
        ("tags", TaggedFields),
    )


ListPartitionReassignmentsRequestStruct: TypeAlias = (
    ListPartitionReassignmentsRequest_v0
)


class ListPartitionReassignmentsRequest(
    Request[ListPartitionReassignmentsRequestStruct]
):
    API_KEY = 46

    def __init__(
        self,
        timeout_ms: int,
        topics: list[tuple[str, tuple[int, list[int], TaggedFields], TaggedFields]],
        tags: TaggedFields,
    ):
        self._timeout_ms = timeout_ms
        self._topics = topics
        self._tags = tags

    def build(
        self, request_struct_class: type[ListPartitionReassignmentsRequestStruct]
    ) -> ListPartitionReassignmentsRequestStruct:
        return request_struct_class(self._timeout_ms, self._topics, self._tags)


class DeleteRecordsResponse_v0(Response):
    API_KEY = 21
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "topics",
            Array(
                ("name", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition_index", Int32),
                        ("low_watermark", Int64),
                        ("error_code", Int16),
                    ),
                ),
            ),
        ),
    )


class DeleteRecordsResponse_v1(Response):
    API_KEY = 21
    API_VERSION = 1
    SCHEMA = DeleteRecordsResponse_v0.SCHEMA


class DeleteRecordsResponse_v2(Response):
    API_KEY = 21
    API_VERSION = 2
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "topics",
            CompactArray(
                ("name", CompactString("utf-8")),
                (
                    "partitions",
                    CompactArray(
                        ("partition_index", Int32),
                        ("low_watermark", Int64),
                        ("error_code", Int16),
                        ("tags", TaggedFields),
                    ),
                ),
                ("tags", TaggedFields),
            ),
        ),
        ("tags", TaggedFields),
    )


class DeleteRecordsRequest_v0(RequestStruct):
    API_KEY = 21
    API_VERSION = 0
    RESPONSE_TYPE = DeleteRecordsResponse_v0
    SCHEMA = Schema(
        (
            "topics",
            Array(
                ("name", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition_index", Int32),
                        ("offset", Int64),
                    ),
                ),
            ),
        ),
        ("timeout_ms", Int32),
    )


class DeleteRecordsRequest_v1(RequestStruct):
    API_KEY = 21
    API_VERSION = 1
    RESPONSE_TYPE = DeleteRecordsResponse_v1
    SCHEMA = DeleteRecordsRequest_v0.SCHEMA


class DeleteRecordsRequest_v2(RequestStruct):
    API_KEY = 21
    API_VERSION = 2
    FLEXIBLE_VERSION = True
    RESPONSE_TYPE = DeleteRecordsResponse_v2
    SCHEMA = Schema(
        (
            "topics",
            CompactArray(
                ("name", CompactString("utf-8")),
                (
                    "partitions",
                    CompactArray(
                        ("partition_index", Int32),
                        ("offset", Int64),
                        ("tags", TaggedFields),
                    ),
                ),
                ("tags", TaggedFields),
            ),
        ),
        ("timeout_ms", Int32),
        ("tags", TaggedFields),
    )


DeleteRecordsRequestStruct: TypeAlias = (
    DeleteRecordsRequest_v0 | DeleteRecordsRequest_v1 | DeleteRecordsRequest_v2
)


class DeleteRecordsRequest(Request[DeleteRecordsRequestStruct]):
    API_KEY = 21

    def __init__(
        self,
        topics: Iterable[tuple[str, Iterable[tuple[int, int]]]],
        timeout_ms: int,
        tags: dict[int, bytes] | None = None,
    ) -> None:
        self._topics = topics
        self._timeout_ms = timeout_ms
        self._tags = tags

    def build(
        self, request_struct_class: type[DeleteRecordsRequestStruct]
    ) -> DeleteRecordsRequestStruct:
        if request_struct_class.API_VERSION < 2:
            if self._tags is not None:
                raise IncompatibleBrokerVersion(
                    "tags requires DeleteRecordsRequest >= v2"
                )

            return request_struct_class(
                [
                    (
                        topic,
                        list(partitions),
                    )
                    for (topic, partitions) in self._topics
                ],
                self._timeout_ms,
            )
        return request_struct_class(
            [
                (
                    topic,
                    [
                        (partition, before_offset, {})
                        for partition, before_offset in partitions
                    ],
                    {},
                )
                for (topic, partitions) in self._topics
            ],
            self._timeout_ms,
            self._tags or {},
        )
