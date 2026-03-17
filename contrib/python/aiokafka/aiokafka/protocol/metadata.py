from typing import TypeAlias

from .api import Request, RequestStruct, Response
from .types import Array, Boolean, Int16, Int32, Schema, String


class MetadataResponse_v0(Response):
    API_KEY = 3
    API_VERSION = 0
    SCHEMA = Schema(
        (
            "brokers",
            Array(("node_id", Int32), ("host", String("utf-8")), ("port", Int32)),
        ),
        (
            "topics",
            Array(
                ("error_code", Int16),
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("error_code", Int16),
                        ("partition", Int32),
                        ("leader", Int32),
                        ("replicas", Array(Int32)),
                        ("isr", Array(Int32)),
                    ),
                ),
            ),
        ),
    )


class MetadataResponse_v1(Response):
    API_KEY = 3
    API_VERSION = 1
    SCHEMA = Schema(
        (
            "brokers",
            Array(
                ("node_id", Int32),
                ("host", String("utf-8")),
                ("port", Int32),
                ("rack", String("utf-8")),
            ),
        ),
        ("controller_id", Int32),
        (
            "topics",
            Array(
                ("error_code", Int16),
                ("topic", String("utf-8")),
                ("is_internal", Boolean),
                (
                    "partitions",
                    Array(
                        ("error_code", Int16),
                        ("partition", Int32),
                        ("leader", Int32),
                        ("replicas", Array(Int32)),
                        ("isr", Array(Int32)),
                    ),
                ),
            ),
        ),
    )


class MetadataResponse_v2(Response):
    API_KEY = 3
    API_VERSION = 2
    SCHEMA = Schema(
        (
            "brokers",
            Array(
                ("node_id", Int32),
                ("host", String("utf-8")),
                ("port", Int32),
                ("rack", String("utf-8")),
            ),
        ),
        ("cluster_id", String("utf-8")),  # <-- Added cluster_id field in v2
        ("controller_id", Int32),
        (
            "topics",
            Array(
                ("error_code", Int16),
                ("topic", String("utf-8")),
                ("is_internal", Boolean),
                (
                    "partitions",
                    Array(
                        ("error_code", Int16),
                        ("partition", Int32),
                        ("leader", Int32),
                        ("replicas", Array(Int32)),
                        ("isr", Array(Int32)),
                    ),
                ),
            ),
        ),
    )


class MetadataResponse_v3(Response):
    API_KEY = 3
    API_VERSION = 3
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "brokers",
            Array(
                ("node_id", Int32),
                ("host", String("utf-8")),
                ("port", Int32),
                ("rack", String("utf-8")),
            ),
        ),
        ("cluster_id", String("utf-8")),
        ("controller_id", Int32),
        (
            "topics",
            Array(
                ("error_code", Int16),
                ("topic", String("utf-8")),
                ("is_internal", Boolean),
                (
                    "partitions",
                    Array(
                        ("error_code", Int16),
                        ("partition", Int32),
                        ("leader", Int32),
                        ("replicas", Array(Int32)),
                        ("isr", Array(Int32)),
                    ),
                ),
            ),
        ),
    )


class MetadataResponse_v4(Response):
    API_KEY = 3
    API_VERSION = 4
    SCHEMA = MetadataResponse_v3.SCHEMA


class MetadataResponse_v5(Response):
    API_KEY = 3
    API_VERSION = 5
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "brokers",
            Array(
                ("node_id", Int32),
                ("host", String("utf-8")),
                ("port", Int32),
                ("rack", String("utf-8")),
            ),
        ),
        ("cluster_id", String("utf-8")),
        ("controller_id", Int32),
        (
            "topics",
            Array(
                ("error_code", Int16),
                ("topic", String("utf-8")),
                ("is_internal", Boolean),
                (
                    "partitions",
                    Array(
                        ("error_code", Int16),
                        ("partition", Int32),
                        ("leader", Int32),
                        ("replicas", Array(Int32)),
                        ("isr", Array(Int32)),
                        ("offline_replicas", Array(Int32)),
                    ),
                ),
            ),
        ),
    )


class MetadataRequest_v0(RequestStruct):
    # topics:
    #     None: Empty Array (len 0) for topics returns all topics

    API_KEY = 3
    API_VERSION = 0
    RESPONSE_TYPE = MetadataResponse_v0
    SCHEMA = Schema(("topics", Array(String("utf-8"))))


class MetadataRequest_v1(RequestStruct):
    # topics:
    #    -1: Null Array (len -1) for topics returns all topics
    #    None: Empty array (len 0) for topics returns no topics

    API_KEY = 3
    API_VERSION = 1
    RESPONSE_TYPE = MetadataResponse_v1
    SCHEMA = MetadataRequest_v0.SCHEMA


class MetadataRequest_v2(RequestStruct):
    # topics:
    #    -1: Null Array (len -1) for topics returns all topics
    #    None: Empty array (len 0) for topics returns no topics

    API_KEY = 3
    API_VERSION = 2
    RESPONSE_TYPE = MetadataResponse_v2
    SCHEMA = MetadataRequest_v1.SCHEMA


class MetadataRequest_v3(RequestStruct):
    # topics:
    #    -1: Null Array (len -1) for topics returns all topics
    #    None: Empty array (len 0) for topics returns no topics

    API_KEY = 3
    API_VERSION = 3
    RESPONSE_TYPE = MetadataResponse_v3
    SCHEMA = MetadataRequest_v1.SCHEMA


class MetadataRequest_v4(RequestStruct):
    # topics:
    #    -1: Null Array (len -1) for topics returns all topics
    #    None: Empty array (len 0) for topics returns no topics

    API_KEY = 3
    API_VERSION = 4
    RESPONSE_TYPE = MetadataResponse_v4
    SCHEMA = Schema(
        ("topics", Array(String("utf-8"))), ("allow_auto_topic_creation", Boolean)
    )


class MetadataRequest_v5(RequestStruct):
    """
    The v5 metadata request is the same as v4.
    An additional field for offline_replicas has been added to the v5 metadata response
    """

    # topics:
    #     -1: Null Array (len -1) for topics returns all topics
    #     None: Empty array (len 0) for topics returns no topics

    API_KEY = 3
    API_VERSION = 5
    RESPONSE_TYPE = MetadataResponse_v5
    SCHEMA = MetadataRequest_v4.SCHEMA


MetadataRequestStruct: TypeAlias = (
    MetadataRequest_v0
    | MetadataRequest_v1
    | MetadataRequest_v2
    | MetadataRequest_v3
    | MetadataRequest_v4
    | MetadataRequest_v5
)


class MetadataRequest(Request[MetadataRequestStruct]):
    API_KEY = 3

    def __init__(
        self,
        topics: list[str] | None = None,
        allow_auto_topic_creation: bool | None = None,
    ):
        self._topics = topics
        self._allow_auto_topic_creation = allow_auto_topic_creation

    def build(
        self, request_struct_class: type[MetadataRequestStruct]
    ) -> MetadataRequestStruct:
        if request_struct_class.API_VERSION >= 4:
            return request_struct_class(
                self._topics,
                self._allow_auto_topic_creation
                if self._allow_auto_topic_creation is not None
                else True,
            )
        if request_struct_class.API_VERSION < 1:
            return request_struct_class(self._topics or [])

        return request_struct_class(self._topics)
