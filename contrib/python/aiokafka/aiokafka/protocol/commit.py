from typing import TypeAlias

from aiokafka.errors import IncompatibleBrokerVersion

from .api import Request, RequestStruct, Response
from .types import Array, Int16, Int32, Int64, Schema, String


class OffsetCommitResponse_v0(Response):
    API_KEY = 8
    API_VERSION = 0
    SCHEMA = Schema(
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                ("partitions", Array(("partition", Int32), ("error_code", Int16))),
            ),
        )
    )


class OffsetCommitResponse_v1(Response):
    API_KEY = 8
    API_VERSION = 1
    SCHEMA = OffsetCommitResponse_v0.SCHEMA


class OffsetCommitResponse_v2(Response):
    API_KEY = 8
    API_VERSION = 2
    SCHEMA = OffsetCommitResponse_v1.SCHEMA


class OffsetCommitResponse_v3(Response):
    API_KEY = 8
    API_VERSION = 3
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                ("partitions", Array(("partition", Int32), ("error_code", Int16))),
            ),
        ),
    )


class OffsetCommitRequest_v0(RequestStruct):
    API_KEY = 8
    API_VERSION = 0  # Zookeeper-backed storage
    RESPONSE_TYPE = OffsetCommitResponse_v0
    SCHEMA = Schema(
        ("consumer_group", String("utf-8")),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("offset", Int64),
                        ("metadata", String("utf-8")),
                    ),
                ),
            ),
        ),
    )


class OffsetCommitRequest_v1(RequestStruct):
    API_KEY = 8
    API_VERSION = 1  # Kafka-backed storage
    RESPONSE_TYPE = OffsetCommitResponse_v1
    SCHEMA = Schema(
        ("consumer_group", String("utf-8")),
        ("consumer_group_generation_id", Int32),
        ("consumer_id", String("utf-8")),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("offset", Int64),
                        ("timestamp", Int64),
                        ("metadata", String("utf-8")),
                    ),
                ),
            ),
        ),
    )


class OffsetCommitRequest_v2(RequestStruct):
    API_KEY = 8
    API_VERSION = 2  # added retention_time, dropped timestamp
    RESPONSE_TYPE = OffsetCommitResponse_v2
    SCHEMA = Schema(
        ("consumer_group", String("utf-8")),
        ("consumer_group_generation_id", Int32),
        ("consumer_id", String("utf-8")),
        ("retention_time", Int64),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("offset", Int64),
                        ("metadata", String("utf-8")),
                    ),
                ),
            ),
        ),
    )


class OffsetCommitRequest_v3(RequestStruct):
    API_KEY = 8
    API_VERSION = 3
    RESPONSE_TYPE = OffsetCommitResponse_v3
    SCHEMA = OffsetCommitRequest_v2.SCHEMA


OffsetCommitRequestStruct: TypeAlias = OffsetCommitRequest_v2 | OffsetCommitRequest_v3


class OffsetCommitRequest(Request[OffsetCommitRequestStruct]):
    API_KEY = 8
    DEFAULT_GENERATION_ID = -1
    DEFAULT_RETENTION_TIME = -1

    def __init__(
        self,
        consumer_group: str,
        consumer_group_generation_id: int,
        consumer_id: str,
        retention_time: int,
        topics: list[tuple[str, list[tuple[int, int, str]]]],
    ):
        self._consumer_group = consumer_group
        self._consumer_group_generation_id = consumer_group_generation_id
        self._consumer_id = consumer_id
        self._retention_time = retention_time
        self._topics = topics

    def build(
        self, request_struct_class: type[OffsetCommitRequestStruct]
    ) -> OffsetCommitRequestStruct:
        return request_struct_class(
            self._consumer_group,
            self._consumer_group_generation_id,
            self._consumer_id,
            self._retention_time,
            self._topics,
        )


class OffsetFetchResponse_v0(Response):
    API_KEY = 9
    API_VERSION = 0
    SCHEMA = Schema(
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("offset", Int64),
                        ("metadata", String("utf-8")),
                        ("error_code", Int16),
                    ),
                ),
            ),
        )
    )


class OffsetFetchResponse_v1(Response):
    API_KEY = 9
    API_VERSION = 1
    SCHEMA = OffsetFetchResponse_v0.SCHEMA


class OffsetFetchResponse_v2(Response):
    # Added in KIP-88
    API_KEY = 9
    API_VERSION = 2
    SCHEMA = Schema(
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("offset", Int64),
                        ("metadata", String("utf-8")),
                        ("error_code", Int16),
                    ),
                ),
            ),
        ),
        ("error_code", Int16),
    )


class OffsetFetchResponse_v3(Response):
    API_KEY = 9
    API_VERSION = 3
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("offset", Int64),
                        ("metadata", String("utf-8")),
                        ("error_code", Int16),
                    ),
                ),
            ),
        ),
        ("error_code", Int16),
    )


class OffsetFetchRequest_v0(RequestStruct):
    API_KEY = 9
    API_VERSION = 0  # zookeeper-backed storage
    RESPONSE_TYPE = OffsetFetchResponse_v0
    SCHEMA = Schema(
        ("consumer_group", String("utf-8")),
        ("topics", Array(("topic", String("utf-8")), ("partitions", Array(Int32)))),
    )


class OffsetFetchRequest_v1(RequestStruct):
    API_KEY = 9
    API_VERSION = 1  # kafka-backed storage
    RESPONSE_TYPE = OffsetFetchResponse_v1
    SCHEMA = OffsetFetchRequest_v0.SCHEMA


class OffsetFetchRequest_v2(RequestStruct):
    # KIP-88: Allows passing null topics to return offsets for all partitions
    # that the consumer group has a stored offset for, even if no consumer in
    # the group is currently consuming that partition.
    API_KEY = 9
    API_VERSION = 2
    RESPONSE_TYPE = OffsetFetchResponse_v2
    SCHEMA = OffsetFetchRequest_v1.SCHEMA


class OffsetFetchRequest_v3(RequestStruct):
    API_KEY = 9
    API_VERSION = 3
    RESPONSE_TYPE = OffsetFetchResponse_v3
    SCHEMA = OffsetFetchRequest_v2.SCHEMA


OffsetFetchRequestStruct: TypeAlias = (
    OffsetFetchRequest_v1 | OffsetFetchRequest_v2 | OffsetFetchRequest_v3
)


class OffsetFetchRequest(Request[OffsetFetchRequestStruct]):
    API_KEY = 9

    def __init__(
        self, consumer_group: str, partitions: list[tuple[str, list[int]]] | None
    ):
        self._consumer_group = consumer_group
        self._partitions = partitions

    def build(
        self, request_struct_class: type[OffsetFetchRequestStruct]
    ) -> OffsetFetchRequestStruct:
        if request_struct_class.API_VERSION < 2 and self._partitions is None:
            raise IncompatibleBrokerVersion(
                "OffsetFetchRequest < v2 requires specifying the "
                "partitions for which to fetch offsets. "
                "Omitting the partitions is only supported on >= v2"
            )

        return request_struct_class(
            self._consumer_group,
            self._partitions,
        )
