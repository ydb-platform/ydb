from typing import TypeAlias

from aiokafka.errors import IncompatibleBrokerVersion

from .api import Request, RequestStruct, Response
from .types import Array, Int8, Int16, Int32, Int64, Schema, String


class OffsetResetStrategy:
    LATEST = -1
    EARLIEST = -2
    NONE = 0


class OffsetResponse_v0(Response):
    API_KEY = 2
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
                        ("error_code", Int16),
                        ("offsets", Array(Int64)),
                    ),
                ),
            ),
        )
    )


class OffsetResponse_v1(Response):
    API_KEY = 2
    API_VERSION = 1
    SCHEMA = Schema(
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("error_code", Int16),
                        ("timestamp", Int64),
                        ("offset", Int64),
                    ),
                ),
            ),
        )
    )


class OffsetResponse_v2(Response):
    API_KEY = 2
    API_VERSION = 2
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
                        ("error_code", Int16),
                        ("timestamp", Int64),
                        ("offset", Int64),
                    ),
                ),
            ),
        ),
    )


class OffsetResponse_v3(Response):
    """
    on quota violation, brokers send out responses before throttling
    """

    API_KEY = 2
    API_VERSION = 3
    SCHEMA = OffsetResponse_v2.SCHEMA


class OffsetResponse_v4(Response):
    """
    Add leader_epoch to response
    """

    API_KEY = 2
    API_VERSION = 4
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
                        ("error_code", Int16),
                        ("timestamp", Int64),
                        ("offset", Int64),
                        ("leader_epoch", Int32),
                    ),
                ),
            ),
        ),
    )


class OffsetResponse_v5(Response):
    """
    adds a new error code, OFFSET_NOT_AVAILABLE
    """

    API_KEY = 2
    API_VERSION = 5
    SCHEMA = OffsetResponse_v4.SCHEMA


class OffsetRequest_v0(RequestStruct):
    API_KEY = 2
    API_VERSION = 0
    RESPONSE_TYPE = OffsetResponse_v0
    SCHEMA = Schema(
        ("replica_id", Int32),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("timestamp", Int64),
                        ("max_offsets", Int32),
                    ),
                ),
            ),
        ),
    )
    DEFAULTS = {"replica_id": -1}


class OffsetRequest_v1(RequestStruct):
    API_KEY = 2
    API_VERSION = 1
    RESPONSE_TYPE = OffsetResponse_v1
    SCHEMA = Schema(
        ("replica_id", Int32),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                ("partitions", Array(("partition", Int32), ("timestamp", Int64))),
            ),
        ),
    )
    DEFAULTS = {"replica_id": -1}


class OffsetRequest_v2(RequestStruct):
    API_KEY = 2
    API_VERSION = 2
    RESPONSE_TYPE = OffsetResponse_v2
    SCHEMA = Schema(
        ("replica_id", Int32),
        ("isolation_level", Int8),  # <- added isolation_level
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                ("partitions", Array(("partition", Int32), ("timestamp", Int64))),
            ),
        ),
    )
    DEFAULTS = {"replica_id": -1}


class OffsetRequest_v3(RequestStruct):
    API_KEY = 2
    API_VERSION = 3
    RESPONSE_TYPE = OffsetResponse_v3
    SCHEMA = OffsetRequest_v2.SCHEMA
    DEFAULTS = {"replica_id": -1}


class OffsetRequest_v4(RequestStruct):
    """
    Add current_leader_epoch to request
    """

    API_KEY = 2
    API_VERSION = 4
    RESPONSE_TYPE = OffsetResponse_v4
    SCHEMA = Schema(
        ("replica_id", Int32),
        ("isolation_level", Int8),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("current_leader_epoch", Int64),
                        ("timestamp", Int64),
                    ),
                ),
            ),
        ),
    )
    DEFAULTS = {"replica_id": -1}


class OffsetRequest_v5(RequestStruct):
    API_KEY = 2
    API_VERSION = 5
    RESPONSE_TYPE = OffsetResponse_v5
    SCHEMA = OffsetRequest_v4.SCHEMA
    DEFAULTS = {"replica_id": -1}


OffsetRequestStruct: TypeAlias = (
    OffsetRequest_v0 | OffsetRequest_v1 | OffsetRequest_v2 | OffsetRequest_v3
    # Not yet supported
    #  | OffsetRequest_v4
    #  | OffsetRequest_v5
)


class OffsetRequest(Request[OffsetRequestStruct]):
    API_KEY = 2

    def __init__(
        self,
        replica_id: int,
        isolation_level: int,
        topics: list[tuple[str, list[tuple[int, int]]]],
    ):
        self._replica_id = replica_id
        self._isolation_level = isolation_level
        self._topics = topics

    def build(
        self, request_struct_class: type[OffsetRequestStruct]
    ) -> OffsetRequestStruct:
        if request_struct_class.API_VERSION < 2:
            if self._isolation_level:
                raise IncompatibleBrokerVersion(
                    "isolation_level requires OffsetRequest >= v2"
                )
            if request_struct_class.API_VERSION == 0:
                topics = []
                for topic, partitions in self._topics:
                    legacy_partitions = []
                    for part, ts in partitions:
                        if ts >= 0:
                            raise IncompatibleBrokerVersion(
                                "search by timestamp requires OffsetRequest >= v1"
                            )
                        legacy_partitions.append((part, ts, 1))
                    topics.append((topic, legacy_partitions))
                return request_struct_class(self._replica_id, topics)
            return request_struct_class(self._replica_id, self._topics)
        return request_struct_class(
            self._replica_id,
            self._isolation_level,
            self._topics,
        )
