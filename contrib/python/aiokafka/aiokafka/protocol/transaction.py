from typing import TypeAlias

from .api import Request, RequestStruct, Response
from .types import Array, Boolean, Int16, Int32, Int64, Schema, String


class InitProducerIdResponse_v0(Response):
    API_KEY = 22
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("error_code", Int16),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
    )


class InitProducerIdRequest_v0(RequestStruct):
    API_KEY = 22
    API_VERSION = 0
    RESPONSE_TYPE = InitProducerIdResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")), ("transaction_timeout_ms", Int32)
    )


InitProducerIdRequestStruct: TypeAlias = InitProducerIdRequest_v0


class InitProducerIdRequest(Request[InitProducerIdRequestStruct]):
    API_KEY = 22
    CLASSES = [InitProducerIdRequest_v0]

    def __init__(self, transactional_id: str, transaction_timeout_ms: int):
        self._transactional_id = transactional_id
        self._transaction_timeout_ms = transaction_timeout_ms

    def build(
        self, request_struct_class: type[InitProducerIdRequestStruct]
    ) -> InitProducerIdRequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._transaction_timeout_ms,
        )


class AddPartitionsToTxnResponse_v0(Response):
    API_KEY = 24
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "errors",
            Array(
                ("topic", String("utf-8")),
                (
                    "partition_errors",
                    Array(("partition", Int32), ("error_code", Int16)),
                ),
            ),
        ),
    )


class AddPartitionsToTxnRequest_v0(RequestStruct):
    API_KEY = 24
    API_VERSION = 0
    RESPONSE_TYPE = AddPartitionsToTxnResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        ("topics", Array(("topic", String("utf-8")), ("partitions", Array(Int32)))),
    )


AddPartitionsToTxnRequestStruct: TypeAlias = AddPartitionsToTxnRequest_v0


class AddPartitionsToTxnRequest(Request[AddPartitionsToTxnRequestStruct]):
    API_KEY = 24

    def __init__(
        self,
        transactional_id: str,
        producer_id: int,
        producer_epoch: int,
        topics: list[tuple[str, list[int]]],
    ):
        self._transactional_id = transactional_id
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._topics = topics

    def build(
        self, request_struct_class: type[AddPartitionsToTxnRequestStruct]
    ) -> AddPartitionsToTxnRequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._producer_id,
            self._producer_epoch,
            self._topics,
        )


class AddOffsetsToTxnResponse_v0(Response):
    API_KEY = 25
    API_VERSION = 0
    SCHEMA = Schema(("throttle_time_ms", Int32), ("error_code", Int16))


class AddOffsetsToTxnRequest_v0(RequestStruct):
    API_KEY = 25
    API_VERSION = 0
    RESPONSE_TYPE = AddOffsetsToTxnResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        ("group_id", String("utf-8")),
    )


AddOffsetsToTxnRequestStruct: TypeAlias = AddOffsetsToTxnRequest_v0


class AddOffsetsToTxnRequest(Request[AddOffsetsToTxnRequestStruct]):
    API_KEY = 25

    def __init__(
        self,
        transactional_id: str,
        producer_id: int,
        producer_epoch: int,
        group_id: str,
    ):
        self._transactional_id = transactional_id
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._group_id = group_id

    def build(
        self, request_struct_class: type[AddOffsetsToTxnRequestStruct]
    ) -> AddOffsetsToTxnRequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._producer_id,
            self._producer_epoch,
            self._group_id,
        )


class EndTxnResponse_v0(Response):
    API_KEY = 26
    API_VERSION = 0
    SCHEMA = Schema(("throttle_time_ms", Int32), ("error_code", Int16))


class EndTxnRequest_v0(RequestStruct):
    API_KEY = 26
    API_VERSION = 0
    RESPONSE_TYPE = EndTxnResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        ("transaction_result", Boolean),
    )


EndTxnRequestStruct: TypeAlias = EndTxnRequest_v0


class EndTxnRequest(Request[EndTxnRequestStruct]):
    API_KEY = 26

    def __init__(
        self,
        transactional_id: str,
        producer_id: int,
        producer_epoch: int,
        transaction_result: bool,
    ):
        self._transactional_id = transactional_id
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._transaction_result = transaction_result

    def build(
        self, request_struct_class: type[EndTxnRequestStruct]
    ) -> EndTxnRequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._producer_id,
            self._producer_epoch,
            self._transaction_result,
        )


class TxnOffsetCommitResponse_v0(Response):
    API_KEY = 28
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "errors",
            Array(
                ("topic", String("utf-8")),
                (
                    "partition_errors",
                    Array(("partition", Int32), ("error_code", Int16)),
                ),
            ),
        ),
    )


class TxnOffsetCommitRequest_v0(RequestStruct):
    API_KEY = 28
    API_VERSION = 0
    RESPONSE_TYPE = TxnOffsetCommitResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")),
        ("group_id", String("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
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


TxnOffsetCommitRequestStruct: TypeAlias = TxnOffsetCommitRequest_v0


class TxnOffsetCommitRequest(Request[TxnOffsetCommitRequestStruct]):
    API_KEY = 28

    def __init__(
        self,
        transactional_id: str,
        group_id: str,
        producer_id: int,
        producer_epoch: int,
        topics: list[tuple[str, list[tuple[int, int, str]]]],
    ):
        self._transactional_id = transactional_id
        self._group_id = group_id
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._topics = topics

    def build(
        self, request_struct_class: type[TxnOffsetCommitRequestStruct]
    ) -> TxnOffsetCommitRequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._group_id,
            self._producer_id,
            self._producer_epoch,
            self._topics,
        )
