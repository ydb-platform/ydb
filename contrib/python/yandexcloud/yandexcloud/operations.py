from typing import TYPE_CHECKING, Generic, Optional, TypeVar

if TYPE_CHECKING:
    import google.protobuf.message

    from yandex.cloud.operation.operation_pb2 import Operation


RequestType = TypeVar("RequestType", bound="google.protobuf.message.Message")  # pylint: disable=C0103
ResponseType = TypeVar("ResponseType", bound="google.protobuf.message.Message")  # pylint: disable=C0103
MetaType = TypeVar("MetaType", bound="google.protobuf.message.Message")  # pylint: disable=C0103


class OperationResult(Generic[ResponseType, MetaType]):
    def __init__(
        self,
        operation: "Operation",
        response: Optional["ResponseType"] = None,
        meta: Optional["MetaType"] = None,
    ):
        self.operation = operation
        self.response = response
        self.meta = meta


class OperationError(RuntimeError):
    def __init__(self, message: str, operation_result: OperationResult[ResponseType, MetaType]):
        super().__init__(message)  # pylint: disable=super-with-arguments
        self.message = message
        self.operation_result = operation_result
