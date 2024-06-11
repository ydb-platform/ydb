from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import google.protobuf.message

    from yandex.cloud.operation.operation_pb2 import Operation

    # from yandex.cloud.api.operation_pb2 import Operation


class OperationResult:
    def __init__(
        self,
        operation: "Operation",
        response: Optional["google.protobuf.message.Message"] = None,
        meta: Optional["google.protobuf.message.Message"] = None,
    ):
        self.operation = operation
        self.response = response
        self.meta = meta


class OperationError(RuntimeError):
    def __init__(self, message: str, operation_result: OperationResult):
        super(OperationError, self).__init__(message)  # pylint: disable=super-with-arguments
        self.message = message
        self.operation_result = operation_result
