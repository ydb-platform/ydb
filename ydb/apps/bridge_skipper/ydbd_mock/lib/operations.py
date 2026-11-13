"""Operation envelopes. Application errors are ready operations with a status, never gRPC errors."""
import typing

from ydb.public.api.protos import ydb_issue_message_pb2
from ydb.public.api.protos import ydb_operation_pb2
from ydb.public.api.protos import ydb_status_codes_pb2

SUCCESS = ydb_status_codes_pb2.StatusIds.SUCCESS
ERROR_SEVERITY = 1  # NYql::TSeverityIds::S_ERROR


def ok(result=None) -> ydb_operation_pb2.Operation:
    """A ready, successful operation. `result` is any protobuf message, packed into Any."""
    op = ydb_operation_pb2.Operation(ready=True, status=SUCCESS)
    if result is not None:
        op.result.Pack(result)
    return op


def error(status: int, text: str) -> ydb_operation_pb2.Operation:
    """A ready operation that failed with `status` and one issue carrying `text`."""
    op = ydb_operation_pb2.Operation(ready=True, status=status)
    op.issues.append(ydb_issue_message_pb2.IssueMessage(message=text, severity=ERROR_SEVERITY))
    return op


def unpack(operation: ydb_operation_pb2.Operation, result_cls) -> typing.Any:
    """Test helper: the typed result of a successful operation."""
    result = result_cls()
    if not operation.result.Unpack(result):
        raise ValueError("operation result is not a %s" % result_cls.DESCRIPTOR.full_name)
    return result


def issue_texts(operation: ydb_operation_pb2.Operation) -> typing.List[str]:
    return [i.message for i in operation.issues]
