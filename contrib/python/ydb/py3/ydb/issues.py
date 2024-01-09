# -*- coding: utf-8 -*-
from __future__ import annotations

from google.protobuf import text_format
import enum
import queue
import typing

from . import _apis

try:
    from ydb.public.api.protos import ydb_issue_message_pb2, ydb_operation_pb2
except ImportError:
    from contrib.ydb.public.api.protos import ydb_issue_message_pb2, ydb_operation_pb2


_TRANSPORT_STATUSES_FIRST = 401000
_CLIENT_STATUSES_FIRST = 402000


@enum.unique
class StatusCode(enum.IntEnum):
    STATUS_CODE_UNSPECIFIED = _apis.StatusIds.STATUS_CODE_UNSPECIFIED
    SUCCESS = _apis.StatusIds.SUCCESS
    BAD_REQUEST = _apis.StatusIds.BAD_REQUEST
    UNAUTHORIZED = _apis.StatusIds.UNAUTHORIZED
    INTERNAL_ERROR = _apis.StatusIds.INTERNAL_ERROR
    ABORTED = _apis.StatusIds.ABORTED
    UNAVAILABLE = _apis.StatusIds.UNAVAILABLE
    OVERLOADED = _apis.StatusIds.OVERLOADED
    SCHEME_ERROR = _apis.StatusIds.SCHEME_ERROR
    GENERIC_ERROR = _apis.StatusIds.GENERIC_ERROR
    TIMEOUT = _apis.StatusIds.TIMEOUT
    BAD_SESSION = _apis.StatusIds.BAD_SESSION
    PRECONDITION_FAILED = _apis.StatusIds.PRECONDITION_FAILED
    ALREADY_EXISTS = _apis.StatusIds.ALREADY_EXISTS
    NOT_FOUND = _apis.StatusIds.NOT_FOUND
    SESSION_EXPIRED = _apis.StatusIds.SESSION_EXPIRED
    CANCELLED = _apis.StatusIds.CANCELLED
    UNDETERMINED = _apis.StatusIds.UNDETERMINED
    UNSUPPORTED = _apis.StatusIds.UNSUPPORTED
    SESSION_BUSY = _apis.StatusIds.SESSION_BUSY
    EXTERNAL_ERROR = _apis.StatusIds.EXTERNAL_ERROR

    CONNECTION_LOST = _TRANSPORT_STATUSES_FIRST + 10
    CONNECTION_FAILURE = _TRANSPORT_STATUSES_FIRST + 20
    DEADLINE_EXCEEDED = _TRANSPORT_STATUSES_FIRST + 30
    CLIENT_INTERNAL_ERROR = _TRANSPORT_STATUSES_FIRST + 40
    UNIMPLEMENTED = _TRANSPORT_STATUSES_FIRST + 50

    UNAUTHENTICATED = _CLIENT_STATUSES_FIRST + 30
    SESSION_POOL_EMPTY = _CLIENT_STATUSES_FIRST + 40


# TODO: convert from proto IssueMessage
class _IssueMessage:
    def __init__(self, message: str, issue_code: int, severity: int, issues) -> None:
        self.message = message
        self.issue_code = issue_code
        self.severity = severity
        self.issues = issues


class Error(Exception):
    status = None

    def __init__(self, message: str, issues: typing.Optional[typing.Iterable[_IssueMessage]] = None):
        super(Error, self).__init__(message)
        self.issues = issues
        self.message = message


class TruncatedResponseError(Error):
    status = None


class ConnectionError(Error):
    status = None


class ConnectionFailure(ConnectionError):
    status = StatusCode.CONNECTION_FAILURE


class ConnectionLost(ConnectionError):
    status = StatusCode.CONNECTION_LOST


class DeadlineExceed(ConnectionError):
    status = StatusCode.DEADLINE_EXCEEDED


class Unimplemented(ConnectionError):
    status = StatusCode.UNIMPLEMENTED


class Unauthenticated(Error):
    status = StatusCode.UNAUTHENTICATED


class BadRequest(Error):
    status = StatusCode.BAD_REQUEST


class Unauthorized(Error):
    status = StatusCode.UNAUTHORIZED


class InternalError(Error):
    status = StatusCode.INTERNAL_ERROR


class Aborted(Error):
    status = StatusCode.ABORTED


class Unavailable(Error):
    status = StatusCode.UNAVAILABLE


class Overloaded(Error):
    status = StatusCode.OVERLOADED


class SchemeError(Error):
    status = StatusCode.SCHEME_ERROR


class GenericError(Error):
    status = StatusCode.GENERIC_ERROR


class BadSession(Error):
    status = StatusCode.BAD_SESSION


class Timeout(Error):
    status = StatusCode.TIMEOUT


class PreconditionFailed(Error):
    status = StatusCode.PRECONDITION_FAILED


class NotFound(Error):
    status = StatusCode.NOT_FOUND


class AlreadyExists(Error):
    status = StatusCode.ALREADY_EXISTS


class SessionExpired(Error):
    status = StatusCode.SESSION_EXPIRED


class Cancelled(Error):
    status = StatusCode.CANCELLED


class Undetermined(Error):
    status = StatusCode.UNDETERMINED


class Unsupported(Error):
    status = StatusCode.UNSUPPORTED


class SessionBusy(Error):
    status = StatusCode.SESSION_BUSY


class ExternalError(Error):
    status = StatusCode.EXTERNAL_ERROR


class SessionPoolEmpty(Error, queue.Empty):
    status = StatusCode.SESSION_POOL_EMPTY


class UnexpectedGrpcMessage(Error):
    def __init__(self, message: str):
        super().__init__(message)


def _format_issues(issues: typing.Iterable[ydb_issue_message_pb2.IssueMessage]) -> str:
    if not issues:
        return ""

    return " ,".join(text_format.MessageToString(issue, as_utf8=False, as_one_line=True) for issue in issues)


def _format_response(response: ydb_operation_pb2.Operation) -> str:
    fmt_issues = _format_issues(response.issues)
    return f"{fmt_issues} (server_code: {response.status})"


_success_status_codes = {StatusCode.STATUS_CODE_UNSPECIFIED, StatusCode.SUCCESS}
_server_side_error_map = {
    StatusCode.BAD_REQUEST: BadRequest,
    StatusCode.UNAUTHORIZED: Unauthorized,
    StatusCode.INTERNAL_ERROR: InternalError,
    StatusCode.ABORTED: Aborted,
    StatusCode.UNAVAILABLE: Unavailable,
    StatusCode.OVERLOADED: Overloaded,
    StatusCode.SCHEME_ERROR: SchemeError,
    StatusCode.GENERIC_ERROR: GenericError,
    StatusCode.TIMEOUT: Timeout,
    StatusCode.BAD_SESSION: BadSession,
    StatusCode.PRECONDITION_FAILED: PreconditionFailed,
    StatusCode.ALREADY_EXISTS: AlreadyExists,
    StatusCode.NOT_FOUND: NotFound,
    StatusCode.SESSION_EXPIRED: SessionExpired,
    StatusCode.CANCELLED: Cancelled,
    StatusCode.UNDETERMINED: Undetermined,
    StatusCode.UNSUPPORTED: Unsupported,
    StatusCode.SESSION_BUSY: SessionBusy,
    StatusCode.EXTERNAL_ERROR: ExternalError,
}


def _process_response(response_proto: ydb_operation_pb2.Operation) -> None:
    if response_proto.status not in _success_status_codes:
        exc_obj = _server_side_error_map.get(response_proto.status)
        raise exc_obj(_format_response(response_proto), response_proto.issues)
