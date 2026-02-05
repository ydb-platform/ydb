# -*- coding: utf-8 -*-
from __future__ import annotations

from google.protobuf import text_format
import enum
import queue
import typing
from typing import ClassVar, Optional, Iterable, Any, Union, Protocol, runtime_checkable

from . import _apis

try:
    from ydb.public.api.protos import ydb_issue_message_pb2
except ImportError:
    from contrib.ydb.public.api.protos import ydb_issue_message_pb2


@runtime_checkable
class _StatusResponseProtocol(Protocol):
    """Protocol for objects that have status and issues attributes."""

    @property
    def status(self) -> Union[StatusCode, int]:
        ...

    @property
    def issues(self) -> Iterable[Any]:
        ...


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
    SESSION_POOL_CLOSED = _CLIENT_STATUSES_FIRST + 50


# TODO: convert from proto IssueMessage
class _IssueMessage:
    def __init__(self, message: str, issue_code: int, severity: int, issues) -> None:
        self.message = message
        self.issue_code = issue_code
        self.severity = severity
        self.issues = issues


class Error(Exception):
    status: ClassVar[Optional[StatusCode]] = None

    def __init__(self, message: str, issues: typing.Optional[typing.Iterable[_IssueMessage]] = None):
        super(Error, self).__init__(message)
        self.issues = issues
        self.message = message


class TruncatedResponseError(Error):
    status: ClassVar[Optional[StatusCode]] = None


class ConnectionError(Error):
    status: ClassVar[Optional[StatusCode]] = None


class ConnectionFailure(ConnectionError):
    status: ClassVar[Optional[StatusCode]] = StatusCode.CONNECTION_FAILURE


class ConnectionLost(ConnectionError):
    status: ClassVar[Optional[StatusCode]] = StatusCode.CONNECTION_LOST


class DeadlineExceed(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.DEADLINE_EXCEEDED


class Unimplemented(ConnectionError):
    status: ClassVar[Optional[StatusCode]] = StatusCode.UNIMPLEMENTED


class Unauthenticated(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.UNAUTHENTICATED


class BadRequest(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.BAD_REQUEST


class Unauthorized(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.UNAUTHORIZED


class InternalError(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.INTERNAL_ERROR


class Aborted(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.ABORTED


class Unavailable(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.UNAVAILABLE


class Overloaded(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.OVERLOADED


class SchemeError(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.SCHEME_ERROR


class GenericError(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.GENERIC_ERROR


class BadSession(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.BAD_SESSION


class Timeout(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.TIMEOUT


class PreconditionFailed(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.PRECONDITION_FAILED


class NotFound(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.NOT_FOUND


class AlreadyExists(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.ALREADY_EXISTS


class SessionExpired(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.SESSION_EXPIRED


class Cancelled(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.CANCELLED


class Undetermined(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.UNDETERMINED


class Unsupported(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.UNSUPPORTED


class SessionBusy(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.SESSION_BUSY


class ExternalError(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.EXTERNAL_ERROR


class SessionPoolEmpty(Error, queue.Empty):
    status: ClassVar[Optional[StatusCode]] = StatusCode.SESSION_POOL_EMPTY


class SessionPoolClosed(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.SESSION_POOL_CLOSED

    def __init__(self):
        super().__init__("Session pool is closed.")


class ClientInternalError(Error):
    status: ClassVar[Optional[StatusCode]] = StatusCode.CLIENT_INTERNAL_ERROR


class UnexpectedGrpcMessage(Error):
    def __init__(self, message: str):
        super().__init__(message)


def _format_issues(issues: typing.Iterable[ydb_issue_message_pb2.IssueMessage]) -> str:
    if not issues:
        return ""

    return " ,".join(text_format.MessageToString(issue, as_utf8=False, as_one_line=True) for issue in issues)


def _format_response(response: _StatusResponseProtocol) -> str:
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


def _process_response(response_proto: _StatusResponseProtocol) -> None:
    """Process response and raise appropriate exception if status is not success.

    :param response_proto: Any object with status and issues attributes
        (Operation, ServerStatus, ExecuteQueryResponsePart, etc.)
    :raises: Appropriate YDB error based on status code
    """
    try:
        status = StatusCode(response_proto.status)
    except ValueError:
        # Unknown status code from server - treat as GenericError
        raise GenericError(
            "Unknown status code: %s. %s" % (response_proto.status, _format_response(response_proto)),
            response_proto.issues,
        )

    if status not in _success_status_codes:
        exc_class = _server_side_error_map.get(status, GenericError)
        raise exc_class(_format_response(response_proto), response_proto.issues)
