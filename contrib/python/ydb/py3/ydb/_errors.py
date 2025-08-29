from dataclasses import dataclass
from typing import Optional, Union

import grpc

from . import issues

_errors_retriable_fast_backoff_types = [
    issues.Unavailable,
    issues.ClientInternalError,
]
_errors_retriable_slow_backoff_types = [
    issues.Aborted,
    issues.BadSession,
    issues.Overloaded,
    issues.SessionPoolEmpty,
    issues.ConnectionError,
]
_errors_retriable_slow_backoff_idempotent_types = [
    issues.Undetermined,
]


def check_retriable_error(err, retry_settings, attempt):
    if isinstance(err, issues.NotFound):
        if retry_settings.retry_not_found:
            return ErrorRetryInfo(True, retry_settings.fast_backoff.calc_timeout(attempt))
        else:
            return ErrorRetryInfo(False, None)

    if isinstance(err, issues.InternalError):
        if retry_settings.retry_internal_error:
            return ErrorRetryInfo(True, retry_settings.slow_backoff.calc_timeout(attempt))
        else:
            return ErrorRetryInfo(False, None)

    for t in _errors_retriable_fast_backoff_types:
        if isinstance(err, t):
            return ErrorRetryInfo(True, retry_settings.fast_backoff.calc_timeout(attempt))

    for t in _errors_retriable_slow_backoff_types:
        if isinstance(err, t):
            return ErrorRetryInfo(True, retry_settings.slow_backoff.calc_timeout(attempt))

    if retry_settings.idempotent:
        for t in _errors_retriable_slow_backoff_idempotent_types:
            if isinstance(err, t):
                return ErrorRetryInfo(True, retry_settings.slow_backoff.calc_timeout(attempt))

    return ErrorRetryInfo(False, None)


@dataclass
class ErrorRetryInfo:
    is_retriable: bool
    sleep_timeout_seconds: Optional[float]


def stream_error_converter(exc: BaseException) -> Union[issues.Error, BaseException]:
    """Converts gRPC stream errors to appropriate YDB exception types.

    This function takes a base exception and converts specific gRPC aio stream errors
    to their corresponding YDB exception types for better error handling and semantic
    clarity.

    Args:
        exc (BaseException): The original exception to potentially convert.

    Returns:
        BaseException: Either a converted YDB exception or the original exception
                      if no specific conversion rule applies.
    """
    if isinstance(exc, (grpc.RpcError, grpc.aio.AioRpcError)):
        if exc.code() == grpc.StatusCode.UNAVAILABLE:
            return issues.Unavailable(exc.details() or "")
        if exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            return issues.DeadlineExceed("Deadline exceeded on request")
        return issues.Error("Stream has been terminated. Original exception: {}".format(str(exc.details())))
    return exc
