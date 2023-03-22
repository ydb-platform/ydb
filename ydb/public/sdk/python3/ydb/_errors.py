from dataclasses import dataclass
from typing import Optional

from . import issues

_errors_retriable_fast_backoff_types = [
    issues.Unavailable,
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
            return ErrorRetryInfo(
                True, retry_settings.fast_backoff.calc_timeout(attempt)
            )
        else:
            return ErrorRetryInfo(False, None)

    if isinstance(err, issues.InternalError):
        if retry_settings.retry_internal_error:
            return ErrorRetryInfo(
                True, retry_settings.slow_backoff.calc_timeout(attempt)
            )
        else:
            return ErrorRetryInfo(False, None)

    for t in _errors_retriable_fast_backoff_types:
        if isinstance(err, t):
            return ErrorRetryInfo(
                True, retry_settings.fast_backoff.calc_timeout(attempt)
            )

    for t in _errors_retriable_slow_backoff_types:
        if isinstance(err, t):
            return ErrorRetryInfo(
                True, retry_settings.slow_backoff.calc_timeout(attempt)
            )

    if retry_settings.idempotent:
        for t in _errors_retriable_slow_backoff_idempotent_types:
            if isinstance(err, t):
                return ErrorRetryInfo(
                    True, retry_settings.slow_backoff.calc_timeout(attempt)
                )

    return ErrorRetryInfo(False, None)


@dataclass
class ErrorRetryInfo:
    is_retriable: bool
    sleep_timeout_seconds: Optional[float]
