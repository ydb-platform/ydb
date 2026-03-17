from __future__ import annotations

import functools
import importlib.util
import json
from enum import Enum
from inspect import iscoroutinefunction
from typing import Any
from typing import Callable

import ydb

from .errors import DatabaseError
from .errors import DataError
from .errors import IntegrityError
from .errors import InternalError
from .errors import NotSupportedError
from .errors import OperationalError
from .errors import ProgrammingError


def handle_ydb_errors(func: Callable) -> Callable:  # noqa: C901
    if iscoroutinefunction(func):

        @functools.wraps(func)
        async def awrapper(*args: tuple, **kwargs: dict) -> Any:
            try:
                return await func(*args, **kwargs)
            except (
                ydb.issues.AlreadyExists,
                ydb.issues.PreconditionFailed,
            ) as e:
                raise IntegrityError(e.message, original_error=e) from e
            except (ydb.issues.Unsupported, ydb.issues.Unimplemented) as e:
                raise NotSupportedError(e.message, original_error=e) from e
            except (ydb.issues.BadRequest, ydb.issues.SchemeError) as e:
                raise ProgrammingError(e.message, original_error=e) from e
            except (
                ydb.issues.TruncatedResponseError,
                ydb.issues.ConnectionError,
                ydb.issues.Aborted,
                ydb.issues.Unavailable,
                ydb.issues.Overloaded,
                ydb.issues.Undetermined,
                ydb.issues.Timeout,
                ydb.issues.Cancelled,
                ydb.issues.SessionBusy,
                ydb.issues.SessionExpired,
                ydb.issues.SessionPoolEmpty,
                ydb.issues.DeadlineExceed,
            ) as e:
                raise OperationalError(e.message, original_error=e) from e
            except ydb.issues.GenericError as e:
                raise DataError(e.message, original_error=e) from e
            except ydb.issues.InternalError as e:
                raise InternalError(e.message, original_error=e) from e
            except ydb.Error as e:
                raise DatabaseError(e.message, original_error=e) from e
            except Exception as e:
                raise DatabaseError("Failed to execute query") from e

        return awrapper

    @functools.wraps(func)
    def wrapper(*args: tuple, **kwargs: dict) -> Any:
        try:
            return func(*args, **kwargs)
        except (
            ydb.issues.AlreadyExists,
            ydb.issues.PreconditionFailed,
        ) as e:
            raise IntegrityError(e.message, original_error=e) from e
        except (ydb.issues.Unsupported, ydb.issues.Unimplemented) as e:
            raise NotSupportedError(e.message, original_error=e) from e
        except (ydb.issues.BadRequest, ydb.issues.SchemeError) as e:
            raise ProgrammingError(e.message, original_error=e) from e
        except (
            ydb.issues.TruncatedResponseError,
            ydb.issues.ConnectionError,
            ydb.issues.Aborted,
            ydb.issues.Unavailable,
            ydb.issues.Overloaded,
            ydb.issues.Undetermined,
            ydb.issues.Timeout,
            ydb.issues.Cancelled,
            ydb.issues.SessionBusy,
            ydb.issues.SessionExpired,
            ydb.issues.SessionPoolEmpty,
            ydb.issues.DeadlineExceed,
        ) as e:
            raise OperationalError(e.message, original_error=e) from e
        except ydb.issues.GenericError as e:
            raise DataError(e.message, original_error=e) from e
        except ydb.issues.InternalError as e:
            raise InternalError(e.message, original_error=e) from e
        except ydb.Error as e:
            raise DatabaseError(e.message, original_error=e) from e
        except Exception as e:
            raise DatabaseError("Failed to execute query") from e

    return wrapper


class CursorStatus(str, Enum):
    ready = "ready"
    running = "running"
    finished = "finished"
    closed = "closed"


def maybe_get_current_trace_id() -> str | None:
    # Check if OpenTelemetry is available
    if importlib.util.find_spec("opentelemetry"):
        from opentelemetry import trace  # type: ignore

        current_span = trace.get_current_span()

        if current_span.get_span_context().is_valid:
            return format(current_span.get_span_context().trace_id, "032x")

    # Return None if OpenTelemetry is not available or trace ID is invalid
    return None


def prepare_credentials(
    credentials: ydb.Credentials | dict | str | None,
) -> ydb.Credentials | None:
    if not credentials:
        return None

    if isinstance(credentials, ydb.Credentials):
        return credentials

    if isinstance(credentials, str):
        credentials = json.loads(credentials)

    if isinstance(credentials, dict):
        credentials = credentials or {}

        username = credentials.get("username")
        if username:
            password = credentials.get("password")
            return ydb.StaticCredentials.from_user_password(
                username,
                password,
            )

        token = credentials.get("token")
        if token:
            return ydb.AccessTokenCredentials(token)

        service_account_json = credentials.get("service_account_json")
        if service_account_json:
            return ydb.iam.ServiceAccountCredentials.from_content(
                json.dumps(service_account_json),
            )

    return ydb.AnonymousCredentials()
