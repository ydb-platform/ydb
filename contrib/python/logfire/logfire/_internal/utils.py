from __future__ import annotations

import functools
import hashlib
import inspect
import json
import linecache
import logging
import os
import platform
import random
import sys
import traceback
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import time
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    TypedDict,
    TypeVar,
    Union,
)

from opentelemetry import context, trace as trace_api
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace.status import Status
from opentelemetry.util import types as otel_types
from requests import RequestException, Response

from logfire._internal.stack_info import is_user_code
from logfire._internal.ulid import ulid

try:
    _ = BaseExceptionGroup
except NameError:
    if not TYPE_CHECKING:

        class BaseExceptionGroup(BaseException):
            """Stub for BaseExceptionGroup for Python < 3.11."""


if TYPE_CHECKING:
    from typing import ParamSpec

    from packaging.version import Version

    SysExcInfo = Union[tuple[type[BaseException], BaseException, TracebackType | None], tuple[None, None, None]]
    """
    The return type of sys.exc_info(): exc_type, exc_val, exc_tb.
    """

    P = ParamSpec('P')

T = TypeVar('T')

JsonValue = Union[int, float, str, bool, None, list['JsonValue'], tuple['JsonValue', ...], 'JsonDict']
JsonDict = dict[str, JsonValue]

try:
    import pydantic_core
except Exception:  # pragma: no cover

    def dump_json(obj: JsonValue) -> str:
        return json.dumps(obj, separators=(',', ':'))
else:

    def dump_json(obj: JsonValue) -> str:
        return pydantic_core.to_json(obj).decode()


logger = logging.getLogger('logfire')


def uniquify_sequence(seq: Sequence[T]) -> tuple[T, ...]:
    """Remove duplicates from a sequence preserving order."""
    seen: set[T] = set()
    seen_add = seen.add
    return tuple(x for x in seq if not (x in seen or seen_add(x)))


def safe_repr(obj: Any) -> str:
    """Return some kind of non-empty string representation of an object, catching exceptions."""
    try:
        result = repr(obj)
    except Exception:  # pragma: no cover
        result = ''

    # If repr() returns an empty string, don't use that.
    if result:  # pragma: no branch
        return result

    try:  # pragma: no cover
        return f'<{type(obj).__name__} object>'
    except Exception:  # pragma: no cover
        return '<unknown (repr failed)>'


Truncatable = TypeVar('Truncatable', str, bytes, 'list[Any]', 'tuple[Any, ...]')


def truncate_string(s: str, *, max_length: int, middle: str = '...') -> str:
    """Return a string at most max_length characters long, with `middle` in the middle if truncated."""
    return truncate_sequence(s, max_length=max_length, middle=middle)


def truncate_sequence(seq: Truncatable, *, max_length: int, middle: Truncatable) -> Truncatable:
    """Return a sequence at with `len()` at most `max_length`, with `middle` in the middle if truncated."""
    if len(seq) <= max_length:
        return seq
    remaining_length = max_length - len(middle)
    half = remaining_length // 2
    return seq[:half] + middle + seq[-half:]


def read_toml_file(path: Path) -> dict[str, Any]:
    """Read a TOML file and return the parsed data.

    It wraps the `tomllib.load` function from Python 3.11 or the `tomli.load` function from older versions.
    """
    if sys.version_info >= (3, 11):  # pragma: no branch
        from tomllib import load as load_toml
    else:
        from tomli import load as load_toml  # pragma: no cover

    with path.open('rb') as f:
        data = load_toml(f)

    return data


class ReadableSpanDict(TypedDict):
    """A dictionary representation of a ReadableSpan.

    ReadableSpan is immutable, so making modified versions of it is inconvenient and slow.
    Converting a ReadableSpan to a ReadableSpanDict using span_to_dict makes it easier to modify.
    See `SpanProcessorWrapper.on_end` for an example of how this is useful.
    """

    name: str
    context: trace_api.SpanContext | None
    parent: trace_api.SpanContext | None
    resource: Resource | None
    attributes: Mapping[str, otel_types.AttributeValue]
    events: Sequence[Event]
    links: Sequence[trace_api.Link]
    kind: trace_api.SpanKind
    status: Status
    start_time: int | None
    end_time: int | None
    instrumentation_scope: InstrumentationScope | None


def span_to_dict(span: ReadableSpan) -> ReadableSpanDict:
    """See ReadableSpanDict."""
    return ReadableSpanDict(
        name=span.name,
        context=span.context,
        parent=span.parent,
        resource=span.resource,
        attributes=span.attributes or {},
        events=span.events,
        links=span.links,
        kind=span.kind,
        status=span.status,
        start_time=span.start_time,
        end_time=span.end_time,
        instrumentation_scope=span.instrumentation_scope,
    )


class UnexpectedResponse(RequestException):
    """An unexpected response was received from the server."""

    response: Response  # type: ignore

    def __init__(self, response: Response) -> None:
        super().__init__(f'Unexpected response: {response.status_code}', response=response)

    def __str__(self) -> str:
        assert self.response is not None  # silence type checker
        try:
            body_json = self.response.json()
        except ValueError:
            try:
                body = self.response.text
            except ValueError:  # pragma: no cover
                body = '[binary data]'
        else:  # pragma: no cover
            body = json.dumps(body_json, indent=2)
        request = self.response.request
        return (
            f'Unexpected response {self.response.status_code}\n\n'
            f'{request.method} {request.url} —> {self.response.status_code}\n'
            f'body: {truncate_string(body, max_length=120)}\n'
            'If the error persists, please contact us. '
            '(See https://logfire.pydantic.dev/docs/help/ for contact information.)'
        )

    @classmethod
    def raise_for_status(cls, response: Response) -> None:
        """Like the requests method, but raises a more informative exception."""
        if response.status_code not in range(200, 300):
            raise cls(response)


def ensure_data_dir_exists(data_dir: Path) -> None:
    if data_dir.exists():
        if not data_dir.is_dir():  # pragma: no cover
            raise ValueError(f'Data directory {data_dir} exists but is not a directory')
        return
    data_dir.mkdir(parents=True, exist_ok=True)
    gitignore = data_dir / '.gitignore'
    gitignore.write_text('*')


def get_version(version: str) -> Version:
    """Return a packaging.version.Version object from a version string.

    We check if `packaging` is available, falling back to `setuptools._vendor.packaging` if it's not.
    """
    try:
        from packaging.version import Version

    except ImportError:  # pragma: no cover
        # Trigger the sys.path change mentioned below, but discard this.
        from setuptools._vendor.packaging.version import Version

        try:
            # See https://pydanticlogfire.slack.com/archives/C06EDRBSAH3/p1722017944332959
            # Importing setuptools modifies sys.path so that `packaging.version` points to the vendored module.
            # This means that two calls to this function could return instances of
            # `setuptools._vendor.packaging.version.Version` and `packaging.version.Version`
            # (the same file but in different module objects) which cannot be compared.
            # So first try `packaging.version` again.
            from packaging.version import Version

        except ImportError:
            # sys.path is only changed in newer versions, so fallback to just importing the vendored Version directly.
            from setuptools._vendor.packaging.version import Version
    return Version(version)  # type: ignore


# OTEL uses two different keys to suppress instrumentation. We need to check both.
SUPPRESS_INSTRUMENTATION_CONTEXT_KEYS = [
    # This is still used in some places in OTEL, and probably more in older versions.
    'suppress_instrumentation',
]

try:
    # This is the 'main' key used by OTEL in recent versions
    SUPPRESS_INSTRUMENTATION_CONTEXT_KEYS.append(context._SUPPRESS_INSTRUMENTATION_KEY)  # type: ignore
except AttributeError:  # pragma: no cover
    pass


def is_instrumentation_suppressed() -> bool:
    """Return True if the `suppress_instrumentation` context manager is currently active.

    This means that any logs/spans generated by logfire or OpenTelemetry will not be logged in any way.
    """
    return any(context.get_value(key) for key in SUPPRESS_INSTRUMENTATION_CONTEXT_KEYS)


@contextmanager
def suppress_instrumentation():
    """Context manager to suppress all logs/spans generated by logfire or OpenTelemetry."""
    new_context = context.get_current()
    for key in SUPPRESS_INSTRUMENTATION_CONTEXT_KEYS:
        new_context = context.set_value(key, True, new_context)
    token = context.attach(new_context)
    try:
        yield
    finally:
        context.detach(token)


def log_internal_error():
    try:
        # Unless we're specifically testing this function, we should reraise the exception
        # in tests for easier debugging.
        current_test = os.environ.get('PYTEST_CURRENT_TEST', '')
        reraise = bool(current_test and 'test_internal_exception' not in current_test)
    except Exception:  # pragma: no cover
        reraise = False
    if reraise:
        raise

    with suppress_instrumentation():  # prevent infinite recursion from the logging integration
        logger.exception(
            'Caught an internal error in Logfire. '
            'Your code should still be running fine, just with less telemetry. '
            'This is just logging the internal error.',
            exc_info=_internal_error_exc_info(),
        )


def _internal_error_exc_info() -> SysExcInfo:
    """Returns an exc_info tuple with a nicely tweaked traceback."""
    original_exc_info: SysExcInfo = sys.exc_info()
    exc_type, exc_val, original_tb = original_exc_info
    try:
        tb = original_tb
        if tb and tb.tb_frame and tb.tb_frame.f_code is _HANDLE_INTERNAL_ERRORS_WRAPPER_CODE:
            # Skip the redundant `with self:` line in the traceback about where the error was raised.
            tb = tb.tb_next

        # Now add useful outer frames that give context, but skipping frames that are just about handling the error.
        frame = inspect.currentframe()
        # Skip this frame right here.
        assert frame
        frame = frame.f_back

        if frame and frame.f_code is log_internal_error.__code__:  # pragma: no branch
            # This function is always called from log_internal_error, so skip that frame.
            frame = frame.f_back
            assert frame

            if frame.f_code is _HANDLE_INTERNAL_ERRORS_EXIT_CODE:
                # Skip the `log_internal_error()` call in `__exit__`.
                frame = frame.f_back
                assert frame

            # Now skip the line that is either:
            # - A direct call to `log_internal_error`
            # - `with self:` in HandleInternalErrors.__call__.wrapper
            # - `with handle_internal_errors:`
            frame = frame.f_back

        # Now add all remaining frames from internal logfire code.
        while frame and not is_user_code(frame.f_code):
            tb = TracebackType(tb_next=tb, tb_frame=frame, tb_lasti=frame.f_lasti, tb_lineno=frame.f_lineno)
            frame = frame.f_back

        # Add up to 3 frames from user code.
        for _ in range(3):
            if not frame:  # pragma: no cover
                break
            tb = TracebackType(tb_next=tb, tb_frame=frame, tb_lasti=frame.f_lasti, tb_lineno=frame.f_lineno)
            frame = frame.f_back

        assert exc_type
        assert exc_val
        exc_val = exc_val.with_traceback(tb)
        return exc_type, exc_val, tb
    except Exception:  # pragma: no cover
        return original_exc_info


class HandleInternalErrors:
    def __enter__(self):
        pass

    def __exit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> bool | None:
        if isinstance(exc_val, Exception):
            log_internal_error()
            return True

    def __call__(self, func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            with self:
                return func(*args, **kwargs)

        return wrapper


handle_internal_errors = HandleInternalErrors()

_HANDLE_INTERNAL_ERRORS_WRAPPER_CODE = handle_internal_errors(log_internal_error).__code__
assert _HANDLE_INTERNAL_ERRORS_WRAPPER_CODE.co_name == 'wrapper'
_HANDLE_INTERNAL_ERRORS_EXIT_CODE = HandleInternalErrors.__exit__.__code__


def maybe_capture_server_headers(capture: bool):
    if capture:
        os.environ['OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST'] = '.*'
        os.environ['OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE'] = '.*'


def is_asgi_send_receive_span_name(name: str) -> bool:
    return name.endswith((' http send', ' http receive', ' websocket send', ' websocket receive'))


def _default_ms_timestamp_generator() -> int:
    return int(time() * 1000)


@dataclass(repr=True, eq=True)
class SeededRandomIdGenerator(IdGenerator):
    """Generate random span/trace IDs from a seed for deterministic tests.

    Similar to RandomIdGenerator from OpenTelemetry, but with a seed.
    Set the seed to None for non-deterministic randomness.
    In that case the difference from RandomIdGenerator is that it's not affected by `random.seed(...)`.

    Trace IDs are 128-bit integers.
    Span IDs are 64-bit integers.
    """

    seed: int | None = 0
    _ms_timestamp_generator: Callable[[], int] = _default_ms_timestamp_generator
    """Private argument, do not set this directly."""

    def __post_init__(self) -> None:
        self.random = random.Random(self.seed)
        if self.seed is None and hasattr(os, 'register_at_fork'):
            os.register_at_fork(after_in_child=self.random.seed)

    def generate_span_id(self) -> int:
        span_id = self.random.getrandbits(64)
        while span_id == trace_api.INVALID_SPAN_ID:  # pragma: no cover
            span_id = self.random.getrandbits(64)
        return span_id

    def generate_trace_id(self) -> int:
        trace_id = ulid(self.random, self._ms_timestamp_generator)
        while trace_id == trace_api.INVALID_TRACE_ID:  # pragma: no cover
            trace_id = ulid(self.random, self._ms_timestamp_generator)
        return trace_id


def platform_is_emscripten() -> bool:
    """Return True if the platform is Emscripten, e.g. Pyodide.

    Threads cannot be created on Emscripten, so we need to avoid any code that creates threads.
    """
    return platform.system().lower() == 'emscripten'


def canonicalize_exception_traceback(exc: BaseException, seen: set[int] | None = None) -> str:
    """Return a canonical string representation of an exception traceback.

    Exceptions with the same representation are considered the same for fingerprinting purposes.
    The source line is used, but not the line number, so that changes elsewhere in a file are irrelevant.
    The module is used instead of the filename.
    The same line appearing multiple times in a stack is ignored.
    Exception group sub-exceptions are sorted and deduplicated.
    If the exception has a cause or (not suppressed) context, it is included in the representation.
    Cause and context are treated as different.
    """
    try:
        exc_type = type(exc)
        parts = [f'\n{exc_type.__module__}.{exc_type.__qualname__}\n----']
        num_repeats = 0
        if exc.__traceback__:
            visited: set[str] = set()
            for frame, lineno in traceback.walk_tb(exc.__traceback__):
                filename = frame.f_code.co_filename
                source_line = linecache.getline(filename, lineno, frame.f_globals).strip()
                module = frame.f_globals.get('__name__', filename)
                frame_summary = f'{module}.{frame.f_code.co_name}\n   {source_line}'
                if frame_summary in visited:
                    num_repeats += 1
                    if num_repeats >= 100 and isinstance(exc, RecursionError):
                        # The last few frames of a RecursionError traceback are often *not* the recursive function(s)
                        # being called repeatedly (which are already deduped here) but instead some other function(s)
                        # called normally which happen to use up the last bit of the recursion limit.
                        # These can easily vary between runs and we don't want to pay attention to them,
                        # the real problem is the recursion itself.
                        parts.append('\n<recursion detected>')
                        break
                else:  # skip repeated frames
                    visited.add(frame_summary)
                    parts.append(frame_summary)
        seen = seen or set()
        if id(exc) in seen:
            parts.append('\n<repeated exception>')
        else:
            seen.add(id(exc))
            if isinstance(exc, BaseExceptionGroup):
                sub_exceptions: tuple[BaseException] = exc.exceptions  # type: ignore
                parts += [
                    '\n<ExceptionGroup>',
                    *sorted({canonicalize_exception_traceback(nested_exc, seen) for nested_exc in sub_exceptions}),
                    '\n</ExceptionGroup>\n',
                ]
            if exc.__cause__ is not None:
                parts += [
                    '\n__cause__:',
                    canonicalize_exception_traceback(exc.__cause__, seen),
                ]
            if exc.__context__ is not None and not exc.__suppress_context__:
                parts += [
                    '\n__context__:',
                    canonicalize_exception_traceback(exc.__context__, seen),
                ]
        return '\n'.join(parts)
    except Exception:  # pragma: no cover
        log_internal_error()
        return '<error while canonicalizing>'


def sha256_string(s: str) -> str:
    hasher = hashlib.sha256()
    hasher.update(s.encode('utf-8'))
    return hasher.hexdigest()
