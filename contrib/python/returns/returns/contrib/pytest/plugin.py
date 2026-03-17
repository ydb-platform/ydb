import inspect
import sys
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from functools import partial, wraps
from types import FrameType, MappingProxyType
from typing import TYPE_CHECKING, Any, Final, TypeAlias, TypeVar, Union, final
from unittest import mock

import pytest

if TYPE_CHECKING:
    from returns.interfaces.specific.result import ResultLikeN

# We keep track of errors handled by keeping a mapping of <object id>: object.
# If an error is handled, it is in the mapping.
# If it isn't in the mapping, the error is not handled.
#
# Note only storing object IDs would not work, as objects may be GC'ed
# and their object id assigned to another object.
# Also, the object itself cannot be (in) the key because
# (1) we cannot always assume hashability and
# (2) we need to track the object identity, not its value
_ErrorsHandled: TypeAlias = dict[int, Any]

_FunctionType = TypeVar('_FunctionType', bound=Callable)
_ReturnsResultType = TypeVar(
    '_ReturnsResultType',
    bound=Union['ResultLikeN', Callable[..., 'ResultLikeN']],
)


@final
class ReturnsAsserts:
    """Class with helpers assertions to check containers."""

    __slots__ = ('_errors_handled',)

    def __init__(self, errors_handled: _ErrorsHandled) -> None:
        """Constructor for this type."""
        self._errors_handled = errors_handled

    @staticmethod  # noqa: WPS602
    def assert_equal(  # noqa: WPS602
        first,
        second,
        *,
        deps=None,
        backend: str = 'asyncio',
    ) -> None:
        """Can compare two containers even with extra calling and awaiting."""
        from returns.primitives.asserts import assert_equal  # noqa: PLC0415

        assert_equal(first, second, deps=deps, backend=backend)

    def is_error_handled(self, container) -> bool:
        """Ensures that container has its error handled in the end."""
        return id(container) in self._errors_handled

    @staticmethod  # noqa: WPS602
    @contextmanager
    def assert_trace(  # noqa: WPS602
        trace_type: _ReturnsResultType,
        function_to_search: _FunctionType,
    ) -> Iterator[None]:
        """
        Ensures that a given function was called during execution.

        Use it to determine where the failure happened.
        """
        old_tracer = sys.gettrace()
        sys.settrace(partial(_trace_function, trace_type, function_to_search))

        try:
            yield
        except _DesiredFunctionFound:
            pass  # noqa: WPS420
        else:
            pytest.fail(
                f'No container {type(trace_type).__name__} was created',
            )
        finally:
            sys.settrace(old_tracer)


def _trace_function(
    trace_type: _ReturnsResultType,
    function_to_search: _FunctionType,
    frame: FrameType,
    event: str,
    arg: Any,
) -> None:
    is_desired_type_call = event == 'call' and (
        # Some containers is created through functions and others
        # is created directly using class constructors!
        # The first line covers when it's created through a function
        # The second line covers when it's created through a
        # class constructor
        frame.f_code is getattr(trace_type, '__code__', None)
        or frame.f_code is getattr(trace_type.__init__, '__code__', None)  # type: ignore[misc]
    )
    if is_desired_type_call:
        current_call_stack = inspect.stack()
        function_to_search_code = getattr(function_to_search, '__code__', None)
        for frame_info in current_call_stack:
            if function_to_search_code is frame_info.frame.f_code:
                raise _DesiredFunctionFound


class _DesiredFunctionFound(BaseException):  # noqa: WPS418
    """Exception to raise when expected function is found."""


def pytest_configure(config) -> None:
    """
    Hook to be executed on import.

    We use it define custom markers.
    """
    config.addinivalue_line(
        'markers',
        (
            'returns_lawful: all tests under `check_all_laws` '
            + 'is marked this way, '
            + 'use `-m "not returns_lawful"` to skip them.'
        ),
    )


@pytest.fixture
def returns() -> Iterator[ReturnsAsserts]:
    """Returns class with helpers assertions to check containers."""
    with _spy_error_handling() as errors_handled:
        yield ReturnsAsserts(errors_handled)


@contextmanager
def _spy_error_handling() -> Iterator[_ErrorsHandled]:
    """Track error handling of containers."""
    errs: _ErrorsHandled = {}
    with ExitStack() as cleanup:
        for container in _containers_to_patch():
            for method, patch in _ERROR_HANDLING_PATCHERS.items():
                cleanup.enter_context(
                    mock.patch.object(
                        container,
                        method,
                        patch(getattr(container, method), errs=errs),
                    )
                )
        yield errs


# delayed imports are needed to prevent messing up coverage
def _containers_to_patch() -> list:
    from returns.context import (  # noqa: PLC0415
        RequiresContextFutureResult,
        RequiresContextIOResult,
        RequiresContextResult,
    )
    from returns.future import FutureResult  # noqa: PLC0415
    from returns.io import IOFailure, IOSuccess  # noqa: PLC0415
    from returns.result import Failure, Success  # noqa: PLC0415

    return [
        Success,
        Failure,
        IOSuccess,
        IOFailure,
        RequiresContextResult,
        RequiresContextIOResult,
        RequiresContextFutureResult,
        FutureResult,
    ]


def _patched_error_handler(
    original: _FunctionType,
    errs: _ErrorsHandled,
) -> _FunctionType:
    if inspect.iscoroutinefunction(original):

        async def wrapper(self, *args, **kwargs):
            original_result = await original(self, *args, **kwargs)
            errs[id(original_result)] = original_result
            return original_result

    else:

        def wrapper(self, *args, **kwargs):
            original_result = original(self, *args, **kwargs)
            errs[id(original_result)] = original_result
            return original_result

    return wraps(original)(wrapper)  # type: ignore


def _patched_error_copier(
    original: _FunctionType,
    errs: _ErrorsHandled,
) -> _FunctionType:
    if inspect.iscoroutinefunction(original):

        async def wrapper(self, *args, **kwargs):
            original_result = await original(self, *args, **kwargs)
            if id(self) in errs:
                errs[id(original_result)] = original_result
            return original_result

    else:

        def wrapper(self, *args, **kwargs):
            original_result = original(self, *args, **kwargs)
            if id(self) in errs:
                errs[id(original_result)] = original_result
            return original_result

    return wraps(original)(wrapper)  # type: ignore


_ERROR_HANDLING_PATCHERS: Final = MappingProxyType({
    'lash': _patched_error_handler,
    'map': _patched_error_copier,
    'alt': _patched_error_copier,
})
