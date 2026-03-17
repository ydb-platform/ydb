import types
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from inspect import FrameInfo, stack
from typing import TypeVar, overload

from returns.result import Failure

_FunctionType = TypeVar('_FunctionType', bound=Callable)


@overload
def collect_traces() -> AbstractContextManager[None]: ...


@overload
def collect_traces(function: _FunctionType) -> _FunctionType: ...


def collect_traces(
    function: _FunctionType | None = None,
) -> _FunctionType | AbstractContextManager[None]:
    """
    Context Manager/Decorator to active traces collect to the Failures.

    .. code:: python

        >>> from inspect import FrameInfo

        >>> from returns.io import IOResult
        >>> from returns.result import Result
        >>> from returns.primitives.tracing import collect_traces

        >>> with collect_traces():
        ...     traced_failure = Result.from_failure('Traced Failure')
        >>> non_traced_failure = IOResult.from_failure('Non Traced Failure')

        >>> assert non_traced_failure.trace is None
        >>> assert isinstance(traced_failure.trace, list)
        >>> assert all(
        ...     isinstance(trace_line, FrameInfo)
        ...     for trace_line in traced_failure.trace
        ... )

        >>> for trace_line in traced_failure.trace:
        ...     print(  # doctest: +SKIP
        ...         '{0}:{1} in `{2}`'.format(
        ...             trace_line.filename,
        ...             trace_line.lineno,
        ...             trace_line.function,
        ...         ),
        ...     )
        ...
        /returns/returns/result.py:525 in `Failure`
        /returns/returns/result.py:322 in `from_failure`
        /example_folder/example.py:1 in `<module>`
        # doctest: # noqa: DAR301, E501

    """

    @contextmanager
    def factory() -> Iterator[None]:
        unpatched_get_trace = getattr(Failure, '_get_trace')  # noqa: B009
        substitute_get_trace = types.MethodType(_get_trace, Failure)
        setattr(Failure, '_get_trace', substitute_get_trace)  # noqa: B010
        try:  # noqa: WPS501
            yield
        finally:
            setattr(Failure, '_get_trace', unpatched_get_trace)  # noqa: B010

    return factory()(function) if function else factory()


def _get_trace(_self: Failure) -> list[FrameInfo] | None:
    """
    Function to be used on Monkey Patching.

    This function is the substitute for '_get_trace' method from ``Failure``
    class on Monkey Patching promoted by
    :func:`returns.primitives.tracing.collect_traces` function.

    We get all the call stack from the current call and return it from the
    third position, to avoid two useless calls on the call stack.
    Those useless calls are a call to this function and a call to `__init__`
    method from ``Failure`` class. We're just interested in the call stack
    ending on ``Failure`` function call!

    See also:
        - https://github.com/dry-python/returns/issues/409

    """
    current_stack = stack()
    return current_stack[2:]
