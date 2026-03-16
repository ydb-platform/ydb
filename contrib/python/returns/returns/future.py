from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
)
from functools import wraps
from typing import Any, TypeAlias, TypeVar, final, overload

from typing_extensions import ParamSpec

from returns._internal.futures import _future, _future_result
from returns.interfaces.specific.future import FutureBased1
from returns.interfaces.specific.future_result import FutureResultBased2
from returns.io import IO, IOResult
from returns.primitives.container import BaseContainer
from returns.primitives.exceptions import UnwrapFailedError
from returns.primitives.hkt import (
    Kind1,
    Kind2,
    SupportsKind1,
    SupportsKind2,
    dekind,
)
from returns.primitives.reawaitable import ReAwaitable
from returns.result import Failure, Result, Success

# Definitions:
_ValueType_co = TypeVar('_ValueType_co', covariant=True)
_NewValueType = TypeVar('_NewValueType')
_ErrorType_co = TypeVar('_ErrorType_co', covariant=True)
_NewErrorType = TypeVar('_NewErrorType')

_FuncParams = ParamSpec('_FuncParams')

# Aliases:
_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')


# Public composition helpers:


async def async_identity(instance: _FirstType) -> _FirstType:  # noqa: RUF029
    """
    Async function that returns its argument.

    .. code:: python

      >>> import anyio
      >>> from returns.future import async_identity
      >>> assert anyio.run(async_identity, 1) == 1

    See :func:`returns.functions.identity`
    for sync version of this function and more docs and examples.

    """
    return instance


# Future
# ======


@final
class Future(  # type: ignore[type-var]
    BaseContainer,
    SupportsKind1['Future', _ValueType_co],
    FutureBased1[_ValueType_co],
):
    """
    Container to easily compose ``async`` functions.

    Represents a better abstraction over a simple coroutine.

    Is framework, event-loop, and IO-library agnostics.
    Works with ``asyncio``, ``curio``, ``trio``, or any other tool.
    Internally we use ``anyio`` to test
    that it works as expected for any io stack.

    Note that ``Future[a]`` represents a computation
    that never fails and returns ``IO[a]`` type.
    Use ``FutureResult[a, b]`` for operations that might fail.
    Like DB access or network operations.

    Is not related to ``asyncio.Future`` in any kind.

    .. rubric:: Tradeoffs

    Due to possible performance issues we move all coroutines definitions
    to a separate module.

    See also:
        - https://gcanti.github.io/fp-ts/modules/Task.ts.html
        - https://zio.dev/docs/overview/overview_basic_concurrency

    """

    __slots__ = ()

    _inner_value: Awaitable[_ValueType_co]

    def __init__(self, inner_value: Awaitable[_ValueType_co]) -> None:
        """
        Public constructor for this type. Also required for typing.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future
          >>> from returns.io import IO

          >>> async def coro(arg: int) -> int:
          ...     return arg + 1

          >>> container = Future(coro(1))
          >>> assert anyio.run(container.awaitable) == IO(2)

        """
        super().__init__(ReAwaitable(inner_value))

    def __await__(self) -> Generator[None, None, IO[_ValueType_co]]:
        """
        By defining this magic method we make ``Future`` awaitable.

        This means you can use ``await`` keyword to evaluate this container:

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future
          >>> from returns.io import IO

          >>> async def main() -> IO[int]:
          ...     return await Future.from_value(1)

          >>> assert anyio.run(main) == IO(1)

        When awaited we returned the value wrapped
        in :class:`returns.io.IO` container
        to indicate that the computation was impure.

        See also:
            - https://docs.python.org/3/library/asyncio-task.html#awaitables
            - https://bit.ly/2SfayNc

        """
        return self.awaitable().__await__()

    async def awaitable(self) -> IO[_ValueType_co]:
        """
        Transforms ``Future[a]`` to ``Awaitable[IO[a]]``.

        Use this method when you need a real coroutine.
        Like for ``asyncio.run`` calls.

        Note, that returned value will be wrapped
        in :class:`returns.io.IO` container.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future
          >>> from returns.io import IO
          >>> assert anyio.run(Future.from_value(1).awaitable) == IO(1)

        """
        return IO(await self._inner_value)

    def map(
        self,
        function: Callable[[_ValueType_co], _NewValueType],
    ) -> 'Future[_NewValueType]':
        """
        Applies function to the inner value.

        Applies 'function' to the contents of the IO instance
        and returns a new ``Future`` object containing the result.
        'function' should accept a single "normal" (non-container) argument
        and return a non-container result.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future
          >>> from returns.io import IO

          >>> def mappable(x: int) -> int:
          ...    return x + 1

          >>> assert anyio.run(
          ...     Future.from_value(1).map(mappable).awaitable,
          ... ) == IO(2)

        """
        return Future(_future.async_map(function, self._inner_value))

    def apply(
        self,
        container: Kind1['Future', Callable[[_ValueType_co], _NewValueType]],
    ) -> 'Future[_NewValueType]':
        """
        Calls a wrapped function in a container on this container.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future

          >>> def transform(arg: int) -> str:
          ...      return str(arg) + 'b'

          >>> assert anyio.run(
          ...     Future.from_value(1).apply(
          ...         Future.from_value(transform),
          ...     ).awaitable,
          ... ) == IO('1b')

        """
        return Future(_future.async_apply(dekind(container), self._inner_value))

    def bind(
        self,
        function: Callable[[_ValueType_co], Kind1['Future', _NewValueType]],
    ) -> 'Future[_NewValueType]':
        """
        Applies 'function' to the result of a previous calculation.

        'function' should accept a single "normal" (non-container) argument
        and return ``Future`` type object.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future
          >>> from returns.io import IO

          >>> def bindable(x: int) -> Future[int]:
          ...    return Future.from_value(x + 1)

          >>> assert anyio.run(
          ...     Future.from_value(1).bind(bindable).awaitable,
          ... ) == IO(2)

        """
        return Future(_future.async_bind(function, self._inner_value))

    #: Alias for `bind` method. Part of the `FutureBasedN` interface.
    bind_future = bind

    def bind_async(
        self,
        function: Callable[
            [_ValueType_co],
            Awaitable[Kind1['Future', _NewValueType]],
        ],
    ) -> 'Future[_NewValueType]':
        """
        Compose a container and ``async`` function returning a container.

        This function should return a container value.
        See :meth:`~Future.bind_awaitable`
        to bind ``async`` function that returns a plain value.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future
          >>> from returns.io import IO

          >>> async def coroutine(x: int) -> Future[str]:
          ...    return Future.from_value(str(x + 1))

          >>> assert anyio.run(
          ...     Future.from_value(1).bind_async(coroutine).awaitable,
          ... ) == IO('2')

        """
        return Future(_future.async_bind_async(function, self._inner_value))

    #: Alias for `bind_async` method. Part of the `FutureBasedN` interface.
    bind_async_future = bind_async

    def bind_awaitable(
        self,
        function: Callable[[_ValueType_co], 'Awaitable[_NewValueType]'],
    ) -> 'Future[_NewValueType]':
        """
        Allows to compose a container and a regular ``async`` function.

        This function should return plain, non-container value.
        See :meth:`~Future.bind_async`
        to bind ``async`` function that returns a container.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future
          >>> from returns.io import IO

          >>> async def coroutine(x: int) -> int:
          ...    return x + 1

          >>> assert anyio.run(
          ...     Future.from_value(1).bind_awaitable(coroutine).awaitable,
          ... ) == IO(2)

        """
        return Future(
            _future.async_bind_awaitable(
                function,
                self._inner_value,
            )
        )

    def bind_io(
        self,
        function: Callable[[_ValueType_co], IO[_NewValueType]],
    ) -> 'Future[_NewValueType]':
        """
        Applies 'function' to the result of a previous calculation.

        'function' should accept a single "normal" (non-container) argument
        and return ``IO`` type object.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future
          >>> from returns.io import IO

          >>> def bindable(x: int) -> IO[int]:
          ...    return IO(x + 1)

          >>> assert anyio.run(
          ...     Future.from_value(1).bind_io(bindable).awaitable,
          ... ) == IO(2)

        """
        return Future(_future.async_bind_io(function, self._inner_value))

    def __aiter__(self) -> AsyncIterator[_ValueType_co]:  # noqa: WPS611
        """API for :ref:`do-notation`."""

        async def factory() -> AsyncGenerator[_ValueType_co, None]:
            yield await self._inner_value

        return factory()

    @classmethod
    def do(
        cls,
        expr: AsyncGenerator[_NewValueType, None],
    ) -> 'Future[_NewValueType]':
        """
        Allows working with unwrapped values of containers in a safe way.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future
          >>> from returns.io import IO

          >>> async def main() -> bool:
          ...     return await Future.do(
          ...         first + second
          ...         async for first in Future.from_value(2)
          ...         async for second in Future.from_value(3)
          ...     ) == IO(5)

          >>> assert anyio.run(main) is True

        See :ref:`do-notation` to learn more.

        """

        async def factory() -> _NewValueType:
            return await anext(expr)

        return Future(factory())

    @classmethod
    def from_value(cls, inner_value: _NewValueType) -> 'Future[_NewValueType]':
        """
        Allows to create a ``Future`` from a plain value.

        The resulting ``Future`` will just return the given value
        wrapped in :class:`returns.io.IO` container when awaited.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future
          >>> from returns.io import IO

          >>> async def main() -> bool:
          ...    return (await Future.from_value(1)) == IO(1)

          >>> assert anyio.run(main) is True

        """
        return Future(async_identity(inner_value))

    @classmethod
    def from_future(
        cls,
        inner_value: 'Future[_NewValueType]',
    ) -> 'Future[_NewValueType]':
        """
        Creates a new ``Future`` from the existing one.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future
          >>> from returns.io import IO

          >>> future = Future.from_value(1)
          >>> assert anyio.run(Future.from_future(future).awaitable) == IO(1)

        Part of the ``FutureBasedN`` interface.
        """
        return inner_value

    @classmethod
    def from_io(cls, inner_value: IO[_NewValueType]) -> 'Future[_NewValueType]':
        """
        Allows to create a ``Future`` from ``IO`` container.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future
          >>> from returns.io import IO

          >>> async def main() -> bool:
          ...    return (await Future.from_io(IO(1))) == IO(1)

          >>> assert anyio.run(main) is True

        """
        return Future(async_identity(inner_value._inner_value))  # noqa: SLF001

    @classmethod
    def from_future_result(
        cls,
        inner_value: 'FutureResult[_NewValueType, _NewErrorType]',
    ) -> 'Future[Result[_NewValueType, _NewErrorType]]':
        """
        Creates ``Future[Result[a, b]]`` instance from ``FutureResult[a, b]``.

        This method is the inverse of :meth:`~FutureResult.from_typecast`.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future, FutureResult
          >>> from returns.io import IO
          >>> from returns.result import Success

          >>> container = Future.from_future_result(FutureResult.from_value(1))
          >>> assert anyio.run(container.awaitable) == IO(Success(1))

        """
        return Future(inner_value._inner_value)  # noqa: SLF001


# Decorators:


def future(
    function: Callable[
        _FuncParams,
        Coroutine[_FirstType, _SecondType, _ValueType_co],
    ],
) -> Callable[_FuncParams, Future[_ValueType_co]]:
    """
    Decorator to turn a coroutine definition into ``Future`` container.

    .. code:: python

      >>> import anyio
      >>> from returns.io import IO
      >>> from returns.future import future

      >>> @future
      ... async def test(x: int) -> int:
      ...     return x + 1

      >>> assert anyio.run(test(1).awaitable) == IO(2)

    """

    @wraps(function)
    def decorator(
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> Future[_ValueType_co]:
        return Future(function(*args, **kwargs))

    return decorator


def asyncify(
    function: Callable[_FuncParams, _ValueType_co],
) -> Callable[_FuncParams, Coroutine[Any, Any, _ValueType_co]]:
    """
    Decorator to turn a common function into an asynchronous function.

    This decorator is useful for composition with ``Future`` and
    ``FutureResult`` containers.

    .. warning::

      This function will not your sync function **run** like async one.
      It will still be a blocking function that looks like async one.
      We recommend to only use this decorator with functions
      that do not access network or filesystem.
      It is only a composition helper, not a transformer.

    Usage example:

    .. code:: python

      >>> import anyio
      >>> from returns.future import asyncify

      >>> @asyncify
      ... def test(x: int) -> int:
      ...     return x + 1

      >>> assert anyio.run(test, 1) == 2

    Read more about async and sync functions:
    https://journal.stuffwithstuff.com/2015/02/01/what-color-is-your-function/

    """

    @wraps(function)
    async def decorator(  # noqa: RUF029
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> _ValueType_co:
        return function(*args, **kwargs)

    return decorator


# FutureResult
# ============


@final
class FutureResult(  # type: ignore[type-var]
    BaseContainer,
    SupportsKind2['FutureResult', _ValueType_co, _ErrorType_co],
    FutureResultBased2[_ValueType_co, _ErrorType_co],
):
    """
    Container to easily compose ``async`` functions.

    Represents a better abstraction over a simple coroutine.

    Is framework, event-loop, and IO-library agnostics.
    Works with ``asyncio``, ``curio``, ``trio``, or any other tool.
    Internally we use ``anyio`` to test
    that it works as expected for any io stack.

    Note that ``FutureResult[a, b]`` represents a computation
    that can fail and returns ``IOResult[a, b]`` type.
    Use ``Future[a]`` for operations that cannot fail.

    This is a ``Future`` that returns ``Result`` type.
    By providing this utility type we make developers' lives easier.
    ``FutureResult`` has a lot of composition helpers
    to turn complex nested operations into a one function calls.

    .. rubric:: Tradeoffs

    Due to possible performance issues we move all coroutines definitions
    to a separate module.

    See also:
        - https://gcanti.github.io/fp-ts/modules/TaskEither.ts.html
        - https://zio.dev/docs/overview/overview_basic_concurrency

    """

    __slots__ = ()

    _inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]]

    def __init__(
        self,
        inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
    ) -> None:
        """
        Public constructor for this type. Also required for typing.

        .. code:: python

          >>> import anyio
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess
          >>> from returns.result import Success, Result

          >>> async def coro(arg: int) -> Result[int, str]:
          ...     return Success(arg + 1)

          >>> container = FutureResult(coro(1))
          >>> assert anyio.run(container.awaitable) == IOSuccess(2)

        """
        super().__init__(ReAwaitable(inner_value))

    def __await__(
        self,
    ) -> Generator[
        None,
        None,
        IOResult[_ValueType_co, _ErrorType_co],
    ]:
        """
        By defining this magic method we make ``FutureResult`` awaitable.

        This means you can use ``await`` keyword to evaluate this container:

        .. code:: python

          >>> import anyio
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOResult

          >>> async def main() -> IOResult[int, str]:
          ...     return await FutureResult.from_value(1)

          >>> assert anyio.run(main) == IOSuccess(1)

        When awaited we returned the value wrapped
        in :class:`returns.io.IOResult` container
        to indicate that the computation was impure and can fail.

        See also:
            - https://docs.python.org/3/library/asyncio-task.html#awaitables
            - https://bit.ly/2SfayNc

        """
        return self.awaitable().__await__()

    async def awaitable(self) -> IOResult[_ValueType_co, _ErrorType_co]:
        """
        Transforms ``FutureResult[a, b]`` to ``Awaitable[IOResult[a, b]]``.

        Use this method when you need a real coroutine.
        Like for ``asyncio.run`` calls.

        Note, that returned value will be wrapped
        in :class:`returns.io.IOResult` container.

        .. code:: python

          >>> import anyio
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess
          >>> assert anyio.run(
          ...     FutureResult.from_value(1).awaitable,
          ... ) == IOSuccess(1)

        """
        return IOResult.from_result(await self._inner_value)

    def swap(self) -> 'FutureResult[_ErrorType_co, _ValueType_co]':
        """
        Swaps value and error types.

        So, values become errors and errors become values.
        It is useful when you have to work with errors a lot.
        And since we have a lot of ``.bind_`` related methods
        and only a single ``.lash``.
        It is easier to work with values than with errors.

        .. code:: python

          >>> import anyio
          >>> from returns.future import FutureSuccess, FutureFailure
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert anyio.run(FutureSuccess(1).swap) == IOFailure(1)
          >>> assert anyio.run(FutureFailure(1).swap) == IOSuccess(1)

        """
        return FutureResult(_future_result.async_swap(self._inner_value))

    def map(
        self,
        function: Callable[[_ValueType_co], _NewValueType],
    ) -> 'FutureResult[_NewValueType, _ErrorType_co]':
        """
        Applies function to the inner value.

        Applies 'function' to the contents of the IO instance
        and returns a new ``FutureResult`` object containing the result.
        'function' should accept a single "normal" (non-container) argument
        and return a non-container result.

        .. code:: python

          >>> import anyio
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> def mappable(x: int) -> int:
          ...    return x + 1

          >>> assert anyio.run(
          ...     FutureResult.from_value(1).map(mappable).awaitable,
          ... ) == IOSuccess(2)
          >>> assert anyio.run(
          ...     FutureResult.from_failure(1).map(mappable).awaitable,
          ... ) == IOFailure(1)

        """
        return FutureResult(
            _future_result.async_map(
                function,
                self._inner_value,
            )
        )

    def apply(
        self,
        container: Kind2[
            'FutureResult',
            Callable[[_ValueType_co], _NewValueType],
            _ErrorType_co,
        ],
    ) -> 'FutureResult[_NewValueType, _ErrorType_co]':
        """
        Calls a wrapped function in a container on this container.

        .. code:: python

          >>> import anyio
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> def appliable(x: int) -> int:
          ...    return x + 1

          >>> assert anyio.run(
          ...     FutureResult.from_value(1).apply(
          ...         FutureResult.from_value(appliable),
          ...     ).awaitable,
          ... ) == IOSuccess(2)
          >>> assert anyio.run(
          ...     FutureResult.from_failure(1).apply(
          ...         FutureResult.from_value(appliable),
          ...     ).awaitable,
          ... ) == IOFailure(1)

          >>> assert anyio.run(
          ...     FutureResult.from_value(1).apply(
          ...         FutureResult.from_failure(2),
          ...     ).awaitable,
          ... ) == IOFailure(2)
          >>> assert anyio.run(
          ...     FutureResult.from_failure(1).apply(
          ...         FutureResult.from_failure(2),
          ...     ).awaitable,
          ... ) == IOFailure(1)

        """
        return FutureResult(
            _future_result.async_apply(
                dekind(container),
                self._inner_value,
            )
        )

    def bind(
        self,
        function: Callable[
            [_ValueType_co],
            Kind2['FutureResult', _NewValueType, _ErrorType_co],
        ],
    ) -> 'FutureResult[_NewValueType, _ErrorType_co]':
        """
        Applies 'function' to the result of a previous calculation.

        'function' should accept a single "normal" (non-container) argument
        and return ``Future`` type object.

        .. code:: python

          >>> import anyio
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> def bindable(x: int) -> FutureResult[int, str]:
          ...    return FutureResult.from_value(x + 1)

          >>> assert anyio.run(
          ...     FutureResult.from_value(1).bind(bindable).awaitable,
          ... ) == IOSuccess(2)
          >>> assert anyio.run(
          ...     FutureResult.from_failure(1).bind(bindable).awaitable,
          ... ) == IOFailure(1)

        """
        return FutureResult(
            _future_result.async_bind(
                function,
                self._inner_value,
            )
        )

    #: Alias for `bind` method.
    #: Part of the `FutureResultBasedN` interface.
    bind_future_result = bind

    def bind_async(
        self,
        function: Callable[
            [_ValueType_co],
            Awaitable[Kind2['FutureResult', _NewValueType, _ErrorType_co]],
        ],
    ) -> 'FutureResult[_NewValueType, _ErrorType_co]':
        """
        Composes a container and ``async`` function returning container.

        This function should return a container value.
        See :meth:`~FutureResult.bind_awaitable`
        to bind ``async`` function that returns a plain value.

        .. code:: python

          >>> import anyio
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> async def coroutine(x: int) -> FutureResult[str, int]:
          ...    return FutureResult.from_value(str(x + 1))

          >>> assert anyio.run(
          ...     FutureResult.from_value(1).bind_async(coroutine).awaitable,
          ... ) == IOSuccess('2')
          >>> assert anyio.run(
          ...     FutureResult.from_failure(1).bind_async(coroutine).awaitable,
          ... ) == IOFailure(1)

        """
        return FutureResult(
            _future_result.async_bind_async(
                function,
                self._inner_value,
            )
        )

    #: Alias for `bind_async` method.
    #: Part of the `FutureResultBasedN` interface.
    bind_async_future_result = bind_async

    def bind_awaitable(
        self,
        function: Callable[[_ValueType_co], Awaitable[_NewValueType]],
    ) -> 'FutureResult[_NewValueType, _ErrorType_co]':
        """
        Allows to compose a container and a regular ``async`` function.

        This function should return plain, non-container value.
        See :meth:`~FutureResult.bind_async`
        to bind ``async`` function that returns a container.

        .. code:: python

          >>> import anyio
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> async def coro(x: int) -> int:
          ...    return x + 1

          >>> assert anyio.run(
          ...     FutureResult.from_value(1).bind_awaitable(coro).awaitable,
          ... ) == IOSuccess(2)
          >>> assert anyio.run(
          ...     FutureResult.from_failure(1).bind_awaitable(coro).awaitable,
          ... ) == IOFailure(1)

        """
        return FutureResult(
            _future_result.async_bind_awaitable(
                function,
                self._inner_value,
            )
        )

    def bind_result(
        self,
        function: Callable[
            [_ValueType_co], Result[_NewValueType, _ErrorType_co]
        ],
    ) -> 'FutureResult[_NewValueType, _ErrorType_co]':
        """
        Binds a function returning ``Result[a, b]`` container.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IOSuccess, IOFailure
          >>> from returns.result import Result, Success
          >>> from returns.future import FutureResult

          >>> def bind(inner_value: int) -> Result[int, str]:
          ...     return Success(inner_value + 1)

          >>> assert anyio.run(
          ...     FutureResult.from_value(1).bind_result(bind).awaitable,
          ... ) == IOSuccess(2)
          >>> assert anyio.run(
          ...     FutureResult.from_failure('a').bind_result(bind).awaitable,
          ... ) == IOFailure('a')

        """
        return FutureResult(
            _future_result.async_bind_result(
                function,
                self._inner_value,
            )
        )

    def bind_ioresult(
        self,
        function: Callable[
            [_ValueType_co], IOResult[_NewValueType, _ErrorType_co]
        ],
    ) -> 'FutureResult[_NewValueType, _ErrorType_co]':
        """
        Binds a function returning ``IOResult[a, b]`` container.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IOResult, IOSuccess, IOFailure
          >>> from returns.future import FutureResult

          >>> def bind(inner_value: int) -> IOResult[int, str]:
          ...     return IOSuccess(inner_value + 1)

          >>> assert anyio.run(
          ...     FutureResult.from_value(1).bind_ioresult(bind).awaitable,
          ... ) == IOSuccess(2)
          >>> assert anyio.run(
          ...     FutureResult.from_failure('a').bind_ioresult(bind).awaitable,
          ... ) == IOFailure('a')

        """
        return FutureResult(
            _future_result.async_bind_ioresult(
                function,
                self._inner_value,
            )
        )

    def bind_io(
        self,
        function: Callable[[_ValueType_co], IO[_NewValueType]],
    ) -> 'FutureResult[_NewValueType, _ErrorType_co]':
        """
        Binds a function returning ``IO[a]`` container.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IO, IOSuccess, IOFailure
          >>> from returns.future import FutureResult

          >>> def bind(inner_value: int) -> IO[float]:
          ...     return IO(inner_value + 0.5)

          >>> assert anyio.run(
          ...     FutureResult.from_value(1).bind_io(bind).awaitable,
          ... ) == IOSuccess(1.5)
          >>> assert anyio.run(
          ...     FutureResult.from_failure(1).bind_io(bind).awaitable,
          ... ) == IOFailure(1)

        """
        return FutureResult(
            _future_result.async_bind_io(
                function,
                self._inner_value,
            )
        )

    def bind_future(
        self,
        function: Callable[[_ValueType_co], Future[_NewValueType]],
    ) -> 'FutureResult[_NewValueType, _ErrorType_co]':
        """
        Binds a function returning ``Future[a]`` container.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IOSuccess, IOFailure
          >>> from returns.future import Future, FutureResult

          >>> def bind(inner_value: int) -> Future[float]:
          ...     return Future.from_value(inner_value + 0.5)

          >>> assert anyio.run(
          ...     FutureResult.from_value(1).bind_future(bind).awaitable,
          ... ) == IOSuccess(1.5)
          >>> assert anyio.run(
          ...     FutureResult.from_failure(1).bind_future(bind).awaitable,
          ... ) == IOFailure(1)

        """
        return FutureResult(
            _future_result.async_bind_future(
                function,
                self._inner_value,
            )
        )

    def bind_async_future(
        self,
        function: Callable[[_ValueType_co], Awaitable['Future[_NewValueType]']],
    ) -> 'FutureResult[_NewValueType, _ErrorType_co]':
        """
        Composes a container and ``async`` function returning ``Future``.

        Similar to :meth:`~FutureResult.bind_future`
        but works with async functions.

        .. code:: python

          >>> import anyio
          >>> from returns.future import Future, FutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> async def coroutine(x: int) -> Future[str]:
          ...    return Future.from_value(str(x + 1))

          >>> assert anyio.run(
          ...     FutureResult.from_value(1).bind_async_future,
          ...     coroutine,
          ... ) == IOSuccess('2')
          >>> assert anyio.run(
          ...     FutureResult.from_failure(1).bind_async,
          ...     coroutine,
          ... ) == IOFailure(1)

        """
        return FutureResult(
            _future_result.async_bind_async_future(
                function,
                self._inner_value,
            )
        )

    def alt(
        self,
        function: Callable[[_ErrorType_co], _NewErrorType],
    ) -> 'FutureResult[_ValueType_co, _NewErrorType]':
        """
        Composes failed container with a pure function to modify failure.

        .. code:: python

          >>> import anyio
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> def altable(arg: int) -> int:
          ...      return arg + 1

          >>> assert anyio.run(
          ...     FutureResult.from_value(1).alt(altable).awaitable,
          ... ) == IOSuccess(1)
          >>> assert anyio.run(
          ...     FutureResult.from_failure(1).alt(altable).awaitable,
          ... ) == IOFailure(2)

        """
        return FutureResult(
            _future_result.async_alt(
                function,
                self._inner_value,
            )
        )

    def lash(
        self,
        function: Callable[
            [_ErrorType_co],
            Kind2['FutureResult', _ValueType_co, _NewErrorType],
        ],
    ) -> 'FutureResult[_ValueType_co, _NewErrorType]':
        """
        Composes failed container with a function that returns a container.

        .. code:: python

          >>> import anyio
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess

          >>> def lashable(x: int) -> FutureResult[int, str]:
          ...    return FutureResult.from_value(x + 1)

          >>> assert anyio.run(
          ...     FutureResult.from_value(1).lash(lashable).awaitable,
          ... ) == IOSuccess(1)
          >>> assert anyio.run(
          ...     FutureResult.from_failure(1).lash(lashable).awaitable,
          ... ) == IOSuccess(2)

        """
        return FutureResult(
            _future_result.async_lash(
                function,
                self._inner_value,
            )
        )

    def compose_result(
        self,
        function: Callable[
            [Result[_ValueType_co, _ErrorType_co]],
            Kind2['FutureResult', _NewValueType, _ErrorType_co],
        ],
    ) -> 'FutureResult[_NewValueType, _ErrorType_co]':
        """
        Composes inner ``Result`` with ``FutureResult`` returning function.

        Can be useful when you need an access to both states of the result.

        .. code:: python

          >>> import anyio
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOFailure
          >>> from returns.result import Result

          >>> def count(container: Result[int, int]) -> FutureResult[int, int]:
          ...     return FutureResult.from_result(
          ...         container.map(lambda x: x + 1).alt(abs),
          ...     )

          >>> assert anyio.run(
          ...    FutureResult.from_value(1).compose_result, count,
          ... ) == IOSuccess(2)
          >>> assert anyio.run(
          ...    FutureResult.from_failure(-1).compose_result, count,
          ... ) == IOFailure(1)

        """
        return FutureResult(
            _future_result.async_compose_result(
                function,
                self._inner_value,
            )
        )

    def __aiter__(self) -> AsyncIterator[_ValueType_co]:  # noqa: WPS611
        """API for :ref:`do-notation`."""

        async def factory() -> AsyncGenerator[_ValueType_co, None]:
            for inner_value in await self._inner_value:
                yield inner_value  # will only yield once

        return factory()

    @classmethod
    def do(
        cls,
        expr: AsyncGenerator[_NewValueType, None],
    ) -> 'FutureResult[_NewValueType, _NewErrorType]':
        """
        Allows working with unwrapped values of containers in a safe way.

        .. code:: python

          >>> import anyio
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> async def success() -> bool:
          ...     return await FutureResult.do(
          ...         first + second
          ...         async for first in FutureResult.from_value(2)
          ...         async for second in FutureResult.from_value(3)
          ...     ) == IOSuccess(5)

          >>> assert anyio.run(success) is True

          >>> async def failure() -> bool:
          ...     return await FutureResult.do(
          ...         first + second
          ...         async for first in FutureResult.from_value(2)
          ...         async for second in FutureResult.from_failure(3)
          ...     ) == IOFailure(3)

          >>> assert anyio.run(failure) is True

        See :ref:`do-notation` to learn more.

        """

        async def factory() -> Result[_NewValueType, _NewErrorType]:
            try:
                return Success(await anext(expr))
            except UnwrapFailedError as exc:
                return exc.halted_container  # type: ignore

        return FutureResult(factory())

    @classmethod
    def from_typecast(
        cls,
        inner_value: Future[Result[_NewValueType, _NewErrorType]],
    ) -> 'FutureResult[_NewValueType, _NewErrorType]':
        """
        Creates ``FutureResult[a, b]`` from ``Future[Result[a, b]]``.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IOSuccess, IOFailure
          >>> from returns.result import Success, Failure
          >>> from returns.future import Future, FutureResult

          >>> async def main():
          ...     assert await FutureResult.from_typecast(
          ...         Future.from_value(Success(1)),
          ...     ) == IOSuccess(1)
          ...     assert await FutureResult.from_typecast(
          ...         Future.from_value(Failure(1)),
          ...     ) == IOFailure(1)

          >>> anyio.run(main)

        """
        return FutureResult(inner_value._inner_value)  # noqa: SLF001

    @classmethod
    def from_future(
        cls,
        inner_value: Future[_NewValueType],
    ) -> 'FutureResult[_NewValueType, Any]':
        """
        Creates ``FutureResult`` from successful ``Future`` value.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IOSuccess
          >>> from returns.future import Future, FutureResult

          >>> async def main():
          ...     assert await FutureResult.from_future(
          ...         Future.from_value(1),
          ...     ) == IOSuccess(1)

          >>> anyio.run(main)

        """
        return FutureResult(_future_result.async_from_success(inner_value))

    @classmethod
    def from_failed_future(
        cls,
        inner_value: Future[_NewErrorType],
    ) -> 'FutureResult[Any, _NewErrorType]':
        """
        Creates ``FutureResult`` from failed ``Future`` value.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IOFailure
          >>> from returns.future import Future, FutureResult

          >>> async def main():
          ...     assert await FutureResult.from_failed_future(
          ...         Future.from_value(1),
          ...     ) == IOFailure(1)

          >>> anyio.run(main)

        """
        return FutureResult(_future_result.async_from_failure(inner_value))

    @classmethod
    def from_future_result(
        cls,
        inner_value: 'FutureResult[_NewValueType, _NewErrorType]',
    ) -> 'FutureResult[_NewValueType, _NewErrorType]':
        """
        Creates new ``FutureResult`` from existing one.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IOSuccess
          >>> from returns.future import FutureResult

          >>> async def main():
          ...     assert await FutureResult.from_future_result(
          ...         FutureResult.from_value(1),
          ...     ) == IOSuccess(1)

          >>> anyio.run(main)

        Part of the ``FutureResultLikeN`` interface.
        """
        return inner_value

    @classmethod
    def from_io(
        cls,
        inner_value: IO[_NewValueType],
    ) -> 'FutureResult[_NewValueType, Any]':
        """
        Creates ``FutureResult`` from successful ``IO`` value.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IO, IOSuccess
          >>> from returns.future import FutureResult

          >>> async def main():
          ...     assert await FutureResult.from_io(
          ...         IO(1),
          ...     ) == IOSuccess(1)

          >>> anyio.run(main)

        """
        return FutureResult.from_value(inner_value._inner_value)  # noqa: SLF001

    @classmethod
    def from_failed_io(
        cls,
        inner_value: IO[_NewErrorType],
    ) -> 'FutureResult[Any, _NewErrorType]':
        """
        Creates ``FutureResult`` from failed ``IO`` value.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IO, IOFailure
          >>> from returns.future import FutureResult

          >>> async def main():
          ...     assert await FutureResult.from_failed_io(
          ...         IO(1),
          ...     ) == IOFailure(1)

          >>> anyio.run(main)

        """
        return FutureResult.from_failure(inner_value._inner_value)  # noqa: SLF001

    @classmethod
    def from_ioresult(
        cls,
        inner_value: IOResult[_NewValueType, _NewErrorType],
    ) -> 'FutureResult[_NewValueType, _NewErrorType]':
        """
        Creates ``FutureResult`` from ``IOResult`` value.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IOSuccess, IOFailure
          >>> from returns.future import FutureResult

          >>> async def main():
          ...     assert await FutureResult.from_ioresult(
          ...         IOSuccess(1),
          ...     ) == IOSuccess(1)
          ...     assert await FutureResult.from_ioresult(
          ...         IOFailure(1),
          ...     ) == IOFailure(1)

          >>> anyio.run(main)

        """
        return FutureResult(async_identity(inner_value._inner_value))  # noqa: SLF001

    @classmethod
    def from_result(
        cls,
        inner_value: Result[_NewValueType, _NewErrorType],
    ) -> 'FutureResult[_NewValueType, _NewErrorType]':
        """
        Creates ``FutureResult`` from ``Result`` value.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IOSuccess, IOFailure
          >>> from returns.result import Success, Failure
          >>> from returns.future import FutureResult

          >>> async def main():
          ...     assert await FutureResult.from_result(
          ...         Success(1),
          ...     ) == IOSuccess(1)
          ...     assert await FutureResult.from_result(
          ...         Failure(1),
          ...     ) == IOFailure(1)

          >>> anyio.run(main)

        """
        return FutureResult(async_identity(inner_value))

    @classmethod
    def from_value(
        cls,
        inner_value: _NewValueType,
    ) -> 'FutureResult[_NewValueType, Any]':
        """
        Creates ``FutureResult`` from successful value.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IOSuccess
          >>> from returns.future import FutureResult

          >>> async def main():
          ...     assert await FutureResult.from_value(
          ...         1,
          ...     ) == IOSuccess(1)

          >>> anyio.run(main)

        """
        return FutureResult(async_identity(Success(inner_value)))

    @classmethod
    def from_failure(
        cls,
        inner_value: _NewErrorType,
    ) -> 'FutureResult[Any, _NewErrorType]':
        """
        Creates ``FutureResult`` from failed value.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IOFailure
          >>> from returns.future import FutureResult

          >>> async def main():
          ...     assert await FutureResult.from_failure(
          ...         1,
          ...     ) == IOFailure(1)

          >>> anyio.run(main)

        """
        return FutureResult(async_identity(Failure(inner_value)))


def FutureSuccess(  # noqa: N802
    inner_value: _NewValueType,
) -> FutureResult[_NewValueType, Any]:
    """
    Public unit function to create successful ``FutureResult`` objects.

    Is the same as :meth:`~FutureResult.from_value`.

    .. code:: python

      >>> import anyio
      >>> from returns.future import FutureResult, FutureSuccess

      >>> assert anyio.run(FutureSuccess(1).awaitable) == anyio.run(
      ...     FutureResult.from_value(1).awaitable,
      ... )

    """
    return FutureResult.from_value(inner_value)


def FutureFailure(  # noqa: N802
    inner_value: _NewErrorType,
) -> FutureResult[Any, _NewErrorType]:
    """
    Public unit function to create failed ``FutureResult`` objects.

    Is the same as :meth:`~FutureResult.from_failure`.

    .. code:: python

      >>> import anyio
      >>> from returns.future import FutureResult, FutureFailure

      >>> assert anyio.run(FutureFailure(1).awaitable) == anyio.run(
      ...     FutureResult.from_failure(1).awaitable,
      ... )

    """
    return FutureResult.from_failure(inner_value)


#: Alias for ``FutureResult[_ValueType_co, Exception]``.
FutureResultE: TypeAlias = FutureResult[_ValueType_co, Exception]


_ExceptionType = TypeVar('_ExceptionType', bound=Exception)


# Decorators:


@overload
def future_safe(
    exceptions: Callable[
        _FuncParams,
        Coroutine[_FirstType, _SecondType, _ValueType_co],
    ],
    /,
) -> Callable[_FuncParams, FutureResultE[_ValueType_co]]: ...


@overload
def future_safe(
    exceptions: tuple[type[_ExceptionType], ...],
) -> Callable[
    [
        Callable[
            _FuncParams,
            Coroutine[_FirstType, _SecondType, _ValueType_co],
        ],
    ],
    Callable[_FuncParams, FutureResult[_ValueType_co, _ExceptionType]],
]: ...


def future_safe(  # noqa: WPS212, WPS234,
    exceptions: (
        Callable[
            _FuncParams,
            Coroutine[_FirstType, _SecondType, _ValueType_co],
        ]
        | tuple[type[_ExceptionType], ...]
    ),
) -> (
    Callable[_FuncParams, FutureResultE[_ValueType_co]]
    | Callable[
        [
            Callable[
                _FuncParams,
                Coroutine[_FirstType, _SecondType, _ValueType_co],
            ],
        ],
        Callable[_FuncParams, FutureResult[_ValueType_co, _ExceptionType]],
    ]
):
    """
    Decorator to convert exception-throwing coroutine to ``FutureResult``.

    Should be used with care, since it only catches ``Exception`` subclasses.
    It does not catch ``BaseException`` subclasses.

    If you need to mark sync function as ``safe``,
    use :func:`returns.future.future_safe` instead.
    This decorator only works with ``async`` functions. Example:

    .. code:: python

      >>> import anyio
      >>> from returns.future import future_safe
      >>> from returns.io import IOFailure, IOSuccess

      >>> @future_safe
      ... async def might_raise(arg: int) -> float:
      ...     return 1 / arg
      ...

      >>> assert anyio.run(might_raise(2).awaitable) == IOSuccess(0.5)
      >>> assert isinstance(
      ...     anyio.run(might_raise(0).awaitable),
      ...     IOFailure,
      ... )

    You can also use it with explicit exception types as the first argument:

    .. code:: python

      >>> from returns.future import future_safe
      >>> from returns.io import IOFailure, IOSuccess

      >>> @future_safe(exceptions=(ZeroDivisionError,))
      ... async def might_raise(arg: int) -> float:
      ...     return 1 / arg

      >>> assert anyio.run(might_raise(2).awaitable) == IOSuccess(0.5)
      >>> assert isinstance(
      ...     anyio.run(might_raise(0).awaitable),
      ...     IOFailure,
      ... )

    In this case, only exceptions that are explicitly
    listed are going to be caught.

    Similar to :func:`returns.io.impure_safe` and :func:`returns.result.safe`
    decorators, but works with ``async`` functions.

    """

    def _future_safe_factory(  # noqa: WPS430
        function: Callable[
            _FuncParams,
            Coroutine[_FirstType, _SecondType, _ValueType_co],
        ],
        inner_exceptions: tuple[type[_ExceptionType], ...],
    ) -> Callable[_FuncParams, FutureResult[_ValueType_co, _ExceptionType]]:
        async def factory(
            *args: _FuncParams.args,
            **kwargs: _FuncParams.kwargs,
        ) -> Result[_ValueType_co, _ExceptionType]:
            try:
                return Success(await function(*args, **kwargs))
            except inner_exceptions as exc:
                return Failure(exc)

        @wraps(function)
        def decorator(
            *args: _FuncParams.args,
            **kwargs: _FuncParams.kwargs,
        ) -> FutureResult[_ValueType_co, _ExceptionType]:
            return FutureResult(factory(*args, **kwargs))

        return decorator

    if isinstance(exceptions, tuple):
        return lambda function: _future_safe_factory(function, exceptions)
    return _future_safe_factory(
        exceptions,
        (Exception,),  # type: ignore[arg-type]
    )
