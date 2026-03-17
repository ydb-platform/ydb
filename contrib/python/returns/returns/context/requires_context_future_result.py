from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, TypeVar, final

from returns._internal.futures import _reader_future_result
from returns.context import NoDeps
from returns.future import Future, FutureResult
from returns.interfaces.specific import future_result, reader_future_result
from returns.io import IO, IOResult
from returns.primitives.container import BaseContainer
from returns.primitives.hkt import Kind3, SupportsKind3, dekind
from returns.result import Result

if TYPE_CHECKING:
    from returns.context.requires_context import RequiresContext
    from returns.context.requires_context_ioresult import (
        ReaderIOResult,
        RequiresContextIOResult,
    )
    from returns.context.requires_context_result import RequiresContextResult

# Context:
_EnvType_contra = TypeVar('_EnvType_contra', contravariant=True)
_NewEnvType = TypeVar('_NewEnvType')

# Result:
_ValueType_co = TypeVar('_ValueType_co', covariant=True)
_NewValueType = TypeVar('_NewValueType')
_ErrorType_co = TypeVar('_ErrorType_co', covariant=True)
_NewErrorType = TypeVar('_NewErrorType')

# Helpers:
_FirstType = TypeVar('_FirstType')


@final
class RequiresContextFutureResult(  # type: ignore[type-var]
    BaseContainer,
    SupportsKind3[
        'RequiresContextFutureResult',
        _ValueType_co,
        _ErrorType_co,
        _EnvType_contra,
    ],
    reader_future_result.ReaderFutureResultBasedN[
        _ValueType_co,
        _ErrorType_co,
        _EnvType_contra,
    ],
    future_result.FutureResultLike3[
        _ValueType_co, _ErrorType_co, _EnvType_contra
    ],
):
    """
    The ``RequiresContextFutureResult`` combinator.

    This probably the main type people are going to use in ``async`` programs.

    See :class:`returns.context.requires_context.RequiresContext`,
    :class:`returns.context.requires_context_result.RequiresContextResult`,
    and
    :class:`returns.context.requires_context_result.RequiresContextIOResult`
    for more docs.

    This is just a handy wrapper around
    ``RequiresContext[FutureResult[a, b], env]``
    which represents a context-dependent impure async operation that might fail.

    So, this is a thin wrapper, without any changes in logic.
    Why do we need this wrapper? That's just for better usability!

    This way ``RequiresContextIOResult`` allows to simply work with:

    - raw values and pure functions
    - ``RequiresContext`` values and pure functions returning it
    - ``RequiresContextResult`` values and pure functions returning it
    - ``RequiresContextIOResult`` values and pure functions returning it
    - ``Result`` and pure functions returning it
    - ``IOResult`` and functions returning it
    - ``FutureResult`` and functions returning it
    - other ``RequiresContextFutureResult`` related functions and values

    This is a complex type for complex tasks!

    .. rubric:: Important implementation details

    Due it is meaning, ``RequiresContextFutureResult``
    cannot have ``Success`` and ``Failure`` subclasses.

    We only have just one type. That's by design.

    Different converters are also not supported for this type.
    Use converters inside the ``RequiresContext`` context, not outside.

    See also:
        - https://dev.to/gcanti/getting-started-with-fp-ts-reader-1ie5
        - https://en.wikipedia.org/wiki/Lazy_evaluation
        - https://bit.ly/2R8l4WK
        - https://bit.ly/2RwP4fp

    """

    __slots__ = ()

    #: Inner value of `RequiresContext`
    #: is just a function that returns `FutureResult`.
    #: This field has an extra 'RequiresContext' just because `mypy` needs it.
    _inner_value: Callable[
        [_EnvType_contra], FutureResult[_ValueType_co, _ErrorType_co]
    ]

    #: A convenient placeholder to call methods created by `.from_value()`.
    no_args: ClassVar[NoDeps] = object()

    def __init__(
        self,
        inner_value: Callable[
            [_EnvType_contra], FutureResult[_ValueType_co, _ErrorType_co]
        ],
    ) -> None:
        """
        Public constructor for this type. Also required for typing.

        Only allows functions of kind ``* -> *``
        and returning :class:`returns.result.Result` instances.

        .. code:: python

          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.future import FutureResult

          >>> instance = RequiresContextFutureResult(
          ...    lambda deps: FutureResult.from_value(1),
          ... )
          >>> str(instance)
          '<RequiresContextFutureResult: <function <lambda> at ...>>'

        """
        super().__init__(inner_value)

    def __call__(
        self, deps: _EnvType_contra
    ) -> FutureResult[_ValueType_co, _ErrorType_co]:
        """
        Evaluates the wrapped function.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess

          >>> def first(lg: bool) -> RequiresContextFutureResult[int, str, int]:
          ...     # `deps` has `int` type here:
          ...     return RequiresContextFutureResult(
          ...         lambda deps: FutureResult.from_value(
          ...             deps if lg else -deps,
          ...         ),
          ...     )

          >>> instance = first(False)
          >>> assert anyio.run(instance(3).awaitable) == IOSuccess(-3)

          >>> instance = first(True)
          >>> assert anyio.run(instance(3).awaitable) == IOSuccess(3)

        In other things, it is a regular Python magic method.

        """
        return self._inner_value(deps)

    def swap(
        self,
    ) -> RequiresContextFutureResult[
        _ErrorType_co, _ValueType_co, _EnvType_contra
    ]:
        """
        Swaps value and error types.

        So, values become errors and errors become values.
        It is useful when you have to work with errors a lot.
        And since we have a lot of ``.bind_`` related methods
        and only a single ``.lash`` - it is easier to work with values.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> success = RequiresContextFutureResult.from_value(1)
          >>> failure = RequiresContextFutureResult.from_failure(1)

          >>> assert anyio.run(success.swap(), ...) == IOFailure(1)
          >>> assert anyio.run(failure.swap(), ...) == IOSuccess(1)

        """
        return RequiresContextFutureResult(lambda deps: self(deps).swap())

    def map(
        self,
        function: Callable[[_ValueType_co], _NewValueType],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Composes successful container with a pure function.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert anyio.run(RequiresContextFutureResult.from_value(1).map(
          ...     lambda x: x + 1,
          ... )(...).awaitable) == IOSuccess(2)

          >>> assert anyio.run(RequiresContextFutureResult.from_failure(1).map(
          ...     lambda x: x + 1,
          ... )(...).awaitable) == IOFailure(1)

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).map(function),
        )

    def apply(
        self,
        container: Kind3[
            RequiresContextFutureResult,
            Callable[[_ValueType_co], _NewValueType],
            _ErrorType_co,
            _EnvType_contra,
        ],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Calls a wrapped function in a container on this container.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> def transform(arg: str) -> str:
          ...     return arg + 'b'

          >>> assert anyio.run(
          ...    RequiresContextFutureResult.from_value('a').apply(
          ...        RequiresContextFutureResult.from_value(transform),
          ...    ),
          ...    RequiresContextFutureResult.no_args,
          ... ) == IOSuccess('ab')

          >>> assert anyio.run(
          ...    RequiresContextFutureResult.from_failure('a').apply(
          ...        RequiresContextFutureResult.from_value(transform),
          ...    ),
          ...    RequiresContextFutureResult.no_args,
          ... ) == IOFailure('a')

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).apply(dekind(container)(deps)),
        )

    def bind(
        self,
        function: Callable[
            [_ValueType_co],
            Kind3[
                RequiresContextFutureResult,
                _NewValueType,
                _ErrorType_co,
                _EnvType_contra,
            ],
        ],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Composes this container with a function returning the same type.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> def function(
          ...     number: int,
          ... ) -> RequiresContextFutureResult[str, int, int]:
          ...     # `deps` has `int` type here:
          ...     return RequiresContextFutureResult(
          ...         lambda deps: FutureResult.from_value(str(number + deps)),
          ...     )

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_value(2).bind(function),
          ...     3,
          ... ) == IOSuccess('5')
          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_failure(2).bind(function),
          ...     3,
          ... ) == IOFailure(2)

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).bind(
                lambda inner: dekind(  # type: ignore[misc]
                    function(inner),
                )(deps),
            ),
        )

    #: Alias for `bind_context_future_result` method,
    #: it is the same as `bind` here.
    bind_context_future_result = bind

    def bind_async(
        self,
        function: Callable[
            [_ValueType_co],
            Awaitable[
                Kind3[
                    RequiresContextFutureResult,
                    _NewValueType,
                    _ErrorType_co,
                    _EnvType_contra,
                ],
            ],
        ],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Composes this container with a async function returning the same type.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> async def function(
          ...     number: int,
          ... ) -> RequiresContextFutureResult[str, int, int]:
          ...     return RequiresContextFutureResult.from_value(number + 1)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_value(1).bind_async(
          ...        function,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(2)
          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_failure(1).bind_async(
          ...        function,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextFutureResult(
            lambda deps: FutureResult(
                _reader_future_result.async_bind_async(
                    function,
                    self,
                    deps,
                )
            ),
        )

    #: Alias for `bind_async_context_future_result` method,
    #: it is the same as `bind_async` here.
    bind_async_context_future_result = bind_async

    def bind_awaitable(
        self,
        function: Callable[[_ValueType_co], Awaitable[_NewValueType]],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Allows to compose a container and a regular ``async`` function.

        This function should return plain, non-container value.
        See :meth:`~RequiresContextFutureResult.bind_async`
        to bind ``async`` function that returns a container.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> async def coroutine(x: int) -> int:
          ...    return x + 1

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_value(1).bind_awaitable(
          ...         coroutine,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(2)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_failure(1).bind_awaitable(
          ...         coroutine,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).bind_awaitable(function),
        )

    def bind_result(
        self,
        function: Callable[
            [_ValueType_co], Result[_NewValueType, _ErrorType_co]
        ],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Binds ``Result`` returning function to the current container.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.result import Success, Result
          >>> from returns.io import IOSuccess, IOFailure

          >>> def function(num: int) -> Result[int, str]:
          ...     return Success(num + 1)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_value(1).bind_result(
          ...         function,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(2)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_failure(':(').bind_result(
          ...         function,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(':(')

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).bind_result(function),
        )

    def bind_context(
        self,
        function: Callable[
            [_ValueType_co],
            RequiresContext[_NewValueType, _EnvType_contra],
        ],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Binds ``RequiresContext`` returning function to current container.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContext
          >>> from returns.io import IOSuccess, IOFailure

          >>> def function(arg: int) -> RequiresContext[int, str]:
          ...     return RequiresContext(lambda deps: len(deps) + arg)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_value(2).bind_context(
          ...         function,
          ...     ),
          ...     'abc',
          ... ) == IOSuccess(5)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_failure(0).bind_context(
          ...         function,
          ...     ),
          ...     'abc',
          ... ) == IOFailure(0)

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).map(
                lambda inner: function(inner)(deps),  # type: ignore[misc]
            ),
        )

    def bind_context_result(
        self,
        function: Callable[
            [_ValueType_co],
            RequiresContextResult[
                _NewValueType, _ErrorType_co, _EnvType_contra
            ],
        ],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Binds ``RequiresContextResult`` returning function to the current one.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextResult
          >>> from returns.io import IOSuccess, IOFailure
          >>> from returns.result import Success

          >>> def function(arg: int) -> RequiresContextResult[int, int, str]:
          ...     return RequiresContextResult(
          ...         lambda deps: Success(len(deps) + arg),
          ...     )

          >>> instance = RequiresContextFutureResult.from_value(
          ...    2,
          ... ).bind_context_result(
          ...     function,
          ... )('abc')
          >>> assert anyio.run(instance.awaitable) == IOSuccess(5)

          >>> instance = RequiresContextFutureResult.from_failure(
          ...    2,
          ... ).bind_context_result(
          ...     function,
          ... )('abc')
          >>> assert anyio.run(instance.awaitable) == IOFailure(2)

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).bind_result(
                lambda inner: function(inner)(deps),  # type: ignore[misc]
            ),
        )

    def bind_context_ioresult(
        self,
        function: Callable[
            [_ValueType_co],
            RequiresContextIOResult[
                _NewValueType, _ErrorType_co, _EnvType_contra
            ],
        ],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Binds ``RequiresContextIOResult`` returning function to the current one.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> def function(arg: int) -> RequiresContextIOResult[int, int, str]:
          ...     return RequiresContextIOResult(
          ...         lambda deps: IOSuccess(len(deps) + arg),
          ...     )

          >>> instance = RequiresContextFutureResult.from_value(
          ...    2,
          ... ).bind_context_ioresult(
          ...     function,
          ... )('abc')
          >>> assert anyio.run(instance.awaitable) == IOSuccess(5)

          >>> instance = RequiresContextFutureResult.from_failure(
          ...    2,
          ... ).bind_context_ioresult(
          ...     function,
          ... )('abc')
          >>> assert anyio.run(instance.awaitable) == IOFailure(2)

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).bind_ioresult(
                lambda inner: function(inner)(deps),  # type: ignore[misc]
            ),
        )

    def bind_io(
        self,
        function: Callable[[_ValueType_co], IO[_NewValueType]],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Binds ``IO`` returning function to the current container.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.io import IO, IOSuccess, IOFailure

          >>> def do_io(number: int) -> IO[str]:
          ...     return IO(str(number))  # not IO operation actually

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_value(1).bind_io(do_io),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess('1')

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_failure(1).bind_io(do_io),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).bind_io(function),
        )

    def bind_ioresult(
        self,
        function: Callable[
            [_ValueType_co], IOResult[_NewValueType, _ErrorType_co]
        ],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Binds ``IOResult`` returning function to the current container.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.io import IOResult, IOSuccess, IOFailure

          >>> def function(num: int) -> IOResult[int, str]:
          ...     return IOSuccess(num + 1)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_value(1).bind_ioresult(
          ...         function,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(2)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_failure(':(').bind_ioresult(
          ...         function,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(':(')

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).bind_ioresult(function),
        )

    def bind_future(
        self,
        function: Callable[[_ValueType_co], Future[_NewValueType]],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Binds ``Future`` returning function to the current container.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.future import Future
          >>> from returns.io import IOSuccess, IOFailure

          >>> def function(num: int) -> Future[int]:
          ...     return Future.from_value(num + 1)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_value(1).bind_future(
          ...         function,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(2)

          >>> failed = RequiresContextFutureResult.from_failure(':(')
          >>> assert anyio.run(
          ...     failed.bind_future(function),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(':(')

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).bind_future(function),
        )

    def bind_future_result(
        self,
        function: Callable[
            [_ValueType_co],
            FutureResult[_NewValueType, _ErrorType_co],
        ],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Binds ``FutureResult`` returning function to the current container.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> def function(num: int) -> FutureResult[int, str]:
          ...     return FutureResult.from_value(num + 1)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_value(1).bind_future_result(
          ...         function,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(2)

          >>> failed = RequiresContextFutureResult.from_failure(':(')
          >>> assert anyio.run(
          ...     failed.bind_future_result(function),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(':(')

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).bind(function),
        )

    def bind_async_future(
        self,
        function: Callable[[_ValueType_co], Awaitable[Future[_NewValueType]]],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Binds ``Future`` returning async function to the current container.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.future import Future
          >>> from returns.io import IOSuccess, IOFailure

          >>> async def function(num: int) -> Future[int]:
          ...     return Future.from_value(num + 1)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_value(1).bind_async_future(
          ...         function,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(2)

          >>> failed = RequiresContextFutureResult.from_failure(':(')
          >>> assert anyio.run(
          ...     failed.bind_async_future(function),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(':(')

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).bind_async_future(function),
        )

    def bind_async_future_result(
        self,
        function: Callable[
            [_ValueType_co],
            Awaitable[FutureResult[_NewValueType, _ErrorType_co]],
        ],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Bind ``FutureResult`` returning async function to the current container.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> async def function(num: int) -> FutureResult[int, str]:
          ...     return FutureResult.from_value(num + 1)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_value(
          ...         1,
          ...     ).bind_async_future_result(
          ...         function,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(2)

          >>> failed = RequiresContextFutureResult.from_failure(':(')
          >>> assert anyio.run(
          ...     failed.bind_async_future_result(function),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(':(')

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).bind_async(function),
        )

    def alt(
        self,
        function: Callable[[_ErrorType_co], _NewErrorType],
    ) -> RequiresContextFutureResult[
        _ValueType_co, _NewErrorType, _EnvType_contra
    ]:
        """
        Composes failed container with a pure function.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_value(1).alt(
          ...        lambda x: x + 1,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(1)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_failure(1).alt(
          ...        lambda x: x + 1,
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(2)

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).alt(function),
        )

    def lash(
        self,
        function: Callable[
            [_ErrorType_co],
            Kind3[
                RequiresContextFutureResult,
                _ValueType_co,
                _NewErrorType,
                _EnvType_contra,
            ],
        ],
    ) -> RequiresContextFutureResult[
        _ValueType_co, _NewErrorType, _EnvType_contra
    ]:
        """
        Composes this container with a function returning the same type.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess

          >>> def lashable(
          ...     arg: str,
          ... ) -> RequiresContextFutureResult[str, str, str]:
          ...      return RequiresContextFutureResult(
          ...          lambda deps: FutureResult.from_value(
          ...              deps + arg,
          ...          ),
          ...      )

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_value('a').lash(lashable),
          ...     'c',
          ... ) == IOSuccess('a')

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_failure('aa').lash(
          ...         lashable,
          ...     ),
          ...     'b',
          ... ) == IOSuccess('baa')

        """
        return RequiresContextFutureResult(
            lambda deps: self(deps).lash(
                lambda inner: function(inner)(deps),  # type: ignore
            ),
        )

    def compose_result(
        self,
        function: Callable[
            [Result[_ValueType_co, _ErrorType_co]],
            Kind3[
                RequiresContextFutureResult,
                _NewValueType,
                _ErrorType_co,
                _EnvType_contra,
            ],
        ],
    ) -> RequiresContextFutureResult[
        _NewValueType, _ErrorType_co, _EnvType_contra
    ]:
        """
        Composes inner ``Result`` with ``ReaderFutureResult`` returning func.

        Can be useful when you need an access to both states of the result.

        .. code:: python

          >>> import anyio
          >>> from returns.context import ReaderFutureResult, NoDeps
          >>> from returns.io import IOSuccess, IOFailure
          >>> from returns.result import Result

          >>> def count(
          ...    container: Result[int, int],
          ... ) -> ReaderFutureResult[int, int, NoDeps]:
          ...     return ReaderFutureResult.from_result(
          ...         container.map(lambda x: x + 1).alt(abs),
          ...     )

          >>> success = ReaderFutureResult.from_value(1)
          >>> failure = ReaderFutureResult.from_failure(-1)

          >>> assert anyio.run(
          ...     success.compose_result(count), ReaderFutureResult.no_args,
          ... ) == IOSuccess(2)
          >>> assert anyio.run(
          ...     failure.compose_result(count), ReaderFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextFutureResult(
            lambda deps: FutureResult(
                _reader_future_result.async_compose_result(
                    function,
                    self,
                    deps,
                ),
            ),
        )

    def modify_env(
        self,
        function: Callable[[_NewEnvType], _EnvType_contra],
    ) -> RequiresContextFutureResult[_ValueType_co, _ErrorType_co, _NewEnvType]:
        """
        Allows to modify the environment type.

        .. code:: python

          >>> import anyio
          >>> from returns.future import future_safe, asyncify
          >>> from returns.context import RequiresContextFutureResultE
          >>> from returns.io import IOSuccess

          >>> def div(arg: int) -> RequiresContextFutureResultE[float, int]:
          ...     return RequiresContextFutureResultE(
          ...         future_safe(asyncify(lambda deps: arg / deps)),
          ...     )

          >>> assert anyio.run(div(3).modify_env(int), '2') == IOSuccess(1.5)
          >>> assert anyio.run(div(3).modify_env(int), '0').failure()

        """
        return RequiresContextFutureResult(lambda deps: self(function(deps)))

    @classmethod
    def ask(
        cls,
    ) -> RequiresContextFutureResult[
        _EnvType_contra, _ErrorType_co, _EnvType_contra
    ]:
        """
        Is used to get the current dependencies inside the call stack.

        Similar to
        :meth:`returns.context.requires_context.RequiresContext.ask`,
        but returns ``FutureResult`` instead of a regular value.

        Please, refer to the docs there to learn how to use it.

        One important note that is worth duplicating here:
        you might need to provide type annotations explicitly,
        so ``mypy`` will know about it statically.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResultE
          >>> from returns.io import IOSuccess

          >>> assert anyio.run(
          ...     RequiresContextFutureResultE[int, int].ask().map(str),
          ...     1,
          ... ) == IOSuccess('1')

        """
        return RequiresContextFutureResult(FutureResult.from_value)

    @classmethod
    def from_result(
        cls,
        inner_value: Result[_NewValueType, _NewErrorType],
    ) -> RequiresContextFutureResult[_NewValueType, _NewErrorType, NoDeps]:
        """
        Creates new container with ``Result`` as a unit value.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.result import Success, Failure
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert anyio.run(
          ...    RequiresContextFutureResult.from_result(Success(1)),
          ...    RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(1)

          >>> assert anyio.run(
          ...    RequiresContextFutureResult.from_result(Failure(1)),
          ...    RequiresContextFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextFutureResult(
            lambda _: FutureResult.from_result(inner_value),
        )

    @classmethod
    def from_io(
        cls,
        inner_value: IO[_NewValueType],
    ) -> RequiresContextFutureResult[_NewValueType, Any, NoDeps]:
        """
        Creates new container from successful ``IO`` value.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IO, IOSuccess
          >>> from returns.context import RequiresContextFutureResult

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_io(IO(1)),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(1)

        """
        return RequiresContextFutureResult(
            lambda deps: FutureResult.from_io(inner_value),
        )

    @classmethod
    def from_failed_io(
        cls,
        inner_value: IO[_NewErrorType],
    ) -> RequiresContextFutureResult[Any, _NewErrorType, NoDeps]:
        """
        Creates a new container from failed ``IO`` value.

        .. code:: python

          >>> import anyio
          >>> from returns.io import IO, IOFailure
          >>> from returns.context import RequiresContextFutureResult

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_failed_io(IO(1)),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextFutureResult(
            lambda deps: FutureResult.from_failed_io(inner_value),
        )

    @classmethod
    def from_ioresult(
        cls,
        inner_value: IOResult[_NewValueType, _NewErrorType],
    ) -> RequiresContextFutureResult[_NewValueType, _NewErrorType, NoDeps]:
        """
        Creates new container with ``IOResult`` as a unit value.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert anyio.run(
          ...    RequiresContextFutureResult.from_ioresult(IOSuccess(1)),
          ...    RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(1)

          >>> assert anyio.run(
          ...    RequiresContextFutureResult.from_ioresult(IOFailure(1)),
          ...    RequiresContextFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextFutureResult(
            lambda _: FutureResult.from_ioresult(inner_value),
        )

    @classmethod
    def from_future(
        cls,
        inner_value: Future[_NewValueType],
    ) -> RequiresContextFutureResult[_NewValueType, Any, NoDeps]:
        """
        Creates new container with successful ``Future`` as a unit value.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.future import Future
          >>> from returns.io import IOSuccess

          >>> assert anyio.run(
          ...    RequiresContextFutureResult.from_future(Future.from_value(1)),
          ...    RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(1)

        """
        return RequiresContextFutureResult(
            lambda _: FutureResult.from_future(inner_value),
        )

    @classmethod
    def from_failed_future(
        cls,
        inner_value: Future[_NewErrorType],
    ) -> RequiresContextFutureResult[Any, _NewErrorType, NoDeps]:
        """
        Creates new container with failed ``Future`` as a unit value.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.future import Future
          >>> from returns.io import IOFailure

          >>> assert anyio.run(
          ...    RequiresContextFutureResult.from_failed_future(
          ...        Future.from_value(1),
          ...    ),
          ...    RequiresContextFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextFutureResult(
            lambda _: FutureResult.from_failed_future(inner_value),
        )

    @classmethod
    def from_future_result_context(
        cls,
        inner_value: ReaderFutureResult[
            _NewValueType, _NewErrorType, _NewEnvType
        ],
    ) -> ReaderFutureResult[_NewValueType, _NewErrorType, _NewEnvType]:
        """
        Creates new container with ``ReaderFutureResult`` as a unit value.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert anyio.run(
          ...    RequiresContextFutureResult.from_future_result_context(
          ...        RequiresContextFutureResult.from_value(1),
          ...    ),
          ...    RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(1)

          >>> assert anyio.run(
          ...    RequiresContextFutureResult.from_future_result_context(
          ...        RequiresContextFutureResult.from_failure(1),
          ...    ),
          ...    RequiresContextFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return inner_value

    @classmethod
    def from_future_result(
        cls,
        inner_value: FutureResult[_NewValueType, _NewErrorType],
    ) -> RequiresContextFutureResult[_NewValueType, _NewErrorType, NoDeps]:
        """
        Creates new container with ``FutureResult`` as a unit value.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert anyio.run(
          ...    RequiresContextFutureResult.from_future_result(
          ...        FutureResult.from_value(1),
          ...    ),
          ...    RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(1)

          >>> assert anyio.run(
          ...    RequiresContextFutureResult.from_future_result(
          ...        FutureResult.from_failure(1),
          ...    ),
          ...    RequiresContextFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextFutureResult(lambda _: inner_value)

    @classmethod
    def from_typecast(
        cls,
        inner_value: RequiresContext[
            FutureResult[_NewValueType, _NewErrorType],
            _EnvType_contra,
        ],
    ) -> RequiresContextFutureResult[
        _NewValueType, _NewErrorType, _EnvType_contra
    ]:
        """
        You might end up with ``RequiresContext[FutureResult]`` as a value.

        This method is designed to turn it into ``RequiresContextFutureResult``.
        It will save all the typing information.

        It is just more useful!

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContext
          >>> from returns.future import FutureResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_typecast(
          ...         RequiresContext.from_value(FutureResult.from_value(1)),
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(1)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_typecast(
          ...         RequiresContext.from_value(FutureResult.from_failure(1)),
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextFutureResult(inner_value)

    @classmethod
    def from_context(
        cls,
        inner_value: RequiresContext[_NewValueType, _NewEnvType],
    ) -> RequiresContextFutureResult[_NewValueType, Any, _NewEnvType]:
        """
        Creates new container from ``RequiresContext`` as a success unit.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContext
          >>> from returns.io import IOSuccess

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_context(
          ...         RequiresContext.from_value(1),
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(1)

        """
        return RequiresContextFutureResult(
            lambda deps: FutureResult.from_value(inner_value(deps)),
        )

    @classmethod
    def from_failed_context(
        cls,
        inner_value: RequiresContext[_NewValueType, _NewEnvType],
    ) -> RequiresContextFutureResult[Any, _NewValueType, _NewEnvType]:
        """
        Creates new container from ``RequiresContext`` as a failure unit.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContext
          >>> from returns.io import IOFailure

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_failed_context(
          ...         RequiresContext.from_value(1),
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextFutureResult(
            lambda deps: FutureResult.from_failure(inner_value(deps)),
        )

    @classmethod
    def from_result_context(
        cls,
        inner_value: RequiresContextResult[
            _NewValueType,
            _NewErrorType,
            _NewEnvType,
        ],
    ) -> ReaderFutureResult[_NewValueType, _NewErrorType, _NewEnvType]:
        """
        Creates new container from ``RequiresContextResult`` as a unit value.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_result_context(
          ...         RequiresContextResult.from_value(1),
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(1)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_result_context(
          ...         RequiresContextResult.from_failure(1),
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextFutureResult(
            lambda deps: FutureResult.from_result(inner_value(deps)),
        )

    @classmethod
    def from_ioresult_context(
        cls,
        inner_value: ReaderIOResult[_NewValueType, _NewErrorType, _NewEnvType],
    ) -> ReaderFutureResult[_NewValueType, _NewErrorType, _NewEnvType]:
        """
        Creates new container from ``RequiresContextIOResult`` as a unit value.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_ioresult_context(
          ...         RequiresContextIOResult.from_value(1),
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOSuccess(1)

          >>> assert anyio.run(
          ...     RequiresContextFutureResult.from_ioresult_context(
          ...         RequiresContextIOResult.from_failure(1),
          ...     ),
          ...     RequiresContextFutureResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextFutureResult(
            lambda deps: FutureResult.from_ioresult(inner_value(deps)),
        )

    @classmethod
    def from_value(
        cls,
        inner_value: _FirstType,
    ) -> RequiresContextFutureResult[_FirstType, Any, NoDeps]:
        """
        Creates new container with successful ``FutureResult`` as a unit value.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.io import IOSuccess

          >>> assert anyio.run(RequiresContextFutureResult.from_value(1)(
          ...    RequiresContextFutureResult.no_args,
          ... ).awaitable) == IOSuccess(1)

        """
        return RequiresContextFutureResult(
            lambda _: FutureResult.from_value(inner_value),
        )

    @classmethod
    def from_failure(
        cls,
        inner_value: _FirstType,
    ) -> RequiresContextFutureResult[Any, _FirstType, NoDeps]:
        """
        Creates new container with failed ``FutureResult`` as a unit value.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.io import IOFailure

          >>> assert anyio.run(RequiresContextFutureResult.from_failure(1)(
          ...     RequiresContextFutureResult.no_args,
          ... ).awaitable) == IOFailure(1)

        """
        return RequiresContextFutureResult(
            lambda _: FutureResult.from_failure(inner_value),
        )


# Aliases:

#: Alias for a popular case when ``Result`` has ``Exception`` as error type.
RequiresContextFutureResultE: TypeAlias = RequiresContextFutureResult[
    _ValueType_co,
    Exception,
    _EnvType_contra,
]

#: Sometimes `RequiresContextFutureResult` is too long to type.
ReaderFutureResult: TypeAlias = RequiresContextFutureResult

#: Alias to save you some typing. Uses ``Exception`` as error type.
ReaderFutureResultE: TypeAlias = RequiresContextFutureResult[
    _ValueType_co,
    Exception,
    _EnvType_contra,
]
