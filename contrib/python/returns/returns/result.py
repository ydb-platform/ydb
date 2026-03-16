from abc import ABC
from collections.abc import Callable, Generator, Iterator
from functools import wraps
from inspect import FrameInfo
from typing import TYPE_CHECKING, Any, TypeAlias, TypeVar, final, overload

from typing_extensions import Never, ParamSpec

from returns.interfaces.specific import result
from returns.primitives.container import BaseContainer, container_equality
from returns.primitives.exceptions import UnwrapFailedError
from returns.primitives.hkt import Kind2, SupportsKind2

# Definitions:
_ValueType_co = TypeVar('_ValueType_co', covariant=True)
_NewValueType = TypeVar('_NewValueType')
_ErrorType_co = TypeVar('_ErrorType_co', covariant=True)
_NewErrorType = TypeVar('_NewErrorType')

_FirstType = TypeVar('_FirstType')
_FuncParams = ParamSpec('_FuncParams')


class Result(  # type: ignore[type-var]
    BaseContainer,
    SupportsKind2['Result', _ValueType_co, _ErrorType_co],
    result.ResultBased2[_ValueType_co, _ErrorType_co],
    ABC,
):
    """
    Base class for :class:`~Failure` and :class:`~Success`.

    :class:`~Result` does not have a public constructor.
    Use :func:`~Success` and :func:`~Failure` to construct the needed values.

    See also:
        - https://bit.ly/361qQhi
        - https://hackernoon.com/the-throw-keyword-was-a-mistake-l9e532di

    """

    __slots__ = ('_trace',)
    __match_args__ = ('_inner_value',)

    _inner_value: _ValueType_co | _ErrorType_co
    _trace: list[FrameInfo] | None

    #: Typesafe equality comparison with other `Result` objects.
    equals = container_equality

    @property
    def trace(self) -> list[FrameInfo] | None:
        """Returns a list with stack trace when :func:`~Failure` was called."""
        return self._trace

    def swap(self) -> 'Result[_ErrorType_co, _ValueType_co]':
        """
        Swaps value and error types.

        So, values become errors and errors become values.
        It is useful when you have to work with errors a lot.
        And since we have a lot of ``.bind_`` related methods
        and only a single ``.lash`` - it is easier to work with values.

        .. code:: python

          >>> from returns.result import Success, Failure
          >>> assert Success(1).swap() == Failure(1)
          >>> assert Failure(1).swap() == Success(1)

        """

    def map(
        self,
        function: Callable[[_ValueType_co], _NewValueType],
    ) -> 'Result[_NewValueType, _ErrorType_co]':
        """
        Composes successful container with a pure function.

        .. code:: python

          >>> from returns.result import Failure, Success

          >>> def mappable(string: str) -> str:
          ...      return string + 'b'

          >>> assert Success('a').map(mappable) == Success('ab')
          >>> assert Failure('a').map(mappable) == Failure('a')

        """

    def apply(
        self,
        container: Kind2[
            'Result',
            Callable[[_ValueType_co], _NewValueType],
            _ErrorType_co,
        ],
    ) -> 'Result[_NewValueType, _ErrorType_co]':
        """
        Calls a wrapped function in a container on this container.

        .. code:: python

          >>> from returns.result import Failure, Success

          >>> def appliable(string: str) -> str:
          ...      return string + 'b'

          >>> assert Success('a').apply(Success(appliable)) == Success('ab')
          >>> assert Failure('a').apply(Success(appliable)) == Failure('a')

          >>> assert Success('a').apply(Failure(1)) == Failure(1)
          >>> assert Failure(1).apply(Failure(2)) == Failure(1)

        """

    def bind(
        self,
        function: Callable[
            [_ValueType_co],
            Kind2['Result', _NewValueType, _ErrorType_co],
        ],
    ) -> 'Result[_NewValueType, _ErrorType_co]':
        """
        Composes successful container with a function that returns a container.

        .. code:: python

          >>> from returns.result import Result, Success, Failure

          >>> def bindable(arg: str) -> Result[str, str]:
          ...      if len(arg) > 1:
          ...          return Success(arg + 'b')
          ...      return Failure(arg + 'c')

          >>> assert Success('aa').bind(bindable) == Success('aab')
          >>> assert Success('a').bind(bindable) == Failure('ac')
          >>> assert Failure('a').bind(bindable) == Failure('a')

        """

    #: Alias for `bind_result` method, it is the same as `bind` here.
    bind_result = bind

    def alt(
        self,
        function: Callable[[_ErrorType_co], _NewErrorType],
    ) -> 'Result[_ValueType_co, _NewErrorType]':
        """
        Composes failed container with a pure function to modify failure.

        .. code:: python

          >>> from returns.result import Failure, Success

          >>> def altable(arg: str) -> str:
          ...      return arg + 'b'

          >>> assert Success('a').alt(altable) == Success('a')
          >>> assert Failure('a').alt(altable) == Failure('ab')

        """

    def lash(
        self,
        function: Callable[
            [_ErrorType_co],
            Kind2['Result', _ValueType_co, _NewErrorType],
        ],
    ) -> 'Result[_ValueType_co, _NewErrorType]':
        """
        Composes failed container with a function that returns a container.

        .. code:: python

          >>> from returns.result import Result, Success, Failure

          >>> def lashable(arg: str) -> Result[str, str]:
          ...      if len(arg) > 1:
          ...          return Success(arg + 'b')
          ...      return Failure(arg + 'c')

          >>> assert Success('a').lash(lashable) == Success('a')
          >>> assert Failure('a').lash(lashable) == Failure('ac')
          >>> assert Failure('aa').lash(lashable) == Success('aab')

        """

    def __iter__(self) -> Iterator[_ValueType_co]:
        """API for :ref:`do-notation`."""
        yield self.unwrap()

    @classmethod
    def do(
        cls,
        expr: Generator[_NewValueType, None, None],
    ) -> 'Result[_NewValueType, _NewErrorType]':
        """
        Allows working with unwrapped values of containers in a safe way.

        .. code:: python

          >>> from returns.result import Result, Failure, Success

          >>> assert Result.do(
          ...     first + second
          ...     for first in Success(2)
          ...     for second in Success(3)
          ... ) == Success(5)

          >>> assert Result.do(
          ...     first + second
          ...     for first in Failure('a')
          ...     for second in Success(3)
          ... ) == Failure('a')

        See :ref:`do-notation` to learn more.
        This feature requires our :ref:`mypy plugin <mypy-plugins>`.

        """
        try:
            return Result.from_value(next(expr))
        except UnwrapFailedError as exc:
            return exc.halted_container  # type: ignore

    def value_or(
        self,
        default_value: _NewValueType,
    ) -> _ValueType_co | _NewValueType:
        """
        Get value or default value.

        .. code:: python

          >>> from returns.result import Failure, Success
          >>> assert Success(1).value_or(2) == 1
          >>> assert Failure(1).value_or(2) == 2

        """

    def unwrap(self) -> _ValueType_co:
        """
        Get value or raise exception.

        .. code:: pycon
          :force:

          >>> from returns.result import Failure, Success
          >>> assert Success(1).unwrap() == 1

          >>> Failure(1).unwrap()
          Traceback (most recent call last):
            ...
          returns.primitives.exceptions.UnwrapFailedError

        """

    def failure(self) -> _ErrorType_co:
        """
        Get failed value or raise exception.

        .. code:: pycon
          :force:

          >>> from returns.result import Failure, Success
          >>> assert Failure(1).failure() == 1

          >>> Success(1).failure()
          Traceback (most recent call last):
            ...
          returns.primitives.exceptions.UnwrapFailedError

        """

    @classmethod
    def from_value(
        cls,
        inner_value: _NewValueType,
    ) -> 'Result[_NewValueType, Any]':
        """
        One more value to create success unit values.

        It is useful as a united way to create a new value from any container.

        .. code:: python

          >>> from returns.result import Result, Success
          >>> assert Result.from_value(1) == Success(1)

        You can use this method or :func:`~Success`,
        choose the most convenient for you.

        """
        return Success(inner_value)

    @classmethod
    def from_failure(
        cls,
        inner_value: _NewErrorType,
    ) -> 'Result[Any, _NewErrorType]':
        """
        One more value to create failure unit values.

        It is useful as a united way to create a new value from any container.

        .. code:: python

          >>> from returns.result import Result, Failure
          >>> assert Result.from_failure(1) == Failure(1)

        You can use this method or :func:`~Failure`,
        choose the most convenient for you.

        """
        return Failure(inner_value)

    @classmethod
    def from_result(
        cls,
        inner_value: 'Result[_NewValueType, _NewErrorType]',
    ) -> 'Result[_NewValueType, _NewErrorType]':
        """
        Creates a new ``Result`` instance from existing ``Result`` instance.

        .. code:: python

          >>> from returns.result import Result, Failure, Success
          >>> assert Result.from_result(Success(1)) == Success(1)
          >>> assert Result.from_result(Failure(1)) == Failure(1)

        This is a part of
        :class:`returns.interfaces.specific.result.ResultBasedN` interface.
        """
        return inner_value


@final  # noqa: WPS338
class Failure(Result[Any, _ErrorType_co]):  # noqa: WPS338
    """
    Represents a calculation which has failed.

    It should contain an error code or message.
    """

    __slots__ = ()

    _inner_value: _ErrorType_co

    def __init__(self, inner_value: _ErrorType_co) -> None:
        """Failure constructor."""
        super().__init__(inner_value)
        object.__setattr__(self, '_trace', self._get_trace())

    if not TYPE_CHECKING:  # noqa: WPS604  # pragma: no branch

        def alt(self, function):
            """Composes failed container with a pure function to modify failure."""  # noqa: E501
            return Failure(function(self._inner_value))

        def map(self, function):
            """Does nothing for ``Failure``."""
            return self

        def bind(self, function):
            """Does nothing for ``Failure``."""
            return self

        #: Alias for `bind` method. Part of the `ResultBasedN` interface.
        bind_result = bind

        def lash(self, function):
            """Composes this container with a function returning container."""
            return function(self._inner_value)

        def apply(self, container):
            """Does nothing for ``Failure``."""
            return self

        def value_or(self, default_value):
            """Returns default value for failed container."""
            return default_value

    def swap(self):
        """Failures swap to :class:`Success`."""
        return Success(self._inner_value)

    def unwrap(self) -> Never:
        """Raises an exception, since it does not have a value inside."""
        if isinstance(self._inner_value, Exception):
            raise UnwrapFailedError(self) from self._inner_value
        raise UnwrapFailedError(self)

    def failure(self) -> _ErrorType_co:
        """Returns failed value."""
        return self._inner_value

    def _get_trace(self) -> list[FrameInfo] | None:
        """Method that will be monkey patched when trace is active."""


@final
class Success(Result[_ValueType_co, Any]):
    """
    Represents a calculation which has succeeded and contains the result.

    Contains the computation value.
    """

    __slots__ = ()

    _inner_value: _ValueType_co

    def __init__(self, inner_value: _ValueType_co) -> None:
        """Success constructor."""
        super().__init__(inner_value)

    if not TYPE_CHECKING:  # noqa: WPS604  # pragma: no branch

        def alt(self, function):
            """Does nothing for ``Success``."""
            return self

        def map(self, function):
            """Composes current container with a pure function."""
            return Success(function(self._inner_value))

        def bind(self, function):
            """Binds current container to a function that returns container."""
            return function(self._inner_value)

        #: Alias for `bind` method. Part of the `ResultBasedN` interface.
        bind_result = bind

        def lash(self, function):
            """Does nothing for ``Success``."""
            return self

        def apply(self, container):
            """Calls a wrapped function in a container on this container."""
            if isinstance(container, Success):
                return self.map(container.unwrap())
            return container

        def value_or(self, default_value):
            """Returns the value for successful container."""
            return self._inner_value

    def swap(self):
        """Successes swap to :class:`Failure`."""
        return Failure(self._inner_value)

    def unwrap(self) -> _ValueType_co:
        """Returns the unwrapped value from successful container."""
        return self._inner_value

    def failure(self) -> Never:
        """Raises an exception for successful container."""
        raise UnwrapFailedError(self)


# Aliases:

#: Alias for ``Result[_ValueType_co, Exception]``.
ResultE: TypeAlias = Result[_ValueType_co, Exception]


# Decorators:

_ExceptionType = TypeVar('_ExceptionType', bound=Exception)


@overload
def safe(
    function: Callable[_FuncParams, _ValueType_co],
    /,
) -> Callable[_FuncParams, ResultE[_ValueType_co]]: ...


@overload
def safe(
    exceptions: tuple[type[_ExceptionType], ...],
) -> Callable[
    [Callable[_FuncParams, _ValueType_co]],
    Callable[_FuncParams, Result[_ValueType_co, _ExceptionType]],
]: ...


def safe(  # noqa: WPS234
    exceptions: (
        Callable[_FuncParams, _ValueType_co] | tuple[type[_ExceptionType], ...]
    ),
) -> (
    Callable[_FuncParams, ResultE[_ValueType_co]]
    | Callable[
        [Callable[_FuncParams, _ValueType_co]],
        Callable[_FuncParams, Result[_ValueType_co, _ExceptionType]],
    ]
):
    """
    Decorator to convert exception-throwing function to ``Result`` container.

    Should be used with care, since it only catches ``Exception`` subclasses.
    It does not catch ``BaseException`` subclasses.

    If you need to mark ``async`` function as ``safe``,
    use :func:`returns.future.future_safe` instead.
    This decorator only works with sync functions. Example:

    .. code:: python

      >>> from returns.result import Failure, Success, safe

      >>> @safe
      ... def might_raise(arg: int) -> float:
      ...     return 1 / arg

      >>> assert might_raise(1) == Success(1.0)
      >>> assert isinstance(might_raise(0), Failure)

    You can also use it with explicit exception types as the first argument:

    .. code:: python

      >>> from returns.result import Failure, Success, safe

      >>> @safe(exceptions=(ZeroDivisionError,))
      ... def might_raise(arg: int) -> float:
      ...     return 1 / arg

      >>> assert might_raise(1) == Success(1.0)
      >>> assert isinstance(might_raise(0), Failure)

    In this case, only exceptions that are explicitly
    listed are going to be caught.

    Similar to :func:`returns.io.impure_safe`
    and :func:`returns.future.future_safe` decorators.
    """

    def factory(
        inner_function: Callable[_FuncParams, _ValueType_co],
        inner_exceptions: tuple[type[_ExceptionType], ...],
    ) -> Callable[_FuncParams, Result[_ValueType_co, _ExceptionType]]:
        @wraps(inner_function)
        def decorator(
            *args: _FuncParams.args,
            **kwargs: _FuncParams.kwargs,
        ) -> Result[_ValueType_co, _ExceptionType]:
            try:
                return Success(inner_function(*args, **kwargs))
            except inner_exceptions as exc:
                return Failure(exc)

        return decorator

    if isinstance(exceptions, tuple):
        return lambda function: factory(function, exceptions)
    return factory(
        exceptions,
        (Exception,),  # type: ignore[arg-type]
    )


def attempt(
    func: Callable[[_FirstType], _NewValueType],
) -> Callable[[_FirstType], Result[_NewValueType, _FirstType]]:
    """
    Decorator to convert exception-throwing function to ``Result`` container.

    It's very similar with :func:`returns.result.safe`, the difference is when
    an exception is raised it won't wrap that given exception into a Failure,
    it'll wrap the argument that lead to the exception.

    .. code:: python

        >>> import json
        >>> from typing import Dict, Any

        >>> from returns.result import Failure, Success, attempt

        >>> @attempt
        ... def parse_json(string: str) -> Dict[str, Any]:
        ...     return json.loads(string)

        >>> assert parse_json('{"key": "value"}') == Success({'key': 'value'})
        >>> assert parse_json('incorrect input') == Failure('incorrect input')

    """

    @wraps(func)
    def decorator(arg: _FirstType) -> Result[_NewValueType, _FirstType]:
        try:
            return Success(func(arg))
        except Exception:
            return Failure(arg)

    return decorator
