from abc import ABC
from collections.abc import Callable, Generator, Iterator
from functools import wraps
from inspect import FrameInfo
from typing import TYPE_CHECKING, Any, TypeAlias, TypeVar, final, overload

from typing_extensions import ParamSpec

from returns.interfaces.specific import io, ioresult
from returns.primitives.container import BaseContainer, container_equality
from returns.primitives.exceptions import UnwrapFailedError
from returns.primitives.hkt import (
    Kind1,
    Kind2,
    SupportsKind1,
    SupportsKind2,
    dekind,
)
from returns.result import Failure, Result, Success

_ValueType_co = TypeVar('_ValueType_co', covariant=True)
_NewValueType = TypeVar('_NewValueType')

_FuncParams = ParamSpec('_FuncParams')

# Result related:
_ErrorType_co = TypeVar('_ErrorType_co', covariant=True)
_NewErrorType = TypeVar('_NewErrorType')


class IO(  # type: ignore[type-var]
    BaseContainer,
    SupportsKind1['IO', _ValueType_co],
    io.IOLike1[_ValueType_co],
):
    """
    Explicit container for impure function results.

    We also sometimes call it "marker" since once it is marked,
    it cannot be ever unmarked.
    There's no way to directly get its internal value.

    Note that ``IO`` represents a computation that never fails.

    Examples of such computations are:

    - read / write to localStorage
    - get the current time
    - write to the console
    - get a random number

    Use ``IOResult[...]`` for operations that might fail.
    Like DB access or network operations.

    See also:
        - https://dev.to/gcanti/getting-started-with-fp-ts-io-36p6
        - https://gist.github.com/chris-taylor/4745921

    """

    __slots__ = ()

    _inner_value: _ValueType_co

    #: Typesafe equality comparison with other `Result` objects.
    equals = container_equality

    def __init__(self, inner_value: _ValueType_co) -> None:
        """
        Public constructor for this type. Also required for typing.

        .. code:: python

          >>> from returns.io import IO
          >>> assert str(IO(1)) == '<IO: 1>'

        """
        super().__init__(inner_value)

    def map(
        self,
        function: Callable[[_ValueType_co], _NewValueType],
    ) -> 'IO[_NewValueType]':
        """
        Applies function to the inner value.

        Applies 'function' to the contents of the IO instance
        and returns a new IO object containing the result.
        'function' should accept a single "normal" (non-container) argument
        and return a non-container result.

        .. code:: python

          >>> def mappable(string: str) -> str:
          ...      return string + 'b'

          >>> assert IO('a').map(mappable) == IO('ab')

        """
        return IO(function(self._inner_value))

    def apply(
        self,
        container: Kind1['IO', Callable[[_ValueType_co], _NewValueType]],
    ) -> 'IO[_NewValueType]':
        """
        Calls a wrapped function in a container on this container.

        .. code:: python

          >>> from returns.io import IO
          >>> assert IO('a').apply(IO(lambda inner: inner + 'b')) == IO('ab')

        Or more complex example that shows how we can work
        with regular functions and multiple ``IO`` arguments:

        .. code:: python

          >>> from returns.curry import curry

          >>> @curry
          ... def appliable(first: str, second: str) -> str:
          ...      return first + second

          >>> assert IO('b').apply(IO('a').apply(IO(appliable))) == IO('ab')

        """
        return self.map(dekind(container)._inner_value)  # noqa: SLF001

    def bind(
        self,
        function: Callable[[_ValueType_co], Kind1['IO', _NewValueType]],
    ) -> 'IO[_NewValueType]':
        """
        Applies 'function' to the result of a previous calculation.

        'function' should accept a single "normal" (non-container) argument
        and return ``IO`` type object.

        .. code:: python

          >>> def bindable(string: str) -> IO[str]:
          ...      return IO(string + 'b')

          >>> assert IO('a').bind(bindable) == IO('ab')

        """
        return dekind(function(self._inner_value))

    #: Alias for `bind` method. Part of the `IOLikeN` interface.
    bind_io = bind

    def __iter__(self) -> Iterator[_ValueType_co]:
        """API for :ref:`do-notation`."""
        yield self._inner_value

    @classmethod
    def do(
        cls,
        expr: Generator[_NewValueType, None, None],
    ) -> 'IO[_NewValueType]':
        """
        Allows working with unwrapped values of containers in a safe way.

        .. code:: python

          >>> from returns.io import IO
          >>> assert IO.do(
          ...     first + second
          ...     for first in IO(2)
          ...     for second in IO(3)
          ... ) == IO(5)

        See :ref:`do-notation` to learn more.

        """
        return IO(next(expr))

    @classmethod
    def from_value(cls, inner_value: _NewValueType) -> 'IO[_NewValueType]':
        """
        Unit function to construct new ``IO`` values.

        Is the same as regular constructor:

        .. code:: python

          >>> from returns.io import IO
          >>> assert IO(1) == IO.from_value(1)

        Part of the :class:`returns.interfaces.applicative.ApplicativeN`
        interface.
        """
        return IO(inner_value)

    @classmethod
    def from_io(cls, inner_value: 'IO[_NewValueType]') -> 'IO[_NewValueType]':
        """
        Unit function to construct new ``IO`` values from existing ``IO``.

        .. code:: python

          >>> from returns.io import IO
          >>> assert IO(1) == IO.from_io(IO(1))

        Part of the :class:`returns.interfaces.specific.IO.IOLikeN` interface.

        """
        return inner_value

    @classmethod
    def from_ioresult(
        cls,
        inner_value: 'IOResult[_NewValueType, _NewErrorType]',
    ) -> 'IO[Result[_NewValueType, _NewErrorType]]':
        """
        Converts ``IOResult[a, b]`` back to ``IO[Result[a, b]]``.

        Can be really helpful for composition.

        .. code:: python

            >>> from returns.io import IO, IOSuccess
            >>> from returns.result import Success
            >>> assert IO.from_ioresult(IOSuccess(1)) == IO(Success(1))

        Is the reverse of :meth:`returns.io.IOResult.from_typecast`.
        """
        return IO(inner_value._inner_value)  # noqa: SLF001


# Helper functions:


def impure(
    function: Callable[_FuncParams, _NewValueType],
) -> Callable[_FuncParams, IO[_NewValueType]]:
    """
    Decorator to mark function that it returns :class:`~IO` container.

    If you need to mark ``async`` function as impure,
    use :func:`returns.future.future` instead.
    This decorator only works with sync functions. Example:

    .. code:: python

      >>> from returns.io import IO, impure

      >>> @impure
      ... def function(arg: int) -> int:
      ...     return arg + 1  # this action is pure, just an example
      ...

      >>> assert function(1) == IO(2)

    """

    @wraps(function)
    def decorator(
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> IO[_NewValueType]:
        return IO(function(*args, **kwargs))

    return decorator


# IO and Result:


class IOResult(  # type: ignore[type-var]
    BaseContainer,
    SupportsKind2['IOResult', _ValueType_co, _ErrorType_co],
    ioresult.IOResultBased2[_ValueType_co, _ErrorType_co],
    ABC,
):
    """
    Explicit container for impure function results that might fail.

    .. rubric:: Definition

    This type is similar to :class:`returns.result.Result`.
    This basically a more useful version of ``IO[Result[a, b]]``.
    Use this type for ``IO`` computations that might fail.
    Examples of ``IO`` computations that might fail are:

    - access database
    - access network
    - access filesystem

    Use :class:`~IO` for operations that do ``IO`` but do not fail.

    Note, that even methods like :meth:`~IOResult.unwrap``
    and :meth:`~IOResult.value_or` return values wrapped in ``IO``.

    ``IOResult`` is a complex compound value that consists of:

    - raw value
    - ``Result``
    - ``IO``

    This is why it has so many helper and factory methods:

    - You can construct ``IOResult`` from raw values
      with :func:`~IOSuccess` and :func:`~IOFailure` public type constructors
    - You can construct ``IOResult`` from ``IO`` values
      with :meth:`~IOResult.from_failed_io`
      and :meth:`IOResult.from_io`
    - You can construct ``IOResult`` from ``Result`` values
      with :meth:`~IOResult.from_result`

    We also have a lot of utility methods for better function composition like:

    - :meth:`~IOResult.bind_result` to work
      with functions which return ``Result``
    - :meth:`~IOResult.from_typecast` to work with ``IO[Result[...]]`` values

    See also:
        https://github.com/gcanti/fp-ts/blob/master/docs/modules/IOEither.ts.md

    .. rubric:: Implementation

    This class contains all the methods that can be delegated to ``Result``.
    But, some methods are not implemented which means
    that we have to use special :class:`~_IOSuccess` and :class:`~_IOFailure`
    implementation details to correctly handle these callbacks.

    Do not rely on them! Use public functions and types instead.

    """

    __slots__ = ()

    _inner_value: Result[_ValueType_co, _ErrorType_co]
    __match_args__ = ('_inner_value',)

    #: Typesafe equality comparison with other `IOResult` objects.
    equals = container_equality

    def __init__(
        self, inner_value: Result[_ValueType_co, _ErrorType_co]
    ) -> None:
        """
        Private type constructor.

        Use :func:`~IOSuccess` and :func:`~IOFailure` instead.
        Or :meth:`~IOResult.from_result` factory.
        """
        super().__init__(inner_value)

    def __repr__(self) -> str:
        """
        Custom ``str`` representation for better readability.

        .. code:: python

          >>> from returns.io import IOSuccess, IOFailure
          >>> assert str(IOSuccess(1)) == '<IOResult: <Success: 1>>'
          >>> assert repr(IOSuccess(1)) == '<IOResult: <Success: 1>>'
          >>> str(IOFailure(ValueError('wrong!')))
          '<IOResult: <Failure: wrong!>>'

        """
        return f'<IOResult: {self._inner_value}>'

    @property
    def trace(self) -> list[FrameInfo] | None:
        """Returns a stack trace when :func:`~IOFailure` was called."""
        return self._inner_value.trace

    def swap(self) -> 'IOResult[_ErrorType_co, _ValueType_co]':
        """
        Swaps value and error types.

        So, values become errors and errors become values.
        It is useful when you have to work with errors a lot.
        And since we have a lot of ``.bind_`` related methods
        and only a single ``.lash`` - it is easier to work with values.

        .. code:: python

          >>> from returns.io import IOSuccess, IOFailure
          >>> assert IOSuccess(1).swap() == IOFailure(1)
          >>> assert IOFailure(1).swap() == IOSuccess(1)

        """
        return self.from_result(self._inner_value.swap())

    def map(
        self,
        function: Callable[[_ValueType_co], _NewValueType],
    ) -> 'IOResult[_NewValueType, _ErrorType_co]':
        """
        Composes successful container with a pure function.

        .. code:: python

          >>> from returns.io import IOSuccess
          >>> assert IOSuccess(1).map(lambda num: num + 1) == IOSuccess(2)

        """
        return self.from_result(self._inner_value.map(function))

    def apply(
        self,
        container: Kind2[
            'IOResult',
            Callable[[_ValueType_co], _NewValueType],
            _ErrorType_co,
        ],
    ) -> 'IOResult[_NewValueType, _ErrorType_co]':
        """
        Calls a wrapped function in a container on this container.

        .. code:: python

          >>> from returns.io import IOSuccess, IOFailure

          >>> def appliable(first: str) -> str:
          ...      return first + 'b'

          >>> assert IOSuccess('a').apply(
          ...     IOSuccess(appliable),
          ... ) == IOSuccess('ab')
          >>> assert IOFailure('a').apply(
          ...     IOSuccess(appliable),
          ... ) == IOFailure('a')

          >>> assert IOSuccess('a').apply(IOFailure(1)) == IOFailure(1)
          >>> assert IOFailure('a').apply(IOFailure('b')) == IOFailure('a')

        """
        if isinstance(self, IOFailure):
            return self
        if isinstance(container, IOSuccess):
            return self.from_result(
                self._inner_value.map(
                    container.unwrap()._inner_value,  # noqa: SLF001
                ),
            )
        return container  # type: ignore

    def bind(
        self,
        function: Callable[
            [_ValueType_co],
            Kind2['IOResult', _NewValueType, _ErrorType_co],
        ],
    ) -> 'IOResult[_NewValueType, _ErrorType_co]':
        """
        Composes successful container with a function that returns a container.

        .. code:: python

          >>> from returns.io import IOResult, IOFailure, IOSuccess
          >>> def bindable(string: str) -> IOResult[str, str]:
          ...      if len(string) > 1:
          ...          return IOSuccess(string + 'b')
          ...      return IOFailure(string + 'c')

          >>> assert IOSuccess('aa').bind(bindable) == IOSuccess('aab')
          >>> assert IOSuccess('a').bind(bindable) == IOFailure('ac')
          >>> assert IOFailure('a').bind(bindable) == IOFailure('a')

        """

    #: Alias for `bind_ioresult` method. Part of the `IOResultBasedN` interface.
    bind_ioresult = bind

    def bind_result(
        self,
        function: Callable[
            [_ValueType_co],
            Result[_NewValueType, _ErrorType_co],
        ],
    ) -> 'IOResult[_NewValueType, _ErrorType_co]':
        """
        Composes successful container with a function that returns a container.

        Similar to :meth:`~IOResult.bind`, but works with containers
        that return :class:`returns.result.Result`
        instead of :class:`~IOResult`.

        .. code:: python

          >>> from returns.io import IOFailure, IOSuccess
          >>> from returns.result import Result, Success

          >>> def bindable(string: str) -> Result[str, str]:
          ...      if len(string) > 1:
          ...          return Success(string + 'b')
          ...      return Failure(string + 'c')

          >>> assert IOSuccess('aa').bind_result(bindable) == IOSuccess('aab')
          >>> assert IOSuccess('a').bind_result(bindable) == IOFailure('ac')
          >>> assert IOFailure('a').bind_result(bindable) == IOFailure('a')

        """

    def bind_io(
        self,
        function: Callable[[_ValueType_co], IO[_NewValueType]],
    ) -> 'IOResult[_NewValueType, _ErrorType_co]':
        """
        Composes successful container with a function that returns a container.

        Similar to :meth:`~IOResult.bind`, but works with containers
        that return :class:`returns.io.IO` instead of :class:`~IOResult`.

        .. code:: python

          >>> from returns.io import IO, IOFailure, IOSuccess

          >>> def bindable(string: str) -> IO[str]:
          ...      return IO(string + 'z')

          >>> assert IOSuccess('a').bind_io(bindable) == IOSuccess('az')
          >>> assert IOFailure('a').bind_io(bindable) == IOFailure('a')

        """

    def alt(
        self,
        function: Callable[[_ErrorType_co], _NewErrorType],
    ) -> 'IOResult[_ValueType_co, _NewErrorType]':
        """
        Composes failed container with a pure function to modify failure.

        .. code:: python

          >>> from returns.io import IOFailure
          >>> assert IOFailure(1).alt(float) == IOFailure(1.0)

        """
        return self.from_result(self._inner_value.alt(function))

    def lash(
        self,
        function: Callable[
            [_ErrorType_co],
            Kind2['IOResult', _ValueType_co, _NewErrorType],
        ],
    ) -> 'IOResult[_ValueType_co, _NewErrorType]':
        """
        Composes failed container with a function that returns a container.

        .. code:: python

          >>> from returns.io import IOFailure, IOSuccess, IOResult
          >>> def lashable(state: str) -> IOResult[int, str]:
          ...     if len(state) > 1:
          ...         return IOSuccess(len(state))
          ...     return IOFailure('oops')

          >>> assert IOFailure('a').lash(lashable) == IOFailure('oops')
          >>> assert IOFailure('abc').lash(lashable) == IOSuccess(3)
          >>> assert IOSuccess('a').lash(lashable) == IOSuccess('a')

        """

    def value_or(
        self,
        default_value: _NewValueType,
    ) -> IO[_ValueType_co | _NewValueType]:
        """
        Get value from successful container or default value from failed one.

        .. code:: python

          >>> from returns.io import IO, IOFailure, IOSuccess
          >>> assert IOSuccess(1).value_or(None) == IO(1)
          >>> assert IOFailure(1).value_or(None) == IO(None)

        """
        return IO(self._inner_value.value_or(default_value))

    def unwrap(self) -> IO[_ValueType_co]:
        """
        Get value from successful container or raise exception for failed one.

        .. code:: pycon
          :force:

          >>> from returns.io import IO, IOFailure, IOSuccess
          >>> assert IOSuccess(1).unwrap() == IO(1)

          >>> IOFailure(1).unwrap()
          Traceback (most recent call last):
            ...
          returns.primitives.exceptions.UnwrapFailedError

        """
        return IO(self._inner_value.unwrap())

    def failure(self) -> IO[_ErrorType_co]:
        """
        Get failed value from failed container or raise exception from success.

        .. code:: pycon
          :force:

          >>> from returns.io import IO, IOFailure, IOSuccess
          >>> assert IOFailure(1).failure() == IO(1)

          >>> IOSuccess(1).failure()
          Traceback (most recent call last):
            ...
          returns.primitives.exceptions.UnwrapFailedError

        """
        return IO(self._inner_value.failure())

    def compose_result(
        self,
        function: Callable[
            [Result[_ValueType_co, _ErrorType_co]],
            Kind2['IOResult', _NewValueType, _ErrorType_co],
        ],
    ) -> 'IOResult[_NewValueType, _ErrorType_co]':
        """
        Composes inner ``Result`` with ``IOResult`` returning function.

        Can be useful when you need an access to both states of the result.

        .. code:: python

          >>> from returns.io import IOResult, IOSuccess, IOFailure
          >>> from returns.result import Result

          >>> def count(container: Result[int, int]) -> IOResult[int, int]:
          ...     return IOResult.from_result(
          ...         container.map(lambda x: x + 1).alt(abs),
          ...     )

          >>> assert IOSuccess(1).compose_result(count) == IOSuccess(2)
          >>> assert IOFailure(-1).compose_result(count) == IOFailure(1)

        """
        return dekind(function(self._inner_value))

    def __iter__(self) -> Iterator[_ValueType_co]:
        """API for :ref:`do-notation`."""
        # We also unwrap `IO` here.
        yield self.unwrap()._inner_value  # noqa: SLF001

    @classmethod
    def do(
        cls,
        expr: Generator[_NewValueType, None, None],
    ) -> 'IOResult[_NewValueType, _NewErrorType]':
        """
        Allows working with unwrapped values of containers in a safe way.

        .. code:: python

          >>> from returns.io import IOResult, IOFailure, IOSuccess

          >>> assert IOResult.do(
          ...     first + second
          ...     for first in IOSuccess(2)
          ...     for second in IOSuccess(3)
          ... ) == IOSuccess(5)

          >>> assert IOResult.do(
          ...     first + second
          ...     for first in IOFailure('a')
          ...     for second in IOSuccess(3)
          ... ) == IOFailure('a')

        See :ref:`do-notation` to learn more.
        This feature requires our :ref:`mypy plugin <mypy-plugins>`.

        """
        try:
            return IOResult.from_value(next(expr))
        except UnwrapFailedError as exc:
            return IOResult.from_result(exc.halted_container)  # type: ignore

    @classmethod
    def from_typecast(
        cls,
        inner_value: IO[Result[_NewValueType, _NewErrorType]],
    ) -> 'IOResult[_NewValueType, _NewErrorType]':
        """
        Converts ``IO[Result[_ValueType_co, _ErrorType_co]]`` to ``IOResult``.

        Also prevails the type of ``Result`` to ``IOResult``, so:
        ``IO[Result[_ValueType_co, _ErrorType_co]]`` would become
        ``IOResult[_ValueType_co, _ErrorType_co]``.

        .. code:: python

          >>> from returns.result import Success
          >>> from returns.io import IO, IOResult, IOSuccess
          >>> container = IO(Success(1))
          >>> assert IOResult.from_typecast(container) == IOSuccess(1)

        Can be reverted via :meth:`returns.io.IO.from_ioresult` method.
        """
        return cls.from_result(inner_value._inner_value)  # noqa: SLF001

    @classmethod
    def from_failed_io(
        cls,
        inner_value: IO[_NewErrorType],
    ) -> 'IOResult[Any, _NewErrorType]':
        """
        Creates new ``IOResult`` from "failed" ``IO`` container.

        .. code:: python

          >>> from returns.io import IO, IOResult, IOFailure
          >>> container = IO(1)
          >>> assert IOResult.from_failed_io(container) == IOFailure(1)

        """
        return IOFailure(inner_value._inner_value)  # noqa: SLF001

    @classmethod
    def from_io(
        cls,
        inner_value: IO[_NewValueType],
    ) -> 'IOResult[_NewValueType, Any]':
        """
        Creates new ``IOResult`` from "successful" ``IO`` container.

        .. code:: python

          >>> from returns.io import IO, IOResult, IOSuccess
          >>> container = IO(1)
          >>> assert IOResult.from_io(container) == IOSuccess(1)

        """
        return IOSuccess(inner_value._inner_value)  # noqa: SLF001

    @classmethod
    def from_result(
        cls,
        inner_value: Result[_NewValueType, _NewErrorType],
    ) -> 'IOResult[_NewValueType, _NewErrorType]':
        """
        Creates ``IOResult`` from ``Result`` value.

        .. code:: python

          >>> from returns.io import IOResult, IOSuccess, IOFailure
          >>> from returns.result import Success, Failure

          >>> assert IOResult.from_result(Success(1)) == IOSuccess(1)
          >>> assert IOResult.from_result(Failure(2)) == IOFailure(2)

        """
        if isinstance(inner_value, Success):
            return IOSuccess(inner_value._inner_value)  # noqa: SLF001
        return IOFailure(inner_value._inner_value)  # type: ignore[arg-type]  # noqa: SLF001

    @classmethod
    def from_ioresult(
        cls,
        inner_value: 'IOResult[_NewValueType, _NewErrorType]',
    ) -> 'IOResult[_NewValueType, _NewErrorType]':
        """
        Creates ``IOResult`` from existing ``IOResult`` value.

        .. code:: python

          >>> from returns.io import IOResult, IOSuccess, IOFailure

          >>> assert IOResult.from_ioresult(IOSuccess(1)) == IOSuccess(1)
          >>> assert IOResult.from_ioresult(IOFailure(2)) == IOFailure(2)

        """
        return inner_value

    @classmethod
    def from_value(
        cls,
        inner_value: _NewValueType,
    ) -> 'IOResult[_NewValueType, Any]':
        """
        One more value to create success unit values.

        It is useful as a united way to create a new value from any container.

        .. code:: python

          >>> from returns.io import IOResult, IOSuccess
          >>> assert IOResult.from_value(1) == IOSuccess(1)

        You can use this method or :func:`~IOSuccess`,
        choose the most convenient for you.

        """
        return IOSuccess(inner_value)

    @classmethod
    def from_failure(
        cls,
        inner_value: _NewErrorType,
    ) -> 'IOResult[Any, _NewErrorType]':
        """
        One more value to create failure unit values.

        It is useful as a united way to create a new value from any container.

        .. code:: python

          >>> from returns.io import IOResult, IOFailure
          >>> assert IOResult.from_failure(1) == IOFailure(1)

        You can use this method or :func:`~IOFailure`,
        choose the most convenient for you.

        """
        return IOFailure(inner_value)


@final
class IOFailure(IOResult[Any, _ErrorType_co]):
    """``IOFailure`` representation."""

    __slots__ = ()

    _inner_value: Result[Any, _ErrorType_co]

    def __init__(self, inner_value: _ErrorType_co) -> None:
        """IOFailure constructor."""
        super().__init__(Failure(inner_value))

    if not TYPE_CHECKING:  # noqa: WPS604  # pragma: no branch

        def bind(self, function):
            """Does nothing for ``IOFailure``."""
            return self

        #: Alias for `bind_ioresult` method. Part of the `IOResultBasedN` interface.  # noqa: E501
        bind_ioresult = bind

        def bind_result(self, function):
            """Does nothing for ``IOFailure``."""
            return self

        def bind_io(self, function):
            """Does nothing for ``IOFailure``."""
            return self

        def lash(self, function):
            """Composes this container with a function returning ``IOResult``."""  # noqa: E501
            return function(self._inner_value.failure())


@final
class IOSuccess(IOResult[_ValueType_co, Any]):
    """``IOSuccess`` representation."""

    __slots__ = ()

    _inner_value: Result[_ValueType_co, Any]

    def __init__(self, inner_value: _ValueType_co) -> None:
        """IOSuccess constructor."""
        super().__init__(Success(inner_value))

    if not TYPE_CHECKING:  # noqa: WPS604  # pragma: no branch

        def bind(self, function):
            """Composes this container with a function returning ``IOResult``."""  # noqa: E501
            return function(self._inner_value.unwrap())

        #: Alias for `bind_ioresult` method. Part of the `IOResultBasedN` interface.  # noqa: E501
        bind_ioresult = bind

        def bind_result(self, function):
            """Binds ``Result`` returning function to current container."""
            return self.from_result(function(self._inner_value.unwrap()))

        def bind_io(self, function):
            """Binds ``IO`` returning function to current container."""
            return self.from_io(function(self._inner_value.unwrap()))

        def lash(self, function):
            """Does nothing for ``IOSuccess``."""
            return self


# Aliases:


#: Alias for ``IOResult[_ValueType_co, Exception]``.
IOResultE: TypeAlias = IOResult[_ValueType_co, Exception]


# impure_safe decorator:

_ExceptionType = TypeVar('_ExceptionType', bound=Exception)


@overload
def impure_safe(
    function: Callable[_FuncParams, _NewValueType],
    /,
) -> Callable[_FuncParams, IOResultE[_NewValueType]]: ...


@overload
def impure_safe(
    exceptions: tuple[type[_ExceptionType], ...],
) -> Callable[
    [Callable[_FuncParams, _NewValueType]],
    Callable[_FuncParams, IOResult[_NewValueType, _ExceptionType]],
]: ...


def impure_safe(  # noqa: WPS234
    exceptions: (
        Callable[_FuncParams, _NewValueType] | tuple[type[_ExceptionType], ...]
    ),
) -> (
    Callable[_FuncParams, IOResultE[_NewValueType]]
    | Callable[
        [Callable[_FuncParams, _NewValueType]],
        Callable[_FuncParams, IOResult[_NewValueType, _ExceptionType]],
    ]
):
    """
    Decorator to mark function that it returns :class:`~IOResult` container.

    Should be used with care, since it only catches ``Exception`` subclasses.
    It does not catch ``BaseException`` subclasses.

    If you need to mark ``async`` function as impure,
    use :func:`returns.future.future_safe` instead.
    This decorator only works with sync functions. Example:

    .. code:: python

      >>> from returns.io import IOSuccess, impure_safe

      >>> @impure_safe
      ... def function(arg: int) -> float:
      ...     return 1 / arg
      ...

      >>> assert function(1) == IOSuccess(1.0)
      >>> assert function(0).failure()

    You can also use it with explicit exception types as the first argument:

    .. code:: python

      >>> from returns.io import IOSuccess, IOFailure, impure_safe

      >>> @impure_safe(exceptions=(ZeroDivisionError,))
      ... def might_raise(arg: int) -> float:
      ...     return 1 / arg

      >>> assert might_raise(1) == IOSuccess(1.0)
      >>> assert isinstance(might_raise(0), IOFailure)

    In this case, only exceptions that are explicitly
    listed are going to be caught.

    Similar to :func:`returns.future.future_safe`
    and :func:`returns.result.safe` decorators.
    """

    def factory(
        inner_function: Callable[_FuncParams, _NewValueType],
        inner_exceptions: tuple[type[_ExceptionType], ...],
    ) -> Callable[_FuncParams, IOResult[_NewValueType, _ExceptionType]]:
        @wraps(inner_function)
        def decorator(
            *args: _FuncParams.args,
            **kwargs: _FuncParams.kwargs,
        ) -> IOResult[_NewValueType, _ExceptionType]:
            try:
                return IOSuccess(inner_function(*args, **kwargs))
            except inner_exceptions as exc:
                return IOFailure(exc)

        return decorator

    if isinstance(exceptions, tuple):
        return lambda function: factory(function, exceptions)
    return factory(
        exceptions,
        (Exception,),  # type: ignore[arg-type]
    )
