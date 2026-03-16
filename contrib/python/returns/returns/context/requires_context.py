from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, TypeVar, final

from returns.functions import identity
from returns.future import FutureResult
from returns.interfaces.specific import reader
from returns.io import IOResult
from returns.primitives.container import BaseContainer
from returns.primitives.hkt import Kind2, SupportsKind2, dekind
from returns.result import Result

if TYPE_CHECKING:
    # We need this condition to make sure Python can solve cycle imports.
    # But, since we only use these values in types, it is not important.
    from returns.context.requires_context_future_result import (
        RequiresContextFutureResult,
    )
    from returns.context.requires_context_ioresult import (
        RequiresContextIOResult,
    )
    from returns.context.requires_context_result import RequiresContextResult

# Context:
_EnvType_contra = TypeVar('_EnvType_contra', contravariant=True)
_NewEnvType = TypeVar('_NewEnvType')
_ReturnType_co = TypeVar('_ReturnType_co', covariant=True)
_NewReturnType = TypeVar('_NewReturnType')

_ValueType = TypeVar('_ValueType')
_ErrorType = TypeVar('_ErrorType')

# Helpers:
_FirstType = TypeVar('_FirstType')

# Type Aliases:
#: Sometimes ``RequiresContext`` and other similar types might be used with
#: no explicit dependencies so we need to have this type alias for Any.
NoDeps = Any


@final
class RequiresContext(  # type: ignore[type-var]
    BaseContainer,
    SupportsKind2['RequiresContext', _ReturnType_co, _EnvType_contra],
    reader.ReaderBased2[_ReturnType_co, _EnvType_contra],
):
    """
    The ``RequiresContext`` container.

    It's main purpose is to wrap some specific function
    and to provide tools to compose other functions around it
    without actually calling it.

    The ``RequiresContext`` container passes the state
    you want to share between functions.
    Functions may read that state, but can't change it.
    The ``RequiresContext`` container
    lets us access shared immutable state within a specific context.

    It can be used for lazy evaluation and typed dependency injection.

    ``RequiresContext`` is used with functions that never fail.
    If you want to use ``RequiresContext`` with returns ``Result``
    then consider using ``RequiresContextResult`` instead.

    Note:
        This container does not wrap ANY value. It wraps only functions.
        You won't be able to supply arbitrary types!

    See also:
        - https://dev.to/gcanti/getting-started-with-fp-ts-reader-1ie5
        - https://en.wikipedia.org/wiki/Lazy_evaluation
        - https://bit.ly/2R8l4WK

    """

    __slots__ = ()

    #: This field has an extra 'RequiresContext' just because `mypy` needs it.
    _inner_value: Callable[[_EnvType_contra], _ReturnType_co]

    #: A convenient placeholder to call methods created by `.from_value()`:
    no_args: ClassVar[NoDeps] = object()

    def __init__(
        self,
        inner_value: Callable[[_EnvType_contra], _ReturnType_co],
    ) -> None:
        """
        Public constructor for this type. Also required for typing.

        Only allows functions of kind ``* -> *``.

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> str(RequiresContext(lambda deps: deps + 1))
          '<RequiresContext: <function <lambda> at ...>>'

        """
        super().__init__(inner_value)

    def __call__(self, deps: _EnvType_contra) -> _ReturnType_co:
        """
        Evaluates the wrapped function.

        .. code:: python

          >>> from returns.context import RequiresContext

          >>> def first(lg: bool) -> RequiresContext[int, float]:
          ...     # `deps` has `float` type here:
          ...     return RequiresContext(
          ...         lambda deps: deps if lg else -deps,
          ...     )

          >>> instance = first(False)  # creating `RequiresContext` instance
          >>> assert instance(3.5) == -3.5 # calling it with `__call__`

          >>> # Example with another logic:
          >>> assert first(True)(3.5) == 3.5

        In other things, it is a regular python magic method.
        """
        return self._inner_value(deps)

    def map(
        self,
        function: Callable[[_ReturnType_co], _NewReturnType],
    ) -> RequiresContext[_NewReturnType, _EnvType_contra]:
        """
        Allows to compose functions inside the wrapped container.

        Here's how it works:

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> def first(lg: bool) -> RequiresContext[int, float]:
          ...     # `deps` has `float` type here:
          ...     return RequiresContext(
          ...         lambda deps: deps if lg else -deps,
          ...     )

          >>> assert first(True).map(lambda number: number * 10)(2.5) == 25.0
          >>> assert first(False).map(lambda number: number * 10)(0.1) -1.0

        """
        return RequiresContext(lambda deps: function(self(deps)))

    def apply(
        self,
        container: Kind2[
            RequiresContext,
            Callable[[_ReturnType_co], _NewReturnType],
            _EnvType_contra,
        ],
    ) -> RequiresContext[_NewReturnType, _EnvType_contra]:
        """
        Calls a wrapped function in a container on this container.

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> assert RequiresContext.from_value('a').apply(
          ...    RequiresContext.from_value(lambda inner: inner + 'b')
          ... )(...) == 'ab'

        """
        return RequiresContext(
            lambda deps: self.map(dekind(container)(deps))(deps),
        )

    def bind(
        self,
        function: Callable[
            [_ReturnType_co],
            Kind2[RequiresContext, _NewReturnType, _EnvType_contra],
        ],
    ) -> RequiresContext[_NewReturnType, _EnvType_contra]:
        """
        Composes a container with a function returning another container.

        This is useful when you do several computations that rely on the
        same context.

        .. code:: python

          >>> from returns.context import RequiresContext

          >>> def first(lg: bool) -> RequiresContext[int, float]:
          ...     # `deps` has `float` type here:
          ...     return RequiresContext(
          ...         lambda deps: deps if lg else -deps,
          ...     )

          >>> def second(number: int) -> RequiresContext[str, float]:
          ...     # `deps` has `float` type here:
          ...     return RequiresContext(
          ...         lambda deps: '>=' if number >= deps else '<',
          ...     )

          >>> assert first(True).bind(second)(1) == '>='
          >>> assert first(False).bind(second)(2) == '<'

        """
        return RequiresContext(lambda deps: dekind(function(self(deps)))(deps))

    #: Alias for `bind_context` method, it is the same as `bind` here.
    bind_context = bind

    def modify_env(
        self,
        function: Callable[[_NewEnvType], _EnvType_contra],
    ) -> RequiresContext[_ReturnType_co, _NewEnvType]:
        """
        Allows to modify the environment type.

        .. code:: python

          >>> from returns.context import RequiresContext

          >>> def mul(arg: int) -> RequiresContext[float, int]:
          ...     return RequiresContext(lambda deps: arg * deps)

          >>> assert mul(3).modify_env(int)('2') == 6

        """
        return RequiresContext(lambda deps: self(function(deps)))

    @classmethod
    def ask(cls) -> RequiresContext[_EnvType_contra, _EnvType_contra]:
        """
        Get current context to use the dependencies.

        It is a common scenario when you need to use the environment.
        For example, you want to do some context-related computation,
        but you don't have the context instance at your disposal.
        That's where ``.ask()`` becomes useful!

        .. code:: python

          >>> from typing_extensions import TypedDict
          >>> class Deps(TypedDict):
          ...     message: str

          >>> def first(lg: bool) -> RequiresContext[int, Deps]:
          ...     # `deps` has `Deps` type here:
          ...     return RequiresContext(
          ...         lambda deps: deps['message'] if lg else 'error',
          ...     )

          >>> def second(text: str) -> RequiresContext[int, Deps]:
          ...     return first(len(text) > 3)

          >>> assert second('abc')({'message': 'ok'}) == 'error'
          >>> assert second('abcd')({'message': 'ok'}) == 'ok'

        And now imagine that you have to change this ``3`` limit.
        And you want to be able to set it via environment as well.
        Ok, let's fix it with the power of ``RequiresContext.ask()``!

        .. code:: python

          >>> from typing_extensions import TypedDict
          >>> class Deps(TypedDict):
          ...     message: str
          ...     limit: int   # note this new field!

          >>> def new_first(lg: bool) -> RequiresContext[int, Deps]:
          ...     # `deps` has `Deps` type here:
          ...     return RequiresContext(
          ...         lambda deps: deps['message'] if lg else 'err',
          ...     )

          >>> def new_second(text: str) -> RequiresContext[int, Deps]:
          ...     return RequiresContext[int, Deps].ask().bind(
          ...         lambda deps: new_first(len(text) > deps.get('limit', 3)),
          ...     )

          >>> assert new_second('abc')({'message': 'ok', 'limit': 2}) == 'ok'
          >>> assert new_second('abcd')({'message': 'ok'}) == 'ok'
          >>> assert new_second('abcd')({'message': 'ok', 'limit': 5}) == 'err'

        That's how ``ask`` works.

        This class contains methods that require
        to explicitly set type annotations. Why?
        Because it is impossible to figure out the type without them.
        So, here's how you should use them:

        .. code:: python

            RequiresContext[int, Dict[str, str]].ask()

        Otherwise, your ``.ask()`` method
        will return ``RequiresContext[Never, Never]``,
        which is unusable:

        .. code:: python

            env = RequiresContext.ask()
            env(some_deps)

        And ``mypy`` will warn you: ``error: Need type annotation for '...'``

        See also:
            - https://dev.to/gcanti/getting-started-with-fp-ts-reader-1ie5

        """
        return RequiresContext(identity)

    @classmethod
    def from_value(
        cls,
        inner_value: _FirstType,
    ) -> RequiresContext[_FirstType, NoDeps]:
        """
        Used to return some specific value from the container.

        Consider this method as some kind of factory.
        Passed value will be a return type.
        Make sure to use :attr:`~RequiresContext.no_args`
        for getting the unit value.

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> unit = RequiresContext.from_value(5)
          >>> assert unit(RequiresContext.no_args) == 5

        Might be used with or without direct type hint.
        """
        return RequiresContext(lambda _: inner_value)

    @classmethod
    def from_context(
        cls,
        inner_value: RequiresContext[_NewReturnType, _NewEnvType],
    ) -> RequiresContext[_NewReturnType, _NewEnvType]:
        """
        Used to create new containers from existing ones.

        Used as a part of ``ReaderBased2`` interface.

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> unit = RequiresContext.from_value(5)
          >>> assert RequiresContext.from_context(unit)(...) == unit(...)

        """
        return inner_value

    @classmethod
    def from_requires_context_result(
        cls,
        inner_value: RequiresContextResult[
            _ValueType, _ErrorType, _EnvType_contra
        ],
    ) -> RequiresContext[Result[_ValueType, _ErrorType], _EnvType_contra]:
        """
        Typecasts ``RequiresContextResult`` to ``RequiresContext`` instance.

        Breaks ``RequiresContextResult[a, b, e]``
        into ``RequiresContext[Result[a, b], e]``.

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> from returns.context import RequiresContextResult
          >>> from returns.result import Success
          >>> assert RequiresContext.from_requires_context_result(
          ...    RequiresContextResult.from_value(1),
          ... )(...) == Success(1)

        Can be reverted with ``RequiresContextResult.from_typecast``.

        """
        return RequiresContext(inner_value)

    @classmethod
    def from_requires_context_ioresult(
        cls,
        inner_value: RequiresContextIOResult[
            _ValueType, _ErrorType, _EnvType_contra
        ],
    ) -> RequiresContext[IOResult[_ValueType, _ErrorType], _EnvType_contra]:
        """
        Typecasts ``RequiresContextIOResult`` to ``RequiresContext`` instance.

        Breaks ``RequiresContextIOResult[a, b, e]``
        into ``RequiresContext[IOResult[a, b], e]``.

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess
          >>> assert RequiresContext.from_requires_context_ioresult(
          ...    RequiresContextIOResult.from_value(1),
          ... )(...) == IOSuccess(1)

        Can be reverted with ``RequiresContextIOResult.from_typecast``.

        """
        return RequiresContext(inner_value)

    @classmethod
    def from_requires_context_future_result(
        cls,
        inner_value: RequiresContextFutureResult[
            _ValueType,
            _ErrorType,
            _EnvType_contra,
        ],
    ) -> RequiresContext[FutureResult[_ValueType, _ErrorType], _EnvType_contra]:
        """
        Typecasts ``RequiresContextIOResult`` to ``RequiresContext`` instance.

        Breaks ``RequiresContextIOResult[a, b, e]``
        into ``RequiresContext[IOResult[a, b], e]``.

        .. code:: python

          >>> import anyio
          >>> from returns.context import RequiresContext
          >>> from returns.context import RequiresContextFutureResult
          >>> from returns.io import IOSuccess

          >>> container = RequiresContext.from_requires_context_future_result(
          ...    RequiresContextFutureResult.from_value(1),
          ... )
          >>> assert anyio.run(
          ...     container, RequiresContext.no_args,
          ... ) == IOSuccess(1)

        Can be reverted with ``RequiresContextFutureResult.from_typecast``.

        """
        return RequiresContext(inner_value)


# Aliases

#: Sometimes `RequiresContext` is too long to type.
Reader: TypeAlias = RequiresContext
