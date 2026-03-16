from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, TypeVar, final

from returns.context import NoDeps
from returns.interfaces.specific import reader_result
from returns.primitives.container import BaseContainer
from returns.primitives.hkt import Kind3, SupportsKind3, dekind
from returns.result import Failure, Result, Success

if TYPE_CHECKING:
    from returns.context.requires_context import RequiresContext

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
class RequiresContextResult(  # type: ignore[type-var]
    BaseContainer,
    SupportsKind3[
        'RequiresContextResult', _ValueType_co, _ErrorType_co, _EnvType_contra
    ],
    reader_result.ReaderResultBasedN[
        _ValueType_co, _ErrorType_co, _EnvType_contra
    ],
):
    """
    The ``RequiresContextResult`` combinator.

    See :class:`returns.context.requires_context.RequiresContext` for more docs.

    This is just a handy wrapper around ``RequiresContext[Result[a, b], env]``
    which represents a context-dependent pure operation
    that might fail and return :class:`returns.result.Result`.

    It has several important differences from the regular ``Result`` classes.
    It does not have ``Success`` and ``Failure`` subclasses.
    Because, the computation is not yet performed.
    And we cannot know the type in advance.

    So, this is a thin wrapper, without any changes in logic.

    Why do we need this wrapper? That's just for better usability!

    .. code:: python

      >>> from returns.context import RequiresContext
      >>> from returns.result import Success, Result

      >>> def function(arg: int) -> Result[int, str]:
      ...      return Success(arg + 1)

      >>> # Without wrapper:
      >>> assert RequiresContext.from_value(Success(1)).map(
      ...     lambda result: result.bind(function),
      ... )(...) == Success(2)

      >>> # With wrapper:
      >>> assert RequiresContextResult.from_value(1).bind_result(
      ...     function,
      ... )(...) == Success(2)

    This way ``RequiresContextResult`` allows to simply work with:

    - raw values and pure functions
    - ``RequiresContext`` values and pure functions returning it
    - ``Result`` and functions returning it

    .. rubric:: Important implementation details

    Due it is meaning, ``RequiresContextResult``
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

    #: This field has an extra 'RequiresContext' just because `mypy` needs it.
    _inner_value: Callable[
        [_EnvType_contra], Result[_ValueType_co, _ErrorType_co]
    ]

    #: A convenient placeholder to call methods created by `.from_value()`.
    no_args: ClassVar[NoDeps] = object()

    def __init__(
        self,
        inner_value: Callable[
            [_EnvType_contra], Result[_ValueType_co, _ErrorType_co]
        ],
    ) -> None:
        """
        Public constructor for this type. Also required for typing.

        Only allows functions of kind ``* -> *``
        and returning :class:`returns.result.Result` instances.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.result import Success
          >>> str(RequiresContextResult(lambda deps: Success(deps + 1)))
          '<RequiresContextResult: <function <lambda> at ...>>'

        """
        super().__init__(inner_value)

    def __call__(
        self, deps: _EnvType_contra
    ) -> Result[_ValueType_co, _ErrorType_co]:
        """
        Evaluates the wrapped function.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.result import Success

          >>> def first(lg: bool) -> RequiresContextResult[int, str, float]:
          ...     # `deps` has `float` type here:
          ...     return RequiresContextResult(
          ...         lambda deps: Success(deps if lg else -deps),
          ...     )

          >>> instance = first(False)
          >>> assert instance(3.5) == Success(-3.5)

        In other things, it is a regular Python magic method.

        """
        return self._inner_value(deps)

    def swap(
        self,
    ) -> RequiresContextResult[_ErrorType_co, _ValueType_co, _EnvType_contra]:
        """
        Swaps value and error types.

        So, values become errors and errors become values.
        It is useful when you have to work with errors a lot.
        And since we have a lot of ``.bind_`` related methods
        and only a single ``.lash`` - it is easier to work with values.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.result import Failure, Success

          >>> success = RequiresContextResult.from_value(1)
          >>> failure = RequiresContextResult.from_failure(1)

          >>> assert success.swap()(...) == Failure(1)
          >>> assert failure.swap()(...) == Success(1)

        """
        return RequiresContextResult(lambda deps: self(deps).swap())

    def map(
        self,
        function: Callable[[_ValueType_co], _NewValueType],
    ) -> RequiresContextResult[_NewValueType, _ErrorType_co, _EnvType_contra]:
        """
        Composes successful container with a pure function.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.result import Success, Failure

          >>> assert RequiresContextResult.from_value(1).map(
          ...     lambda x: x + 1,
          ... )(...) == Success(2)

          >>> assert RequiresContextResult.from_failure(1).map(
          ...     lambda x: x + 1,
          ... )(...) == Failure(1)

        """
        return RequiresContextResult(lambda deps: self(deps).map(function))

    def apply(
        self,
        container: Kind3[
            RequiresContextResult,
            Callable[[_ValueType_co], _NewValueType],
            _ErrorType_co,
            _EnvType_contra,
        ],
    ) -> RequiresContextResult[_NewValueType, _ErrorType_co, _EnvType_contra]:
        """
        Calls a wrapped function in a container on this container.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.result import Failure, Success

          >>> def transform(arg: str) -> str:
          ...     return arg + 'b'

          >>> assert RequiresContextResult.from_value('a').apply(
          ...    RequiresContextResult.from_value(transform),
          ... )(...) == Success('ab')

          >>> assert RequiresContextResult.from_failure('a').apply(
          ...    RequiresContextResult.from_value(transform),
          ... )(...) == Failure('a')

          >>> assert isinstance(RequiresContextResult.from_value('a').apply(
          ...    RequiresContextResult.from_failure(transform),
          ... )(...), Failure) is True

        """
        return RequiresContextResult(
            lambda deps: self(deps).apply(dekind(container)(deps)),
        )

    def bind(
        self,
        function: Callable[
            [_ValueType_co],
            Kind3[
                RequiresContextResult,
                _NewValueType,
                _ErrorType_co,
                _EnvType_contra,
            ],
        ],
    ) -> RequiresContextResult[_NewValueType, _ErrorType_co, _EnvType_contra]:
        """
        Composes this container with a function returning the same type.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.result import Success, Failure

          >>> def first(lg: bool) -> RequiresContextResult[int, int, float]:
          ...     # `deps` has `float` type here:
          ...     return RequiresContextResult(
          ...         lambda deps: Success(deps) if lg else Failure(-deps),
          ...     )

          >>> def second(
          ...     number: int,
          ... ) -> RequiresContextResult[str, int, float]:
          ...     # `deps` has `float` type here:
          ...     return RequiresContextResult(
          ...         lambda deps: Success('>=' if number >= deps else '<'),
          ...     )

          >>> assert first(True).bind(second)(1) == Success('>=')
          >>> assert first(False).bind(second)(2) == Failure(-2)

        """
        return RequiresContextResult(
            lambda deps: self(deps).bind(
                lambda inner: function(inner)(deps),  # type: ignore
            ),
        )

    #: Alias for `bind_context_result` method, it is the same as `bind` here.
    bind_context_result = bind

    def bind_result(
        self,
        function: Callable[
            [_ValueType_co], Result[_NewValueType, _ErrorType_co]
        ],
    ) -> RequiresContextResult[_NewValueType, _ErrorType_co, _EnvType_contra]:
        """
        Binds ``Result`` returning function to current container.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.result import Success, Failure, Result

          >>> def function(num: int) -> Result[str, int]:
          ...     return Success(num + 1) if num > 0 else Failure('<0')

          >>> assert RequiresContextResult.from_value(1).bind_result(
          ...     function,
          ... )(RequiresContextResult.no_args) == Success(2)

          >>> assert RequiresContextResult.from_value(0).bind_result(
          ...     function,
          ... )(RequiresContextResult.no_args) == Failure('<0')

          >>> assert RequiresContextResult.from_failure(':(').bind_result(
          ...     function,
          ... )(RequiresContextResult.no_args) == Failure(':(')

        """
        return RequiresContextResult(lambda deps: self(deps).bind(function))

    def bind_context(
        self,
        function: Callable[
            [_ValueType_co],
            RequiresContext[_NewValueType, _EnvType_contra],
        ],
    ) -> RequiresContextResult[_NewValueType, _ErrorType_co, _EnvType_contra]:
        """
        Binds ``RequiresContext`` returning function to current container.

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> from returns.result import Success, Failure

          >>> def function(arg: int) -> RequiresContext[int, str]:
          ...     return RequiresContext(lambda deps: len(deps) + arg)

          >>> assert function(2)('abc') == 5

          >>> assert RequiresContextResult.from_value(2).bind_context(
          ...     function,
          ... )('abc') == Success(5)

          >>> assert RequiresContextResult.from_failure(2).bind_context(
          ...     function,
          ... )('abc') == Failure(2)

        """
        return RequiresContextResult(
            lambda deps: self(deps).map(
                lambda inner: function(inner)(deps),  # type: ignore[misc]
            ),
        )

    def alt(
        self,
        function: Callable[[_ErrorType_co], _NewErrorType],
    ) -> RequiresContextResult[_ValueType_co, _NewErrorType, _EnvType_contra]:
        """
        Composes failed container with a pure function.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.result import Success, Failure

          >>> assert RequiresContextResult.from_value(1).alt(
          ...     lambda x: x + 1,
          ... )(...) == Success(1)

          >>> assert RequiresContextResult.from_failure(1).alt(
          ...     lambda x: x + 1,
          ... )(...) == Failure(2)

        """
        return RequiresContextResult(lambda deps: self(deps).alt(function))

    def lash(
        self,
        function: Callable[
            [_ErrorType_co],
            Kind3[
                RequiresContextResult,
                _ValueType_co,
                _NewErrorType,
                _EnvType_contra,
            ],
        ],
    ) -> RequiresContextResult[_ValueType_co, _NewErrorType, _EnvType_contra]:
        """
        Composes this container with a function returning the same type.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.result import Success, Failure

          >>> def lashable(arg: str) -> RequiresContextResult[str, str, str]:
          ...      if len(arg) > 1:
          ...          return RequiresContextResult(
          ...              lambda deps: Success(deps + arg),
          ...          )
          ...      return RequiresContextResult(
          ...          lambda deps: Failure(arg + deps),
          ...      )

          >>> assert RequiresContextResult.from_value('a').lash(
          ...     lashable,
          ... )('c') == Success('a')
          >>> assert RequiresContextResult.from_failure('a').lash(
          ...     lashable,
          ... )('c') == Failure('ac')
          >>> assert RequiresContextResult.from_failure('aa').lash(
          ...     lashable,
          ... )('b') == Success('baa')

        """
        return RequiresContextResult(
            lambda deps: self(deps).lash(
                lambda inner: function(inner)(deps),  # type: ignore
            ),
        )

    def modify_env(
        self,
        function: Callable[[_NewEnvType], _EnvType_contra],
    ) -> RequiresContextResult[_ValueType_co, _ErrorType_co, _NewEnvType]:
        """
        Allows to modify the environment type.

        .. code:: python

          >>> from returns.context import RequiresContextResultE
          >>> from returns.result import Success, safe

          >>> def div(arg: int) -> RequiresContextResultE[float, int]:
          ...     return RequiresContextResultE(
          ...         safe(lambda deps: arg / deps),
          ...     )

          >>> assert div(3).modify_env(int)('2') == Success(1.5)
          >>> assert div(3).modify_env(int)('0').failure()

        """
        return RequiresContextResult(lambda deps: self(function(deps)))

    @classmethod
    def ask(
        cls,
    ) -> RequiresContextResult[_EnvType_contra, _ErrorType_co, _EnvType_contra]:
        """
        Is used to get the current dependencies inside the call stack.

        Similar to :meth:`returns.context.requires_context.RequiresContext.ask`,
        but returns ``Result`` instead of a regular value.

        Please, refer to the docs there to learn how to use it.

        One important note that is worth duplicating here:
        you might need to provide ``_EnvType_contra`` explicitly,
        so ``mypy`` will know about it statically.

        .. code:: python

          >>> from returns.context import RequiresContextResultE
          >>> from returns.result import Success
          >>> assert RequiresContextResultE[int, int].ask().map(
          ...    str,
          ... )(1) == Success('1')

        """
        return RequiresContextResult(Success)

    @classmethod
    def from_result(
        cls,
        inner_value: Result[_NewValueType, _NewErrorType],
    ) -> RequiresContextResult[_NewValueType, _NewErrorType, NoDeps]:
        """
        Creates new container with ``Result`` as a unit value.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.result import Success, Failure
          >>> deps = RequiresContextResult.no_args

          >>> assert RequiresContextResult.from_result(
          ...    Success(1),
          ... )(deps) == Success(1)

          >>> assert RequiresContextResult.from_result(
          ...    Failure(1),
          ... )(deps) == Failure(1)

        """
        return RequiresContextResult(lambda _: inner_value)

    @classmethod
    def from_typecast(
        cls,
        inner_value: RequiresContext[
            Result[_NewValueType, _NewErrorType],
            _EnvType_contra,
        ],
    ) -> RequiresContextResult[_NewValueType, _NewErrorType, _EnvType_contra]:
        """
        You might end up with ``RequiresContext[Result[...]]`` as a value.

        This method is designed to turn it into ``RequiresContextResult``.
        It will save all the typing information.

        It is just more useful!

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> from returns.result import Success, Failure

          >>> assert RequiresContextResult.from_typecast(
          ...     RequiresContext.from_value(Success(1)),
          ... )(RequiresContextResult.no_args) == Success(1)

          >>> assert RequiresContextResult.from_typecast(
          ...     RequiresContext.from_value(Failure(1)),
          ... )(RequiresContextResult.no_args) == Failure(1)

        """
        return RequiresContextResult(inner_value)

    @classmethod
    def from_context(
        cls,
        inner_value: RequiresContext[_NewValueType, _NewEnvType],
    ) -> RequiresContextResult[_NewValueType, Any, _NewEnvType]:
        """
        Creates new container from ``RequiresContext`` as a success unit.

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> from returns.result import Success
          >>> assert RequiresContextResult.from_context(
          ...     RequiresContext.from_value(1),
          ... )(...) == Success(1)

        """
        return RequiresContextResult(lambda deps: Success(inner_value(deps)))

    @classmethod
    def from_failed_context(
        cls,
        inner_value: RequiresContext[_NewValueType, _NewEnvType],
    ) -> RequiresContextResult[Any, _NewValueType, _NewEnvType]:
        """
        Creates new container from ``RequiresContext`` as a failure unit.

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> from returns.result import Failure
          >>> assert RequiresContextResult.from_failed_context(
          ...     RequiresContext.from_value(1),
          ... )(...) == Failure(1)

        """
        return RequiresContextResult(lambda deps: Failure(inner_value(deps)))

    @classmethod
    def from_result_context(
        cls,
        inner_value: RequiresContextResult[
            _NewValueType,
            _NewErrorType,
            _NewEnvType,
        ],
    ) -> RequiresContextResult[_NewValueType, _NewErrorType, _NewEnvType]:
        """
        Creates ``RequiresContextResult`` from another instance of it.

        .. code:: python

          >>> from returns.context import ReaderResult
          >>> from returns.result import Success, Failure

          >>> assert ReaderResult.from_result_context(
          ...     ReaderResult.from_value(1),
          ... )(...) == Success(1)

          >>> assert ReaderResult.from_result_context(
          ...     ReaderResult.from_failure(1),
          ... )(...) == Failure(1)

        """
        return inner_value

    @classmethod
    def from_value(
        cls,
        inner_value: _FirstType,
    ) -> RequiresContextResult[_FirstType, Any, NoDeps]:
        """
        Creates new container with ``Success(inner_value)`` as a unit value.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.result import Success
          >>> assert RequiresContextResult.from_value(1)(...) == Success(1)

        """
        return RequiresContextResult(lambda _: Success(inner_value))

    @classmethod
    def from_failure(
        cls,
        inner_value: _FirstType,
    ) -> RequiresContextResult[Any, _FirstType, NoDeps]:
        """
        Creates new container with ``Failure(inner_value)`` as a unit value.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.result import Failure
          >>> assert RequiresContextResult.from_failure(1)(...) == Failure(1)

        """
        return RequiresContextResult(lambda _: Failure(inner_value))


# Aliases:

#: Alias for a popular case when ``Result`` has ``Exception`` as error type.
RequiresContextResultE: TypeAlias = RequiresContextResult[
    _ValueType_co,
    Exception,
    _EnvType_contra,
]

#: Alias to save you some typing. Uses original name from Haskell.
ReaderResult: TypeAlias = RequiresContextResult

#: Alias to save you some typing. Has ``Exception`` as error type.
ReaderResultE: TypeAlias = RequiresContextResult[
    _ValueType_co,
    Exception,
    _EnvType_contra,
]
