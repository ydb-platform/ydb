from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, TypeVar, final

from returns.context import NoDeps
from returns.interfaces.specific import reader_ioresult
from returns.io import IO, IOFailure, IOResult, IOSuccess
from returns.primitives.container import BaseContainer
from returns.primitives.hkt import Kind3, SupportsKind3, dekind
from returns.result import Result

if TYPE_CHECKING:
    from returns.context.requires_context import RequiresContext
    from returns.context.requires_context_result import RequiresContextResult

# Context:
_EnvType_contra = TypeVar('_EnvType_contra', contravariant=True)
_NewEnvType = TypeVar('_NewEnvType')

# Result:
_ValueType_co = TypeVar('_ValueType_co', covariant=True)
_NewValueType = TypeVar('_NewValueType')
_ErrorType = TypeVar('_ErrorType')
_NewErrorType = TypeVar('_NewErrorType')


@final
class RequiresContextIOResult(  # type: ignore[type-var]
    BaseContainer,
    SupportsKind3[
        'RequiresContextIOResult', _ValueType_co, _ErrorType, _EnvType_contra
    ],
    reader_ioresult.ReaderIOResultBasedN[
        _ValueType_co, _ErrorType, _EnvType_contra
    ],
):
    """
    The ``RequiresContextIOResult`` combinator.

    See :class:`returns.context.requires_context.RequiresContext`
    and :class:`returns.context.requires_context_result.RequiresContextResult`
    for more docs.

    This is just a handy wrapper around
    ``RequiresContext[IOResult[a, b], env]``
    which represents a context-dependent impure operation that might fail.

    It has several important differences from the regular ``Result`` classes.
    It does not have ``Success`` and ``Failure`` subclasses.
    Because, the computation is not yet performed.
    And we cannot know the type in advance.

    So, this is a thin wrapper, without any changes in logic.

    Why do we need this wrapper? That's just for better usability!

    .. code:: python

      >>> from returns.context import RequiresContext
      >>> from returns.io import IOSuccess, IOResult

      >>> def function(arg: int) -> IOResult[int, str]:
      ...      return IOSuccess(arg + 1)

      >>> # Without wrapper:
      >>> assert RequiresContext.from_value(IOSuccess(1)).map(
      ...     lambda ioresult: ioresult.bind(function),
      ... )(...) == IOSuccess(2)

      >>> # With wrapper:
      >>> assert RequiresContextIOResult.from_value(1).bind_ioresult(
      ...     function,
      ... )(...) == IOSuccess(2)

    This way ``RequiresContextIOResult`` allows to simply work with:

    - raw values and pure functions
    - ``RequiresContext`` values and pure functions returning it
    - ``RequiresContextResult`` values and pure functions returning it
    - ``Result`` and pure functions returning it
    - ``IOResult`` and functions returning it
    - other ``RequiresContextIOResult`` related functions and values

    This is a complex type for complex tasks!

    .. rubric:: Important implementation details

    Due it is meaning, ``RequiresContextIOResult``
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
    #: is just a function that returns `IOResult`.
    #: This field has an extra 'RequiresContext' just because `mypy` needs it.
    _inner_value: Callable[
        [_EnvType_contra], IOResult[_ValueType_co, _ErrorType]
    ]

    #: A convenient placeholder to call methods created by `.from_value()`.
    no_args: ClassVar[NoDeps] = object()

    def __init__(
        self,
        inner_value: Callable[
            [_EnvType_contra], IOResult[_ValueType_co, _ErrorType]
        ],
    ) -> None:
        """
        Public constructor for this type. Also required for typing.

        Only allows functions of kind ``* -> *``
        and returning :class:`returns.result.Result` instances.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess
          >>> str(RequiresContextIOResult(lambda deps: IOSuccess(deps + 1)))
          '<RequiresContextIOResult: <function <lambda> at ...>>'

        """
        super().__init__(inner_value)

    def __call__(
        self, deps: _EnvType_contra
    ) -> IOResult[_ValueType_co, _ErrorType]:
        """
        Evaluates the wrapped function.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess

          >>> def first(lg: bool) -> RequiresContextIOResult[int, str, float]:
          ...     # `deps` has `float` type here:
          ...     return RequiresContextIOResult(
          ...         lambda deps: IOSuccess(deps if lg else -deps),
          ...     )

          >>> instance = first(False)
          >>> assert instance(3.5) == IOSuccess(-3.5)

        In other things, it is a regular Python magic method.

        """
        return self._inner_value(deps)

    def swap(
        self,
    ) -> RequiresContextIOResult[_ErrorType, _ValueType_co, _EnvType_contra]:
        """
        Swaps value and error types.

        So, values become errors and errors become values.
        It is useful when you have to work with errors a lot.
        And since we have a lot of ``.bind_`` related methods
        and only a single ``.lash`` - it is easier to work with values.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> success = RequiresContextIOResult.from_value(1)
          >>> failure = RequiresContextIOResult.from_failure(1)

          >>> assert success.swap()(...) == IOFailure(1)
          >>> assert failure.swap()(...) == IOSuccess(1)

        """
        return RequiresContextIOResult(lambda deps: self(deps).swap())

    def map(
        self,
        function: Callable[[_ValueType_co], _NewValueType],
    ) -> RequiresContextIOResult[_NewValueType, _ErrorType, _EnvType_contra]:
        """
        Composes successful container with a pure function.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert RequiresContextIOResult.from_value(1).map(
          ...     lambda x: x + 1,
          ... )(...) == IOSuccess(2)

          >>> assert RequiresContextIOResult.from_failure(1).map(
          ...     lambda x: x + 1,
          ... )(...) == IOFailure(1)

        """
        return RequiresContextIOResult(lambda deps: self(deps).map(function))

    def apply(
        self,
        container: Kind3[
            RequiresContextIOResult,
            Callable[[_ValueType_co], _NewValueType],
            _ErrorType,
            _EnvType_contra,
        ],
    ) -> RequiresContextIOResult[_NewValueType, _ErrorType, _EnvType_contra]:
        """
        Calls a wrapped function in a container on this container.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> def transform(arg: str) -> str:
          ...     return arg + 'b'

          >>> assert RequiresContextIOResult.from_value('a').apply(
          ...    RequiresContextIOResult.from_value(transform),
          ... )(...) == IOSuccess('ab')

          >>> assert RequiresContextIOResult.from_value('a').apply(
          ...    RequiresContextIOResult.from_failure(1),
          ... )(...) == IOFailure(1)

          >>> assert RequiresContextIOResult.from_failure('a').apply(
          ...    RequiresContextIOResult.from_value(transform),
          ... )(...) == IOFailure('a')

          >>> assert RequiresContextIOResult.from_failure('a').apply(
          ...    RequiresContextIOResult.from_failure('b'),
          ... )(...) == IOFailure('a')

        """
        return RequiresContextIOResult(
            lambda deps: self(deps).apply(dekind(container)(deps)),
        )

    def bind(
        self,
        function: Callable[
            [_ValueType_co],
            Kind3[
                RequiresContextIOResult,
                _NewValueType,
                _ErrorType,
                _EnvType_contra,
            ],
        ],
    ) -> RequiresContextIOResult[_NewValueType, _ErrorType, _EnvType_contra]:
        """
        Composes this container with a function returning the same type.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> def first(lg: bool) -> RequiresContextIOResult[int, int, float]:
          ...     # `deps` has `float` type here:
          ...     return RequiresContextIOResult(
          ...         lambda deps: IOSuccess(deps) if lg else IOFailure(-deps),
          ...     )

          >>> def second(
          ...     number: int,
          ... ) -> RequiresContextIOResult[str, int, float]:
          ...     # `deps` has `float` type here:
          ...     return RequiresContextIOResult(
          ...         lambda deps: IOSuccess('>=' if number >= deps else '<'),
          ...     )

          >>> assert first(True).bind(second)(1) == IOSuccess('>=')
          >>> assert first(False).bind(second)(2) == IOFailure(-2)

        """
        return RequiresContextIOResult(
            lambda deps: self(deps).bind(
                lambda inner: dekind(  # type: ignore[misc]
                    function(inner),
                )(deps),
            ),
        )

    #: Alias for `bind_context_ioresult` method, it is the same as `bind` here.
    bind_context_ioresult = bind

    def bind_result(
        self,
        function: Callable[[_ValueType_co], Result[_NewValueType, _ErrorType]],
    ) -> RequiresContextIOResult[_NewValueType, _ErrorType, _EnvType_contra]:
        """
        Binds ``Result`` returning function to the current container.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.result import Failure, Result, Success
          >>> from returns.io import IOSuccess, IOFailure

          >>> def function(num: int) -> Result[int, str]:
          ...     return Success(num + 1) if num > 0 else Failure('<0')

          >>> assert RequiresContextIOResult.from_value(1).bind_result(
          ...     function,
          ... )(RequiresContextIOResult.no_args) == IOSuccess(2)

          >>> assert RequiresContextIOResult.from_value(0).bind_result(
          ...     function,
          ... )(RequiresContextIOResult.no_args) == IOFailure('<0')

          >>> assert RequiresContextIOResult.from_failure(':(').bind_result(
          ...     function,
          ... )(RequiresContextIOResult.no_args) == IOFailure(':(')

        """
        return RequiresContextIOResult(
            lambda deps: self(deps).bind_result(function),
        )

    def bind_context(
        self,
        function: Callable[
            [_ValueType_co],
            RequiresContext[_NewValueType, _EnvType_contra],
        ],
    ) -> RequiresContextIOResult[_NewValueType, _ErrorType, _EnvType_contra]:
        """
        Binds ``RequiresContext`` returning function to current container.

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> from returns.io import IOSuccess, IOFailure

          >>> def function(arg: int) -> RequiresContext[int, str]:
          ...     return RequiresContext(lambda deps: len(deps) + arg)

          >>> assert function(2)('abc') == 5

          >>> assert RequiresContextIOResult.from_value(2).bind_context(
          ...     function,
          ... )('abc') == IOSuccess(5)

          >>> assert RequiresContextIOResult.from_failure(2).bind_context(
          ...     function,
          ... )('abc') == IOFailure(2)

        """
        return RequiresContextIOResult(
            lambda deps: self(deps).map(
                lambda inner: function(inner)(deps),  # type: ignore[misc]
            ),
        )

    def bind_context_result(
        self,
        function: Callable[
            [_ValueType_co],
            RequiresContextResult[_NewValueType, _ErrorType, _EnvType_contra],
        ],
    ) -> RequiresContextIOResult[_NewValueType, _ErrorType, _EnvType_contra]:
        """
        Binds ``RequiresContextResult`` returning function to the current one.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.io import IOSuccess, IOFailure
          >>> from returns.result import Success, Failure

          >>> def function(arg: int) -> RequiresContextResult[int, int, str]:
          ...     if arg > 0:
          ...         return RequiresContextResult(
          ...             lambda deps: Success(len(deps) + arg),
          ...         )
          ...     return RequiresContextResult(
          ...         lambda deps: Failure(len(deps) + arg),
          ...     )

          >>> assert function(2)('abc') == Success(5)
          >>> assert function(-1)('abc') == Failure(2)

          >>> assert RequiresContextIOResult.from_value(
          ...    2,
          ... ).bind_context_result(
          ...     function,
          ... )('abc') == IOSuccess(5)

          >>> assert RequiresContextIOResult.from_value(
          ...    -1,
          ... ).bind_context_result(
          ...     function,
          ... )('abc') == IOFailure(2)

          >>> assert RequiresContextIOResult.from_failure(
          ...    2,
          ... ).bind_context_result(
          ...     function,
          ... )('abc') == IOFailure(2)

        """
        return RequiresContextIOResult(
            lambda deps: self(deps).bind_result(
                lambda inner: function(inner)(deps),  # type: ignore[misc]
            ),
        )

    def bind_io(
        self,
        function: Callable[[_ValueType_co], IO[_NewValueType]],
    ) -> RequiresContextIOResult[_NewValueType, _ErrorType, _EnvType_contra]:
        """
        Binds ``IO`` returning function to the current container.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IO, IOSuccess, IOFailure

          >>> def function(number: int) -> IO[str]:
          ...     return IO(str(number))

          >>> assert RequiresContextIOResult.from_value(1).bind_io(
          ...     function,
          ... )(RequiresContextIOResult.no_args) == IOSuccess('1')

          >>> assert RequiresContextIOResult.from_failure(1).bind_io(
          ...     function,
          ... )(RequiresContextIOResult.no_args) == IOFailure(1)

        """
        return RequiresContextIOResult(
            lambda deps: self(deps).bind_io(function),
        )

    def bind_ioresult(
        self,
        function: Callable[
            [_ValueType_co], IOResult[_NewValueType, _ErrorType]
        ],
    ) -> RequiresContextIOResult[_NewValueType, _ErrorType, _EnvType_contra]:
        """
        Binds ``IOResult`` returning function to the current container.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOResult, IOSuccess, IOFailure

          >>> def function(num: int) -> IOResult[int, str]:
          ...     return IOSuccess(num + 1) if num > 0 else IOFailure('<0')

          >>> assert RequiresContextIOResult.from_value(1).bind_ioresult(
          ...     function,
          ... )(RequiresContextIOResult.no_args) == IOSuccess(2)

          >>> assert RequiresContextIOResult.from_value(0).bind_ioresult(
          ...     function,
          ... )(RequiresContextIOResult.no_args) == IOFailure('<0')

          >>> assert RequiresContextIOResult.from_failure(':(').bind_ioresult(
          ...     function,
          ... )(RequiresContextIOResult.no_args) == IOFailure(':(')

        """
        return RequiresContextIOResult(
            lambda deps: self(deps).bind(function),
        )

    def alt(
        self,
        function: Callable[[_ErrorType], _NewErrorType],
    ) -> RequiresContextIOResult[_ValueType_co, _NewErrorType, _EnvType_contra]:
        """
        Composes failed container with a pure function.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert RequiresContextIOResult.from_value(1).alt(
          ...     lambda x: x + 1,
          ... )(...) == IOSuccess(1)

          >>> assert RequiresContextIOResult.from_failure(1).alt(
          ...     lambda x: x + 1,
          ... )(...) == IOFailure(2)

        """
        return RequiresContextIOResult(lambda deps: self(deps).alt(function))

    def lash(
        self,
        function: Callable[
            [_ErrorType],
            Kind3[
                RequiresContextIOResult,
                _ValueType_co,
                _NewErrorType,
                _EnvType_contra,
            ],
        ],
    ) -> RequiresContextIOResult[_ValueType_co, _NewErrorType, _EnvType_contra]:
        """
        Composes this container with a function returning the same type.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> def lashable(
          ...     arg: str,
          ... ) -> RequiresContextIOResult[str, str, str]:
          ...      if len(arg) > 1:
          ...          return RequiresContextIOResult(
          ...              lambda deps: IOSuccess(deps + arg),
          ...          )
          ...      return RequiresContextIOResult(
          ...          lambda deps: IOFailure(arg + deps),
          ...      )

          >>> assert RequiresContextIOResult.from_value('a').lash(
          ...     lashable,
          ... )('c') == IOSuccess('a')
          >>> assert RequiresContextIOResult.from_failure('a').lash(
          ...     lashable,
          ... )('c') == IOFailure('ac')
          >>> assert RequiresContextIOResult.from_failure('aa').lash(
          ...     lashable,
          ... )('b') == IOSuccess('baa')

        """
        return RequiresContextIOResult(
            lambda deps: self(deps).lash(
                lambda inner: function(inner)(deps),  # type: ignore
            ),
        )

    def compose_result(
        self,
        function: Callable[
            [Result[_ValueType_co, _ErrorType]],
            Kind3[
                RequiresContextIOResult,
                _NewValueType,
                _ErrorType,
                _EnvType_contra,
            ],
        ],
    ) -> RequiresContextIOResult[_NewValueType, _ErrorType, _EnvType_contra]:
        """
        Composes inner ``Result`` with ``ReaderIOResult`` returning function.

        Can be useful when you need an access to both states of the result.

        .. code:: python

          >>> from returns.context import ReaderIOResult, NoDeps
          >>> from returns.io import IOSuccess, IOFailure
          >>> from returns.result import Result

          >>> def count(
          ...    container: Result[int, int],
          ... ) -> ReaderIOResult[int, int, NoDeps]:
          ...     return ReaderIOResult.from_result(
          ...         container.map(lambda x: x + 1).alt(abs),
          ...     )

          >>> success = ReaderIOResult.from_value(1)
          >>> failure = ReaderIOResult.from_failure(-1)
          >>> assert success.compose_result(count)(...) == IOSuccess(2)
          >>> assert failure.compose_result(count)(...) == IOFailure(1)

        """
        return RequiresContextIOResult(
            lambda deps: dekind(
                function(self(deps)._inner_value),  # noqa: SLF001
            )(deps),
        )

    def modify_env(
        self,
        function: Callable[[_NewEnvType], _EnvType_contra],
    ) -> RequiresContextIOResult[_ValueType_co, _ErrorType, _NewEnvType]:
        """
        Allows to modify the environment type.

        .. code:: python

          >>> from returns.context import RequiresContextIOResultE
          >>> from returns.io import IOSuccess, impure_safe

          >>> def div(arg: int) -> RequiresContextIOResultE[float, int]:
          ...     return RequiresContextIOResultE(
          ...         impure_safe(lambda deps: arg / deps),
          ...     )

          >>> assert div(3).modify_env(int)('2') == IOSuccess(1.5)
          >>> assert div(3).modify_env(int)('0').failure()

        """
        return RequiresContextIOResult(lambda deps: self(function(deps)))

    @classmethod
    def ask(
        cls,
    ) -> RequiresContextIOResult[_EnvType_contra, _ErrorType, _EnvType_contra]:
        """
        Is used to get the current dependencies inside the call stack.

        Similar to :meth:`returns.context.requires_context.RequiresContext.ask`,
        but returns ``IOResult`` instead of a regular value.

        Please, refer to the docs there to learn how to use it.

        One important note that is worth duplicating here:
        you might need to provide ``_EnvType_contra`` explicitly,
        so ``mypy`` will know about it statically.

        .. code:: python

          >>> from returns.context import RequiresContextIOResultE
          >>> from returns.io import IOSuccess
          >>> assert RequiresContextIOResultE[int, int].ask().map(
          ...     str,
          ... )(1) == IOSuccess('1')

        """
        return RequiresContextIOResult(IOSuccess)

    @classmethod
    def from_result(
        cls,
        inner_value: Result[_NewValueType, _NewErrorType],
    ) -> RequiresContextIOResult[_NewValueType, _NewErrorType, NoDeps]:
        """
        Creates new container with ``Result`` as a unit value.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.result import Success, Failure
          >>> from returns.io import IOSuccess, IOFailure
          >>> deps = RequiresContextIOResult.no_args

          >>> assert RequiresContextIOResult.from_result(
          ...    Success(1),
          ... )(deps) == IOSuccess(1)

          >>> assert RequiresContextIOResult.from_result(
          ...    Failure(1),
          ... )(deps) == IOFailure(1)

        """
        return RequiresContextIOResult(
            lambda _: IOResult.from_result(inner_value),
        )

    @classmethod
    def from_io(
        cls,
        inner_value: IO[_NewValueType],
    ) -> RequiresContextIOResult[_NewValueType, Any, NoDeps]:
        """
        Creates new container from successful ``IO`` value.

        .. code:: python

          >>> from returns.io import IO, IOSuccess
          >>> from returns.context import RequiresContextIOResult

          >>> assert RequiresContextIOResult.from_io(IO(1))(
          ...     RequiresContextIOResult.no_args,
          ... ) == IOSuccess(1)

        """
        return RequiresContextIOResult(
            lambda deps: IOResult.from_io(inner_value),
        )

    @classmethod
    def from_failed_io(
        cls,
        inner_value: IO[_NewErrorType],
    ) -> RequiresContextIOResult[Any, _NewErrorType, NoDeps]:
        """
        Creates a new container from failed ``IO`` value.

        .. code:: python

          >>> from returns.io import IO, IOFailure
          >>> from returns.context import RequiresContextIOResult

          >>> assert RequiresContextIOResult.from_failed_io(IO(1))(
          ...     RequiresContextIOResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextIOResult(
            lambda deps: IOResult.from_failed_io(inner_value),
        )

    @classmethod
    def from_ioresult(
        cls,
        inner_value: IOResult[_NewValueType, _NewErrorType],
    ) -> RequiresContextIOResult[_NewValueType, _NewErrorType, NoDeps]:
        """
        Creates new container with ``IOResult`` as a unit value.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess, IOFailure
          >>> deps = RequiresContextIOResult.no_args

          >>> assert RequiresContextIOResult.from_ioresult(
          ...    IOSuccess(1),
          ... )(deps) == IOSuccess(1)

          >>> assert RequiresContextIOResult.from_ioresult(
          ...    IOFailure(1),
          ... )(deps) == IOFailure(1)

        """
        return RequiresContextIOResult(lambda _: inner_value)

    @classmethod
    def from_ioresult_context(
        cls,
        inner_value: ReaderIOResult[_NewValueType, _NewErrorType, _NewEnvType],
    ) -> ReaderIOResult[_NewValueType, _NewErrorType, _NewEnvType]:
        """
        Creates new container with ``ReaderIOResult`` as a unit value.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert RequiresContextIOResult.from_ioresult_context(
          ...    RequiresContextIOResult.from_value(1),
          ... )(...) == IOSuccess(1)

          >>> assert RequiresContextIOResult.from_ioresult_context(
          ...    RequiresContextIOResult.from_failure(1),
          ... )(...) == IOFailure(1)

        """
        return inner_value

    @classmethod
    def from_typecast(
        cls,
        inner_value: RequiresContext[
            IOResult[_NewValueType, _NewErrorType],
            _EnvType_contra,
        ],
    ) -> RequiresContextIOResult[_NewValueType, _NewErrorType, _EnvType_contra]:
        """
        You might end up with ``RequiresContext[IOResult]`` as a value.

        This method is designed to turn it into ``RequiresContextIOResult``.
        It will save all the typing information.

        It is just more useful!

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert RequiresContextIOResult.from_typecast(
          ...     RequiresContext.from_value(IOSuccess(1)),
          ... )(RequiresContextIOResult.no_args) == IOSuccess(1)

          >>> assert RequiresContextIOResult.from_typecast(
          ...     RequiresContext.from_value(IOFailure(1)),
          ... )(RequiresContextIOResult.no_args) == IOFailure(1)

        """
        return RequiresContextIOResult(inner_value)

    @classmethod
    def from_context(
        cls,
        inner_value: RequiresContext[_NewValueType, _NewEnvType],
    ) -> RequiresContextIOResult[_NewValueType, Any, _NewEnvType]:
        """
        Creates new container from ``RequiresContext`` as a success unit.

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> from returns.io import IOSuccess

          >>> assert RequiresContextIOResult.from_context(
          ...     RequiresContext.from_value(1),
          ... )(...) == IOSuccess(1)

        """
        return RequiresContextIOResult(
            lambda deps: IOSuccess(inner_value(deps)),
        )

    @classmethod
    def from_failed_context(
        cls,
        inner_value: RequiresContext[_NewValueType, _NewEnvType],
    ) -> RequiresContextIOResult[Any, _NewValueType, _NewEnvType]:
        """
        Creates new container from ``RequiresContext`` as a failure unit.

        .. code:: python

          >>> from returns.context import RequiresContext
          >>> from returns.io import IOFailure

          >>> assert RequiresContextIOResult.from_failed_context(
          ...     RequiresContext.from_value(1),
          ... )(...) == IOFailure(1)

        """
        return RequiresContextIOResult(
            lambda deps: IOFailure(inner_value(deps)),
        )

    @classmethod
    def from_result_context(
        cls,
        inner_value: RequiresContextResult[
            _NewValueType,
            _NewErrorType,
            _NewEnvType,
        ],
    ) -> RequiresContextIOResult[_NewValueType, _NewErrorType, _NewEnvType]:
        """
        Creates new container from ``RequiresContextResult`` as a unit value.

        .. code:: python

          >>> from returns.context import RequiresContextResult
          >>> from returns.io import IOSuccess, IOFailure

          >>> assert RequiresContextIOResult.from_result_context(
          ...     RequiresContextResult.from_value(1),
          ... )(...) == IOSuccess(1)

          >>> assert RequiresContextIOResult.from_result_context(
          ...     RequiresContextResult.from_failure(1),
          ... )(...) == IOFailure(1)

        """
        return RequiresContextIOResult(
            lambda deps: IOResult.from_result(inner_value(deps)),
        )

    @classmethod
    def from_value(
        cls,
        inner_value: _NewValueType,
    ) -> RequiresContextIOResult[_NewValueType, Any, NoDeps]:
        """
        Creates new container with ``IOSuccess(inner_value)`` as a unit value.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOSuccess

          >>> assert RequiresContextIOResult.from_value(1)(
          ...    RequiresContextIOResult.no_args,
          ... ) == IOSuccess(1)

        """
        return RequiresContextIOResult(lambda _: IOSuccess(inner_value))

    @classmethod
    def from_failure(
        cls,
        inner_value: _NewErrorType,
    ) -> RequiresContextIOResult[Any, _NewErrorType, NoDeps]:
        """
        Creates new container with ``IOFailure(inner_value)`` as a unit value.

        .. code:: python

          >>> from returns.context import RequiresContextIOResult
          >>> from returns.io import IOFailure

          >>> assert RequiresContextIOResult.from_failure(1)(
          ...     RequiresContextIOResult.no_args,
          ... ) == IOFailure(1)

        """
        return RequiresContextIOResult(lambda _: IOFailure(inner_value))


# Aliases:

#: Alias for a popular case when ``Result`` has ``Exception`` as error type.
RequiresContextIOResultE: TypeAlias = RequiresContextIOResult[
    _ValueType_co,
    Exception,
    _EnvType_contra,
]

#: Alias to save you some typing. Uses original name from Haskell.
ReaderIOResult: TypeAlias = RequiresContextIOResult

#: Alias to save you some typing. Uses ``Exception`` as error type.
ReaderIOResultE: TypeAlias = RequiresContextIOResult[
    _ValueType_co,
    Exception,
    _EnvType_contra,
]
