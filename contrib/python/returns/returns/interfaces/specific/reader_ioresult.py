from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, ClassVar, TypeVar, final

from returns.interfaces.specific import ioresult, reader, reader_result
from returns.primitives.hkt import KindN
from returns.primitives.laws import (
    Law,
    Law2,
    Lawful,
    LawSpecDef,
    law_definition,
)

if TYPE_CHECKING:
    from returns.context import ReaderIOResult  # noqa: WPS433
    from returns.io import IOResult  # noqa: F401, WPS433

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_ValueType = TypeVar('_ValueType')
_ErrorType = TypeVar('_ErrorType')
_EnvType = TypeVar('_EnvType')

_ReaderIOResultLikeType = TypeVar(
    '_ReaderIOResultLikeType',
    bound='ReaderIOResultLikeN',
)


class ReaderIOResultLikeN(
    reader_result.ReaderResultLikeN[_FirstType, _SecondType, _ThirdType],
    ioresult.IOResultLikeN[_FirstType, _SecondType, _ThirdType],
):
    """
    Base interface for all types that do look like ``ReaderIOResult`` instance.

    Cannot be called.
    """

    __slots__ = ()

    @abstractmethod
    def bind_context_ioresult(
        self: _ReaderIOResultLikeType,
        function: Callable[
            [_FirstType],
            ReaderIOResult[_UpdatedType, _SecondType, _ThirdType],
        ],
    ) -> KindN[_ReaderIOResultLikeType, _UpdatedType, _SecondType, _ThirdType]:
        """Binds a ``ReaderIOResult`` returning function over a container."""

    @classmethod
    @abstractmethod
    def from_ioresult_context(
        cls: type[_ReaderIOResultLikeType],
        inner_value: ReaderIOResult[_ValueType, _ErrorType, _EnvType],
    ) -> KindN[_ReaderIOResultLikeType, _ValueType, _ErrorType, _EnvType]:
        """Unit method to create new containers from ``ReaderIOResult``."""


#: Type alias for kinds with three type arguments.
ReaderIOResultLike3 = ReaderIOResultLikeN[_FirstType, _SecondType, _ThirdType]


@final
class _LawSpec(LawSpecDef):
    """
    Concrete laws for ``ReaderIOResultBasedN``.

    See: https://github.com/haskell/mtl/pull/61/files
    """

    __slots__ = ()

    @law_definition
    def asking_law(
        container: ReaderIOResultBasedN[_FirstType, _SecondType, _ThirdType],
        env: _ThirdType,
    ) -> None:
        """Asking for an env, always returns the env."""
        assert container.ask().__call__(  # noqa: PLC2801
            env,
        ) == container.from_value(env).__call__(env)  # noqa: PLC2801


class ReaderIOResultBasedN(
    ReaderIOResultLikeN[_FirstType, _SecondType, _ThirdType],
    reader.CallableReader3[
        _FirstType,
        _SecondType,
        _ThirdType,
        # Calls:
        'IOResult[_FirstType, _SecondType]',
        _ThirdType,
    ],
    Lawful['ReaderIOResultBasedN[_FirstType, _SecondType, _ThirdType]'],
):
    """
    This interface is very specific to our ``ReaderIOResult`` type.

    The only thing that differs from ``ReaderIOResultLikeN`` is that we know
    the specific types for its ``__call__`` method.

    In this case the return type of ``__call__`` is ``IOResult``.
    """

    __slots__ = ()

    _laws: ClassVar[Sequence[Law]] = (Law2(_LawSpec.asking_law),)


#: Type alias for kinds with three type arguments.
ReaderIOResultBased3 = ReaderIOResultBasedN[_FirstType, _SecondType, _ThirdType]
