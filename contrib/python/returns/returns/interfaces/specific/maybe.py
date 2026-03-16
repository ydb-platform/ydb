from abc import abstractmethod
from collections.abc import Callable, Sequence
from typing import ClassVar, TypeVar, final

from typing_extensions import Never

from returns.interfaces import equable, failable, unwrappable
from returns.primitives.asserts import assert_equal
from returns.primitives.hkt import KindN
from returns.primitives.laws import (
    Law,
    Law2,
    Law3,
    Lawful,
    LawSpecDef,
    law_definition,
)

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_MaybeLikeType = TypeVar('_MaybeLikeType', bound='MaybeLikeN')

# New values:
_ValueType = TypeVar('_ValueType')

# Only used in laws:
_NewType1 = TypeVar('_NewType1')


@final
class _LawSpec(LawSpecDef):
    """
    Maybe laws.

    We need to be sure that
    ``.map``, ``.bind``, ``.bind_optional``, and ``.lash``
    works correctly for both successful and failed types.
    """

    __slots__ = ()

    @law_definition
    def map_short_circuit_law(
        container: 'MaybeLikeN[_FirstType, _SecondType, _ThirdType]',
        function: Callable[[_FirstType], _NewType1],
    ) -> None:
        """Ensures that you cannot map from failures."""
        assert_equal(
            container.from_optional(None).map(function),
            container.from_optional(None),
        )

    @law_definition
    def bind_short_circuit_law(
        container: 'MaybeLikeN[_FirstType, _SecondType, _ThirdType]',
        function: Callable[
            [_FirstType],
            KindN['MaybeLikeN', _NewType1, _SecondType, _ThirdType],
        ],
    ) -> None:
        """Ensures that you cannot bind from failures."""
        assert_equal(
            container.from_optional(None).bind(function),
            container.from_optional(None),
        )

    @law_definition
    def bind_optional_short_circuit_law(
        container: 'MaybeLikeN[_FirstType, _SecondType, _ThirdType]',
        function: Callable[[_FirstType], _NewType1 | None],
    ) -> None:
        """Ensures that you cannot bind from failures."""
        assert_equal(
            container.from_optional(None).bind_optional(function),
            container.from_optional(None),
        )

    @law_definition
    def lash_short_circuit_law(
        raw_value: _FirstType,
        container: 'MaybeLikeN[_FirstType, _SecondType, _ThirdType]',
        function: Callable[
            [_SecondType],
            KindN['MaybeLikeN', _FirstType, _NewType1, _ThirdType],
        ],
    ) -> None:
        """Ensures that you cannot lash a success."""
        assert_equal(
            container.from_value(raw_value).lash(function),
            container.from_value(raw_value),
        )

    @law_definition
    def unit_structure_law(
        container: 'MaybeLikeN[_FirstType, _SecondType, _ThirdType]',
        function: Callable[[_FirstType], None],
    ) -> None:
        """Ensures ``None`` is treated specially."""
        assert_equal(
            container.bind_optional(function),
            container.from_optional(None),
        )


class MaybeLikeN(
    failable.SingleFailableN[_FirstType, _SecondType, _ThirdType],
    Lawful['MaybeLikeN[_FirstType, _SecondType, _ThirdType]'],
):
    """
    Type for values that do look like a ``Maybe``.

    For example, ``RequiresContextMaybe`` should be created from this interface.
    Cannot be unwrapped or compared.
    """

    __slots__ = ()

    _laws: ClassVar[Sequence[Law]] = (
        Law2(_LawSpec.map_short_circuit_law),
        Law2(_LawSpec.bind_short_circuit_law),
        Law2(_LawSpec.bind_optional_short_circuit_law),
        Law3(_LawSpec.lash_short_circuit_law),
        Law2(_LawSpec.unit_structure_law),
    )

    @abstractmethod
    def bind_optional(
        self: _MaybeLikeType,
        function: Callable[[_FirstType], _UpdatedType | None],
    ) -> KindN[_MaybeLikeType, _UpdatedType, _SecondType, _ThirdType]:
        """Binds a function that returns ``Optional`` values."""

    @classmethod
    @abstractmethod
    def from_optional(
        cls: type[_MaybeLikeType],
        inner_value: _ValueType | None,
    ) -> KindN[_MaybeLikeType, _ValueType, _SecondType, _ThirdType]:
        """Unit method to create containers from ``Optional`` value."""


#: Type alias for kinds with two type arguments.
MaybeLike2 = MaybeLikeN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
MaybeLike3 = MaybeLikeN[_FirstType, _SecondType, _ThirdType]


class MaybeBasedN(
    MaybeLikeN[_FirstType, _SecondType, _ThirdType],
    unwrappable.Unwrappable[_FirstType, None],
    equable.Equable,
):
    """
    Concrete interface for ``Maybe`` type.

    Can be unwrapped and compared.
    """

    __slots__ = ()

    @abstractmethod
    def or_else_call(
        self,
        function: Callable[[], _ValueType],
    ) -> _FirstType | _ValueType:
        """Calls a function in case there nothing to unwrap."""


#: Type alias for kinds with two type arguments.
MaybeBased2 = MaybeBasedN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
MaybeBased3 = MaybeBasedN[_FirstType, _SecondType, _ThirdType]
