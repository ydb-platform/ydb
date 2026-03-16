from abc import abstractmethod
from collections.abc import Callable, Sequence
from typing import ClassVar, Generic, TypeVar, final

from typing_extensions import Never

from returns.functions import compose, identity
from returns.primitives.asserts import assert_equal
from returns.primitives.hkt import KindN
from returns.primitives.laws import (
    Law,
    Law1,
    Law3,
    Lawful,
    LawSpecDef,
    law_definition,
)

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_AltableType = TypeVar('_AltableType', bound='AltableN')

# Used in laws:
_NewType1 = TypeVar('_NewType1')
_NewType2 = TypeVar('_NewType2')


@final
class _LawSpec(LawSpecDef):
    """
    Mappable or functor laws.

    https://en.wikibooks.org/wiki/Haskell/The_Functor_class#The_functor_laws
    """

    __slots__ = ()

    @law_definition
    def identity_law(
        altable: 'AltableN[_FirstType, _SecondType, _ThirdType]',
    ) -> None:
        """Mapping identity over a value must return the value unchanged."""
        assert_equal(altable.alt(identity), altable)

    @law_definition
    def associative_law(
        altable: 'AltableN[_FirstType, _SecondType, _ThirdType]',
        first: Callable[[_SecondType], _NewType1],
        second: Callable[[_NewType1], _NewType2],
    ) -> None:
        """Mapping twice or mapping a composition is the same thing."""
        assert_equal(
            altable.alt(first).alt(second),
            altable.alt(compose(first, second)),
        )


class AltableN(
    Lawful['AltableN[_FirstType, _SecondType, _ThirdType]'],
    Generic[_FirstType, _SecondType, _ThirdType],
):
    """Modifies the second type argument with a pure function."""

    __slots__ = ()

    _laws: ClassVar[Sequence[Law]] = (
        Law1(_LawSpec.identity_law),
        Law3(_LawSpec.associative_law),
    )

    @abstractmethod
    def alt(
        self: _AltableType,
        function: Callable[[_SecondType], _UpdatedType],
    ) -> KindN[_AltableType, _FirstType, _UpdatedType, _ThirdType]:
        """Allows to run a pure function over a container."""


#: Type alias for kinds with two type arguments.
Altable2 = AltableN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
Altable3 = AltableN[_FirstType, _SecondType, _ThirdType]
