from abc import abstractmethod
from collections.abc import Sequence
from typing import ClassVar, TypeVar, final

from returns.primitives.laws import (
    Law,
    Law1,
    Law2,
    Law3,
    Lawful,
    LawSpecDef,
    law_definition,
)

_EqualType = TypeVar('_EqualType', bound='Equable')


@final
class _LawSpec(LawSpecDef):
    """
    Equality laws.

    Description: https://bit.ly/34D40iT
    """

    __slots__ = ()

    @law_definition
    def reflexive_law(
        first: _EqualType,
    ) -> None:
        """Value should be equal to itself."""
        assert first.equals(first)

    @law_definition
    def symmetry_law(
        first: _EqualType,
        second: _EqualType,
    ) -> None:
        """If ``A == B`` then ``B == A``."""
        assert first.equals(second) == second.equals(first)

    @law_definition
    def transitivity_law(
        first: _EqualType,
        second: _EqualType,
        third: _EqualType,
    ) -> None:
        """If ``A == B`` and ``B == C`` then ``A == C``."""
        # We use this notation, because `first` might be equal to `third`,
        # but not to `second`. Example: Some(1), Some(2), Some(1)
        if first.equals(second) and second.equals(third):
            assert first.equals(third)


class Equable(Lawful['Equable']):
    """
    Interface for types that can be compared with real values.

    Not all types can, because some don't have the value at a time:
    - ``Future`` has to be awaited to get the value
    - ``Reader`` has to be called to get the value

    """

    __slots__ = ()

    _laws: ClassVar[Sequence[Law]] = (
        Law1(_LawSpec.reflexive_law),
        Law2(_LawSpec.symmetry_law),
        Law3(_LawSpec.transitivity_law),
    )

    @abstractmethod
    def equals(self: _EqualType, other: _EqualType) -> bool:
        """Type-safe equality check for values of the same type."""
