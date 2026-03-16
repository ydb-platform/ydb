from abc import abstractmethod
from collections.abc import Callable, Sequence
from typing import ClassVar, TypeVar, final

from typing_extensions import Never

from returns.interfaces import container as _container
from returns.interfaces import lashable, swappable
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

_SingleFailableType = TypeVar('_SingleFailableType', bound='SingleFailableN')
_DiverseFailableType = TypeVar('_DiverseFailableType', bound='DiverseFailableN')

# Used in laws:
_NewFirstType = TypeVar('_NewFirstType')


@final
class _FailableLawSpec(LawSpecDef):
    """
    Failable laws.

    We need to be sure that ``.lash`` won't lash success types.
    """

    __slots__ = ()

    @law_definition
    def lash_short_circuit_law(
        raw_value: _FirstType,
        container: 'FailableN[_FirstType, _SecondType, _ThirdType]',
        function: Callable[
            [_SecondType],
            KindN['FailableN', _FirstType, _NewFirstType, _ThirdType],
        ],
    ) -> None:
        """Ensures that you cannot lash a success."""
        assert_equal(
            container.from_value(raw_value),
            container.from_value(raw_value).lash(function),
        )


class FailableN(
    _container.ContainerN[_FirstType, _SecondType, _ThirdType],
    lashable.LashableN[_FirstType, _SecondType, _ThirdType],
    Lawful['FailableN[_FirstType, _SecondType, _ThirdType]'],
):
    """
    Base type for types that can fail.

    It is a raw type and should not be used directly.
    Use ``SingleFailableN`` and ``DiverseFailableN`` instead.
    """

    __slots__ = ()

    _laws: ClassVar[Sequence[Law]] = (
        Law3(_FailableLawSpec.lash_short_circuit_law),
    )


#: Type alias for kinds with two type arguments.
Failable2 = FailableN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
Failable3 = FailableN[_FirstType, _SecondType, _ThirdType]


@final
class _SingleFailableLawSpec(LawSpecDef):
    """
    Single Failable laws.

    We need to be sure that ``.map`` and ``.bind``
    works correctly for ``empty`` property.
    """

    __slots__ = ()

    @law_definition
    def map_short_circuit_law(
        container: 'SingleFailableN[_FirstType, _SecondType, _ThirdType]',
        function: Callable[[_FirstType], _NewFirstType],
    ) -> None:
        """Ensures that you cannot map from the `empty` property."""
        assert_equal(
            container.empty,
            container.empty.map(function),
        )

    @law_definition
    def bind_short_circuit_law(
        container: 'SingleFailableN[_FirstType, _SecondType, _ThirdType]',
        function: Callable[
            [_FirstType],
            KindN['SingleFailableN', _NewFirstType, _SecondType, _ThirdType],
        ],
    ) -> None:
        """Ensures that you cannot bind from the `empty` property."""
        assert_equal(
            container.empty,
            container.empty.bind(function),
        )

    @law_definition
    def apply_short_circuit_law(
        container: 'SingleFailableN[_FirstType, _SecondType, _ThirdType]',
        function: Callable[[_FirstType], _NewFirstType],
    ) -> None:
        """Ensures that you cannot apply from the `empty` property."""
        wrapped_function = container.from_value(function)
        assert_equal(
            container.empty,
            container.empty.apply(wrapped_function),
        )


class SingleFailableN(
    FailableN[_FirstType, _SecondType, _ThirdType],
):
    """
    Base type for types that have just only one failed value.

    Like ``Maybe`` types where the only failed value is ``Nothing``.
    """

    __slots__ = ()

    _laws: ClassVar[Sequence[Law]] = (
        Law2(_SingleFailableLawSpec.map_short_circuit_law),
        Law2(_SingleFailableLawSpec.bind_short_circuit_law),
        Law2(_SingleFailableLawSpec.apply_short_circuit_law),
    )

    @property
    @abstractmethod
    def empty(
        self: _SingleFailableType,
    ) -> 'SingleFailableN[_FirstType, _SecondType, _ThirdType]':
        """This property represents the failed value."""


#: Type alias for kinds with two types arguments.
SingleFailable2 = SingleFailableN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
SingleFailable3 = SingleFailableN[_FirstType, _SecondType, _ThirdType]


@final
class _DiverseFailableLawSpec(LawSpecDef):
    """
    Diverse Failable laws.

    We need to be sure that ``.map``, ``.bind``, ``.apply`` and ``.alt``
    works correctly for both success and failure types.
    """

    __slots__ = ()

    @law_definition
    def map_short_circuit_law(
        raw_value: _SecondType,
        container: 'DiverseFailableN[_FirstType, _SecondType, _ThirdType]',
        function: Callable[[_FirstType], _NewFirstType],
    ) -> None:
        """Ensures that you cannot map a failure."""
        assert_equal(
            container.from_failure(raw_value),
            container.from_failure(raw_value).map(function),
        )

    @law_definition
    def bind_short_circuit_law(
        raw_value: _SecondType,
        container: 'DiverseFailableN[_FirstType, _SecondType, _ThirdType]',
        function: Callable[
            [_FirstType],
            KindN['DiverseFailableN', _NewFirstType, _SecondType, _ThirdType],
        ],
    ) -> None:
        """
        Ensures that you cannot bind a failure.

        See: https://wiki.haskell.org/Typeclassopedia#MonadFail
        """
        assert_equal(
            container.from_failure(raw_value),
            container.from_failure(raw_value).bind(function),
        )

    @law_definition
    def apply_short_circuit_law(
        raw_value: _SecondType,
        container: 'DiverseFailableN[_FirstType, _SecondType, _ThirdType]',
        function: Callable[[_FirstType], _NewFirstType],
    ) -> None:
        """Ensures that you cannot apply a failure."""
        wrapped_function = container.from_value(function)
        assert_equal(
            container.from_failure(raw_value),
            container.from_failure(raw_value).apply(wrapped_function),
        )

    @law_definition
    def alt_short_circuit_law(
        raw_value: _SecondType,
        container: 'DiverseFailableN[_FirstType, _SecondType, _ThirdType]',
        function: Callable[[_SecondType], _NewFirstType],
    ) -> None:
        """Ensures that you cannot alt a success."""
        assert_equal(
            container.from_value(raw_value),
            container.from_value(raw_value).alt(function),
        )


class DiverseFailableN(
    FailableN[_FirstType, _SecondType, _ThirdType],
    swappable.SwappableN[_FirstType, _SecondType, _ThirdType],
    Lawful['DiverseFailableN[_FirstType, _SecondType, _ThirdType]'],
):
    """
    Base type for types that have any failed value.

    Like ``Result`` types.
    """

    __slots__ = ()

    _laws: ClassVar[Sequence[Law]] = (
        Law3(_DiverseFailableLawSpec.map_short_circuit_law),
        Law3(_DiverseFailableLawSpec.bind_short_circuit_law),
        Law3(_DiverseFailableLawSpec.apply_short_circuit_law),
        Law3(_DiverseFailableLawSpec.alt_short_circuit_law),
    )

    @classmethod
    @abstractmethod
    def from_failure(
        cls: type[_DiverseFailableType],
        inner_value: _UpdatedType,
    ) -> KindN[_DiverseFailableType, _FirstType, _UpdatedType, _ThirdType]:
        """Unit method to create new containers from any raw value."""


#: Type alias for kinds with two type arguments.
DiverseFailable2 = DiverseFailableN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
DiverseFailable3 = DiverseFailableN[_FirstType, _SecondType, _ThirdType]
