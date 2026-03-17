from collections.abc import Callable, Sequence
from typing import ClassVar, Final, Generic, TypeVar, final

from returns.primitives.types import Immutable

_Caps = TypeVar('_Caps')
_ReturnType = TypeVar('_ReturnType')
_TypeArgType1 = TypeVar('_TypeArgType1')
_TypeArgType2 = TypeVar('_TypeArgType2')
_TypeArgType3 = TypeVar('_TypeArgType3')

#: Special alias to define laws as functions even inside a class
law_definition = staticmethod

LAWS_ATTRIBUTE: Final = '_laws'


class Law(Immutable):
    """
    Base class for all laws. Does not have an attached signature.

    Should not be used directly.
    Use ``Law1``, ``Law2`` or ``Law3`` instead.
    """

    __slots__ = ('definition',)

    #: Function used to define this law.
    definition: Callable

    def __init__(self, function) -> None:
        """Saves function to the inner state."""
        object.__setattr__(self, 'definition', function)

    @final
    @property
    def name(self) -> str:
        """Returns a name of the given law. Basically a name of the function."""
        return self.definition.__name__


@final
class Law1(
    Law,
    Generic[_TypeArgType1, _ReturnType],
):
    """Law definition for functions with a single argument."""

    __slots__ = ()

    definition: Callable[['Law1', _TypeArgType1], _ReturnType]

    def __init__(
        self,
        function: Callable[[_TypeArgType1], _ReturnType],
    ) -> None:
        """Saves function of one argument to the inner state."""
        super().__init__(function)


@final
class Law2(
    Law,
    Generic[_TypeArgType1, _TypeArgType2, _ReturnType],
):
    """Law definition for functions with two arguments."""

    __slots__ = ()

    definition: Callable[['Law2', _TypeArgType1, _TypeArgType2], _ReturnType]

    def __init__(
        self,
        function: Callable[[_TypeArgType1, _TypeArgType2], _ReturnType],
    ) -> None:
        """Saves function of two arguments to the inner state."""
        super().__init__(function)


@final
class Law3(
    Law,
    Generic[_TypeArgType1, _TypeArgType2, _TypeArgType3, _ReturnType],
):
    """Law definition for functions with three argument."""

    __slots__ = ()

    definition: Callable[
        ['Law3', _TypeArgType1, _TypeArgType2, _TypeArgType3],
        _ReturnType,
    ]

    def __init__(
        self,
        function: Callable[
            [_TypeArgType1, _TypeArgType2, _TypeArgType3],
            _ReturnType,
        ],
    ) -> None:
        """Saves function of three arguments to the inner state."""
        super().__init__(function)


class Lawful(Generic[_Caps]):
    """
    Base class for all lawful classes.

    Allows to smartly collect all defined laws from all parent classes.
    """

    __slots__ = ()

    #: Some classes and interfaces might have laws, some might not have any.
    _laws: ClassVar[Sequence[Law]]

    @final  # noqa: WPS210
    @classmethod
    def laws(cls) -> dict[type['Lawful'], Sequence[Law]]:  # noqa: WPS210
        """
        Collects all laws from all parent classes.

        Algorithm:

        1. First, we collect all unique parents in ``__mro__``
        2. Then we get the laws definition from each of them
        3. Then we structure them in a ``type: its_laws`` way

        """
        seen = {
            f'{parent.__module__}.{parent.__qualname__}': parent
            for parent in cls.__mro__
        }

        laws = {}
        for klass in seen.values():
            current_laws = klass.__dict__.get(LAWS_ATTRIBUTE, ())
            if not current_laws:
                continue
            laws[klass] = current_laws
        return laws


class LawSpecDef:
    """Base class for all collection of laws aka LawSpecs."""

    __slots__ = ()
