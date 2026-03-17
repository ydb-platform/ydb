from typing import TypeVar

from typing_extensions import Never

from returns.interfaces import altable, mappable

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')


class BiMappableN(
    mappable.MappableN[_FirstType, _SecondType, _ThirdType],
    altable.AltableN[_FirstType, _SecondType, _ThirdType],
):
    """
    Allows to change both types of a container at the same time.

    Uses ``.map`` to change first type and ``.alt`` to change second type.

    See also:
        - https://typelevel.org/cats/typeclasses/bifunctor.html

    """

    __slots__ = ()


#: Type alias for kinds with two type arguments.
BiMappable2 = BiMappableN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
BiMappable3 = BiMappableN[_FirstType, _SecondType, _ThirdType]
