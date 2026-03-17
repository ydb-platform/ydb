from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from returns.interfaces.unwrappable import Unwrappable  # noqa: WPS433


class UnwrapFailedError(Exception):
    """Raised when a container can not be unwrapped into a meaningful value."""

    __slots__ = ('halted_container',)

    def __init__(self, container: Unwrappable) -> None:
        """
        Saves halted container in the inner state.

        So, this container can later be unpacked from this exception
        and used as a regular value.
        """
        super().__init__()
        self.halted_container = container

    def __reduce__(self):  # noqa: WPS603
        """Custom reduce method for pickle protocol.

        This helps properly reconstruct the exception during unpickling.
        """
        return (
            self.__class__,  # callable
            (self.halted_container,),  # args to callable
        )


class ImmutableStateError(AttributeError):
    """
    Raised when a container is forced to be mutated.

    It is a sublclass of ``AttributeError`` for two reasons:

    1. It seems kinda reasonable to expect ``AttributeError``
       on attribute modification
    2. It is used inside ``typing.py`` this way,
       we do have several typing features that requires that behaviour

    See: https://github.com/dry-python/returns/issues/394
    """
