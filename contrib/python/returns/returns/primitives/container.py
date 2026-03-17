from abc import ABC
from typing import Any, TypeVar

from typing_extensions import TypedDict

from returns.interfaces.equable import Equable
from returns.primitives.hkt import Kind1
from returns.primitives.types import Immutable

_EqualType = TypeVar('_EqualType', bound=Equable)


class _PickleState(TypedDict):
    """Dict to represent the `BaseContainer` state to be pickled."""

    # TODO: Remove `__slots__` from here when `slotscheck` allow ignore classes
    # by using comments. We don't need the slots here since this class is just
    # a representation of a dictionary and should not be instantiated by any
    # means.
    # See: https://github.com/ariebovenberg/slotscheck/issues/71
    __slots__ = ('container_value',)  # type: ignore

    container_value: Any


class BaseContainer(Immutable, ABC):
    """Utility class to provide all needed magic methods to the context."""

    __slots__ = ('_inner_value',)
    _inner_value: Any

    def __init__(self, inner_value) -> None:
        """
        Wraps the given value in the Container.

        'value' is any arbitrary value of any type including functions.
        """
        object.__setattr__(self, '_inner_value', inner_value)

    def __repr__(self) -> str:
        """Used to display details of object."""
        return '<{}: {}>'.format(
            self.__class__.__qualname__.strip('_'),
            str(self._inner_value),
        )

    def __eq__(self, other: object) -> bool:
        """Used to compare two 'Container' objects."""
        return container_equality(self, other)  # type: ignore

    def __hash__(self) -> int:
        """Used to use this value as a key."""
        return hash(self._inner_value)

    def __getstate__(self) -> _PickleState:
        """That's how this object will be pickled."""
        return {'container_value': self._inner_value}  # type: ignore

    def __setstate__(self, state: _PickleState | Any) -> None:
        """Loading state from pickled data."""
        if isinstance(state, dict) and 'container_value' in state:
            object.__setattr__(
                self,
                '_inner_value',
                state['container_value'],
            )
        else:
            # backward compatibility with 0.19.0 and earlier
            object.__setattr__(self, '_inner_value', state)


def container_equality(
    self: Kind1[_EqualType, Any],
    other: Kind1[_EqualType, Any],
) -> bool:
    """
    Function to compare similar containers.

    Compares both their types and their inner values.
    """
    if type(self) != type(other):  # noqa: WPS516, E721
        return False
    return bool(
        self._inner_value == other._inner_value,  # type: ignore # noqa: SLF001
    )
