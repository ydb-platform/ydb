import sys
from collections.abc import Container, Sized
from typing import TYPE_CHECKING, Any, Optional, TypeVar, Union, overload

from ._base import DirtyEquals
from ._utils import Omit, plain_repr

if TYPE_CHECKING:
    from typing import TypeAlias

if sys.version_info >= (3, 10):
    from types import EllipsisType
else:
    EllipsisType = Any

__all__ = 'HasLen', 'Contains', 'IsListOrTuple', 'IsList', 'IsTuple'
T = TypeVar('T', list[Any], tuple[Any, ...])
LengthType: 'TypeAlias' = 'Union[None, int, tuple[int, Union[int, Any]], EllipsisType]'


class HasLen(DirtyEquals[Sized]):
    """
    Check that some has a given length, or length in a given range.
    """

    @overload
    def __init__(self, length: int): ...

    @overload
    def __init__(self, min_length: int, max_length: Union[int, Any]): ...

    def __init__(self, min_length: int, max_length: Union[None, int, Any] = None):  # type: ignore[misc]
        """
        Args:
            min_length: Expected length if `max_length` is not given, else minimum length.
            max_length: Expected maximum length, use an ellipsis `...` to indicate that there's no maximum.

        ```py title="HasLen"
        from dirty_equals import HasLen

        assert [1, 2, 3] == HasLen(3)  # (1)!
        assert '123' == HasLen(3, ...)  # (2)!
        assert (1, 2, 3) == HasLen(3, 5)  # (3)!
        assert (1, 2, 3) == HasLen(0, ...)  # (4)!
        ```

        1. Length must be 3.
        2. Length must be 3 or higher.
        3. Length must be between 3 and 5 inclusive.
        4. Length is required but can take any value.
        """
        if max_length is None:
            self.length: LengthType = min_length
            super().__init__(self.length)
        else:
            self.length = (min_length, max_length)
            super().__init__(*_length_repr(self.length))

    def equals(self, other: Any) -> bool:
        return _length_correct(self.length, other)


class Contains(DirtyEquals[Container[Any]]):
    """
    Check that an object contains one or more values.
    """

    def __init__(self, contained_value: Any, *more_contained_values: Any):
        """
        Args:
            contained_value: value that must be contained in the compared object.
            *more_contained_values: more values that must be contained in the compared object.

        ```py title="Contains"
        from dirty_equals import Contains

        assert [1, 2, 3] == Contains(1)
        assert [1, 2, 3] == Contains(1, 2)
        assert (1, 2, 3) == Contains(1)
        assert 'abc' == Contains('b')
        assert {'a': 1, 'b': 2} == Contains('a')
        assert [1, 2, 3] != Contains(10)
        ```
        """
        self.contained_values: tuple[Any, ...] = (contained_value,) + more_contained_values
        super().__init__(*self.contained_values)

    def equals(self, other: Any) -> bool:
        return all(v in other for v in self.contained_values)


class IsListOrTuple(DirtyEquals[T]):
    """
    Check that some object is a list or tuple and optionally its values match some constraints.
    """

    allowed_type: Union[type[T], tuple[type[list[Any]], type[tuple[Any, ...]]]] = (list, tuple)

    @overload
    def __init__(self, *items: Any, check_order: bool = True, length: 'LengthType' = None): ...

    @overload
    def __init__(self, positions: dict[int, Any], length: 'LengthType' = None): ...

    def __init__(
        self,
        *items: Any,
        positions: Optional[dict[int, Any]] = None,
        check_order: bool = True,
        length: 'LengthType' = None,
    ):
        """
        `IsListOrTuple` and its subclasses can be initialised in two ways:

        Args:
            *items: Positional members of an object to check. These must start from the zeroth position, but
                (depending on the value of `length`) may not include all values of the list/tuple being checked.
            check_order: Whether to enforce the order of the items.
            length (Union[int, tuple[int, Union[int, Any]]]): length constraints, int or tuple matching the arguments
                of [`HasLen`][dirty_equals.HasLen].

        or,

        Args:
            positions (dict[int, Any]): Instead of `*items`, a dictionary of positions and
                values to check and be provided.
            length (Union[int, tuple[int, Union[int, Any]]]): length constraints, int or tuple matching the arguments
                of [`HasLen`][dirty_equals.HasLen].

        ```py title="IsListOrTuple"
        from dirty_equals import AnyThing, IsListOrTuple

        assert [1, 2, 3] == IsListOrTuple(1, 2, 3)
        assert (1, 3, 2) == IsListOrTuple(1, 2, 3, check_order=False)
        assert [{'a': 1}, {'a': 2}] == (
            IsListOrTuple({'a': 2}, {'a': 1}, check_order=False)  # (1)!
        )
        assert [1, 2, 3, 3] != IsListOrTuple(1, 2, 3, check_order=False)  # (2)!

        assert [1, 2, 3, 4, 5] == IsListOrTuple(1, 2, 3, length=...)  # (3)!
        assert [1, 2, 3, 4, 5] != IsListOrTuple(1, 2, 3, length=(8, 10))  # (4)!

        assert ['a', 'b', 'c', 'd'] == (IsListOrTuple(positions={2: 'c', 3: 'd'}))  # (5)!
        assert ['a', 'b', 'c', 'd'] == (
            IsListOrTuple(positions={2: 'c', 3: 'd'}, length=4)  # (6)!
        )

        assert [1, 2, 3, 4] == IsListOrTuple(3, check_order=False, length=(0, ...))  # (7)!

        assert [1, 2, 3] == IsListOrTuple(AnyThing, AnyThing, 3)  # (8)!
        ```

        1. Unlike using sets for comparison, we can do order-insensitive comparisons on objects that are not hashable.
        2. And we won't get caught out by duplicate values
        3. Here we're just checking the first 3 items, the compared list or tuple can be of any length
        4. Compared list is not long enough
        5. Compare using `positions`, here no length if enforced
        6. Compare using `positions` but with a length constraint
        7. Here we're just confirming that the value `3` is in the list
        8. If you don't care about the first few values of a list or tuple,
            you can use [`AnyThing`][dirty_equals.AnyThing] in your arguments.
        """
        if positions is not None:
            self.positions: Optional[dict[int, Any]] = positions
            if items:
                raise TypeError(f'{self.__class__.__name__} requires either args or positions, not both')
            if not check_order:
                raise TypeError('check_order=False is not compatible with positions')
        else:
            self.positions = None
            self.items = items
        self.check_order = check_order

        self.length: Any = length
        if self.length is not None and not isinstance(self.length, int):
            if self.length == Ellipsis:
                self.length = 0, ...
            else:
                self.length = tuple(self.length)

        super().__init__(
            *items,
            positions=Omit if positions is None else positions,
            length=_length_repr(self.length),
            check_order=self.check_order and Omit,
        )

    def equals(self, other: Any) -> bool:
        if not isinstance(other, self.allowed_type):
            return False

        if not _length_correct(self.length, other):
            return False

        if self.check_order:
            if self.positions is None:
                if self.length is None:
                    return list(self.items) == list(other)
                else:
                    return list(self.items) == list(other[: len(self.items)])
            else:
                return all(v == other[k] for k, v in self.positions.items())
        else:
            # order insensitive comparison
            # if we haven't checked length yet, check it now
            if self.length is None and len(other) != len(self.items):
                return False

            other_copy = list(other)
            for item in self.items:
                try:
                    other_copy.remove(item)
                except ValueError:
                    return False
            return True


class IsList(IsListOrTuple[list[Any]]):
    """
    All the same functionality as [`IsListOrTuple`][dirty_equals.IsListOrTuple], but the compared value must be a list.

    ```py title="IsList"
    from dirty_equals import IsList

    assert [1, 2, 3] == IsList(1, 2, 3)
    assert [1, 2, 3] == IsList(positions={2: 3})
    assert [1, 2, 3] == IsList(1, 2, 3, check_order=False)
    assert [1, 2, 3, 4] == IsList(1, 2, 3, length=4)
    assert [1, 2, 3, 4] == IsList(1, 2, 3, length=(4, 5))
    assert [1, 2, 3, 4] == IsList(1, 2, 3, length=...)

    assert (1, 2, 3) != IsList(1, 2, 3)
    ```
    """

    allowed_type = list


class IsTuple(IsListOrTuple[tuple[Any, ...]]):
    """
    All the same functionality as [`IsListOrTuple`][dirty_equals.IsListOrTuple], but the compared value must be a tuple.

    ```py title="IsTuple"
    from dirty_equals import IsTuple

    assert (1, 2, 3) == IsTuple(1, 2, 3)
    assert (1, 2, 3) == IsTuple(positions={2: 3})
    assert (1, 2, 3) == IsTuple(1, 2, 3, check_order=False)
    assert (1, 2, 3, 4) == IsTuple(1, 2, 3, length=4)
    assert (1, 2, 3, 4) == IsTuple(1, 2, 3, length=(4, 5))
    assert (1, 2, 3, 4) == IsTuple(1, 2, 3, length=...)

    assert [1, 2, 3] != IsTuple(1, 2, 3)
    ```
    """

    allowed_type = tuple


def _length_repr(length: 'LengthType') -> Any:
    if length is None:
        return Omit
    elif isinstance(length, int) or length is Ellipsis:
        return length
    else:
        if len(length) != 2:
            raise TypeError(f'length must be a tuple of length 2, not {len(length)}')
        max_value = length[1] if isinstance(length[1], int) else plain_repr('...')
        return length[0], max_value


def _length_correct(length: 'LengthType', other: 'Sized') -> bool:
    if isinstance(length, int):
        if len(other) != length:
            return False
    elif isinstance(length, tuple):
        other_len = len(other)
        min_length, max_length = length
        if other_len < min_length:
            return False
        if isinstance(max_length, int) and other_len > max_length:
            return False
    return True
