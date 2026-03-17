"""
This module provides custom container classes with enhanced functionality.

Classes:
    CastedDictBase: Abstract base class for dictionaries that cast keys and
        values.
    CastedDict: Dictionary that casts keys and values to specified types.
    LazyCastedDict: Dictionary that lazily casts values to specified types upon
        access.
    UniqueList: List that only allows unique values, with configurable behavior
        on duplicates.
    SliceableDeque: Deque that supports slicing and enhanced equality checks.

Type Aliases:
    KT: Type variable for dictionary keys.
    VT: Type variable for dictionary values.
    DT: Type alias for a dictionary with keys of type KT and values of type VT.
    KT_cast: Type alias for a callable that casts dictionary keys.
    VT_cast: Type alias for a callable that casts dictionary values.
    HT: Type variable for hashable values in UniqueList.
    T: Type variable for generic types.
    DictUpdateArgs: Union type for arguments that can be used to update a
        dictionary.
    OnDuplicate: Literal type for handling duplicate values in UniqueList.

Usage:
    - CastedDict and LazyCastedDict can be used to create dictionaries with
        automatic type casting.
    - UniqueList ensures all elements are unique and can raise an error on
        duplicates.
    - SliceableDeque extends deque with slicing support and enhanced equality
        checks.

Examples:
    >>> d = CastedDict(int, int)
    >>> d[1] = 2
    >>> d['3'] = '4'
    >>> d.update({'5': '6'})
    >>> d.update([('7', '8')])
    >>> d
    {1: 2, 3: 4, 5: 6, 7: 8}

    >>> l = UniqueList(1, 2, 3)
    >>> l.append(4)
    >>> l.append(4)
    >>> l.insert(0, 4)
    >>> l.insert(0, 5)
    >>> l[1] = 10
    >>> l
    [5, 10, 2, 3, 4]

    >>> d = SliceableDeque([1, 2, 3, 4, 5])
    >>> d[1:4]
    SliceableDeque([2, 3, 4])
"""

# pyright: reportIncompatibleMethodOverride=false
import abc
import collections
import typing

from . import types

if typing.TYPE_CHECKING:
    import _typeshed  # noqa: F401

#: A type alias for a type that can be used as a key in a dictionary.
KT = types.TypeVar('KT')
#: A type alias for a type that can be used as a value in a dictionary.
VT = types.TypeVar('VT')
#: A type alias for a dictionary with keys of type KT and values of type VT.
DT = types.Dict[KT, VT]
#: A type alias for the casted type of a dictionary key.
KT_cast = types.Optional[types.Callable[..., KT]]
#: A type alias for the casted type of a dictionary value.
VT_cast = types.Optional[types.Callable[..., VT]]
#: A type alias for the hashable values of the `UniqueList`
HT = types.TypeVar('HT', bound=types.Hashable)
#: A type alias for a regular generic type
T = types.TypeVar('T')

# Using types.Union instead of | since Python 3.7 doesn't fully support it
DictUpdateArgs = types.Union[
    types.Mapping[KT, VT],
    types.Iterable[types.Tuple[KT, VT]],
    types.Iterable[types.Mapping[KT, VT]],
    '_typeshed.SupportsKeysAndGetItem[KT, VT]',
]

OnDuplicate = types.Literal['ignore', 'raise']


class CastedDictBase(types.Dict[KT, VT], abc.ABC):
    """
    Abstract base class for dictionaries that cast keys and values.

    Attributes:
        _key_cast (KT_cast[KT]): Callable to cast dictionary keys.
        _value_cast (VT_cast[VT]): Callable to cast dictionary values.

    Methods:
        __init__(key_cast: KT_cast[KT] = None, value_cast: VT_cast[VT] = None,
            *args: DictUpdateArgs[KT, VT], **kwargs: VT) -> None:
            Initializes the dictionary with optional key and value casting
            callables.
        update(*args: DictUpdateArgs[types.Any, types.Any],
            **kwargs: types.Any) -> None:
            Updates the dictionary with the given arguments.
        __setitem__(key: types.Any, value: types.Any) -> None:
            Sets the item in the dictionary, casting the key if a key cast
            callable is provided.
    """

    _key_cast: KT_cast[KT]
    _value_cast: VT_cast[VT]

    def __init__(
        self,
        key_cast: KT_cast[KT] = None,
        value_cast: VT_cast[VT] = None,
        *args: DictUpdateArgs[KT, VT],
        **kwargs: VT,
    ) -> None:
        """
        Initializes the CastedDictBase with optional key and value
        casting callables.

        Args:
            key_cast (KT_cast[KT], optional): Callable to cast
                dictionary keys. Defaults to None.
            value_cast (VT_cast[VT], optional): Callable to cast
                dictionary values. Defaults to None.
            *args (DictUpdateArgs[KT, VT]): Arguments to initialize
                the dictionary.
            **kwargs (VT): Keyword arguments to initialize the
                dictionary.
        """
        self._value_cast = value_cast
        self._key_cast = key_cast
        self.update(*args, **kwargs)

    def update(
        self, *args: DictUpdateArgs[types.Any, types.Any], **kwargs: types.Any
    ) -> None:
        """
        Updates the dictionary with the given arguments.

        Args:
            *args (DictUpdateArgs[types.Any, types.Any]): Arguments to update
                the dictionary.
            **kwargs (types.Any): Keyword arguments to update the dictionary.
        """
        if args:
            kwargs.update(*args)

        if kwargs:
            for key, value in kwargs.items():
                self[key] = value

    def __setitem__(self, key: types.Any, value: types.Any) -> None:
        """
        Sets the item in the dictionary, casting the key if a key cast
        callable is provided.

        Args:
            key (types.Any): The key to set in the dictionary.
            value (types.Any): The value to set in the dictionary.
        """
        if self._key_cast is not None:
            key = self._key_cast(key)

        return super().__setitem__(key, value)


class CastedDict(CastedDictBase[KT, VT]):
    """
    Custom dictionary that casts keys and values to the specified typing.

    Note that you can specify the types for mypy and type hinting with:
    CastedDict[int, int](int, int)

    >>> d: CastedDict[int, int] = CastedDict(int, int)
    >>> d[1] = 2
    >>> d['3'] = '4'
    >>> d.update({'5': '6'})
    >>> d.update([('7', '8')])
    >>> d
    {1: 2, 3: 4, 5: 6, 7: 8}
    >>> list(d.keys())
    [1, 3, 5, 7]
    >>> list(d)
    [1, 3, 5, 7]
    >>> list(d.values())
    [2, 4, 6, 8]
    >>> list(d.items())
    [(1, 2), (3, 4), (5, 6), (7, 8)]
    >>> d[3]
    4

    # Casts are optional and can be disabled by passing None as the cast
    >>> d = CastedDict()
    >>> d[1] = 2
    >>> d['3'] = '4'
    >>> d.update({'5': '6'})
    >>> d.update([('7', '8')])
    >>> d
    {1: 2, '3': '4', '5': '6', '7': '8'}
    """

    def __setitem__(self, key: typing.Any, value: typing.Any) -> None:
        """Sets `key` to `cast(value)` in the dictionary."""
        if self._value_cast is not None:
            value = self._value_cast(value)

        super().__setitem__(key, value)


class LazyCastedDict(CastedDictBase[KT, VT]):
    """
    Custom dictionary that casts keys and lazily casts values to the specified
    typing. Note that the values are cast only when they are accessed and
    are not cached between executions.

    Note that you can specify the types for mypy and type hinting with:
    LazyCastedDict[int, int](int, int)

    >>> d: LazyCastedDict[int, int] = LazyCastedDict(int, int)
    >>> d[1] = 2
    >>> d['3'] = '4'
    >>> d.update({'5': '6'})
    >>> d.update([('7', '8')])
    >>> d
    {1: 2, 3: '4', 5: '6', 7: '8'}
    >>> list(d.keys())
    [1, 3, 5, 7]
    >>> list(d)
    [1, 3, 5, 7]
    >>> list(d.values())
    [2, 4, 6, 8]
    >>> list(d.items())
    [(1, 2), (3, 4), (5, 6), (7, 8)]
    >>> d[3]
    4

    # Casts are optional and can be disabled by passing None as the cast
    >>> d = LazyCastedDict()
    >>> d[1] = 2
    >>> d['3'] = '4'
    >>> d.update({'5': '6'})
    >>> d.update([('7', '8')])
    >>> d
    {1: 2, '3': '4', '5': '6', '7': '8'}
    >>> list(d.keys())
    [1, '3', '5', '7']
    >>> list(d.values())
    [2, '4', '6', '8']

    >>> list(d.items())
    [(1, 2), ('3', '4'), ('5', '6'), ('7', '8')]
    >>> d['3']
    '4'
    """

    def __setitem__(self, key: types.Any, value: types.Any) -> None:
        """
        Sets the item in the dictionary, casting the key if a key cast
        callable is provided.

        Args:
            key (types.Any): The key to set in the dictionary.
            value (types.Any): The value to set in the dictionary.
        """
        if self._key_cast is not None:
            key = self._key_cast(key)

        super().__setitem__(key, value)

    def __getitem__(self, key: types.Any) -> VT:
        """
        Gets the item from the dictionary, casting the value if a value cast
        callable is provided.

        Args:
            key (types.Any): The key to get from the dictionary.

        Returns:
            VT: The value from the dictionary.
        """
        if self._key_cast is not None:
            key = self._key_cast(key)

        value = super().__getitem__(key)

        if self._value_cast is not None:
            value = self._value_cast(value)

        return value

    def items(  # type: ignore[override]
        self,
    ) -> types.Generator[types.Tuple[KT, VT], None, None]:
        """
        Returns a generator of the dictionary's items, casting the values if a
        value cast callable is provided.

        Yields:
            types.Generator[types.Tuple[KT, VT], None, None]: A generator of
                the dictionary's items.
        """
        if self._value_cast is None:
            yield from super().items()
        else:
            for key, value in super().items():
                yield key, self._value_cast(value)

    def values(self) -> types.Generator[VT, None, None]:  # type: ignore[override]
        """
        Returns a generator of the dictionary's values, casting the values if a
        value cast callable is provided.

        Yields:
            types.Generator[VT, None, None]: A generator of the dictionary's
                values.
        """
        if self._value_cast is None:
            yield from super().values()
        else:
            for value in super().values():
                yield self._value_cast(value)


class UniqueList(types.List[HT]):
    """
    A list that only allows unique values. Duplicate values are ignored by
    default, but can be configured to raise an exception instead.

    >>> l = UniqueList(1, 2, 3)
    >>> l.append(4)
    >>> l.append(4)
    >>> l.insert(0, 4)
    >>> l.insert(0, 5)
    >>> l[1] = 10
    >>> l
    [5, 10, 2, 3, 4]

    >>> l = UniqueList(1, 2, 3, on_duplicate='raise')
    >>> l.append(4)
    >>> l.append(4)
    Traceback (most recent call last):
    ...
    ValueError: Duplicate value: 4
    >>> l.insert(0, 4)
    Traceback (most recent call last):
    ...
    ValueError: Duplicate value: 4
    >>> 4 in l
    True
    >>> l[0]
    1
    >>> l[1] = 4
    Traceback (most recent call last):
    ...
    ValueError: Duplicate value: 4
    """

    _set: types.Set[HT]

    def __init__(
        self,
        *args: HT,
        on_duplicate: OnDuplicate = 'ignore',
    ):
        """
        Initializes the UniqueList with optional duplicate handling behavior.

        Args:
            *args (HT): Initial values for the list.
            on_duplicate (OnDuplicate, optional): Behavior on duplicates.
                Defaults to 'ignore'.
        """
        self.on_duplicate = on_duplicate
        self._set = set()
        super().__init__()
        for arg in args:
            self.append(arg)

    def insert(self, index: types.SupportsIndex, value: HT) -> None:
        """
        Inserts a value at the specified index, ensuring uniqueness.

        Args:
            index (types.SupportsIndex): The index to insert the value at.
            value (HT): The value to insert.

        Raises:
            ValueError: If the value is a duplicate and `on_duplicate` is set
                to 'raise'.
        """
        if value in self._set:
            if self.on_duplicate == 'raise':
                raise ValueError(f'Duplicate value: {value}')
            else:
                return

        self._set.add(value)
        super().insert(index, value)

    def append(self, value: HT) -> None:
        """
        Appends a value to the list, ensuring uniqueness.

        Args:
            value (HT): The value to append.

        Raises:
            ValueError: If the value is a duplicate and `on_duplicate` is set
                to 'raise'.
        """
        if value in self._set:
            if self.on_duplicate == 'raise':
                raise ValueError(f'Duplicate value: {value}')
            else:
                return

        self._set.add(value)
        super().append(value)

    def __contains__(self, item: HT) -> bool:  # type: ignore[override]
        """
        Checks if the list contains the specified item.

        Args:
            item (HT): The item to check for.

        Returns:
            bool: True if the item is in the list, False otherwise.
        """
        return item in self._set

    @typing.overload
    def __setitem__(
        self, indices: types.SupportsIndex, values: HT
    ) -> None: ...

    @typing.overload
    def __setitem__(
        self, indices: slice, values: types.Iterable[HT]
    ) -> None: ...

    def __setitem__(
        self,
        indices: types.Union[slice, types.SupportsIndex],
        values: types.Union[types.Iterable[HT], HT],
    ) -> None:
        """
        Sets the item(s) at the specified index/indices, ensuring uniqueness.

        Args:
            indices (types.Union[slice, types.SupportsIndex]): The index or
                slice to set the value(s) at.
            values (types.Union[types.Iterable[HT], HT]): The value(s) to set.

        Raises:
            RuntimeError: If `on_duplicate` is 'ignore' and setting slices.
            ValueError: If the value(s) are duplicates and `on_duplicate` is
                set to 'raise'.
        """
        if isinstance(indices, slice):
            values = types.cast(types.Iterable[HT], values)
            if self.on_duplicate == 'ignore':
                raise RuntimeError(
                    'ignore mode while setting slices introduces ambiguous '
                    'behaviour and is therefore not supported'
                )

            duplicates: types.Set[HT] = set(values) & self._set
            if duplicates and values != list(self[indices]):
                raise ValueError(f'Duplicate values: {duplicates}')

            self._set.update(values)
        else:
            values = types.cast(HT, values)
            if values in self._set and values != self[indices]:
                if self.on_duplicate == 'raise':
                    raise ValueError(f'Duplicate value: {values}')
                else:
                    return

            self._set.add(values)

        super().__setitem__(
            types.cast(slice, indices), types.cast(types.List[HT], values)
        )

    def __delitem__(
        self, index: types.Union[types.SupportsIndex, slice]
    ) -> None:
        """
        Deletes the item(s) at the specified index/indices.

        Args:
            index (types.Union[types.SupportsIndex, slice]): The index or slice
                to delete the item(s) at.
        """
        if isinstance(index, slice):
            for value in self[index]:
                self._set.remove(value)
        else:
            self._set.remove(self[index])

        super().__delitem__(index)


# Type hinting `collections.deque` does not work consistently between Python
# runtime, mypy and pyright currently so we have to ignore the errors
class SliceableDeque(types.Generic[T], collections.deque[T]):
    """
    A deque that supports slicing and enhanced equality checks.

    Methods:
        __getitem__(index: types.Union[types.SupportsIndex, slice]) ->
            types.Union[T, 'SliceableDeque[T]']:
            Returns the item or slice at the given index.
        __eq__(other: types.Any) -> bool:
            Checks equality with another object, allowing for comparison with
             lists, tuples, and sets.
        pop(index: int = -1) -> T:
            Removes and returns the item at the given index. Only supports
            index 0 and the last index.
    """

    @typing.overload
    def __getitem__(self, index: types.SupportsIndex) -> T: ...

    @typing.overload
    def __getitem__(self, index: slice) -> 'SliceableDeque[T]': ...

    def __getitem__(
        self, index: types.Union[types.SupportsIndex, slice]
    ) -> types.Union[T, 'SliceableDeque[T]']:
        """
        Return the item or slice at the given index.

        Args:
            index (types.Union[types.SupportsIndex, slice]): The index or
             slice to retrieve.

        Returns:
            types.Union[T, 'SliceableDeque[T]']: The item or slice at the
            given index.

        Examples:
            >>> d = SliceableDeque[int]([1, 2, 3, 4, 5])
            >>> d[1:4]
            SliceableDeque([2, 3, 4])

            >>> d = SliceableDeque[str](['a', 'b', 'c'])
            >>> d[-2:]
            SliceableDeque(['b', 'c'])
        """
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            return self.__class__(self[i] for i in range(start, stop, step))
        else:
            return super().__getitem__(index)

    def __eq__(self, other: types.Any) -> bool:
        """
        Checks equality with another object, allowing for comparison with
        lists, tuples, and sets.

        Args:
            other (types.Any): The object to compare with.

        Returns:
            bool: True if the objects are equal, False otherwise.
        """
        if isinstance(other, list):
            return list(self) == other
        elif isinstance(other, tuple):
            return tuple(self) == other
        elif isinstance(other, set):
            return set(self) == other
        else:
            return super().__eq__(other)

    def pop(self, index: int = -1) -> T:
        """
        Removes and returns the item at the given index. Only supports index 0
        and the last index.

        Args:
            index (int, optional): The index of the item to remove. Defaults to
            -1.

        Returns:
            T: The removed item.

        Raises:
            IndexError: If the index is not 0 or the last index.

        Examples:
            >>> d = SliceableDeque([1, 2, 3])
            >>> d.pop(0)
            1
            >>> d.pop()
            3
        """
        if index == 0:
            return super().popleft()
        elif index in {-1, len(self) - 1}:
            return super().pop()
        else:
            raise IndexError(
                'Only index 0 and the last index (`N-1` or `-1`) '
                'are supported'
            )


if __name__ == '__main__':
    import doctest

    doctest.testmod()
