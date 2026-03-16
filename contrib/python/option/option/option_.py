# MIT License

# Copyright (c) 2018-2022 Peijun Ma

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
This module contains the Option class.

.. data:: NONE

    Represents a None value.
"""

from typing import Callable, Generic, Mapping, Optional, Union

from option.types_ import A, K, T, U, V
from option.types_ import SupportsDunderLT, SupportsDunderGT
from option.types_ import SupportsDunderLE, SupportsDunderGE


class Option(Generic[T]):
    """
    :py:class:`Option` represents an optional value. Every :py:class:`Option`
    is either ``Some`` and contains a value, or :py:data:`NONE` and
    does not.

    To create a ``Some`` value, please use :py:meth:`Option.Some` or :py:func:`Some`.

    To create a :py:data:`NONE` value, please use :py:meth:`Option.NONE` or import the
    constant :py:data:`NONE` directly.

    To let :py:class:`Option` guess the type of :py:class:`Option` to create,
    please use :py:meth:`Option.maybe` or :py:func:`maybe`.

    Calling the ``__init__``  method directly will raise a ``TypeError``.

    Examples:
        >>> Option.Some(1)
        Some(1)
        >>> Option.NONE()
        NONE
        >>> Option.maybe(1)
        Some(1)
        >>> Option.maybe(None)
        NONE
    """
    __slots__ = ('_val', '_is_some', '_type')

    def __init__(self, value: T, is_some: bool, *, _force: bool = False) -> None:
        if not _force:
            raise TypeError(
                'Cannot directly initialize, '
                'please use one of the factory functions instead.'
            )
        self._val = value
        self._is_some = is_some
        self._type = type(self)

    @classmethod
    def Some(cls, val: T) -> 'Option[T]':
        """Some value ``val``."""
        return cls(val, True, _force=True)

    @classmethod
    def NONE(cls) -> 'Option[T]':
        """No Value."""
        return NONE  # type: ignore

    @classmethod
    def maybe(cls, val: Optional[T]) -> 'Option[T]':
        """
        Shortcut method to return ``Some`` or :py:data:`NONE` based on ``val``.

        Args:
            val: Some value.

        Returns:
            ``Some(val)`` if the ``val`` is not None, otherwise :py:data:`NONE`.

        Examples:
            >>> Option.maybe(0)
            Some(0)
            >>> Option.maybe(None)
            NONE
        """
        return NONE if val is None else cls.Some(val)  # type: ignore

    def __bool__(self) -> bool:
        """
        Returns the truth value of the :py:class:`Option` based on its value.

        Returns:
            True if the :py:class:`Option` is ``Some`` value, otherwise False.

        Examples:
            >>> bool(Some(False))
            True
            >>> bool(NONE)
            False
        """
        return self._is_some

    @property
    def is_some(self) -> bool:
        """
        Returns ``True`` if the option is a ``Some`` value.

        Examples:
            >>> Some(0).is_some
            True
            >>> NONE.is_some
            False
        """
        return self._is_some

    @property
    def is_none(self) -> bool:
        """
        Returns ``True`` if the option is a :py:data:`NONE` value.

        Examples:
            >>> Some(0).is_none
            False
            >>> NONE.is_none
            True
        """
        return not self._is_some

    def expect(self, msg: object) -> T:
        """
        Unwraps the option. Raises an exception if the value is :py:data:`NONE`.

        Args:
            msg: The exception message.

        Returns:
            The wrapped value.

        Raises:
            ``ValueError`` with message provided by ``msg`` if the value is :py:data:`NONE`.

        Examples:
            >>> Some(0).expect('sd')
            0
            >>> try:
            ...     NONE.expect('Oh No!')
            ... except ValueError as e:
            ...     print(e)
            Oh No!
        """
        if self._is_some:
            return self._val
        raise ValueError(msg)

    def unwrap(self) -> T:
        """
        Returns the value in the :py:class:`Option` if it is ``Some``.

        Returns:
            The ```Some`` value of the :py:class:`Option`.

        Raises:
            ``ValueError`` if the value is :py:data:`NONE`.

        Examples:
            >>> Some(0).unwrap()
            0
            >>> try:
            ...     NONE.unwrap()
            ... except ValueError as e:
            ...     print(e)
            Value is NONE.
        """
        return self.value

    @property
    def value(self) -> T:
        """Property version of :py:meth:`unwrap`."""
        if self._is_some:
            return self._val
        raise ValueError('Value is NONE.')

    def unwrap_or(self, default: U) -> Union[T, U]:
        """
        Returns the contained value or ``default``.

        Args:
            default: The default value.

        Returns:
            The contained value if the :py:class:`Option` is ``Some``,
            otherwise ``default``.

        Notes:
            If you wish to use a result of a function call as the default,
            it is recommnded to use :py:meth:`unwrap_or_else` instead.

        Examples:
            >>> Some(0).unwrap_or(3)
            0
            >>> NONE.unwrap_or(0)
            0
        """
        return self.unwrap_or_else(lambda: default)

    def unwrap_or_else(self, callback: Callable[[], U]) -> Union[T, U]:
        """
        Returns the contained value or computes it from ``callback``.

        Args:
            callback: The the default callback.

        Returns:
            The contained value if the :py:class:`Option` is ``Some``,
            otherwise ``callback()``.

        Examples:
            >>> Some(0).unwrap_or_else(lambda: 111)
            0
            >>> NONE.unwrap_or_else(lambda: 'ha')
            'ha'
        """
        return self._val if self._is_some else callback()

    def map(self, callback: Callable[[T], U]) -> 'Option[U]':
        """
        Applies the ``callback`` with the contained value as its argument or
        returns :py:data:`NONE`.

        Args:
            callback: The callback to apply to the contained value.

        Returns:
            The ``callback`` result wrapped in an :class:`Option` if the
            contained value is ``Some``, otherwise :py:data:`NONE`

        Examples:
            >>> Some(10).map(lambda x: x * x)
            Some(100)
            >>> NONE.map(lambda x: x * x)
            NONE
        """
        return self._type.Some(callback(self._val)) if self._is_some else NONE  # type: ignore

    def map_or(self, callback: Callable[[T], U], default: A) -> Union[U, A]:
        """
        Applies the ``callback`` to the contained value or returns ``default``.

        Args:
            callback: The callback to apply to the contained value.
            default: The default value.

        Returns:
            The ``callback`` result if the contained value is ``Some``,
            otherwise ``default``.

        Notes:
            If you wish to use the result of a function call as ``default``,
            it is recommended to use :py:meth:`map_or_else` instead.

        Examples:
            >>> Some(0).map_or(lambda x: x + 1, 1000)
            1
            >>> NONE.map_or(lambda x: x * x, 1)
            1
        """
        return callback(self._val) if self._is_some else default

    def map_or_else(self, callback: Callable[[T], U], default: Callable[[], A]) -> Union[U, A]:
        """
        Applies the ``callback`` to the contained value or computes a default
        with ``default``.

        Args:
            callback: The callback to apply to the contained value.
            default: The callback fot the default value.

        Returns:
            The ``callback`` result if the contained value is ``Some``,
            otherwise the result of ``default``.

        Examples:
            >>> Some(0).map_or_else(lambda x: x * x, lambda: 1)
            0
            >>> NONE.map_or_else(lambda x: x * x, lambda: 1)
            1
        """
        return callback(self._val) if self._is_some else default()

    def flatmap(self, callback: 'Callable[[T], Option[U]]') -> 'Option[U]':
        """
        Applies the callback to the contained value if the option
        is not :py:data:`NONE`.

        This is different than :py:meth:`Option.map` because the result
        of the callback isn't wrapped in a new :py:class:`Option`

        Args:
            callback: The callback to apply to the contained value.

        Returns:
            :py:data:`NONE` if the option is :py:data:`NONE`.

            otherwise calls `callback` with the contained value and
            returns the result.

        Examples:
            >>> def square(x): return Some(x * x)
            >>> def nope(x): return NONE
            >>> Some(2).flatmap(square).flatmap(square)
            Some(16)
            >>> Some(2).flatmap(square).flatmap(nope)
            NONE
            >>> Some(2).flatmap(nope).flatmap(square)
            NONE
            >>> NONE.flatmap(square).flatmap(square)
            NONE
        """
        return callback(self._val) if self._is_some else NONE  # type: ignore

    def filter(self, predicate: Callable[[T], bool]) -> 'Option[T]':
        """
        Returns :py:data:`NONE` if the :py:class:`Option` is :py:data:`NONE`,
        otherwise filter the contained value by ``predicate``.

        Args:
            predicate: The fitler function.

        Returns:
            :py:data:`NONE` if the contained value is :py:data:`NONE`, otherwise:
                * The option itself if the predicate returns True
                * :py:data:`NONE` if the predicate returns False

        Examples:
            >>> Some(0).filter(lambda x: x % 2 == 1)
            NONE
            >>> Some(1).filter(lambda x: x % 2 == 1)
            Some(1)
            >>> NONE.filter(lambda x: True)
            NONE
        """
        if self._is_some and predicate(self._val):
            return self
        return NONE  # type: ignore

    def get(
            self: 'Option[Mapping[K,V]]',
            key: K,
            default: Union[V, None] = None
    ) -> 'Option[V]':
        """
        Gets a mapping value by key in the contained value or returns
        ``default`` if the key doesn't exist.

        Args:
            key: The mapping key.
            default: The defauilt value.

        Returns:
            * ``Some`` variant of the mapping value if the key exists
               and the value is not None.
            * ``Some(default)`` if ``default`` is not None.
            * :py:data:`NONE` if ``default`` is None.

        Examples:
            >>> Some({'hi': 1}).get('hi')
            Some(1)
            >>> Some({}).get('hi', 12)
            Some(12)
            >>> NONE.get('hi', 12)
            Some(12)
            >>> NONE.get('hi')
            NONE
        """
        if self._is_some:
            return self._type.maybe(self._val.get(key, default))  # type: ignore
        return self._type.maybe(default)  # type: ignore

    def __hash__(self) -> int:
        return hash((self.__class__, self._is_some, self._val))

    def __eq__(self, other: object) -> bool:
        return (isinstance(other, self._type)
                and self._is_some == other._is_some
                and self._val == other._val)

    def __ne__(self, other: object) -> bool:
        return (not isinstance(other, self._type)
                or self._is_some != other._is_some
                or self._val != other._val)

    def __lt__(self: 'Option[SupportsDunderLT]', other: object) -> bool:
        if isinstance(other, self._type):
            if self._is_some == other._is_some:
                return self._val < other._val if self._is_some else False
            else:
                return other._is_some
        return NotImplemented

    def __le__(self: 'Option[SupportsDunderLE]', other: object) -> bool:
        if isinstance(other, self._type):
            if self._is_some == other._is_some:
                return self._val <= other._val if self._is_some else True
            return other._is_some
        return NotImplemented

    def __gt__(self: 'Option[SupportsDunderGT]', other: object) -> bool:
        if isinstance(other, self._type):
            if self._is_some == other._is_some:
                return self._val > other._val if self._is_some else False
            else:
                return self._is_some
        return NotImplemented

    def __ge__(self: 'Option[SupportsDunderGE]', other: object) -> bool:
        if isinstance(other, self._type):
            if self._is_some == other._is_some:
                return self._val >= other._val if self._is_some else True
            return self._is_some
        return NotImplemented

    def __repr__(self) -> str:
        return 'NONE' if self.is_none else f'Some({self._val!r})'


def Some(val: T) -> Option[T]:
    """Shortcut function to :py:meth:`Option.Some`."""
    return Option.Some(val)


def maybe(val: Optional[T]) -> Option[T]:
    """Shortcut function to :py:meth:`Option.maybe`."""
    return Option.maybe(val)


NONE = Option(None, False, _force=True)

if __name__ == '__main__':
    import doctest

    doctest.testmod()
