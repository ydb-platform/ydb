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

"""This module contains the Result type."""
from typing import Any, Callable, Generic, Union

from option.option_ import NONE, Option
from option.types_ import E, F, T, U
from option.types_ import SupportsDunderLT, SupportsDunderGT
from option.types_ import SupportsDunderLE, SupportsDunderGE


class Result(Generic[T, E]):
    """
    :class:`Result` is a type that either success (:meth:`Result.Ok`)
    or failure (:meth:`Result.Err`).

    To create an Ok value, use :meth:`Result.Ok` or :func:`Ok`.

    To create a Err value, use :meth:`Result.Err` or :func:`Err`.

    Calling the :class:`Result` constructor directly will raise a ``TypeError``.

    Examples:
        >>> Result.Ok(1)
        Ok(1)
        >>> Result.Err('Fail!')
        Err('Fail!')
    """
    __slots__ = ('_val', '_is_ok', '_type')

    def __init__(self, val: Union[T, E], is_ok: bool, *, _force: bool = False) -> None:
        if not _force:
            raise TypeError(
                'Cannot directly initialize, '
                'please use one of the factory functions instead.'
            )
        self._val = val
        self._is_ok = is_ok
        self._type = type(self)

    @classmethod
    def Ok(cls, val: T) -> 'Result[T, Any]':
        """
        Contains the success value.

        Args:
             val: The success value.

        Returns:
             The :class:`Result` containing the success value.

        Examples:
            >>> res = Result.Ok(1)
            >>> res
            Ok(1)
            >>> res.is_ok
            True
        """
        return cls(val, True, _force=True)

    @classmethod
    def Err(cls, err: E) -> 'Result[Any, E]':
        """
        Contains the error value.

        Args:
            err: The error value.

        Returns:
            The :class:`Result` containing the error value.

        Examples:
            >>> res = Result.Err('Oh No')
            >>> res
            Err('Oh No')
            >>> res.is_err
            True
        """
        return cls(err, False, _force=True)

    def __bool__(self) -> bool:
        return self._is_ok

    @property
    def is_ok(self) -> bool:
        """
        Returns `True` if the result is :meth:`Result.Ok`.

        Examples:
            >>> Ok(1).is_ok
            True
            >>> Err(1).is_ok
            False
        """
        return self._is_ok

    @property
    def is_err(self) -> bool:
        """
        Returns `True` if the result is :meth:`Result.Err`.

        Examples:
            >>> Ok(1).is_err
            False
            >>> Err(1).is_err
            True
        """
        return not self._is_ok

    def ok(self) -> Option[T]:
        """
        Converts from :class:`Result` [T, E] to :class:`option.option_.Option` [T].

        Returns:
            :class:`Option` containing the success value if `self` is
            :meth:`Result.Ok`, otherwise :data:`option.option_.NONE`.

        Examples:
            >>> Ok(1).ok()
            Some(1)
            >>> Err(1).ok()
            NONE
        """
        return Option.Some(self._val) if self._is_ok else NONE  # type: ignore

    def err(self) -> Option[E]:
        """
        Converts from :class:`Result` [T, E] to :class:`option.option_.Option` [E].

        Returns:
            :class:`Option` containing the error value if `self` is
            :meth:`Result.Err`, otherwise :data:`option.option_.NONE`.

        Examples:
            >>> Ok(1).err()
            NONE
            >>> Err(1).err()
            Some(1)
        """
        return NONE if self._is_ok else Option.Some(self._val)  # type: ignore

    def map(self, op: Callable[[T], U]) -> 'Union[Result[U, E], Result[T, E]]':
        """
        Applies a function to the contained :meth:`Result.Ok` value.

        Args:
            op: The function to apply to the :meth:`Result.Ok` value.

        Returns:
            A :class:`Result` with its success value as the function result
            if `self` is an :meth:`Result.Ok` value, otherwise returns
            `self`.

        Examples:
            >>> Ok(1).map(lambda x: x * 2)
            Ok(2)
            >>> Err(1).map(lambda x: x * 2)
            Err(1)
        """
        return self._type.Ok(op(self._val)) if self._is_ok else self  # type: ignore

    def flatmap(self, op: 'Callable[[T], Result[U, E]]') -> 'Result[U, E]':
        """
        Applies a function to the contained :meth:`Result.Ok` value.

        This is different than :meth:`Result.map` because the function
        result is not wrapped in a new :class:`Result`.

        Args:
            op: The function to apply to the contained :meth:`Result.Ok` value.

        Returns:
            The result of the function if `self` is an :meth:`Result.Ok` value,
             otherwise returns `self`.

        Examples:
            >>> def sq(x): return Ok(x * x)
            >>> def err(x): return Err(x)
            >>> Ok(2).flatmap(sq).flatmap(sq)
            Ok(16)
            >>> Ok(2).flatmap(sq).flatmap(err)
            Err(4)
            >>> Ok(2).flatmap(err).flatmap(sq)
            Err(2)
            >>> Err(3).flatmap(sq).flatmap(sq)
            Err(3)
        """
        return op(self._val) if self._is_ok else self  # type: ignore

    def map_err(self, op: Callable[[E], F]) -> 'Union[Result[T, F], Result[T, E]]':
        """
        Applies a function to the contained :meth:`Result.Err` value.

        Args:
            op: The function to apply to the :meth:`Result.Err` value.

        Returns:
            A :class:`Result` with its error value as the function result
            if `self` is a :meth:`Result.Err` value, otherwise returns
            `self`.

        Examples:
            >>> Ok(1).map_err(lambda x: x * 2)
            Ok(1)
            >>> Err(1).map_err(lambda x: x * 2)
            Err(2)
        """
        return self if self._is_ok else self._type.Err(op(self._val))  # type: ignore

    def unwrap(self) -> T:
        """
        Returns the success value in the :class:`Result`.

        Returns:
            The success value in the :class:`Result`.

        Raises:
            ``ValueError`` with the message provided by the error value
             if the :class:`Result` is a :meth:`Result.Err` value.

        Examples:
            >>> Ok(1).unwrap()
            1
            >>> try:
            ...     Err(1).unwrap()
            ... except ValueError as e:
            ...     print(e)
            1
        """
        if self._is_ok:
            return self._val  # type: ignore
        raise ValueError(self._val)

    def unwrap_or(self, optb: T) -> T:
        """
        Returns the success value in the :class:`Result` or ``optb``.

        Args:
            optb: The default return value.

        Returns:
            The success value in the :class:`Result` if it is a
            :meth:`Result.Ok` value, otherwise ``optb``.

        Notes:
            If you wish to use a result of a function call as the default,
            it is recommnded to use :meth:`unwrap_or_else` instead.

        Examples:
            >>> Ok(1).unwrap_or(2)
            1
            >>> Err(1).unwrap_or(2)
            2
        """
        return self._val if self._is_ok else optb  # type: ignore

    def unwrap_or_else(self, op: Callable[[E], U]) -> Union[T, U]:
        """
        Returns the sucess value in the :class:`Result` or computes a default
        from the error value.

        Args:
            op: The function to computes default with.

        Returns:
            The success value in the :class:`Result` if it is
             a :meth:`Result.Ok` value, otherwise ``op(E)``.

        Examples:
            >>> Ok(1).unwrap_or_else(lambda e: e * 10)
            1
            >>> Err(1).unwrap_or_else(lambda e: e * 10)
            10
        """
        return self._val if self._is_ok else op(self._val)  # type: ignore

    def expect(self, msg: object) -> T:
        """
        Returns the success value in the :class:`Result` or raises
        a ``ValueError`` with a provided message.

        Args:
            msg: The error message.

        Returns:
            The success value in the :class:`Result` if it is
            a :meth:`Result.Ok` value.

        Raises:
            ``ValueError`` with ``msg`` as the message if the
            :class:`Result` is a :meth:`Result.Err` value.

        Examples:
            >>> Ok(1).expect('no')
            1
            >>> try:
            ...     Err(1).expect('no')
            ... except ValueError as e:
            ...     print(e)
            no
        """
        if self._is_ok:
            return self._val  # type: ignore
        raise ValueError(msg)

    def unwrap_err(self) -> E:
        """
        Returns the error value in a :class:`Result`.

        Returns:
            The error value in the :class:`Result` if it is a
            :meth:`Result.Err` value.

        Raises:
            ``ValueError`` with the message provided by the success value
             if the :class:`Result` is a :meth:`Result.Ok` value.

        Examples:
            >>> try:
            ...     Ok(1).unwrap_err()
            ... except ValueError as e:
            ...     print(e)
            1
            >>> Err('Oh No').unwrap_err()
            'Oh No'
        """
        if self._is_ok:
            raise ValueError(self._val)
        return self._val  # type: ignore

    def expect_err(self, msg: object) -> E:
        """
        Returns the error value in a :class:`Result`, or raises a
        ``ValueError`` with the provided message.

        Args:
            msg: The error message.

        Returns:
            The error value in the :class:`Result` if it is a
            :meth:`Result.Err` value.

        Raises:
            ``ValueError`` with the message provided by ``msg`` if
            the :class:`Result` is a :meth:`Result.Ok` value.

        Examples:
            >>> try:
            ...     Ok(1).expect_err('Oh No')
            ... except ValueError as e:
            ...     print(e)
            Oh No
            >>> Err(1).expect_err('Yes')
            1
        """
        if self._is_ok:
            raise ValueError(msg)
        return self._val  # type: ignore

    def __repr__(self) -> str:
        return f'Ok({self._val!r})' if self._is_ok else f'Err({self._val!r})'

    def __hash__(self) -> int:
        return hash((self._type, self._is_ok, self._val))

    def __eq__(self, other: object) -> bool:
        return (isinstance(other, self._type)
                and self._is_ok == other._is_ok
                and self._val == other._val)

    def __ne__(self, other: object) -> bool:
        return (not isinstance(other, self._type)
                or self._is_ok != other._is_ok
                or self._val != other._val)

    def __lt__(self: 'Result[SupportsDunderLT, SupportsDunderLT]', other: object) -> bool:
        if isinstance(other, self._type):
            if self._is_ok == other._is_ok:
                return self._val < other._val
            return self._is_ok
        return NotImplemented

    def __le__(self: 'Result[SupportsDunderLE, SupportsDunderLE]', other: object) -> bool:
        if isinstance(other, self._type):
            if self._is_ok == other._is_ok:
                return self._val <= other._val
            return self._is_ok
        return NotImplemented

    def __gt__(self: 'Result[SupportsDunderGT, SupportsDunderGT]', other: object) -> bool:
        if isinstance(other, self._type):
            if self._is_ok == other._is_ok:
                return self._val > other._val
            return other._is_ok
        return NotImplemented

    def __ge__(self: 'Result[SupportsDunderGE, SupportsDunderGE]', other: object) -> bool:
        if isinstance(other, self._type):
            if self._is_ok == other._is_ok:
                return self._val >= other._val
            return other._is_ok
        return NotImplemented


def Ok(val: T) -> Result[T, Any]:
    """Shortcut function for :meth:`Result.Ok`."""
    return Result.Ok(val)


def Err(err: E) -> Result[Any, E]:
    """Shortcut function for :meth:`Result.Err`."""
    return Result.Err(err)


if __name__ == '__main__':
    import doctest

    doctest.testmod()
