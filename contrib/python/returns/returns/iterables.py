from abc import abstractmethod
from collections.abc import Callable, Iterable
from typing import TypeVar, final

from returns.interfaces.applicative import ApplicativeN
from returns.interfaces.failable import FailableN
from returns.primitives.hkt import KindN, kinded

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_ApplicativeKind = TypeVar('_ApplicativeKind', bound=ApplicativeN)
_FailableKind = TypeVar('_FailableKind', bound=FailableN)


class AbstractFold:
    """
    A collection of different helpers to write declarative ``Iterable`` actions.

    Allows to work with iterables.

    .. rubric:: Implementation

    ``AbstractFold`` and ``Fold`` types are special.
    They have double definition for each method: public and protected ones.
    Why?

    Because you cannot override ``@kinded`` method due to a ``mypy`` bug.
    So, there are two opportunities for us here:

    1. Declare all method as ``@final`` and do not allow to change anything
    2. Use delegation to protected unkinded methods

    We have chosen the second way! Here's how it works:

    1. Public methods are ``@kinded`` for better typing and cannot be overridden
    2. Protected methods are unkinded and can be overridden in subtyping

    Now, if you need to make a change into our implementation,
    then you can subclass ``Fold`` or ``AbstractFold`` and then
    change an implementation of any unkinded protected method.
    """

    __slots__ = ()

    @final
    @kinded
    @classmethod
    def loop(
        cls,
        iterable: Iterable[
            KindN[_ApplicativeKind, _FirstType, _SecondType, _ThirdType],
        ],
        acc: KindN[_ApplicativeKind, _UpdatedType, _SecondType, _ThirdType],
        function: Callable[
            [_FirstType],
            Callable[[_UpdatedType], _UpdatedType],
        ],
    ) -> KindN[_ApplicativeKind, _UpdatedType, _SecondType, _ThirdType]:
        """
        Allows to make declarative loops for any ``ApplicativeN`` subtypes.

        Quick example:

        .. code:: python

          >>> from typing import Callable
          >>> from returns.maybe import Some
          >>> from returns.iterables import Fold

          >>> def sum_two(first: int) -> Callable[[int], int]:
          ...     return lambda second: first + second

          >>> assert Fold.loop(
          ...     [Some(1), Some(2), Some(3)],
          ...     Some(10),
          ...     sum_two,
          ... ) == Some(16)

        Looks like ``foldl`` in some other languages with some more specifics.
        See: https://philipschwarz.dev/fpilluminated/?page_id=348#bwg3/137

        .. image:: https://i.imgur.com/Tza1isS.jpg

        Is also quite similar to ``reduce``.

        Public interface for ``_loop`` method. Cannot be modified directly.
        """
        return cls._loop(iterable, acc, function, _concat_applicative)

    @final
    @kinded
    @classmethod
    def collect(
        cls,
        iterable: Iterable[
            KindN[_ApplicativeKind, _FirstType, _SecondType, _ThirdType],
        ],
        acc: KindN[
            _ApplicativeKind,
            'tuple[_FirstType, ...]',
            _SecondType,
            _ThirdType,
        ],
    ) -> KindN[
        _ApplicativeKind,
        'tuple[_FirstType, ...]',
        _SecondType,
        _ThirdType,
    ]:
        """
        Transforms an iterable of containers into a single container.

        Quick example for regular containers:

        .. code:: python

          >>> from returns.io import IO
          >>> from returns.iterables import Fold

          >>> items = [IO(1), IO(2)]
          >>> assert Fold.collect(items, IO(())) == IO((1, 2))

        If container can have failed values,
        then this strategy fails on any existing failed like type.

        It is enough to have even a single failed value in iterable
        for this type to convert the whole operation result to be a failure.
        Let's see how it works:

        .. code:: python

          >>> from returns.result import Success, Failure
          >>> from returns.iterables import Fold

          >>> empty = []
          >>> all_success = [Success(1), Success(2), Success(3)]
          >>> has_failure = [Success(1), Failure('a'), Success(3)]
          >>> all_failures = [Failure('a'), Failure('b')]

          >>> acc = Success(())  # empty tuple

          >>> assert Fold.collect(empty, acc) == Success(())
          >>> assert Fold.collect(all_success, acc) == Success((1, 2, 3))
          >>> assert Fold.collect(has_failure, acc) == Failure('a')
          >>> assert Fold.collect(all_failures, acc) == Failure('a')

        If that's now what you need, check out
        :meth:`~AbstractFold.collect_all`
        to force collect all non-failed values.

        Public interface for ``_collect`` method. Cannot be modified directly.
        """
        return cls._collect(iterable, acc)

    @final
    @kinded
    @classmethod
    def collect_all(
        cls,
        iterable: Iterable[
            KindN[_FailableKind, _FirstType, _SecondType, _ThirdType],
        ],
        acc: KindN[
            _FailableKind,
            'tuple[_FirstType, ...]',
            _SecondType,
            _ThirdType,
        ],
    ) -> KindN[
        _FailableKind,
        'tuple[_FirstType, ...]',
        _SecondType,
        _ThirdType,
    ]:
        """
        Transforms an iterable of containers into a single container.

        This method only works with ``FailableN`` subtypes,
        not just any ``ApplicativeN`` like :meth:`~AbstractFold.collect`.

        Strategy to extract all successful values
        even if there are failed values.

        If there's at least one successful value
        and any amount of failed values,
        we will still return all collected successful values.

        We can return failed value for this strategy only in a single case:
        when default element is a failed value.

        Let's see how it works:

        .. code:: python

          >>> from returns.result import Success, Failure
          >>> from returns.iterables import Fold

          >>> empty = []
          >>> all_success = [Success(1), Success(2), Success(3)]
          >>> has_failure = [Success(1), Failure('a'), Success(3)]
          >>> all_failures = [Failure('a'), Failure('b')]

          >>> acc = Success(())  # empty tuple

          >>> assert Fold.collect_all(empty, acc) == Success(())
          >>> assert Fold.collect_all(all_success, acc) == Success((1, 2, 3))
          >>> assert Fold.collect_all(has_failure, acc) == Success((1, 3))
          >>> assert Fold.collect_all(all_failures, acc) == Success(())
          >>> assert Fold.collect_all(empty, Failure('c')) == Failure('c')

        If that's now what you need, check out :meth:`~AbstractFold.collect`
        to collect only successful values and fail on any failed ones.

        Public interface for ``_collect_all`` method.
        Cannot be modified directly.
        """
        return cls._collect_all(iterable, acc)

    # Protected part
    # ==============

    @classmethod
    @abstractmethod
    def _loop(
        cls,
        iterable: Iterable[
            KindN[_ApplicativeKind, _FirstType, _SecondType, _ThirdType],
        ],
        acc: KindN[_ApplicativeKind, _UpdatedType, _SecondType, _ThirdType],
        function: Callable[
            [_FirstType],
            Callable[[_UpdatedType], _UpdatedType],
        ],
        concat: Callable[
            [
                KindN[_ApplicativeKind, _FirstType, _SecondType, _ThirdType],
                KindN[_ApplicativeKind, _UpdatedType, _SecondType, _ThirdType],
                KindN[
                    _ApplicativeKind,
                    Callable[
                        [_FirstType],
                        Callable[[_UpdatedType], _UpdatedType],
                    ],
                    _SecondType,
                    _ThirdType,
                ],
            ],
            KindN[_ApplicativeKind, _UpdatedType, _SecondType, _ThirdType],
        ],
    ) -> KindN[_ApplicativeKind, _UpdatedType, _SecondType, _ThirdType]:
        """
        Protected part of ``loop`` method.

        Can be replaced in subclasses for better performance, etc.
        """

    @classmethod
    def _collect(
        cls,
        iterable: Iterable[
            KindN[_ApplicativeKind, _FirstType, _SecondType, _ThirdType],
        ],
        acc: KindN[
            _ApplicativeKind,
            'tuple[_FirstType, ...]',
            _SecondType,
            _ThirdType,
        ],
    ) -> KindN[
        _ApplicativeKind,
        'tuple[_FirstType, ...]',
        _SecondType,
        _ThirdType,
    ]:
        return cls._loop(
            iterable,
            acc,
            _concat_sequence,
            _concat_applicative,
        )

    @classmethod
    def _collect_all(
        cls,
        iterable: Iterable[
            KindN[_FailableKind, _FirstType, _SecondType, _ThirdType],
        ],
        acc: KindN[
            _FailableKind,
            'tuple[_FirstType, ...]',
            _SecondType,
            _ThirdType,
        ],
    ) -> KindN[
        _FailableKind,
        'tuple[_FirstType, ...]',
        _SecondType,
        _ThirdType,
    ]:
        return cls._loop(
            iterable,
            acc,
            _concat_sequence,
            _concat_failable_safely,
        )


class Fold(AbstractFold):
    """
    Concrete implementation of ``AbstractFold`` of end users.

    Use it by default.
    """

    __slots__ = ()

    @classmethod
    def _loop(
        cls,
        iterable: Iterable[
            KindN[_ApplicativeKind, _FirstType, _SecondType, _ThirdType],
        ],
        acc: KindN[_ApplicativeKind, _UpdatedType, _SecondType, _ThirdType],
        function: Callable[
            [_FirstType],
            Callable[[_UpdatedType], _UpdatedType],
        ],
        concat: Callable[
            [
                KindN[_ApplicativeKind, _FirstType, _SecondType, _ThirdType],
                KindN[_ApplicativeKind, _UpdatedType, _SecondType, _ThirdType],
                KindN[
                    _ApplicativeKind,
                    Callable[
                        [_FirstType],
                        Callable[[_UpdatedType], _UpdatedType],
                    ],
                    _SecondType,
                    _ThirdType,
                ],
            ],
            KindN[_ApplicativeKind, _UpdatedType, _SecondType, _ThirdType],
        ],
    ) -> KindN[_ApplicativeKind, _UpdatedType, _SecondType, _ThirdType]:
        """
        Protected part of ``loop`` method.

        Can be replaced in subclasses for better performance, etc.
        """
        wrapped = acc.from_value(function)
        for current in iterable:
            acc = concat(current, acc, wrapped)
        return acc


# Helper functions
# ================


def _concat_sequence(
    first: _FirstType,
) -> Callable[
    ['tuple[_FirstType, ...]'],
    'tuple[_FirstType, ...]',
]:
    """
    Concats a given item to an existing sequence.

    We use explicit curring with ``lambda`` function because,
    ``@curry`` decorator is way slower. And we don't need its features here.
    But, your functions can use ``@curry`` if you need it.
    """
    return lambda second: (*second, first)


def _concat_applicative(
    current: KindN[
        _ApplicativeKind,
        _FirstType,
        _SecondType,
        _ThirdType,
    ],
    acc: KindN[
        _ApplicativeKind,
        _UpdatedType,
        _SecondType,
        _ThirdType,
    ],
    function: KindN[
        _ApplicativeKind,
        Callable[[_FirstType], Callable[[_UpdatedType], _UpdatedType]],
        _SecondType,
        _ThirdType,
    ],
) -> KindN[_ApplicativeKind, _UpdatedType, _SecondType, _ThirdType]:
    """Concats two applicatives using a curried-like function."""
    return acc.apply(current.apply(function))


def _concat_failable_safely(
    current: KindN[
        _FailableKind,
        _FirstType,
        _SecondType,
        _ThirdType,
    ],
    acc: KindN[
        _FailableKind,
        _UpdatedType,
        _SecondType,
        _ThirdType,
    ],
    function: KindN[
        _FailableKind,
        Callable[[_FirstType], Callable[[_UpdatedType], _UpdatedType]],
        _SecondType,
        _ThirdType,
    ],
) -> KindN[_FailableKind, _UpdatedType, _SecondType, _ThirdType]:
    """
    Concats two ``FailableN`` using a curried-like function and a fallback.

    We need both ``.apply`` and ``.lash`` methods here.
    """
    return _concat_applicative(current, acc, function).lash(lambda _: acc)
