from collections.abc import Callable
from typing import TypeVar

from returns.interfaces.specific.ioresult import IOResultLikeN
from returns.primitives.hkt import Kinded, KindN, kinded
from returns.result import Result

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_IOResultLikeType = TypeVar('_IOResultLikeType', bound=IOResultLikeN)


def managed(
    use: Callable[
        [_FirstType],
        KindN[_IOResultLikeType, _UpdatedType, _SecondType, _ThirdType],
    ],
    release: Callable[
        [_FirstType, Result[_UpdatedType, _SecondType]],
        KindN[_IOResultLikeType, None, _SecondType, _ThirdType],
    ],
) -> Kinded[
    Callable[
        [KindN[_IOResultLikeType, _FirstType, _SecondType, _ThirdType]],
        KindN[_IOResultLikeType, _UpdatedType, _SecondType, _ThirdType],
    ]
]:
    """
    Allows to run managed computation.

    Managed computations consist of three steps:

    1. ``acquire`` when we get some initial resource to work with
    2. ``use`` when the main logic is done
    3. ``release`` when we release acquired resource

    Let's look at the example:

    1. We need to acquire an opened file to read it later
    2. We need to use acquired file to read its content
    3. We need to release the acquired file in the end

    Here's a code example:

    .. code:: python

      >>> from returns.pipeline import managed
      >>> from returns.io import IOSuccess, IOFailure, impure_safe

      >>> class Lock(object):
      ...     '''Example class to emulate state to acquire and release.'''
      ...     def __init__(self, default: bool = False) -> None:
      ...         self.set = default
      ...     def __eq__(self, lock) -> bool:  # we need this for testing
      ...         return self.set == lock.set
      ...     def release(self) -> None:
      ...         self.set = False

      >>> pipeline = managed(
      ...     lambda lock: IOSuccess(lock) if lock.set else IOFailure(False),
      ...     lambda lock, use_result: impure_safe(lock.release)(),
      ... )

      >>> assert pipeline(IOSuccess(Lock(True))) == IOSuccess(Lock(False))
      >>> assert pipeline(IOSuccess(Lock())) == IOFailure(False)
      >>> assert pipeline(IOFailure('no lock')) == IOFailure('no lock')

    See also:
        - https://github.com/gcanti/fp-ts/blob/master/src/IOEither.ts
        - https://zio.dev/docs/datatypes/datatypes_managed

    .. rubric:: Implementation

    This class requires some explanation.

    First of all, we modeled this function as a class,
    so it can be partially applied easily.

    Secondly, we used imperative approach of programming inside this class.
    Functional approached was 2 times slower.
    And way more complex to read and understand.

    Lastly, we try to hide these two things for the end user.
    We pretend that this is not a class, but a function.
    We also do not break a functional abstraction for the end user.
    It is just an implementation detail.

    Type inference does not work so well with ``lambda`` functions.
    But, we do not recommend to use this function with ``lambda`` functions.

    """

    @kinded
    def factory(
        acquire: KindN[_IOResultLikeType, _FirstType, _SecondType, _ThirdType],
    ) -> KindN[_IOResultLikeType, _UpdatedType, _SecondType, _ThirdType]:
        return acquire.bind(_use(acquire, use, release))

    return factory


def _use(
    acquire: KindN[_IOResultLikeType, _FirstType, _SecondType, _ThirdType],
    use: Callable[
        [_FirstType],
        KindN[_IOResultLikeType, _UpdatedType, _SecondType, _ThirdType],
    ],
    release: Callable[
        [_FirstType, Result[_UpdatedType, _SecondType]],
        KindN[_IOResultLikeType, None, _SecondType, _ThirdType],
    ],
) -> Callable[
    [_FirstType],
    KindN[_IOResultLikeType, _UpdatedType, _SecondType, _ThirdType],
]:
    """Uses the resource after it is acquired successfully."""
    return lambda initial: use(initial).compose_result(
        _release(acquire, initial, release),
    )


def _release(
    acquire: KindN[_IOResultLikeType, _FirstType, _SecondType, _ThirdType],
    initial: _FirstType,
    release: Callable[
        [_FirstType, Result[_UpdatedType, _SecondType]],
        KindN[_IOResultLikeType, None, _SecondType, _ThirdType],
    ],
) -> Callable[
    [Result[_UpdatedType, _SecondType]],
    KindN[_IOResultLikeType, _UpdatedType, _SecondType, _ThirdType],
]:
    """Release handler. Does its job after resource is acquired and used."""
    return lambda updated: release(initial, updated).bind(
        lambda _: acquire.from_result(updated),  # noqa: WPS430
    )
