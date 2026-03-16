from collections.abc import Awaitable, Callable
from typing import TypeVar

from returns.future import Future
from returns.interfaces.specific.future import FutureLikeN
from returns.primitives.hkt import Kinded, KindN, kinded

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_FutureKind = TypeVar('_FutureKind', bound=FutureLikeN)


def bind_async_future(
    function: Callable[
        [_FirstType],
        Awaitable[Future[_UpdatedType]],
    ],
) -> Kinded[
    Callable[
        [KindN[_FutureKind, _FirstType, _SecondType, _ThirdType]],
        KindN[_FutureKind, _UpdatedType, _SecondType, _ThirdType],
    ]
]:
    """
    Compose a container and async function returning ``Future``.

    In other words, it modifies the function
    signature from:
    ``a -> Awaitable[Future[b]]``
    to:
    ``Container[a] -> Container[b]``

    This is how it should be used:

    .. code:: python

      >>> import anyio
      >>> from returns.pointfree import bind_async_future
      >>> from returns.future import Future
      >>> from returns.io import IO

      >>> async def example(argument: int) -> Future[int]:
      ...     return Future.from_value(argument + 1)

      >>> assert anyio.run(
      ...     bind_async_future(example)(Future.from_value(1)).awaitable,
      ... ) == IO(2)

    Note, that this function works
    for all containers with ``.bind_async_future`` method.
    See :class:`returns.primitives.interfaces.specific.future.FutureLikeN`
    for more info.

    """

    @kinded
    def factory(
        container: KindN[_FutureKind, _FirstType, _SecondType, _ThirdType],
    ) -> KindN[_FutureKind, _UpdatedType, _SecondType, _ThirdType]:
        return container.bind_async_future(function)

    return factory
