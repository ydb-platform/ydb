from collections.abc import Awaitable, Callable
from typing import TypeVar

from returns.interfaces.specific.future import FutureLikeN
from returns.primitives.hkt import Kinded, KindN, kinded

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_FutureKind = TypeVar('_FutureKind', bound=FutureLikeN)


def bind_async(
    function: Callable[
        [_FirstType],
        Awaitable[KindN[_FutureKind, _UpdatedType, _SecondType, _ThirdType]],
    ],
) -> Kinded[
    Callable[
        [KindN[_FutureKind, _FirstType, _SecondType, _ThirdType]],
        KindN[_FutureKind, _UpdatedType, _SecondType, _ThirdType],
    ]
]:
    """
    Compose a container and ``async`` function returning a container.

    In other words, it modifies the function's
    signature from:
    ``a -> Awaitable[Container[b]]``
    to:
    ``Container[a] -> Container[b]``

    This is how it should be used:

    .. code:: python

        >>> import anyio
        >>> from returns.future import Future
        >>> from returns.io import IO
        >>> from returns.pointfree import bind_async

        >>> async def coroutine(x: int) -> Future[str]:
        ...    return Future.from_value(str(x + 1))

        >>> bound = bind_async(coroutine)(Future.from_value(1))
        >>> assert anyio.run(bound.awaitable) == IO('2')

    Note, that this function works
    for all containers with ``.bind_async`` method.
    See :class:`returns.primitives.interfaces.specific.future.FutureLikeN`
    for more info.

    """

    @kinded
    def factory(
        container: KindN[_FutureKind, _FirstType, _SecondType, _ThirdType],
    ) -> KindN[_FutureKind, _UpdatedType, _SecondType, _ThirdType]:
        return container.bind_async(function)

    return factory
