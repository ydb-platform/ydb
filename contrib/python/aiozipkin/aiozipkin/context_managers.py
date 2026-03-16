from collections.abc import Awaitable as AbcAwaitable
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    Awaitable,
    Generator,
    Generic,
    Optional,
    Type,
    TypeVar,
)


T = TypeVar("T", bound=AsyncContextManager["T"])  # type: ignore


if TYPE_CHECKING:

    class _Base(AsyncContextManager[T], AbcAwaitable[T]):
        pass


else:

    class _Base(Generic[T], AsyncContextManager, AbcAwaitable):
        pass


class _ContextManager(_Base[T]):

    __slots__ = ("_coro", "_obj")

    def __init__(self, coro: Awaitable[T]) -> None:
        super().__init__()
        self._coro: Awaitable[T] = coro
        self._obj: Optional[T] = None

    def __await__(self) -> Generator[Any, None, T]:
        return self._coro.__await__()

    async def __aenter__(self) -> T:
        self._obj = await self._coro
        t: T = await self._obj.__aenter__()
        return t

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if self._obj is None:
            raise RuntimeError("__aexit__ called before __aenter__")
        return await self._obj.__aexit__(exc_type, exc, tb)
