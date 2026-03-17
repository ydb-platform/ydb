from types import TracebackType
from typing import (
    Any,
    AsyncContextManager,
    Coroutine,
    Generator,
    Optional,
    Type,
    TypeVar,
)

T = TypeVar("T", bound=AsyncContextManager["T"])  # type: ignore[misc]


class _ContextManager(Coroutine[T, None, T]):
    __slots__ = ("_coro", "_obj")

    def __init__(self, coro: Coroutine[T, None, T]) -> None:
        self._coro = coro
        self._obj: Optional[T] = None

    def send(self, arg: Any) -> T:
        return self._coro.send(arg)  # pragma: no cover

    def throw(self, *args) -> T:  # type: ignore[no-untyped-def]
        return self._coro.throw(*args)  # pragma: no cover

    def close(self) -> None:
        return self._coro.close()  # pragma: no cover

    @property
    def gi_frame(self) -> Any:  # type: ignore[misc]
        return self._coro.gi_frame  # type: ignore[attr-defined]  # pragma: no cover

    @property
    def gi_running(self) -> Any:  # type: ignore[misc]
        return self._coro.gi_running  # type: ignore[attr-defined]  # pragma: no cover

    @property
    def gi_code(self) -> Any:  # type: ignore[misc]
        return self._coro.gi_code  # type: ignore[attr-defined]  # pragma: no cover

    def __next__(self) -> T:
        return self.send(None)  # pragma: no cover

    def __await__(self) -> Generator[T, None, T]:
        return self._coro.__await__()

    async def __aenter__(self) -> T:
        self._obj = await self._coro
        return await self._obj.__aenter__()  # type: ignore[no-any-return]

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if self._obj is None:  # pragma: no cover
            return False
        return await self._obj.__aexit__(exc_type, exc, tb)
