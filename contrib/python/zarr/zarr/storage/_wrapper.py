from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Iterable
    from types import TracebackType
    from typing import Any, Self

    from zarr.abc.buffer import Buffer
    from zarr.abc.store import ByteRequest
    from zarr.core.buffer import BufferPrototype

from zarr.abc.store import Store

T_Store = TypeVar("T_Store", bound=Store)


class WrapperStore(Store, Generic[T_Store]):
    """
    Store that wraps an existing Store.

    By default all of the store methods are delegated to the wrapped store instance, which is
    accessible via the ``._store`` attribute of this class.

    Use this class to modify or extend the behavior of the other store classes.
    """

    _store: T_Store

    def __init__(self, store: T_Store) -> None:
        self._store = store

    @classmethod
    async def open(cls: type[Self], store_cls: type[T_Store], *args: Any, **kwargs: Any) -> Self:
        store = store_cls(*args, **kwargs)
        await store._open()
        return cls(store=store)

    def __enter__(self) -> Self:
        return type(self)(self._store.__enter__())

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return self._store.__exit__(exc_type, exc_value, traceback)

    async def _open(self) -> None:
        await self._store._open()

    async def _ensure_open(self) -> None:
        await self._store._ensure_open()

    async def is_empty(self, prefix: str) -> bool:
        return await self._store.is_empty(prefix)

    @property
    def _is_open(self) -> bool:
        return self._store._is_open

    @_is_open.setter
    def _is_open(self, value: bool) -> None:
        raise NotImplementedError("WrapperStore must be opened via the `_open` method")

    async def clear(self) -> None:
        return await self._store.clear()

    @property
    def read_only(self) -> bool:
        return self._store.read_only

    def _check_writable(self) -> None:
        return self._store._check_writable()

    def __eq__(self, value: object) -> bool:
        return type(self) is type(value) and self._store.__eq__(value._store)  # type: ignore[attr-defined]

    def __str__(self) -> str:
        return f"wrapping-{self._store}"

    def __repr__(self) -> str:
        return f"WrapperStore({self._store.__class__.__name__}, '{self._store}')"

    async def get(
        self, key: str, prototype: BufferPrototype, byte_range: ByteRequest | None = None
    ) -> Buffer | None:
        return await self._store.get(key, prototype, byte_range)

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRequest | None]],
    ) -> list[Buffer | None]:
        return await self._store.get_partial_values(prototype, key_ranges)

    async def exists(self, key: str) -> bool:
        return await self._store.exists(key)

    async def set(self, key: str, value: Buffer) -> None:
        await self._store.set(key, value)

    async def set_if_not_exists(self, key: str, value: Buffer) -> None:
        return await self._store.set_if_not_exists(key, value)

    async def _set_many(self, values: Iterable[tuple[str, Buffer]]) -> None:
        await self._store._set_many(values)

    @property
    def supports_writes(self) -> bool:
        return self._store.supports_writes

    @property
    def supports_deletes(self) -> bool:
        return self._store.supports_deletes

    async def delete(self, key: str) -> None:
        await self._store.delete(key)

    @property
    def supports_listing(self) -> bool:
        return self._store.supports_listing

    def list(self) -> AsyncIterator[str]:
        return self._store.list()

    def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        return self._store.list_prefix(prefix)

    def list_dir(self, prefix: str) -> AsyncIterator[str]:
        return self._store.list_dir(prefix)

    async def delete_dir(self, prefix: str) -> None:
        return await self._store.delete_dir(prefix)

    def close(self) -> None:
        self._store.close()

    async def _get_many(
        self, requests: Iterable[tuple[str, BufferPrototype, ByteRequest | None]]
    ) -> AsyncGenerator[tuple[str, Buffer | None], None]:
        async for req in self._store._get_many(requests):
            yield req
