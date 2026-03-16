from __future__ import annotations

import inspect
import logging
import sys
import time
from collections import defaultdict
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Self, TypeVar

from zarr.abc.store import Store
from zarr.storage._wrapper import WrapperStore

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Iterable

    from zarr.abc.store import ByteRequest
    from zarr.core.buffer import Buffer, BufferPrototype

    counter: defaultdict[str, int]

T_Store = TypeVar("T_Store", bound=Store)


class LoggingStore(WrapperStore[T_Store]):
    """
    Store that logs all calls to another wrapped store.

    Parameters
    ----------
    store : Store
        Store to wrap
    log_level : str
        Log level
    log_handler : logging.Handler
        Log handler

    Attributes
    ----------
    counter : dict
        Counter of number of times each method has been called
    """

    counter: defaultdict[str, int]

    def __init__(
        self,
        store: T_Store,
        log_level: str = "DEBUG",
        log_handler: logging.Handler | None = None,
    ) -> None:
        super().__init__(store)
        self.counter = defaultdict(int)
        self.log_level = log_level
        self.log_handler = log_handler
        self._configure_logger(log_level, log_handler)

    def _configure_logger(
        self, log_level: str = "DEBUG", log_handler: logging.Handler | None = None
    ) -> None:
        self.log_level = log_level
        self.logger = logging.getLogger(f"LoggingStore({self._store})")
        self.logger.setLevel(log_level)

        if not self.logger.hasHandlers():
            if not log_handler:
                log_handler = self._default_handler()
            # Add handler to logger
            self.logger.addHandler(log_handler)

    def _default_handler(self) -> logging.Handler:
        """Define a default log handler"""
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setLevel(self.log_level)
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        return handler

    @contextmanager
    def log(self, hint: Any = "") -> Generator[None, None, None]:
        """Context manager to log method calls

        Each call to the wrapped store is logged to the configured logger and added to
        the counter dict.
        """
        method = inspect.stack()[2].function
        op = f"{type(self._store).__name__}.{method}"
        if hint:
            op = f"{op}({hint})"
        self.logger.info(" Calling %s", op)
        start_time = time.time()
        try:
            self.counter[method] += 1
            yield
        finally:
            end_time = time.time()
            self.logger.info("Finished %s [%.2f s]", op, end_time - start_time)

    @classmethod
    async def open(cls: type[Self], store_cls: type[T_Store], *args: Any, **kwargs: Any) -> Self:
        log_level = kwargs.pop("log_level", "DEBUG")
        log_handler = kwargs.pop("log_handler", None)
        store = store_cls(*args, **kwargs)
        await store._open()
        return cls(store=store, log_level=log_level, log_handler=log_handler)

    @property
    def supports_writes(self) -> bool:
        with self.log():
            return self._store.supports_writes

    @property
    def supports_deletes(self) -> bool:
        with self.log():
            return self._store.supports_deletes

    @property
    def supports_listing(self) -> bool:
        with self.log():
            return self._store.supports_listing

    @property
    def read_only(self) -> bool:
        with self.log():
            return self._store.read_only

    @property
    def _is_open(self) -> bool:
        with self.log():
            return self._store._is_open

    @_is_open.setter
    def _is_open(self, value: bool) -> None:
        raise NotImplementedError("LoggingStore must be opened via the `_open` method")

    async def _open(self) -> None:
        with self.log():
            return await self._store._open()

    async def _ensure_open(self) -> None:
        with self.log():
            return await self._store._ensure_open()

    async def is_empty(self, prefix: str = "") -> bool:
        # docstring inherited
        with self.log():
            return await self._store.is_empty(prefix=prefix)

    async def clear(self) -> None:
        # docstring inherited
        with self.log():
            return await self._store.clear()

    def __str__(self) -> str:
        return f"logging-{self._store}"

    def __repr__(self) -> str:
        return f"LoggingStore({self._store.__class__.__name__}, '{self._store}')"

    def __eq__(self, other: object) -> bool:
        with self.log(other):
            return type(self) is type(other) and self._store.__eq__(other._store)  # type: ignore[attr-defined]

    async def get(
        self,
        key: str,
        prototype: BufferPrototype,
        byte_range: ByteRequest | None = None,
    ) -> Buffer | None:
        # docstring inherited
        with self.log(key):
            return await self._store.get(key=key, prototype=prototype, byte_range=byte_range)

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRequest | None]],
    ) -> list[Buffer | None]:
        # docstring inherited
        keys = ",".join([k[0] for k in key_ranges])
        with self.log(keys):
            return await self._store.get_partial_values(prototype=prototype, key_ranges=key_ranges)

    async def exists(self, key: str) -> bool:
        # docstring inherited
        with self.log(key):
            return await self._store.exists(key)

    async def set(self, key: str, value: Buffer) -> None:
        # docstring inherited
        with self.log(key):
            return await self._store.set(key=key, value=value)

    async def set_if_not_exists(self, key: str, value: Buffer) -> None:
        # docstring inherited
        with self.log(key):
            return await self._store.set_if_not_exists(key=key, value=value)

    async def delete(self, key: str) -> None:
        # docstring inherited
        with self.log(key):
            return await self._store.delete(key=key)

    async def list(self) -> AsyncGenerator[str, None]:
        # docstring inherited
        with self.log():
            async for key in self._store.list():
                yield key

    async def list_prefix(self, prefix: str) -> AsyncGenerator[str, None]:
        # docstring inherited
        with self.log(prefix):
            async for key in self._store.list_prefix(prefix=prefix):
                yield key

    async def list_dir(self, prefix: str) -> AsyncGenerator[str, None]:
        # docstring inherited
        with self.log(prefix):
            async for key in self._store.list_dir(prefix=prefix):
                yield key

    async def delete_dir(self, prefix: str) -> None:
        # docstring inherited
        with self.log(prefix):
            await self._store.delete_dir(prefix=prefix)

    async def getsize(self, key: str) -> int:
        with self.log(key):
            return await self._store.getsize(key)

    async def getsize_prefix(self, prefix: str) -> int:
        with self.log(prefix):
            return await self._store.getsize_prefix(prefix)
