import asyncio
import logging

from collections.abc import AsyncIterator
from functools import partial
from pathlib import Path
from typing import Callable, TypeVar, Union
from uuid import UUID

from .reader import JournalOpenMode, JournalReader, JournalEntry, JournalEvent


R = TypeVar("R")
log = logging.getLogger("cysystemd.async_reader")


class Base:
    def __init__(self, loop=None, executor=None):
        self._executor = executor
        self._loop = loop or asyncio.get_event_loop()

    async def _exec(self, func: Callable[..., R], *args, **kwargs) -> R:
        # noinspection PyTypeChecker
        return await self._loop.run_in_executor(
            self._executor, partial(func, *args, **kwargs)
        )


class AsyncJournalReader(Base):
    def __init__(self, executor=None, loop=None):
        super().__init__(loop=loop, executor=executor)
        self._reader = JournalReader()
        self._flags = None
        self._wait_lock = asyncio.Lock()
        self._iter_lock = asyncio.Lock()

    async def wait(self) -> JournalEvent:
        """ Wait for journal events once """

        async with self._wait_lock:
            loop = self._loop
            reader = self._reader
            # Use asyncio.Event to handle multiple set calls without issues
            event = asyncio.Event()
            loop.add_reader(reader.fd, event.set)
            try:
                await event.wait()
            finally:
                loop.remove_reader(reader.fd)
            return reader.process_events()

    async def open(self, flags=JournalOpenMode.CURRENT_USER) -> int:
        self._flags = flags
        return await self._exec(self._reader.open, flags=flags)

    async def open_directory(self, path: Union[str, Path]) -> None:
        return await self._exec(self._reader.open_directory, path)

    async def open_files(self, *file_names: Union[str, Path]) -> None:
        return await self._exec(self._reader.open_files, *file_names)

    @property
    def data_threshold(self):
        return self._reader.data_threshold

    @data_threshold.setter
    def data_threshold(self, size):
        self._reader.data_threshold = size

    @property
    def closed(self):
        return self._reader.closed

    @property
    def locked(self):
        return self._reader.locked

    @property
    def idle(self):
        return self._reader.idle

    async def seek_head(self) -> bool:
        return await self._exec(self._reader.seek_head)

    def __repr__(self):
        return "<%s[%s]: %s>" % (
            self.__class__.__name__,
            self._flags,
            "closed" if self.closed else "opened",
        )

    @property
    def fd(self):
        return self._reader.fd

    @property
    def events(self):
        return self._reader.events

    @property
    def timeout(self):
        return self._reader.timeout

    async def get_catalog(self) -> Path:
        return await self._exec(self._reader.get_catalog)

    async def get_catalog_for_message_id(self, message_id: UUID):
        return await self._exec(
            self._reader.get_catalog_for_message_id, message_id
        )

    async def seek_tail(self) -> bool:
        return await self._exec(self._reader.seek_tail)

    async def seek_monotonic_usec(self, boot_id: UUID, usec: int) -> bool:
        return await self._exec(
            self._reader.seek_monotonic_usec, boot_id, usec
        )

    async def seek_realtime_usec(self, usec: int) -> bool:
        return await self._exec(self._reader.seek_realtime_usec, usec)

    async def seek_cursor(self, cursor: bytes) -> int:
        return await self._exec(self._reader.seek_cursor, cursor)

    async def skip_next(self, skip: int) -> int:
        return await self._exec(self._reader.skip_next, skip)

    async def previous(self, skip: int = 0) -> JournalEntry:
        return await self._exec(self._reader.previous, skip)

    async def skip_previous(self, skip: int) -> int:
        return await self._exec(self._reader.skip_previous, skip)

    async def add_filter(self, rule) -> None:
        return await self._exec(self._reader.add_filter, rule)

    async def clear_filter(self) -> None:
        return await self._exec(self._reader.clear_filter)

    async def next(self, skip=0) -> JournalEntry:
        return await self._exec(self._reader.next, skip)

    async def on_invalidate(self) -> None:
        log.warning("Journal invalidated.")

    async def on_append(self) -> AsyncIterator[JournalEntry]:
        record = await self.next()
        while record:
            yield record
            record = await self.next()

    async def on_nop(self) -> None:
        log.debug("No operation.")

    async def __aiter__(self) -> AsyncIterator[JournalEntry]:
        async with self._iter_lock:
            loop = self._loop
            reader = self._reader
            read_event = asyncio.Event()
            loop.add_reader(reader.fd, read_event.set)

            callbacks = {
                JournalEvent.APPEND: self.on_append,
                JournalEvent.NOP: self.on_nop,
                JournalEvent.INVALIDATE: self.on_invalidate
            }

            async with self._wait_lock:
                while True:
                    try:
                        await read_event.wait()
                    finally:
                        read_event.clear()

                    event = reader.process_events()
                    cb = callbacks.get(event)
                    if not cb:
                        log.warning(f"Unknown event: {event!r}")
                        continue

                    result = cb()

                    if asyncio.iscoroutine(result):
                        await result
                    else:
                        async for record in result:
                            yield record
