import asyncio
import collections
import inspect
import logging
from typing import ClassVar

from aiozk import WatchEvent, exc

from .recipe import Recipe


log = logging.getLogger(__name__)


class BaseWatcher(Recipe):
    watched_events: ClassVar = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.callbacks = collections.defaultdict(set)
        self.loops = {}

    def add_callback(self, path, callback):
        self.callbacks[path].add(callback)

        if path not in self.loops:
            self.loops[path] = asyncio.create_task(self.watch_loop(path))

    def remove_callback(self, path, callback):
        self.callbacks[path].discard(callback)

        if not self.callbacks[path]:
            self.callbacks.pop(path)
            self.loops.pop(path).cancel()

    async def fetch(self, path):
        raise NotImplementedError

    async def watch_loop(self, path):
        while self.callbacks[path]:
            log.debug('Fetching data for %s', path)
            watch_future = self.client.wait_for_events(self.watched_events, path)
            try:
                result = await self.fetch(path)
            except exc.NoNode:
                result = exc.NoNode
            except exc.ZKError as e:
                log.exception('Exception in watch loop: %s', e)
                log.info('Waiting for safe state...')
                await self.client.session.ensure_safe_state()
                continue
            except Exception:
                log.exception('Not handled in watch loop:')
                raise

            for callback in self.callbacks[path].copy():
                maybe_coroutine = callback(result)
                if inspect.iscoroutine(maybe_coroutine):
                    _ = asyncio.create_task(maybe_coroutine)  # noqa: RUF006
            if WatchEvent.CREATED not in self.watched_events and result == exc.NoNode:
                return
            try:
                await watch_future
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.exception('Not handled in wait_for_events:')
                print('Not handled: {!r}'.format(e))
                raise
