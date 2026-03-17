from typing import ClassVar

from aiozk import WatchEvent
from aiozk.exc import NoNode

from .base_watcher import BaseWatcher


class DataWatcher(BaseWatcher):
    watched_events: ClassVar = [WatchEvent.DATA_CHANGED, WatchEvent.DELETED]

    def __init__(self, *args, wait_for_create=False, **kwargs):
        super().__init__(*args, **kwargs)

        if wait_for_create:
            self.watched_events.append(WatchEvent.CREATED)

    async def fetch(self, path):
        # exists() gives create, delete, and update watches
        watch_via_exists = WatchEvent.CREATED in self.watched_events
        if watch_via_exists:
            exists = await self.client.exists(path, watch=True)
            if not exists:
                raise NoNode
        data = await self.client.get_data(path=path, watch=not watch_via_exists)
        return data
