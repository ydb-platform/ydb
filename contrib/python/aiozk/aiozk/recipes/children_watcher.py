from typing import ClassVar

from aiozk import WatchEvent

from .base_watcher import BaseWatcher


class ChildrenWatcher(BaseWatcher):
    watched_events: ClassVar = [WatchEvent.CHILDREN_CHANGED, WatchEvent.DELETED]

    async def fetch(self, path):
        children = await self.client.get_children(path=path, watch=True)
        return children
