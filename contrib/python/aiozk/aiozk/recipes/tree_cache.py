import asyncio
import logging
from typing import ClassVar

from ..exc import NoNode
from .children_watcher import ChildrenWatcher
from .data_watcher import DataWatcher
from .recipe import Recipe


log = logging.getLogger(__name__)


class TreeCache(Recipe):
    sub_recipes: ClassVar = {
        'data_watcher': DataWatcher,
        'child_watcher': ChildrenWatcher,
    }

    def __init__(self, base_path, defaults=None):
        super().__init__(base_path)
        self.defaults = defaults or {}

        self.root = None

    async def start(self):
        log.debug('Starting znode tree cache at %s', self.base_path)

        self.root = ZNodeCache(
            self.base_path,
            self.defaults,
            self.client,
            self.data_watcher,
            self.child_watcher,
        )

        await self.ensure_path()

        await self.root.start()

    async def stop(self):
        await self.root.stop()

    def __getattr__(self, attribute):
        return getattr(self.root, attribute)

    def as_dict(self):
        return self.root.as_dict()


class ZNodeCache:
    def __init__(self, path, defaults, client, data_watcher, child_watcher):
        self.path = path

        self.client = client
        self.defaults = defaults

        self.data_watcher = data_watcher
        self.child_watcher = child_watcher

        self.children = {}
        self.data = None

    @property
    def dot_path(self):
        return self.path[1:].replace('/', '.')

    @property
    def value(self):
        return self.data

    def __getattr__(self, name):
        if name not in self.children:
            raise AttributeError

        return self.children[name]

    async def start(self):
        self.data = await self.client.get_data(self.path)
        for child in await self.client.get_children(self.path):
            self.children[child] = ZNodeCache(
                self.path + '/' + child,
                self.defaults.get(child, {}),
                self.client,
                self.data_watcher,
                self.child_watcher,
            )

        await asyncio.gather(*(child.start() for child in self.children.values()))

        self.data_watcher.add_callback(self.path, self.data_callback)
        self.child_watcher.add_callback(self.path, self.child_callback)

    async def stop(self):
        await asyncio.sleep(0.02)
        await asyncio.gather(*(child.stop() for child in self.children.values()))
        self.data_watcher.remove_callback(self.path, self.data_callback)
        self.child_watcher.remove_callback(self.path, self.child_callback)

    async def child_callback(self, new_children):
        if new_children == NoNode:
            return
        removed_children = set(self.children.keys()) - set(new_children)
        added_children = set(new_children) - set(self.children.keys())

        for removed in removed_children:
            log.debug('Removed child %s', self.dot_path + '.' + removed)
            child = self.children.pop(removed)
            await child.stop()

        for added in added_children:
            log.debug('Added child %s', self.dot_path + '.' + added)
            self.children[added] = ZNodeCache(
                self.path + '/' + added,
                self.defaults.get(added, {}),
                self.client,
                self.data_watcher,
                self.child_watcher,
            )
        await asyncio.gather(*(self.children[added].start() for added in added_children))

    async def data_callback(self, data):
        log.debug('New value for %s: %r', self.dot_path, data)
        if data == NoNode:
            return
        self.data = data

    def as_dict(self):
        if self.children:
            return {child_path: child_znode.as_dict() for child_path, child_znode in self.children.items()}
        return self.data
