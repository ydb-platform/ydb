import asyncio
from typing import ClassVar

from .children_watcher import ChildrenWatcher
from .sequential import SequentialRecipe


class Party(SequentialRecipe):
    sub_recipes: ClassVar = {
        'watcher': ChildrenWatcher,
    }

    def __init__(self, base_path, name):
        super().__init__(base_path)

        self.name = name
        self.members = []
        self.change_future = None

    async def join(self):
        await self.create_unique_znode(self.name)
        _, siblings = await self.analyze_siblings()
        self.update_members(siblings)
        self.watcher.add_callback(self.base_path, self.update_members)

    async def wait_for_change(self):
        if not self.change_future or self.change_future.done():
            loop = asyncio.get_running_loop()
            self.change_future = loop.create_future()

        await self.change_future

    async def leave(self):
        self.watcher.remove_callback(self.base_path, self.update_members)
        await self.delete_unique_znode(self.name)

    def update_members(self, raw_sibling_names):
        new_members = [self.determine_znode_label(sibling) for sibling in raw_sibling_names]

        self.members = new_members
        if self.change_future and not self.change_future.done():
            self.change_future.set_result(new_members)
