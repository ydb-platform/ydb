import asyncio
import collections
import itertools
import json
from typing import ClassVar

from .data_watcher import DataWatcher
from .lock import Lock
from .party import Party
from .recipe import Recipe


class Allocator(Recipe):
    sub_recipes: ClassVar = {
        'party': (Party, ['member_path', 'name']),
        'lock': (Lock, ['lock_path']),
        'data_watcher': DataWatcher,
    }

    def __init__(self, base_path, name, allocator_fn=None):
        self.name = name

        super().__init__(base_path)

        if allocator_fn is None:
            allocator_fn = round_robin

        self.allocator_fn = allocator_fn

        self.active = False

        self.full_allocation = collections.defaultdict(set)
        self.full_set = set()

    @property
    def lock_path(self):
        return self.base_path + '/lock'

    @property
    def member_path(self):
        return self.base_path + '/members'

    @property
    def allocation(self):
        return self.full_allocation[self.name]

    def validate(self, new_allocation):
        as_list = []
        for subset in new_allocation.values():
            as_list.extend(list(subset))

        # make sure there are no duplicates among the subsets
        assert len(as_list) == len(set(as_list)), 'duplicate items found in allocation: %s' % self.full_allocation
        # make sure there's no mismatch beween the full set and allocations
        assert len(self.full_set.symmetric_difference(set(as_list))) == 0, (
            'mismatch between full set and allocation: %s vs %s' % (self.full_set, self.full_allocation)
        )

    async def start(self):
        self.active = True

        await self.ensure_path()

        await self.party.join()

        self.data_watcher.add_callback(self.base_path, self.handle_data_change)
        self._monitor_task = asyncio.create_task(self.monitor_member_changes())

    async def add(self, new_item):
        new_set = self.full_set.copy().add(new_item)
        await self.update_set(new_set)

    async def remove(self, new_item):
        new_set = self.full_set.copy().remove(new_item)
        await self.update_set(new_set)

    async def update(self, new_items):
        new_items = set(new_items)
        data = json.dumps(list(new_items))

        with await self.lock.acquire():
            await self.client.set_data(self.base_path, data=data)

    def monitor_member_changes(self):
        while self.active:
            yield self.party.wait_for_change()
            if not self.active:
                break

            self.allocate()

    def handle_data_change(self, new_set_data):
        if new_set_data is None:
            return

        new_set_data = set(json.loads(new_set_data))
        if new_set_data == self.full_set:
            return

        self.full_set = new_set_data
        self.allocate()

    def allocate(self):
        new_allocation = self.allocator_fn(self.party.members, self.full_set)
        self.validate(new_allocation)
        self.full_allocation = new_allocation

    async def stop(self):
        await self.party.leave()

        self.data_watcher.remove_callback(self.base_path, self.handle_data_change)


def round_robin(members, items):
    """
    Default allocator with a round robin approach.

    In this algorithm, each member of the group is cycled over and given an
    item until there are no items left.  This assumes roughly equal capacity
    for each member and aims for even distribution of item counts.
    """
    allocation = collections.defaultdict(set)

    for member, item in zip(itertools.cycle(members), items):
        allocation[member].add(item)

    return allocation
