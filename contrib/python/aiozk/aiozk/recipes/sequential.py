import asyncio
import logging
import re
import uuid

from aiozk import Deadline, RetryPolicy, WatchEvent, exc, states

from .recipe import Recipe


log = logging.getLogger(__name__)

sequential_re = re.compile(r'.*[0-9]{10}$')


class SequentialRecipe(Recipe):
    """Can have multiple znodes with different labels"""

    def __init__(self, base_path):
        super().__init__(base_path)
        self.guid = uuid.uuid4().hex

        self.owned_paths = {}

    def sequence_number(self, sibling):
        return int(sibling[-10:])

    def determine_znode_label(self, sibling):
        return sibling.rsplit('-', 2)[0]

    def sibling_path(self, path):
        return '/'.join([self.base_path, path])

    async def create_unique_znode(self, znode_label, data=None):
        if znode_label in self.owned_paths:
            if await self.client.exists(self.owned_paths[znode_label]):
                raise exc.NodeExists

        if '/' in znode_label:
            raise ValueError('slash in label')

        path = self.sibling_path(znode_label + '-' + self.guid + '-')

        try:
            created_path = await self.client.create(path, data=data, ephemeral=True, sequential=True)
        except exc.NoNode:
            await self.ensure_path()
            created_path = await self.client.create(path, data=data, ephemeral=True, sequential=True)
        except Exception as e:
            _ = asyncio.create_task(self.delete_garbage_znodes(znode_label))  # noqa: RUF006

            log.exception('Exception in create_unique_znode')
            raise e

        self.owned_paths[znode_label] = created_path

    async def delete_unique_znode(self, znode_label):
        try:
            await self.client.delete(self.owned_paths[znode_label])
        except exc.NoNode:
            pass

        self.owned_paths.pop(znode_label)

    async def delete_unique_znode_retry(self, znode_label):
        MAXIMUM_WAIT = 60
        retry_policy = RetryPolicy.exponential_backoff(maximum=MAXIMUM_WAIT)
        while True:
            try:
                await retry_policy.enforce()
                await self.client.session.state.wait_for(states.States.CONNECTED)
                await self.delete_unique_znode(znode_label)
                break
            except Exception:
                log.exception('Exception in delete_unique_znode_retry')

    async def analyze_siblings(self):
        """Different labeled siblings can be returned"""
        siblings = await self.get_siblings()
        siblings.sort(key=self.sequence_number)

        owned_positions = {}

        for index, path in enumerate(siblings):
            if self.guid in path:
                owned_positions[self.determine_znode_label(path)] = index
        return (owned_positions, siblings)

    async def get_siblings(self):
        siblings = await self.client.get_children(self.base_path)
        siblings = [name for name in siblings if sequential_re.match(name)]
        return siblings

    async def delete_garbage_znodes(self, znode_label):
        MAXIMUM_WAIT = 60
        retry_policy = RetryPolicy.exponential_backoff(maximum=MAXIMUM_WAIT)
        while True:
            await self.client.session.state.wait_for(states.States.CONNECTED)
            await retry_policy.enforce()
            try:
                siblings = await self.get_siblings()
                for sibling in siblings:
                    if self.guid in sibling and self.determine_znode_label(sibling) == znode_label:
                        path = self.sibling_path(sibling)
                        if path != self.owned_paths.get(znode_label, ''):
                            await self.client.delete(path)

                break
            except Exception:
                log.exception('Exception in delete_garbage_znodes:')

    async def wait_on_sibling(self, sibling, timeout=None):
        deadline = Deadline(timeout)
        log.debug('Waiting on sibling %s', sibling)

        path = self.sibling_path(sibling)

        unblocked = self.client.wait_for_events([WatchEvent.DELETED], path)

        exists = await self.client.exists(path=path, watch=True)
        if not exists and not unblocked.done():
            unblocked.set_result(None)

        try:
            if not deadline.is_indefinite:
                await asyncio.wait_for(unblocked, deadline.timeout)
            else:
                await unblocked
        except asyncio.TimeoutError:
            raise exc.TimeoutError
