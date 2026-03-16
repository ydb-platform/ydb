import asyncio
import logging

from aiozk import Deadline, WatchEvent, exc

from .sequential import SequentialRecipe


log = logging.getLogger(__name__)


class DoubleBarrier(SequentialRecipe):
    ZNODE_LABEL = 'worker'

    def __init__(self, base_path, min_participants):
        super().__init__(base_path)
        self.min_participants = min_participants

    @property
    def sentinel_path(self):
        return self.sibling_path('sentinel')

    async def enter(self, timeout=None):
        deadline = Deadline(timeout)
        log.debug('Entering double barrier %s', self.base_path)
        barrier_lifted = self.client.wait_for_events([WatchEvent.CREATED], self.sentinel_path)

        exists = await self.client.exists(path=self.sentinel_path, watch=True)

        await self.create_unique_znode(self.ZNODE_LABEL)

        if exists:
            return

        try:
            _, participants = await self.analyze_siblings()
            if len(participants) >= self.min_participants:
                await self.create_znode(self.sentinel_path)
                return

            try:
                if not deadline.is_indefinite:
                    await asyncio.wait_for(barrier_lifted, deadline.timeout)
                else:
                    await barrier_lifted
            except asyncio.TimeoutError:
                raise exc.TimeoutError
        except Exception:
            await self.delete_unique_znode_retry(self.ZNODE_LABEL)
            raise

    async def leave(self, timeout=None):
        log.debug('Leaving double barrier %s', self.base_path)
        deadline = Deadline(timeout)
        while True:
            owned_positions, participants = await self.analyze_siblings()
            if not participants:
                return

            if len(participants) == 1:
                await self.delete_unique_znode(self.ZNODE_LABEL)
                try:
                    await self.client.delete(self.sentinel_path)
                except exc.NoNode:
                    pass
                return

            if owned_positions[self.ZNODE_LABEL] == 0:
                await self.wait_on_sibling(participants[-1], deadline.timeout)
            else:
                await self.delete_unique_znode(self.ZNODE_LABEL)
                await self.wait_on_sibling(participants[0], deadline.timeout)
