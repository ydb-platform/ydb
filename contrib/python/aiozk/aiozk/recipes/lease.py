import asyncio
import logging
import time
from functools import partial

from aiozk import exc

from .sequential import SequentialRecipe


log = logging.getLogger(__name__)


class Lease(SequentialRecipe):
    def __init__(self, base_path, limit=1):
        super().__init__(base_path)
        self.limit = limit

    async def obtain(self, duration):
        lessees = await self.client.get_children(self.base_path)

        if len(lessees) >= self.limit:
            return False

        time_limit = time.time() + duration.total_seconds()

        try:
            await self.create_unique_znode('lease', data=str(time_limit))
        except exc.NodeExists:
            log.warning('Lease for %s already obtained.', self.base_path)

        callback = partial(asyncio.create_task, self.release())
        loop = asyncio.get_running_loop()
        loop.call_later(duration.total_seconds(), callback)
        return True

    async def release(self):
        await self.delete_unique_znode('lease')
