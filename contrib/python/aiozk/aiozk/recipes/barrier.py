import asyncio

from aiozk import Deadline, WatchEvent, exc

from .recipe import Recipe


class Barrier(Recipe):
    def __init__(self, path):
        super().__init__()
        self.path = path

    async def create(self):
        await self.ensure_path()
        await self.create_znode(self.path)

    async def lift(self):
        try:
            await self.client.delete(self.path)
        except exc.NoNode:
            pass

    async def wait(self, timeout=None):
        deadline = Deadline(timeout)
        barrier_lifted = self.client.wait_for_events([WatchEvent.DELETED], self.path)

        exists = await self.client.exists(path=self.path, watch=True)
        if not exists:
            return

        try:
            if not deadline.is_indefinite:
                await asyncio.wait_for(barrier_lifted, deadline.timeout)
            else:
                await barrier_lifted
        except asyncio.TimeoutError:
            raise exc.TimeoutError
