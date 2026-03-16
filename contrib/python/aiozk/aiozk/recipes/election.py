import asyncio
import logging

from .. import exc
from .sequential import SequentialRecipe


log = logging.getLogger(__name__)


class LeaderElection(SequentialRecipe):
    LABEL = 'candidate'

    def __init__(self, base_path):
        super().__init__(base_path)
        self.has_leadership = False
        self.leadership_future = None
        self.watch_loop_task = None

    async def volunteer(self):
        await self.create_unique_znode(self.LABEL)
        self.watch_loop_task = asyncio.create_task(self.watch_loop())

    async def watch_loop(self):
        while True:
            owned_positions, candidates = await self.analyze_siblings()
            if self.LABEL not in owned_positions:
                log.error('Znode for leader election does not exist')
                raise exc.NoNode

            position = owned_positions[self.LABEL]
            if position == 0:
                self.has_leadership = True
                if self.leadership_future is not None:
                    self.leadership_future.set_result(None)
                return

            await self.wait_on_sibling(candidates[position - 1])

    async def wait_for_leadership(self, timeout=None):
        if self.has_leadership:
            return

        if self.leadership_future is None:
            loop = asyncio.get_running_loop()
            self.leadership_future = loop.create_future()

        if timeout is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self.leadership_future), timeout)
            except asyncio.TimeoutError:
                raise exc.TimeoutError
        else:
            await self.leadership_future

    async def resign(self):
        if self.watch_loop_task and not self.watch_loop_task.done():
            self.watch_loop_task.cancel()
            self.watch_loop_task = None
        await self.delete_unique_znode(self.LABEL)
        self.has_leadership = False
