import asyncio
import logging

from .. import Deadline, exc, states
from .sequential import SequentialRecipe


log = logging.getLogger(__name__)


class BaseLock(SequentialRecipe):
    def __init__(self, base_path):
        super().__init__(base_path)
        self.monitor_session_task = None

    async def monitor_session(self):
        fut = self.client.session.state.wait_for(states.States.LOST)
        try:
            await fut
            log.warning('Session expired at some point, lock %s no longer acquired.', self)
        except asyncio.CancelledError:
            self.client.session.state.remove_waiting(fut, states.States.LOST)
            raise

    async def wait_in_line(self, znode_label, timeout=None, blocked_by=None):
        """If blocked_by is specified using tuple, wait for prior znodes whose
        label is in the blocked_by. If it is not given, wait for any prior
        znodes"""
        deadline = Deadline(timeout)
        await self.create_unique_znode(znode_label)
        while True:
            if deadline.has_passed:
                await self.delete_unique_znode(znode_label)
                raise exc.TimeoutError

            try:
                owned_positions, contenders = await self.analyze_siblings()
            except exc.TimeoutError:
                # state may change to SUSPENDED
                await self.client.session.state.wait_for(states.States.CONNECTED)
                continue

            if znode_label not in owned_positions:
                raise exc.SessionLost

            blockers = contenders[: owned_positions[znode_label]]
            if blocked_by:
                blockers = [contender for contender in blockers if self.determine_znode_label(contender) in blocked_by]
            if not blockers:
                break

            try:
                await self.wait_on_sibling(blockers[-1], deadline.timeout)
            except exc.TimeoutError:
                # state may change to SUSPENDED
                await self.client.session.state.wait_for(states.States.CONNECTED)
                continue

        if not self.monitor_session_task or self.monitor_session_task.done():
            self.monitor_session_task = asyncio.create_task(self.monitor_session())

    async def get_out_of_line(self, znode_label):
        await self.delete_unique_znode(znode_label)

        if not self.owned_paths and not self.monitor_session_task.done():
            self.monitor_session_task.cancel()
            self.monitor_session_task = None


class LockLostError(exc.ZKError):
    pass
