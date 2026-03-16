from .. import exc
from ..deadline import Deadline
from .base_lock import BaseLock


class Lock(BaseLock):
    def __init__(self, base_path, znode_label='lock', blocked_by=None):
        super().__init__(base_path)
        self.znode_label = znode_label
        self.blocked_by = blocked_by
        self.is_locked = False
        # This lock is for coordination between processes through network. Not
        # for tasks in a process. Simultaneous .acquire calls is misuse.
        self.in_use = False

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, exc_type, exception, tb):
        await self.release()

    async def acquire(self, timeout=None):
        if self.in_use:
            raise RuntimeError('lock is already in use')
        self.in_use = True
        deadline = Deadline(timeout)
        while not self.is_locked and not deadline.has_passed:
            try:
                await self.wait_in_line(self.znode_label, deadline.timeout, blocked_by=self.blocked_by)
            except exc.SessionLost:
                continue
            except Exception as e:
                self.in_use = False
                raise e from None

            self.is_locked = True

    async def release(self):
        try:
            await self.get_out_of_line(self.znode_label)
        finally:
            self.is_locked = False
            self.in_use = False

    def locked(self):
        return self.is_locked
