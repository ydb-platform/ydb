# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import asyncio
import collections
import re
import threading

from .. import _typing as t  # noqa: TC001
from .shims import wait_for


__all__ = [
    "AsyncCondition",
    "AsyncCooperativeLock",
    "AsyncCooperativeRLock",
    "AsyncLock",
    "AsyncRLock",
    "Condition",
    "CooperativeLock",
    "CooperativeRLock",
    "Lock",
    "RLock",
]


AsyncLock = asyncio.Lock


class AsyncRLock(asyncio.Lock):
    """
    Reentrant asyncio.lock.

    Inspired by Python's RLock implementation.

    .. warning::
        In async Python there are no threads. This implementation uses
        :meth:`asyncio.current_task` to determine the owner of the lock. This
        means that the owner changes when using :meth:`asyncio.wait_for` or
        any other method that wraps the work in a new :class:`asyncio.Task`.
    """

    _WAITERS_RE = re.compile(r"(?:\W|^)waiters[:=](\d+)(?:\W|$)")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._owner = None
        self._count = 0

    def __repr__(self):
        res = object.__repr__(self)
        lock_repr = super().__repr__()
        extra = "locked" if self._count > 0 else "unlocked"
        extra += f" count={self._count}"
        waiters_match = self._WAITERS_RE.search(lock_repr)
        if waiters_match:
            extra += f" waiters={waiters_match.group(1)}"
        if self._owner:
            extra += f" owner={self._owner}"
        return f"<{res[1:-1]} [{extra}]>"

    def is_owner(self, task=None):
        if task is None:
            task = asyncio.current_task()
        return self._owner == task

    async def _acquire_non_blocking(self, me):
        if self.is_owner(task=me):
            self._count += 1
            return True
        acquire_coro = super().acquire()
        task = asyncio.ensure_future(acquire_coro)
        # yielding one cycle is as close to non-blocking as it gets
        # (at least without implementing the lock from the ground up)
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            # This is emulating non-blocking. There is no cancelling this!
            # Still, we don't want to silently swallow the cancellation.
            # Hence, we flag this task as cancelled again, so that the next
            # `await` will raise the CancelledError.
            asyncio.current_task().cancel()
        if task.done():
            exception = task.exception()
            if exception is None:
                self._owner = me
                self._count = 1
                return True
            else:
                raise exception
        task.cancel()
        return False

    async def _acquire(self, me):
        if self.is_owner(task=me):
            self._count += 1
            return
        await super().acquire()
        self._owner = me
        self._count = 1

    async def acquire(self, blocking=True, timeout=-1):
        """Acquire the lock."""
        me = asyncio.current_task()
        if timeout < 0 and timeout != -1:
            raise ValueError("timeout value must be positive")
        if not blocking and timeout != -1:
            raise ValueError("can't specify a timeout for a non-blocking call")
        if not blocking:
            return await self._acquire_non_blocking(me)
        if blocking and timeout == -1:
            await self._acquire(me)
            return True
        try:
            fut = asyncio.ensure_future(self._acquire(me))
            try:
                await wait_for(fut, timeout)
            except asyncio.CancelledError:
                if fut.cancelled():
                    raise
                already_finished = not fut.cancel()
                if already_finished:
                    # Too late to cancel the acquisition.
                    # This can only happen in Python 3.7's asyncio
                    # as well as in our wait_for shim.
                    self._release(me)
                raise
            return True
        except asyncio.TimeoutError:
            return False

    __aenter__ = acquire

    def _release(self, me):
        if not self.is_owner(task=me):
            if self._owner is None:
                raise RuntimeError("Cannot release un-acquired lock.")
            raise RuntimeError("Cannot release foreign lock.")
        self._count -= 1
        if not self._count:
            self._owner = None
            super().release()

    def release(self):
        """Release the lock."""
        me = asyncio.current_task()
        return self._release(me)

    async def __aexit__(self, t, v, tb):
        self.release()


class AsyncCooperativeLock:
    """
    Lock placeholder for asyncio Python when working fully cooperatively.

    This lock doesn't do anything in async Python. Its threaded counterpart,
    however, is an ordinary :class:`threading.Lock`.
    The AsyncCooperativeLock only works if there is no await being used
    while the lock is held.
    """

    def __init__(self):
        self._locked = False

    def __repr__(self):
        res = super().__repr__()
        extra = "locked" if self._locked else "unlocked"
        return f"<{res[1:-1]} [{extra}]>"

    def locked(self):
        """Return True if lock is acquired."""
        return self._locked

    def acquire(self):
        """
        Acquire a lock.

        This method will raise a RuntimeError where an ordinary
        (non-placeholder) lock would need to block. I.e., when the lock is
        already taken.

        Returns True if the lock was successfully acquired.
        """
        if self._locked:
            raise RuntimeError("Cannot acquire a locked cooperative lock.")
        self._locked = True
        return True

    def release(self):
        """
        Release a lock.

        When the lock is locked, reset it to unlocked, and return.

        When invoked on an unlocked lock, a RuntimeError is raised.

        There is no return value.
        """
        if self._locked:
            self._locked = False
        else:
            raise RuntimeError("Lock is not acquired.")

    __enter__ = acquire

    def __exit__(self, t, v, tb):
        self.release()

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, t, v, tb):
        self.__exit__(t, v, tb)


class AsyncCooperativeRLock:
    """
    Reentrant lock placeholder for cooperative asyncio Python.

    This lock doesn't do anything in async Python. It's threaded counterpart,
    however, is an ordinary :class:`threading.Lock`.
    The AsyncCooperativeLock only works if there is no await being used
    while the lock is acquired.
    """

    def __init__(self):
        self._owner = None
        self._count = 0

    def __repr__(self):
        res = super().__repr__()
        if self._owner is not None:
            extra = f"locked {self._count} times by owner:{self._owner}"
        else:
            extra = "unlocked"
        return f"<{res[1:-1]} [{extra}]>"

    def locked(self):
        """Return True if lock is acquired."""
        return self._owner is not None

    def acquire(self):
        """
        Acquire a lock.

        This method will raise a RuntimeError where an ordinary
        (non-placeholder) lock would need to block. I.e., when the lock is
        already taken by another Task.

        Returns True if the lock was successfully acquired.
        """
        me = asyncio.current_task()
        if self._owner is None:
            self._owner = me
            self._count = 1
            return True
        if self._owner is me:
            self._count += 1
            return True
        raise RuntimeError("Cannot acquire a foreign locked cooperative lock.")

    def release(self):
        """
        Release a lock.

        When the lock is locked, reset it to unlocked, and return.

        When invoked on an unlocked or foreign lock, a RuntimeError is raised.

        There is no return value.
        """
        me = asyncio.current_task()
        if self._owner is None:
            raise RuntimeError("Lock is not acquired.")
        if self._owner is not me:
            raise RuntimeError("Cannot release a foreign lock.")
        self._count -= 1
        if not self._count:
            self._owner = None

    __enter__ = acquire

    def __exit__(self, t, v, tb):
        self.release()


class AsyncCondition:
    """
    Asynchronous equivalent to threading.Condition.

    This class implements condition variable objects. A condition variable
    allows one or more coroutines to wait until they are notified by another
    coroutine.

    A new Lock object is created and used as the underlying lock.
    """

    # copied and modified from Python 3.11's asyncio package
    # to add support for `.wait(timeout)` and cooperative locks

    # Copyright (c) 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010,
    # 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022
    # Python Software Foundation;
    # All Rights Reserved

    def __init__(self, lock=None):
        if lock is None:
            lock = AsyncLock()

        self._lock = lock
        # Export the lock's locked(), acquire() and release() methods.
        self.locked = lock.locked
        self.acquire = lock.acquire
        self.release = lock.release

        self._waiters = collections.deque()

    _loop = None
    _loop_lock = threading.Lock()

    def _get_loop(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if self._loop is None:
            with self._loop_lock:
                if self._loop is None:
                    self._loop = loop
        if loop is not self._loop:
            raise RuntimeError(f"{self!r} is bound to a different event loop")
        return loop

    async def __aenter__(self):
        if isinstance(
            self._lock, (AsyncCooperativeLock, AsyncCooperativeRLock)
        ):
            self._lock.acquire()
        else:
            await self.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        self.release()

    def __repr__(self):
        res = super().__repr__()
        extra = "locked" if self.locked() else "unlocked"
        if self._waiters:
            extra = f"{extra}, waiters:{len(self._waiters)}"
        return f"<{res[1:-1]} [{extra}]>"

    async def _wait(self, timeout=None, me=None):
        """
        Wait until notified.

        If the calling coroutine has not acquired the lock when this
        method is called, a RuntimeError is raised.

        This method releases the underlying lock, and then blocks
        until it is awakened by a notify() or notify_all() call for
        the same condition variable in another coroutine.  Once
        awakened, it re-acquires the lock and returns True.
        """
        if not self.locked():
            raise RuntimeError("cannot wait on un-acquired lock")

        cancelled = False
        if isinstance(self._lock, AsyncRLock):
            self._lock._release(me)
        else:
            self._lock.release()
        try:
            fut = self._get_loop().create_future()
            self._waiters.append(fut)
            try:
                await wait_for(fut, timeout)
                return True
            except asyncio.TimeoutError:
                return False
            except asyncio.CancelledError:
                cancelled = True
                raise
            finally:
                self._waiters.remove(fut)

        finally:
            # Must reacquire lock even if wait is cancelled
            if isinstance(
                self._lock, (AsyncCooperativeLock, AsyncCooperativeRLock)
            ):
                self._lock.acquire()
            else:
                while True:
                    try:
                        if isinstance(self._lock, AsyncRLock):
                            await self._lock._acquire(me)
                        else:
                            await self._lock.acquire()
                        break
                    except asyncio.CancelledError:
                        cancelled = True
            if cancelled:
                raise asyncio.CancelledError

    async def wait(self, timeout=None):
        me = asyncio.current_task()
        return await self._wait(timeout=timeout, me=me)

    async def wait_for(self, predicate):
        """
        Wait until a predicate becomes true.

        The predicate should be a callable which result will be
        interpreted as a boolean value.  The final predicate value is
        the return value.
        """
        result = predicate()
        while not result:
            await self.wait()
            result = predicate()
        return result

    def notify(self, n=1):
        """
        Wake up a single threads waiting on this condition.

        By default, wake up one coroutine waiting on this condition, if any.
        If the calling coroutine has not acquired the lock when this method
        is called, a RuntimeError is raised.

        This method wakes up at most n of the coroutines waiting for the
        condition variable; it is a no-op if no coroutines are waiting.

        Note: an awakened coroutine does not actually return from its
        wait() call until it can reacquire the lock. Since notify() does
        not release the lock, its caller should.
        """
        if not self.locked():
            raise RuntimeError("cannot notify on un-acquired lock")

        idx = 0
        for fut in self._waiters:
            if idx >= n:
                break

            if not fut.done():
                idx += 1
                fut.set_result(False)

    def notify_all(self):
        """
        Wake up all threads waiting on this condition.

        This method acts like notify(), but wakes up all waiting threads
        instead of one. If the calling thread has not acquired the lock when
        this method is called, a RuntimeError is raised.
        """
        self.notify(len(self._waiters))


Condition: t.TypeAlias = threading.Condition
CooperativeLock: t.TypeAlias = threading.Lock
Lock: t.TypeAlias = threading.Lock
CooperativeRLock: t.TypeAlias = threading.RLock
RLock: t.TypeAlias = threading.RLock
