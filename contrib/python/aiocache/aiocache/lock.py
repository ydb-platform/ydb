import asyncio
import uuid
from typing import Any, Dict, Union

from aiocache.base import BaseCache


class RedLock:
    """
    Implementation of `Redlock <https://redis.io/topics/distlock>`_
    with a single instance because aiocache is focused on single
    instance cache.

    This locking has some limitations and shouldn't be used in
    situations where consistency is critical. Those locks are aimed for
    performance reasons where failing on locking from time to time
    is acceptable. TLDR: do NOT use this if you need real resource
    exclusion.

    Couple of considerations with the implementation:

        - If the lease expires and there are calls waiting, all of them
          will pass (blocking just happens for the first time).
        - When a new call arrives, it will wait always at most lease
          time. This means that the call could end up blocked longer
          than needed in case the lease from the blocker expires.

    Backend specific implementation:

        - Redis implements correctly the redlock algorithm. It sets
          the key if it doesn't exist. To release, it checks the value
          is the same as the instance trying to release and if it is,
          it removes the lock. If not it will do nothing
        - Memcached follows the same approach with a difference. Due
          to memcached lacking a way to execute the operation get and
          delete commands atomically, any client is able to release the
          lock. This is a limitation that can't be fixed without introducing
          race conditions.
        - Memory implementation is not distributed, it will only apply
          to the process running. Say you have 4 processes running
          APIs with aiocache, the locking will apply only per process
          (still useful to reduce load per process).

    Example usage::

        from aiocache import Cache
        from aiocache.lock import RedLock

        cache = Cache(Cache.REDIS)
        async with RedLock(cache, 'key', lease=1):  # Calls will wait here
            result = await cache.get('key')
            if result is not None:
                return result
            result = await super_expensive_function()
            await cache.set('key', result)

    In the example, first call will start computing the ``super_expensive_function``
    while consecutive calls will block at most 1 second. If the blocking lasts for
    more than 1 second, the calls will proceed to also calculate the
    result of ``super_expensive_function``.
    """

    _EVENTS: Dict[str, asyncio.Event] = {}

    def __init__(self, client: BaseCache, key: str, lease: Union[int, float]):
        self.client = client
        self.key = self.client.build_key(key + "-lock")
        self.lease = lease
        self._value = ""

    async def __aenter__(self):
        return await self._acquire()

    async def _acquire(self):
        self._value = str(uuid.uuid4())
        try:
            await self.client._add(self.key, self._value, ttl=self.lease)
            RedLock._EVENTS[self.key] = asyncio.Event()
        except ValueError:
            await self._wait_for_release()

    async def _wait_for_release(self):
        try:
            await asyncio.wait_for(RedLock._EVENTS[self.key].wait(), self.lease)
        except asyncio.TimeoutError:
            pass
        except KeyError:  # lock was released when wait_for was rescheduled
            pass

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._release()

    async def _release(self):
        removed = await self.client._redlock_release(self.key, self._value)
        if removed:
            RedLock._EVENTS.pop(self.key).set()


class OptimisticLock:
    """
    Implementation of
    `optimistic lock <https://en.wikipedia.org/wiki/Optimistic_concurrency_control>`_

    Optimistic locking assumes multiple transactions can happen at the same time
    and they will only fail if before finish, conflicting modifications with other
    transactions are found, producing a roll back.

    Finding a conflict will end up raising an `aiocache.lock.OptimisticLockError`
    exception. A conflict happens when the value at the storage is different from
    the one we retrieved when the lock started.

    Example usage::

        cache = Cache(Cache.REDIS)

        # The value stored in 'key' will be checked here
        async with OptimisticLock(cache, 'key') as lock:
            result = await super_expensive_call()
            await lock.cas(result)

    If any other call sets the value of ``key`` before the ``lock.cas`` is called,
    an :class:`aiocache.lock.OptimisticLockError` will be raised. A way to make
    the same call crash would be to change the value inside the lock like::

        cache = Cache(Cache.REDIS)

        # The value stored in 'key' will be checked here
        async with OptimisticLock(cache, 'key') as lock:
            result = await super_expensive_call()
            await cache.set('random_value')  # This will make the `lock.cas` call fail
            await lock.cas(result)

    If the lock is created with an unexisting key, there will never be conflicts.
    """

    def __init__(self, client: BaseCache, key: str):
        self.client = client
        self.key = key
        self.ns_key = self.client.build_key(key)
        self._token = None

    async def __aenter__(self):
        return await self._acquire()

    async def _acquire(self):
        self._token = await self.client._gets(self.ns_key)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass

    async def cas(self, value: Any, **kwargs: Any) -> bool:
        """
        Checks and sets the specified value for the locked key. If the value has changed
        since the lock was created, it will raise an :class:`aiocache.lock.OptimisticLockError`
        exception.

        :raises: :class:`aiocache.lock.OptimisticLockError`
        """
        success = await self.client.set(self.key, value, _cas_token=self._token, **kwargs)
        if not success:
            raise OptimisticLockError("Value has changed since the lock started")
        return True


class OptimisticLockError(Exception):
    """
    Raised when a conflict is found during an optimistic lock
    """
