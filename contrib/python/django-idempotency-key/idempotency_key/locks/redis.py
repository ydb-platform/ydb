from redis import Redis

from idempotency_key import utils
from idempotency_key.locks.basic import IdempotencyKeyLock


class MultiProcessRedisLock(IdempotencyKeyLock):
    """
    Should be used if a lock is required across processes. Note that this class uses
    Redis in order to perform the lock.
    """

    def __init__(self):
        location = utils.get_lock_location()
        if location is None or location == "":
            raise ValueError("Redis server location must be set in the settings file.")

        self.redis_obj = Redis.from_url(location)
        self.storage_lock = self.redis_obj.lock(
            name=utils.get_lock_name(),
            # Time before lock is forcefully released.
            timeout=utils.get_lock_time_to_live(),
            blocking_timeout=utils.get_lock_timeout(),
        )

    def acquire(self, *args, **kwargs) -> bool:
        return self.storage_lock.acquire()

    def release(self):
        self.storage_lock.release()
