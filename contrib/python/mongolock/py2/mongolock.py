import time
import contextlib
from datetime import datetime, timedelta

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.collection import Collection


class MongoLockException(Exception):
    pass


class MongoLockLocked(Exception):
    pass


class MongoLock(object):
    def __init__(self, host='localhost', db='mongolock', collection='lock', client=None, acquire_retry_step=0.1):
        """Create a new instance of MongoLock.

        :Parameters:
          - `host` (optional) - use it to manually specify mongodb connection string
          - `db` (optional) - db name
          - `collection` (optional) - collection name or :class:`pymongo.Collection` instance
          - `client` - instance of :class:`MongoClient` or :class:`MongoReplicaSetClient`,
          - `acquire_retry_step` (optional)- time in seconds between retries while trying to acquire the lock,
             if specified - `host` parameter will be skipped
        """
        self.acquire_retry_step = acquire_retry_step
        if isinstance(collection, Collection):
            self.collection = collection
        else:
            if client:
                self.client = client
            else:
                self.client = MongoClient(host)
            self.collection = self.client[db][collection]

    @contextlib.contextmanager
    def __call__(self, key, owner, timeout=None, expire=None):
        """See `lock` method. """
        if not self.lock(key, owner, timeout, expire):
            status = self.get_lock_info(key)
            raise MongoLockLocked(
                u'Timeout, lock owned by {owner} since {ts}, expire time is {expire}'.format(
                    owner=status['owner'], ts=status['created'], expire=status['expire']
                )
            )
        try:
            yield
        finally:
            self.release(key, owner)

    def lock(self, key, owner, timeout=None, expire=None):
        """Lock given `key` to `owner`.

        :Parameters:
          - `key` - lock name
          - `owner` - name of application/component/whatever which asks for lock
          - `timeout` (optional) - how long to wait if `key` is locked
          - `expire` (optional) - when given, lock will be released after that number of seconds.

        Raises `MongoLockTimeout` if can't achieve a lock before timeout.
        """
        expire = datetime.utcnow() + timedelta(seconds=expire) if expire else None
        try:
            self.collection.insert({
                '_id': key,
                'locked': True,
                'owner': owner,
                'created': datetime.utcnow(),
                'expire': expire
            })
            return True
        except DuplicateKeyError:
            start_time = datetime.utcnow()
            while True:
                if self._try_get_lock(key, owner, expire):
                    return True

                if not timeout or datetime.utcnow() >= start_time + timedelta(seconds=timeout):
                    return False
                time.sleep(self.acquire_retry_step)

    def release(self, key, owner):
        """Release lock with given name.
          `key` - lock name
          `owner` - name of application/component/whatever which held a lock
        Raises `MongoLockException` if no such a lock.
        """
        status = self.collection.find_and_modify(
            {'_id': key, 'owner': owner},
            {'locked': False, 'owner': None, 'created': None, 'expire': None}
        )

    def get_lock_info(self, key):
        """Get lock status. """
        return self.collection.find_one({'_id': key})

    def is_locked(self, key):
        lock_info = self.get_lock_info(key)
        return not (
            not lock_info 
            or not lock_info['locked'] 
            or (lock_info['expire'] is not None and lock_info['expire'] < datetime.utcnow())
        )

    def touch(self, key, owner, expire=None):
        """Renew lock to avoid expiration. """
        lock = self.collection.find_one({'_id': key, 'owner': owner})
        if not lock:
            raise MongoLockException(u'Can\'t find lock for {key}: {owner}'.format(key=key, owner=owner))
        if not lock['expire']:
            return
        if not expire:
            raise MongoLockException(u'Can\'t touch lock without expire for {0}: {1}'.format(key, owner))
        expire = datetime.utcnow() + timedelta(seconds=expire)
        self.collection.update(
            {'_id': key, 'owner': owner},
            {'$set': {'expire': expire}}
        )

    def _try_get_lock(self, key, owner, expire):
        dtnow = datetime.utcnow()
        result = self.collection.update(
            {
                '$or': [
                    {'_id': key, 'locked': False},
                    {'_id': key, 'expire': {'$lt': dtnow}},
                ]
            },
            {
                'locked': True,
                'owner': owner,
                'created': dtnow,
                'expire': expire
            }
        )
        return True if result['n'] == 1 else False
