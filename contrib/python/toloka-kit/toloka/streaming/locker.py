__all__ = [
    'BaseLocker',
    'FileLocker',
    'NewerInstanceDetectedError',
]

import attr
import filelock
import json
import os
from contextlib import contextmanager
from typing import Any, ContextManager, Optional

from ..util.stored import get_base64_digest, get_stored_meta


try:
    from kazoo.client import KazooClient, Lock as KazooLock
    __all__.append('ZooKeeperLocker')
    KAZOO_INSTALLED = True
except ImportError:
    KAZOO_INSTALLED = False


class NewerInstanceDetectedError(Exception):
    """Exception being thrown in case of new concurrent pipeline was detected.
    Used to safely restore a process from a storage.
    """


class BaseLocker:
    def __call__(self, key: str) -> ContextManager[Any]:
        raise NotImplementedError

    def cleanup(self, lock: Any) -> None:
        pass


@attr.s
class BaseSequentialIdLocker(BaseLocker):
    _id: Optional[int] = attr.ib(default=None, init=False)

    def _process_lock_info(self, key: str, content: bytes) -> None:
        data = json.loads(content.decode()) if content else {'iter': 0}
        if self._id is None:
            self._id = data['iter']
        if data.get('owner', 0) > self._id:
            raise NewerInstanceDetectedError(f'Newer instance for {key}: {data}')
        data['owner'] = self._id
        data['iter'] += 1
        data['meta'] = get_stored_meta()
        return json.dumps(data, separators=(',', ':')).encode()


@attr.s
class FileLocker(BaseSequentialIdLocker):
    """Simplest filesystem-based locker to use with a storage.

    Two locks cannot be taken simultaneously with the same key.
    If the instance detects that the lock was taken by a newer version, it throws an error.

    Attributes:
        dirname: Directory to store lock files ending with ".lock" and ".lock.content".
        timeout: Time in seconds to wait in case of lock being already acquired. Infinite by default.

    Example:
        Try to lock the same key at the same time..

        >>> locker_1 = FileLocker()
        >>> locker_2 = FileLocker(timeout=0)
        >>> with locker_1('some_key') as lock_1:
        >>>     with locker_2('some_key') as lock_2:  # => raise an error: timeout
        >>>         pass
        ...

        Try to lock the same key sequentially.

        >>> locker_1 = FileLocker()
        >>> locker_2 = FileLocker()
        >>> with locker_1('some_key'):
        >>>     pass
        >>> with locker_2('some_key'):
        >>>     pass
        >>> with locker_1('some_key'):  # raise an error: NewerInstanceDetectedError
        >>>     pass
        ...
    """

    dirname: str = attr.ib(default='/tmp')
    timeout: Optional[int] = attr.ib(default=None)

    @contextmanager
    def __call__(self, key: str) -> ContextManager[filelock.BaseFileLock]:
        path = os.path.join(self.dirname, get_base64_digest(key))
        timeout = -1 if self.timeout is None else self.timeout  # In FileLock timeout=-1 means inf.
        lock_path = f'{path}.lock'
        with filelock.FileLock(lock_path, timeout=timeout) as lock:
            lock_content_path = f'{lock.lock_file}.content'
            open(lock_content_path, 'a').close()  # Touch file.
            with open(lock_content_path, 'r+b') as file:
                updated: bytes = self._process_lock_info(key, file.read())
                file.seek(0)
                file.write(updated)
                file.truncate()
            yield lock

    def cleanup(self, lock: filelock.BaseFileLock) -> None:
        for file_path in (f'{lock.lock_file}.content', lock.lock_file):
            try:
                os.remove(file_path)
            except FileNotFoundError:
                pass


if KAZOO_INSTALLED:
    @attr.s
    class ZooKeeperLocker(BaseSequentialIdLocker):
        """Apache ZooKeeper-based locker to use with a storage.

        {% note warning %}

        Requires toloka-kit[zookeeper] extras. Install it with the following command:

        ```shell
        pip install toloka-kit[zookeeper]
        ```

        {% endnote %}

        Two locks cannot be taken simultaneously with the same key.
        If the instance detects that the lock was taken by a newer version, it throws an error.

        Attributes:
            client: KazooClient object.
            dirname: Base node path to put locks in.
            timeout: Time in seconds to wait in case of lock being already acquired. Infinite by default.
            identifier: Optional lock identifier.

        Example:
            Create lock object.

            >>> !pip install toloka-kit[zookeeper]
            >>> from kazoo.client import KazooClient
            >>> zk = KazooClient('127.0.0.1:2181')
            >>> zk.start()
            >>> locker = ZooKeeperLocker(zk, '/my-locks')

            Try to lock the same key at the same time.

            >>> locker_1 = ZooKeeperLocker(zk, '/locks')
            >>> locker_2 = ZooKeeperLocker(zk, '/locks', timeout=0)
            >>> with locker_1('some_key') as lock_1:
            >>>     with locker_2('some_key') as lock_2:  # => raise an error: timeout
            >>>         pass
            ...

            Try to lock the same key sequentially.

            >>> locker_1 = ZooKeeperLocker(zk, '/locks')
            >>> locker_2 = ZooKeeperLocker(zk, '/locks')
            >>> with locker_1('some_key'):
            >>>     pass
            >>> with locker_2('some_key'):
            >>>     pass
            >>> with locker_1('some_key'):  # raise an error: NewerInstanceDetectedError
            >>>     pass
            ...
        """

        client: KazooClient = attr.ib()
        dirname: str = attr.ib()
        timeout: Optional[int] = attr.ib(default=None)
        identifier: str = attr.ib(default='lock')

        @contextmanager
        def __call__(self, key: str) -> ContextManager[KazooLock]:
            path = os.path.join(self.dirname, get_base64_digest(key))
            lock = self.client.Lock(path, self.identifier)
            lock.acquire(timeout=self.timeout)
            try:
                updated: bytes = self._process_lock_info(key, self.client.get(path)[0])
                self.client.set(path, updated)
            except Exception:
                lock.release()
                raise
            yield lock
            lock.release()
else:
    class ZooKeeperLocker:
        def __init__(self, *args, **kwargs):
            raise NotImplementedError('Please install toloka-kit[zookeeper] extras.')
