from functools import wraps
from typing import Callable

from .. import uwsgi


class Lock:
    """Locks related stuff.

    Lock number 0 is always available. More locks need to be registered
    with ``.config.locking.set_basic_params(count=X)`` where ``X`` is the number of locks.

    .. note:: The same lock should be released before next acquiring.

    Can be used as context manager:

        .. code-block:: python

            with Lock():
                do()

    Can de used as a decorator:

        .. code-block:: python

            @Lock()
            def do():
                pass

    """
    __slots__ = ['num']

    def __init__(self, num: int = 0):
        """
        :param num: Lock number (0-64). 0 is always available and is used as default.

        """
        self.num = num

    def __int__(self):
        return self.num

    def __call__(self, func: Callable):

        @wraps(func)
        def wrapper(*args, **kwargs):

            with self:
                return func(*args, **kwargs)

        return wrapper

    @property
    def is_set(self) -> bool:
        """"Checks whether the lock is active.

        :raises ValueError: For Spooler or invalid lock number

        """
        return uwsgi.is_locked(self.num)

    def acquire(self):
        """Sets the lock.

        :raises ValueError: For Spooler or invalid lock number

        """
        uwsgi.lock(self.num)
        return True

    def release(self):
        """Unlocks the lock.

        :raises ValueError: For Spooler or invalid lock number

        """
        uwsgi.unlock(self.num)
        return True

    __enter__ = acquire

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


lock = Lock
"""Convenience alias for ``Lock``."""
