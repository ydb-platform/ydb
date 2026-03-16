from functools import wraps
from typing import Callable

from .. import uwsgi

stop = uwsgi.stop

reload = uwsgi.reload

disconnect = uwsgi.disconnect

set_process_name = uwsgi.setprocname


class harakiri_imposed:
    """Decorator and context manager.

    Allows temporarily setting harakiri timeout for a function or a code block.

    .. note:: This is for workers, mules and spoolers.

    Examples:

        .. code-block:: python

            @harakiri_imposed(1)
            def doomed():
                do()


        .. code-block:: python

            with harakiri_imposed(10):
                do()

    """
    def __init__(self, *, timeout: int):
        """
        :param timeout: Timeout (seconds) before harakiri.

        """
        self._timeout = timeout

    def __call__(self, func: Callable):
        timeout = self._timeout

        @wraps(func)
        def wrapped(*args, **kwargs):
            uwsgi.set_user_harakiri(timeout)

            try:
                result = func(*args, **kwargs)

            finally:
                uwsgi.set_user_harakiri(0)

            return result

        return wrapped

    def __enter__(self):
        uwsgi.set_user_harakiri(self._timeout)

    def __exit__(self, exc_type, exc_val, exc_tb):
        uwsgi.set_user_harakiri(0)
