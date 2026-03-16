from threading import local
from typing import Callable, Type, Tuple, Union, Optional, Dict, List

from .request import _Request
from .. import uwsgi as _uwsgi
from ..utils import decode_deep, decode


class _PostForkHooks:

    funcs = []

    @classmethod
    def add(cls) -> Callable:
        """Decorator. Registers a function to be called after process fork()."""

        def add_(func: Callable):
            cls.funcs.append(func)
            return func

        return add_

    @classmethod
    def run(cls):
        """Runs registered hooks."""
        for func in cls.funcs:
            func()


_uwsgi.post_fork_hook = _PostForkHooks.run


class _Platform:

    request: Type[_Request] = _Request
    """Current request information. 
    
    .. note:: Object is attached runtime.
    
    """

    postfork_hooks = _PostForkHooks
    """uWSGI is a preforking server, so you might need 
    to execute a fixup tasks (hooks) after each fork(). 
    Each hook will be executed in sequence on each process (worker/mule).
    
    .. note:: The fork() happen before app loading, so there's no hooks for dynamic apps.
        But one can still move postfork hooks in a .py file and 
        import it on server startup with `python.import_module()`.

    """

    workers_count: int = _uwsgi.numproc
    """Number of workers (processes) currently running."""

    cores_count: int = _uwsgi.cores
    """Detected number of processor cores."""

    buffer_size: int = _uwsgi.buffer_size
    """The current configured buffer size in bytes."""

    threads_enabled: bool = _uwsgi.has_threads
    """Flag indicating whether thread support is enabled."""

    started_on: int = _uwsgi.started_on
    """uWSGI's startup Unix timestamp."""

    apps_map: Optional[dict] = _uwsgi.applications
    """Applications dictionary mapping mountpoints to application callables."""

    @property
    def hostname(self) -> str:
        """Current host name."""
        return decode(_uwsgi.hostname)

    @property
    def config(self) -> Dict[str, Union[str, List[str]]]:
        """The current configuration options, including any custom placeholders."""
        return decode_deep(_uwsgi.opt)

    @property
    def config_variables(self) -> Dict[str, str]:
        """Current mapping of configuration file "magic" variables."""
        return decode_deep(_uwsgi.magic_table)

    @property
    def worker_id(self) -> int:
        """Returns current worker ID. 0 if not a worker (e.g. mule)."""
        return _uwsgi.worker_id()

    @property
    def workers_info(self) -> Tuple[dict, ...]:
        """Gets statistics for all the workers for the current server.

        Returns tuple of dicts.

        """
        return tuple(decode_deep(item) for item in _uwsgi.workers())

    @property
    def ready_for_requests(self) -> bool:
        """Returns flag indicating whether we are ready to handle requests."""
        return _uwsgi.ready()

    @property
    def master_pid(self) -> int:
        """Return the process identifier (PID) of the uWSGI master process."""
        return _uwsgi.masterpid()

    @property
    def memory(self) -> Tuple[int, int]:
        """Returns memory usage tuple of ints: (rss, vsz)."""
        return _uwsgi.mem()

    @property
    def clock(self) -> int:
        """Returns uWSGI clock microseconds."""
        return _uwsgi.micros()

    def get_listen_queue(self, socket_num: int = 0) -> int:
        """Returns listen queue (backlog size) of the given socket.

        :param socket_num: Socket number.

        :raises ValueError: If socket is not found

        """
        return _uwsgi.listen_queue(socket_num)

    def get_version(self, *, as_tuple: bool = False) -> Union[str, Tuple[int, int, int, int, str]]:
        """Returns uWSGI version string or tuple.

        :param as_tuple:

        """
        if as_tuple:
            return decode_deep(_uwsgi.version_info)

        return decode(_uwsgi.version)


__THREAD_LOCAL = local()


def __get_platform() -> _Platform:
    platform = getattr(__THREAD_LOCAL, 'uwsgi_platform', None)

    if platform is None:
        platform = _Platform()
        platform.request = _Request()

        setattr(__THREAD_LOCAL, 'uwsgi_platform', platform)

    return platform


uwsgi = __get_platform()
"""This is a _Platform instance."""
