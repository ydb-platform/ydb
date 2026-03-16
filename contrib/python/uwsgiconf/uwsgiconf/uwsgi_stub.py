from typing import Optional, Type, Dict, Callable, List, Tuple, Union

is_stub: bool = True
"""Indicates whether stub is used instead of real `uwsgi` module."""


SPOOL_IGNORE: int = 0
"""Spooler function result.

Ignore this task, if multiple languages are loaded in the instance all 
of them will fight for managing the task. 
This return values allows you to skip a task in specific languages.

* http://uwsgi-docs.readthedocs.io/en/latest/Spooler.html#setting-the-spooler-function-callable

"""

SPOOL_OK: int = -2
"""Spooler function result.

The task has been completed, the spool file will be removed.

* http://uwsgi-docs.readthedocs.io/en/latest/Spooler.html#setting-the-spooler-function-callable

"""

SPOOL_RETRY: int = -1
"""Spooler function result.

Something is temporarily wrong, the task will be retried at the next spooler iteration.

* http://uwsgi-docs.readthedocs.io/en/latest/Spooler.html#setting-the-spooler-function-callable

"""

SymbolsImporter: Optional[Type] = None
"""SymbolsImporter type."""

SymbolsZipImporter: Optional[Type] = None
"""SymbolsZipImporter type."""

ZipImporter: Optional[Type] = None
"""ZipImporter type."""

applications: Optional[dict] = None
"""Applications dictionary mapping mountpoints to application callables.

.. note:: Can be ``None``.

* http://uwsgi.readthedocs.io/en/latest/Python.html#application-dictionary

"""

buffer_size: int = 0
"""The current configured buffer size in bytes."""

cores: int = 0
"""Detected number of processor cores."""

env: Dict[str, str] = {}
"""Request environment dictionary."""

has_threads: bool = False
"""Flag indicating whether thread support is enabled."""

hostname: bytes = b''
"""Current host name."""

magic_table: dict = {}
"""Current mapping of configuration file "magic" variables.

* http://uwsgi.readthedocs.io/en/latest/Configuration.html#magic-variables

"""

numproc: int = 0
"""Number of workers (processes) currently running."""

opt: Dict[bytes, bytes] = {}
"""The current configuration options, including any custom placeholders."""

post_fork_hook: Callable = lambda: None
"""Function to be called after process fork (spawning a new worker/mule)."""

spooler: Callable = lambda: None
"""Function to be called for spooler messages processing."""

sockets: List[int] = []
"""Current list of file descriptors for registered sockets."""

start_response: Optional[Callable] = None
"""Callable spitting UWSGI response."""

started_on: int = 0
"""uWSGI's startup Unix timestamp."""

unbit: bool = False
"""Unbit internal flag."""

version: bytes = b'0.0.0'
"""The uWSGI version string."""

version_info: Tuple[int, int, int, int, bytes] = (0, 0, 0, 0, b'')
"""Five-elements version number tuple."""

# todo wait for install_mule_msg_hook merged in master and define here
mule_msg_hook: Callable = lambda: None
"""Registers a function to be called for each mule message."""


def accepting():
    """Called to notify the master that the worker is accepting requests,
    this is required for ``touch_chain_reload`` to work.

    """


def add_cron(signal: int, minute: int, hour: int, day: int, month: int, weekday: int) -> bool:
    """Adds cron. The interface to the uWSGI signal cron facility. The syntax is

    .. note:: The last 5 arguments work similarly to a standard crontab,
        but instead of "*", use -1, and instead of "/2", "/3", etc. use -2 and -3, etc.

    :param signal: Signal to raise.

    :param minute: Minute 0-59. Defaults to `each`.

    :param hour: Hour 0-23. Defaults to `each`.

    :param day: Day of the month number 1-31. Defaults to `each`.

    :param month: Month number 1-12. Defaults to `each`.

    :param weekday: Day of a the week number. Defaults to `each`.
        0 - Sunday  1 - Monday  2 - Tuesday  3 - Wednesday
        4 - Thursday  5 - Friday  6 - Saturday

    :raises ValueError: If unable to add cron rule.

    """


def add_file_monitor(signal: int, filename: str):
    """Maps a specific file/directory modification event to a signal.

    :param signal: Signal to raise.

    :param filename: File or a directory to watch for its modification.

    :raises ValueError: If unable to register monitor.

    """


def add_ms_timer(signal: int, period: int):
    """Add a millisecond resolution timer.

    :param signal: Signal to raise.

    :param period: The interval (milliseconds) at which to raise the signal.

    :raises ValueError: If unable to add timer.

    """


def add_rb_timer(signal: int, period: int, repeat: int = 0):
    """Add a red-black timer.

    :param signal: Signal to raise.

    :param period: The interval (seconds) at which the signal is raised.

    :param repeat: How many times to send signal. Will stop after ther number is reached.
        Default: 0 - infinitely.

    :raises ValueError: If unable to add timer.

    """


def add_timer(signal: int, period: int):
    """Add timer.

    :param signal: Signal to raise.

    :param period: The interval (seconds) at which to raise the signal.

    :raises ValueError: If unable to add timer.

    """


def add_var(name: str, value: str) -> bool:
    """Registers custom request variable.

    Can be used for better integration with the internal routing subsystem.

    :param name:

    :param value:

    :raises ValueError: If buffer size is not enough.

    """


def alarm(name: str, message: str):
    """Issues the given alarm with the given message.

    .. note:: to register an alarm use
        ``section.alarms.register_alarm(section.alarms.alarm_types.log('myalarm'))``

    :param name:

    :param message: Message to pass to alarm.

    """


def async_connect(socket: str) -> int:
    """Issues socket connection. And returns a file descriptor or -1.

    * http://uwsgi.readthedocs.io/en/latest/Async.html

    :param socket:

    """


def async_sleep(seconds: int) -> bytes:
    """Suspends handling the current request and passes control to the next async core.

    * http://uwsgi.readthedocs.io/en/latest/Async.html

    :param seconds: Sleep time, in seconds.

    """


def cache_clear(cache: str):
    """Clears cache with the given name.

    :param cache: Cache name with optional address (if @-syntax is used).

    """


def cache_dec(key: str, value: int = 1, expires: int = None, cache: str = None) -> bool:
    """Decrements the specified key value by the specified value.

    * http://uwsgi.readthedocs.io/en/latest/Changelog-1.9.9.html#math-for-cache

    :param key:

    :param value:

    :param expires: Expire timeout (seconds).

    :param cache: Cache name with optional address (if @-syntax is used).

    """
    return False


def cache_del(key: str, cache: str = None) -> bool:
    """Deletes the given cached key from the cache.

    :param key: The cache key to delete.

    :param cache: Cache name with optional address (if @-syntax is used).

    """
    return False


def cache_div(key: str, value: int = 2, expires: int = None, cache: str = None) -> bool:
    """Divides the specified key value by the specified value.

    * http://uwsgi.readthedocs.io/en/latest/Changelog-1.9.9.html#math-for-cache

    :param key:

    :param value:

    :param expires: Expire timeout (seconds).

    :param cache: Cache name with optional address (if @-syntax is used).

    """
    return False


def cache_exists(key: str, cache: str = None) -> bool:
    """Checks whether there is a value in the cache associated with the given key.

    :param key: The cache key to check.

    :param cache: Cache name with optional address (if @-syntax is used).

    """
    return False


def cache_get(key: str, cache: str = None) -> Optional[bytes]:
    """Gets a value from the cache.

    :param key: The cache key to get value for.

    :param cache: Cache name with optional address (if @-syntax is used).

    """


def cache_inc(key: str, value: int = 1, expires: int = None, cache: str = None) -> bool:
    """Increments the specified key value by the specified value.

    * http://uwsgi.readthedocs.io/en/latest/Changelog-1.9.9.html#math-for-cache

    :param key:

    :param value:

    :param expires: Expire timeout (seconds).

    :param cache: Cache name with optional address (if @-syntax is used).

    """
    return False


def cache_keys(cache: str = None) -> List:
    """Returns a list of keys available in cache.

    :param str cache: Cache name with optional address (if @-syntax is used).

    :raises ValueError: If cache is unavailable.

    """
    return []


def cache_mul(key: str, value: int = 2, expires: int = None, cache: str = None) -> bool:
    """Multiplies the specified key value by the specified value.

    * http://uwsgi.readthedocs.io/en/latest/Changelog-1.9.9.html#math-for-cache

    :param key:

    :param value:

    :param expires: Expire timeout (seconds).

    :param cache: Cache name with optional address (if @-syntax is used).

    """
    return False


def cache_num(key: str, cache: str = None) -> Optional[int]:
    """Gets the 64bit number from the specified item.

    * http://uwsgi.readthedocs.io/en/latest/Changelog-1.9.9.html#math-for-cache

    :param key: The cache key to get value for.

    :param cache: Cache name with optional address (if @-syntax is used).

    """


def cache_set(key: str, value: str, expires: int = None, cache: str = None) -> bool:
    """Sets the specified key value.

    :param key:

    :param value:

    :param expires: Expire timeout (seconds).

    :param cache: Cache name with optional address (if @-syntax is used).

    """
    return False


def cache_update(key: str, value: str, expires: int = None, cache: str = None) -> bool:
    """Updates the specified key value.

    :param key:

    :param value:

    :param expires: Expire timeout (seconds).

    :param cache: Cache name with optional address (if @-syntax is used).

    """
    return False


def call(func_name: bytes, *args: bytes) -> bytes:
    """Performs an [RPC] function call with the given arguments.

    :param func_name: Function name to call
        with optional address (if @-syntax is used).

    :param args:

    """


def chunked_read(timeout: int) -> bytes:
    """Reads chunked input.

    * http://uwsgi.readthedocs.io/en/latest/Chunked.html
    * http://uwsgi.readthedocs.io/en/latest/Changelog-1.9.13.html#chunked-input-api

    :param timeout: Wait timeout (seconds).

    :raises IOError: If unable to receive chunked part.

    """


def chunked_read_nb() -> bytes:
    """Reads chunked input without blocking.

    * http://uwsgi.readthedocs.io/en/latest/Chunked.html
    * http://uwsgi.readthedocs.io/en/latest/Changelog-1.9.13.html#chunked-input-api

    :raises IOError: If unable to receive chunked part.

    """


def cl() -> int:
    """Returns current post content length."""
    return 0


def close(fd: int):
    """Closes the given file descriptor.

    :param fd: File descriptor.

    """


def connect(socket: str, timeout: int = 0) -> int:
    """Connects to the socket.

    :param socket: Socket name.

    :param timeout: Timeout (seconds).

    """


def connection_fd() -> int:
    """Returns current request file descriptor."""


def disconnect():
    """Drops current connection."""


def embedded_data(symbol_name: str) -> bytes:
    """Reads a symbol from the uWSGI binary image.

    * http://uwsgi.readthedocs.io/en/latest/Embed.html

    :param symbol_name: The symbol name to extract.

    :raises ValueError: If symbol is unavailable.

    """


def extract(fname: str) -> bytes:
    """Extracts file contents.

    :param fname:

    """


def farm_get_msg() -> Optional[bytes]:
    """Reads a mule farm message.

     * http://uwsgi.readthedocs.io/en/latest/Embed.html

     :raises ValueError: If not in a mule

     """


def farm_msg(farm: str, message: Union[str, bytes]):
    """Sends a message to the given farm.

    :param farm: Farm name to send message to.

    :param message:

    """


def get_logvar(name: str) -> bytes:
    """Return user-defined log variable contents.

    * http://uwsgi.readthedocs.io/en/latest/LogFormat.html#user-defined-logvars

    :param name:

    """


def green_schedule() -> bool:
    """Switches to another green thread.

    .. note:: Alias for ``suspend``.

    * http://uwsgi.readthedocs.io/en/latest/Async.html#suspend-resume

    """
    return False


def i_am_the_spooler() -> bool:
    """Returns flag indicating whether you are the Spooler."""
    return False


def in_farm(name: str) -> Optional[bool]:
    """Returns flag indicating whether you (mule) belong
    to the given farm. Returns ``None`` is not in a mule.

    :param name: Farm name.

    """
    return False


def is_a_reload() -> bool:
    """Returns flag indicating whether reloading mechanics is used."""
    return False


def is_connected(fd: int) -> bool:
    """Checks the given file descriptor.

    :param fd: File descriptor

    """
    return False


def is_locked(lock_num: int = 0) -> bool:
    """Checks for the given lock.

    .. note:: Lock 0 is always available.

    :param lock_num: Lock number.

    :raises ValueError: For Spooler or invalid lock number

    """
    return False


def listen_queue(socket_num: int = 0) -> int:
    """Returns listen queue (backlog size) of the given socket.

    :param socket_num: Socket number.

    :raises ValueError: If socket is not found

    """
    return 0


def lock(lock_num: int = 0):
    """Sets the given lock.

    .. note:: Lock 0 is always available.

    :param lock_num: Lock number.

    :raises ValueError: For Spooler or invalid lock number

    """


def log(message: str) -> bool:
    """Logs a message.

    :param message:

    """
    return False


def log_this_request():
    """Instructs uWSGI to log current request data."""


def logsize() -> int:
    """Returns current log size."""
    return 0


def loop() -> Optional[str]:
    """Returns current event loop name or None if loop is not set."""


def masterpid() -> int:
    """Return the process identifier (PID) of the uWSGI master process."""
    return -1


def mem() -> Tuple[int, int]:
    """Returns memory usage tuple of ints: (rss, vsz)."""
    return 0, 0


def metric_dec(key: str, value: int = 1) -> bool:
    """Decrements the specified metric key value by the specified value.

    :param key:

    :param value:

    """
    return False


def metric_div(key: str, value: int = 1) -> bool:
    """Divides the specified metric key value by the specified value.

    :param key:

    :param value:

    """
    return False


def metric_get(key: str) -> int:
    """Returns metric value by key.

    :param key:

    """


def metric_inc(key: str, value: int = 1) -> bool:
    """Increments the specified metric key value by the specified value.

    :param key:

    :param value:

    """
    return False


def metric_mul(key: str, value: int = 1) -> bool:
    """Multiplies the specified metric key value by the specified value.

    :param key:

    :param value:

    """
    return False


def metric_set(key: str, value: int) -> bool:
    """Sets metric value.

    :param key:

    :param value:

    """
    return False


def metric_set_max(key: str, value: int) -> bool:
    """Sets metric value if it is greater that the current one.

    :param key:

    :param value:

    """
    return False


def metric_set_min(key: str, value: int) -> bool:
    """Sets metric value if it is less that the current one.

    :param key:

    :param value:

    """
    return False


def micros() -> int:
    """Returns uWSGI clock microseconds."""
    return 0


def mule_get_msg(signals: bool = None, farms: bool = None, buffer_size: int = 65536, timeout: int = -1) -> bytes:
    """Block until a mule message is received and return it.

    This can be called from multiple threads in the same programmed mule.

    :param signals: Whether to manage signals.

    :param farms: Whether to manage farms.

    :param buffer_size:

    :param timeout: Seconds.

    :raises ValueError: If not in a mule.

    """


def mule_id() -> int:
    """Returns current mule ID. 0 if not a mule (e.g. worker)."""
    return 0


def mule_msg(message: Union[str, bytes], mule_farm: Union[str, int] = None) -> bool:
    """Sends a message to a mule(s)/farm.

    :param message:

    :param mule_farm: Mule ID, or farm name.

    :raises ValueError: If no mules, or mule ID or farm name is not recognized.

    """
    return False


def offload(filename: str) -> bytes:
    """Offloads a file.

    .. warning:: Currently not implemented.

    :param filename:

    :raises ValueError: If unable to offload.

    """


def parsefile(fpath: str):
    """Parses the given file.

    Currently implemented only Spooler file parsing.

    :param fpath:

    """


def ready() -> bool:
    """Returns flag indicating whether we are ready to handle requests."""
    return False


def ready_fd() -> bool:
    """Returns flag indicating whether file description related to request is ready."""
    return False


def recv(fd: int, maxsize: int = 4096) -> bytes:
    """Reads data from the given file descriptor.

    :param fd:

    :param maxsize: Chunk size (bytes).

    """


def register_rpc(name: str, func: Callable) -> bool:
    """Registers RPC function.

    * http://uwsgi.readthedocs.io/en/latest/RPC.html

    :param name:

    :param func:

    :raises ValueError: If unable to register function

    """
    return False


def register_signal(number: int, target: str, func: Callable):
    """Registers a signal handler.

    :param number: Signal number.

    :param target:

        * ``workers``  - run the signal handler on all the workers
        * ``workerN`` - run the signal handler only on worker N
        * ``worker/worker0`` - run the signal handler on the first available worker
        * ``active-workers`` - run the signal handlers on all the active [non-cheaped] workers

        * ``mules`` - run the signal handler on all of the mules
        * ``muleN`` - run the signal handler on mule N
        * ``mule/mule0`` - run the signal handler on the first available mule

        * ``spooler`` - run the signal on the first available spooler
        * ``farmN/farm_XXX``  - run the signal handler in the mule farm N or named XXX

        * http://uwsgi.readthedocs.io/en/latest/Signals.html#signals-targets

    :param func:

    :raises ValueError: If unable to register

    """


def reload() -> bool:
    """Gracefully reloads uWSGI.

    * http://uwsgi.readthedocs.io/en/latest/Management.html#reloading-the-server

    """
    return False


def request_id() -> int:
    """Returns current request number (handled by worker on core)."""
    return 0


def route(name: str, args_str: str) -> int:
    """Registers a named route for internal routing subsystem.

    :param name: Route name

    :param args_str: Comma-separated arguments string.

    """


def rpc(address: Optional[bytes], func_name: bytes, *args: bytes) -> bytes:
    """Performs an RPC function call with the given arguments.

    * http://uwsgi.readthedocs.io/en/latest/RPC.html

    :param address:

    :param func_name: Function name to call.

    :param args:

    :raises ValueError: If unable to call RPC function.

    """


def rpc_list() -> Tuple[bytes, ...]:
    """Returns registered RPC functions names."""
    return tuple()


def send(fd_or_data: Union[int, bytes], data: bytes = None) -> bool:
    """Puts data into file descriptor.

    * One argument. Data to write into request file descriptor.
    * Two arguments. 1. File descriptor; 2. Data to write.

    :param fd_or_data:

    :param data:

    """
    return False


def sendfile(fd_or_name: Union[int, str], chunk_size: int = 0, start_pos: int = 0, filesize: int = 0) -> Optional[bool]:
    """Runs a sendfile.

    :param fd_or_name: File path or descriptor number.

    :param chunk_size: Not used.

    :param start_pos:

    :param filesize: Filesize. If ``0`` will be determined automatically.

    """


def send_to_spooler(message: Dict[bytes, bytes] = None, **kwargs):
    """Send data to the The uWSGI Spooler. Also known as spool().

    .. warning:: Either `message` argument should contain a dictionary
        this message dictionary will be constructed from `kwargs`.

    :param message: The message to spool. Keys and values are bytes.
    :param kwargs:

        Possible kwargs (these are also reserved `message` argument dictionary keys):

            * spooler: The spooler (id or directory) to use.
                Specify the ABSOLUTE path of the spooler that has to manage this task

            * priority: Number. The priority of the message. Larger - less important.

                .. warning:: This works only if you enable `order_tasks` option in `spooler.set_basic_params()`.

                This will be the subdirectory in the spooler directory in which the task will be placed,
                you can use that trick to give a good-enough prioritization to tasks.

                .. note:: This is for systems with few resources. For better approach use multiple spoolers.

            * at: Unix time at which the task must be executed.
                The task will not be run until the 'at' time is passed.

            * body: A binary body to add to the message,
                in addition to the message dictionary itself.
                Use this key for objects bigger than 64k, the blob will be appended
                to the serialized uwsgi packet and passed back t
                o the spooler function as the 'body' argument.

    """


def set_logvar(name: str, value: str):
    """Sets log variable.

    :param name:

    :param value:

    """


def set_spooler_frequency(seconds: int) -> bool:
    """Sets how often the spooler runs.

    :param seconds:

    """
    return False


def set_user_harakiri(timeout: int = 0):
    """Sets user level harakiri.

    :param timeout: Seconds. ``0`` disable timer.

    """


def set_warning_message(message: str) -> bool:
    """Sets a warning. This will be reported by pingers.

    :param message:

    """
    return False


def setprocname(name: str):
    """Sets current process name.

    :param name:

    """


def signal(num: int, remote: str = ''):
    """Sends the signal to master or remote.

    :param num: Signal number.

    :param remote: Remote address.

    :raises ValueError: If remote rejected the signal.

    :raises IOError: If unable to deliver to remote.

    """


def signal_received() -> int:
    """Get the number of the last signal received.

    Used in conjunction with ``signal_wait``.

    * http://uwsgi-docs.readthedocs.io/en/latest/Signals.html#signal-wait-and-signal-received

    """


def signal_registered(num: int) -> Optional[int]:
    """Verifies the given signal has been registered.

    :param num:

    """


def signal_wait(num: int = None) -> str:
    """Waits for the given of any signal.

    Block the process/thread/async core until a signal is received. Use signal_received to get the number of
    the signal received. If a registered handler handles a signal, signal_wait will be interrupted and the actual
    handler will handle the signal.

    * http://uwsgi-docs.readthedocs.io/en/latest/Signals.html#signal-wait-and-signal-received

    :param int num:

    :raises SystemError: If something went wrong.

    """


spool = send_to_spooler


def spooler_get_task(path: str) -> Optional[dict]:
    """Returns a spooler task information.

    :param path: The relative or absolute path to the task to read.

    """


def spooler_jobs() -> List[str]:
    """Returns a list of spooler jobs (filenames in spooler directory)."""
    return []


def spooler_pid() -> int:
    """Returns first spooler process ID"""
    return -1


def spooler_pids() -> List[int]:
    """Returns a list of all spooler processes IDs."""
    return []


def stop() -> Optional[bool]:
    """Stops uWSGI."""


def suspend() -> bool:
    """Suspends handling of current coroutine/green thread and passes control
    to the next async core.

    * http://uwsgi.readthedocs.io/en/latest/Async.html#suspend-resume

    """
    return False


def total_requests() -> int:
    """Returns the total number of requests managed so far by the pool of uWSGI workers."""
    return 0


def unlock(lock_num: int = 0):
    """Unlocks the given lock.

    .. note:: Lock 0 is always available.

    :param lock_num: Lock number.

    :raises ValueError: For Spooler or invalid lock number

    """


def wait_fd_read(fd: int, timeout: int = None) -> bytes:
    """Suspends handling of the current request until there is something
    to be read on file descriptor.

    May be called several times before yielding/suspending
    to add more file descriptors to the set to be watched.

    * http://uwsgi-docs.readthedocs.io/en/latest/Async.html#waiting-for-i-o

    :param fd: File descriptor number.

    :param timeout: Timeout. Default:  infinite.

    :raises OSError: If unable to read.

    """


def wait_fd_write(fd: int, timeout: int = None) -> bytes:
    """Suspends handling of the current request until there is nothing more
    to be written on file descriptor.

    May be called several times to add more file descriptors to the set to be watched.

    * http://uwsgi-docs.readthedocs.io/en/latest/Async.html#waiting-for-i-o

    :param fd: File descriptor number.

    :param timeout: Timeout. Default:  infinite.

    :raises OSError: If unable to read.

    """


def websocket_handshake(security_key: str = None, origin: str = None, proto: str = None):
    """Waits for websocket handshake.

    :param security_key: Websocket security key to use.

    :param origin: Override ``Sec-WebSocket-Origin``.

    :param proto: Override ``Sec-WebSocket-Protocol``.

    :raises IOError: If unable to complete handshake.

    """


def websocket_recv(request_context=None) -> bytes:
    """Receives data from websocket.

    :param request_context:

    :raises IOError: If unable to receive a message.

    """


def websocket_recv_nb(request_context=None) -> bytes:
    """Receives data from websocket (non-blocking variant).

    :param request_context:

    :raises IOError: If unable to receive a message.

    """


# todo uWSGI 2.1+ has uwsgi.request_context()


def websocket_send(message: str, request_context=None):
    """Sends a message to websocket.

    :param message: data to send

    :param request_context:
        .. note:: uWSGI 2.1+

    :raises IOError: If unable to send a message.

    """


def websocket_send_binary(message: str, request_context=None):
    """Sends binary message to websocket.

    :param message: data to send

    :param request_context:
         .. note:: uWSGI 2.1+

    :raises IOError: If unable to send a message.

    """


def worker_id() -> int:
    """Returns current worker ID. 0 if not a worker (e.g. mule)."""
    return 0


def workers() -> Tuple[dict, ...]:
    """Gets statistics for all the workers for the current server.

    Returns tuple of dicts.

    """
    return tuple()


##
# Legion-related stuff:


def i_am_the_lord(legion_name: str) -> bool:
    """Returns flag indicating whether you are the lord
    of the given legion.

    * http://uwsgi.readthedocs.io/en/latest/Legion.html#legion-api

    :param legion_name:

    """
    return False


def lord_scroll(legion_name: str) -> bool:
    """Returns a Lord scroll for the Legion.

    * http://uwsgi.readthedocs.io/en/latest/Legion.html#lord-scroll-coming-soon

    :param legion_name:

    """
    return False


def scrolls(legion_name: str) -> List[str]:
    """Returns a list of Legion scrolls defined on cluster.

    :param legion_name:

    """
    return []
