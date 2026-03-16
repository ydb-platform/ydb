"""A threading based handler.

The :class:`SequentialThreadingHandler` is intended for regular Python
environments that use threads.

.. warning::

    Do not use :class:`SequentialThreadingHandler` with applications
    using asynchronous event loops (like gevent). Use the
    :class:`~kazoo.handlers.gevent.SequentialGeventHandler` instead.

"""
from __future__ import absolute_import

import atexit
import logging
import queue
import socket
import threading
import time

from kazoo.handlers import utils
from kazoo.handlers.utils import selector_select


# sentinel objects
_STOP = object()

log = logging.getLogger(__name__)


def _to_fileno(obj):
    if isinstance(obj, int):
        fd = int(obj)
    elif hasattr(obj, "fileno"):
        fd = obj.fileno()
        if not isinstance(fd, int):
            raise TypeError("fileno() returned a non-integer")
        fd = int(fd)
    else:
        raise TypeError("argument must be an int, or have a fileno() method.")

    if fd < 0:
        raise ValueError(
            "file descriptor cannot be a negative integer (%d)" % (fd,)
        )

    return fd


class KazooTimeoutError(Exception):
    pass


class AsyncResult(utils.AsyncResult):
    """A one-time event that stores a value or an exception"""

    def __init__(self, handler):
        super(AsyncResult, self).__init__(
            handler, threading.Condition, KazooTimeoutError
        )


class SequentialThreadingHandler(object):
    """Threading handler for sequentially executing callbacks.

    This handler executes callbacks in a sequential manner. A queue is
    created for each of the callback events, so that each type of event
    has its callback type run sequentially. These are split into two
    queues, one for watch events and one for async result completion
    callbacks.

    Each queue type has a thread worker that pulls the callback event
    off the queue and runs it in the order the client sees it.

    This split helps ensure that watch callbacks won't block session
    re-establishment should the connection be lost during a Zookeeper
    client call.

    Watch and completion callbacks should avoid blocking behavior as
    the next callback of that type won't be run until it completes. If
    you need to block, spawn a new thread and return immediately so
    callbacks can proceed.

    .. note::

        Completion callbacks can block to wait on Zookeeper calls, but
        no other completion callbacks will execute until the callback
        returns.

    """

    name = "sequential_threading_handler"
    timeout_exception = KazooTimeoutError
    sleep_func = staticmethod(time.sleep)
    queue_impl = queue.Queue
    queue_empty = queue.Empty

    def __init__(self):
        """Create a :class:`SequentialThreadingHandler` instance"""
        self.callback_queue = self.queue_impl()
        self.completion_queue = self.queue_impl()
        self._running = False
        self._state_change = threading.Lock()
        self._workers = []

    @property
    def running(self):
        return self._running

    def _create_thread_worker(self, work_queue):
        def _thread_worker():  # pragma: nocover
            while True:
                try:
                    func = work_queue.get()
                    try:
                        if func is _STOP:
                            break
                        func()
                    except Exception:
                        log.exception("Exception in worker queue thread")
                    finally:
                        work_queue.task_done()
                        del func  # release before possible idle
                except self.queue_empty:
                    continue

        t = self.spawn(_thread_worker)
        return t

    def start(self):
        """Start the worker threads."""
        with self._state_change:
            if self._running:
                return

            # Spawn our worker threads, we have
            # - A callback worker for watch events to be called
            # - A completion worker for completion events to be called
            for work_queue in (self.completion_queue, self.callback_queue):
                w = self._create_thread_worker(work_queue)
                self._workers.append(w)
            self._running = True
            atexit.register(self.stop)

    def stop(self):
        """Stop the worker threads and empty all queues."""
        with self._state_change:
            if not self._running:
                return

            self._running = False

            for work_queue in (self.completion_queue, self.callback_queue):
                work_queue.put(_STOP)

            self._workers.reverse()
            while self._workers:
                worker = self._workers.pop()
                worker.join()

            # Clear the queues
            self.callback_queue = self.queue_impl()
            self.completion_queue = self.queue_impl()
            atexit.unregister(self.stop)

    def select(self, *args, **kwargs):
        return selector_select(*args, **kwargs)

    def socket(self):
        return utils.create_tcp_socket(socket)

    def create_connection(self, *args, **kwargs):
        return utils.create_tcp_connection(socket, *args, **kwargs)

    def create_socket_pair(self):
        return utils.create_socket_pair(socket)

    def event_object(self):
        """Create an appropriate Event object"""
        return threading.Event()

    def lock_object(self):
        """Create a lock object"""
        return threading.Lock()

    def rlock_object(self):
        """Create an appropriate RLock object"""
        return threading.RLock()

    def async_result(self):
        """Create a :class:`AsyncResult` instance"""
        return AsyncResult(self)

    def spawn(self, func, *args, **kwargs):
        t = threading.Thread(target=func, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        return t

    def dispatch_callback(self, callback):
        """Dispatch to the callback object

        The callback is put on separate queues to run depending on the
        type as documented for the :class:`SequentialThreadingHandler`.

        """
        self.callback_queue.put(lambda: callback.func(*callback.args))
