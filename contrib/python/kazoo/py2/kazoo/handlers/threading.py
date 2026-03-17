"""A threading based handler.

The :class:`SequentialThreadingHandler` is intended for regular Python
environments that use threads.

.. warning::

    Do not use :class:`SequentialThreadingHandler` with applications
    using asynchronous event loops (like gevent). Use the
    :class:`~kazoo.handlers.gevent.SequentialGeventHandler` instead.

"""
from __future__ import absolute_import

from collections import defaultdict
import errno
from itertools import chain
import logging
import select
import socket
import threading
import time

import six

import kazoo.python2atexit as python2atexit
from kazoo.handlers import utils

try:
    import Queue
except ImportError:  # pragma: nocover
    import queue as Queue


# sentinel objects
_STOP = object()

log = logging.getLogger(__name__)

_HAS_EPOLL = hasattr(select, "epoll")


def _to_fileno(obj):
    if isinstance(obj, six.integer_types):
        fd = int(obj)
    elif hasattr(obj, "fileno"):
        fd = obj.fileno()
        if not isinstance(fd, six.integer_types):
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
        super(AsyncResult, self).__init__(handler,
                                          threading.Condition,
                                          KazooTimeoutError)


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
    queue_impl = Queue.Queue
    queue_empty = Queue.Empty

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

    def _create_thread_worker(self, queue):
        def _thread_worker():  # pragma: nocover
            while True:
                try:
                    func = queue.get()
                    try:
                        if func is _STOP:
                            break
                        func()
                    except Exception:
                        log.exception("Exception in worker queue thread")
                    finally:
                        queue.task_done()
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
            for queue in (self.completion_queue, self.callback_queue):
                w = self._create_thread_worker(queue)
                self._workers.append(w)
            self._running = True
            python2atexit.register(self.stop)

    def stop(self):
        """Stop the worker threads and empty all queues."""
        with self._state_change:
            if not self._running:
                return

            self._running = False

            for queue in (self.completion_queue, self.callback_queue):
                queue.put(_STOP)

            self._workers.reverse()
            while self._workers:
                worker = self._workers.pop()
                worker.join()

            # Clear the queues
            self.callback_queue = self.queue_impl()
            self.completion_queue = self.queue_impl()
            python2atexit.unregister(self.stop)

    def select(self, *args, **kwargs):
        # if we have epoll, and select is not expected to work
        # use an epoll-based "select". Otherwise don't touch
        # anything to minimize changes
        if _HAS_EPOLL:
            # if the highest fd we've seen is > 1023
            if max(map(_to_fileno, chain.from_iterable(args[:3]))) > 1023:
                return self._epoll_select(*args, **kwargs)
        return self._select(*args, **kwargs)

    def _select(self, *args, **kwargs):
        timeout = kwargs.pop('timeout', None)
        # either the time to give up, or None
        end = (time.time() + timeout) if timeout else None
        while end is None or time.time() < end:
            if end is not None:
                # make a list, since tuples aren't mutable
                args = list(args)

                # set the timeout to the remaining time
                args[3] = end - time.time()
            try:
                return select.select(*args, **kwargs)
            except select.error as ex:
                # if the system call was interrupted, we'll retry until timeout
                # in Python 3, system call interruptions are a native exception
                # in Python 2, they are not
                errnum = ex.errno if isinstance(ex, OSError) else ex[0]
                if errnum == errno.EINTR:
                    continue
                raise
        # if we hit our timeout, lets return as a timeout
        return ([], [], [])

    def _epoll_select(self, rlist, wlist, xlist, timeout=None):
        """epoll-based drop-in replacement for select to overcome select
        limitation on a maximum filehandle value
        """
        if timeout is None:
            timeout = -1
        eventmasks = defaultdict(int)
        rfd2obj = defaultdict(list)
        wfd2obj = defaultdict(list)
        xfd2obj = defaultdict(list)
        read_evmask = select.EPOLLIN | select.EPOLLPRI  # Just in case

        def store_evmasks(obj_list, evmask, fd2obj):
            for obj in obj_list:
                fileno = _to_fileno(obj)
                eventmasks[fileno] |= evmask
                fd2obj[fileno].append(obj)

        store_evmasks(rlist, read_evmask, rfd2obj)
        store_evmasks(wlist, select.EPOLLOUT, wfd2obj)
        store_evmasks(xlist, select.EPOLLERR, xfd2obj)

        poller = select.epoll()

        for fileno in eventmasks:
            poller.register(fileno, eventmasks[fileno])

        try:
            events = poller.poll(timeout)
            revents = []
            wevents = []
            xevents = []
            for fileno, event in events:
                if event & read_evmask:
                    revents += rfd2obj.get(fileno, [])
                if event & select.EPOLLOUT:
                    wevents += wfd2obj.get(fileno, [])
                if event & select.EPOLLERR:
                    xevents += xfd2obj.get(fileno, [])
        finally:
            poller.close()

        return revents, wevents, xevents

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
