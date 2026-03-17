"""A gevent based handler."""
from __future__ import absolute_import

import atexit
import logging

import gevent
from gevent import socket
import gevent.event
import gevent.queue
import gevent.select
import gevent.thread
import gevent.selectors

from kazoo.handlers.utils import selector_select

from gevent.lock import Semaphore, RLock

from kazoo.handlers import utils

_using_libevent = gevent.__version__.startswith("0.")

log = logging.getLogger(__name__)

_STOP = object()

AsyncResult = gevent.event.AsyncResult


class SequentialGeventHandler(object):
    """Gevent handler for sequentially executing callbacks.

    This handler executes callbacks in a sequential manner. A queue is
    created for each of the callback events, so that each type of event
    has its callback type run sequentially.

    Each queue type has a greenlet worker that pulls the callback event
    off the queue and runs it in the order the client sees it.

    This split helps ensure that watch callbacks won't block session
    re-establishment should the connection be lost during a Zookeeper
    client call.

    Watch callbacks should avoid blocking behavior as the next callback
    of that type won't be run until it completes. If you need to block,
    spawn a new greenlet and return immediately so callbacks can
    proceed.

    """

    name = "sequential_gevent_handler"
    queue_impl = gevent.queue.Queue
    queue_empty = gevent.queue.Empty
    sleep_func = staticmethod(gevent.sleep)

    def __init__(self):
        """Create a :class:`SequentialGeventHandler` instance"""
        self.callback_queue = self.queue_impl()
        self._running = False
        self._async = None
        self._state_change = Semaphore()
        self._workers = []

    @property
    def running(self):
        return self._running

    class timeout_exception(gevent.Timeout):
        def __init__(self, msg):
            gevent.Timeout.__init__(self, exception=msg)

    def _create_greenlet_worker(self, queue):
        def greenlet_worker():
            while True:
                try:
                    func = queue.get()
                    try:
                        if func is _STOP:
                            break
                        func()
                    except Exception as exc:
                        log.warning("Exception in worker greenlet")
                        log.exception(exc)
                    finally:
                        del func  # release before possible idle
                except self.queue_empty:
                    continue

        return gevent.spawn(greenlet_worker)

    def start(self):
        """Start the greenlet workers."""
        with self._state_change:
            if self._running:
                return

            self._running = True

            # Spawn our worker greenlets, we have
            # - A callback worker for watch events to be called
            for queue in (self.callback_queue,):
                w = self._create_greenlet_worker(queue)
                self._workers.append(w)
            atexit.register(self.stop)

    def stop(self):
        """Stop the greenlet workers and empty all queues."""
        with self._state_change:
            if not self._running:
                return

            self._running = False

            for queue in (self.callback_queue,):
                queue.put(_STOP)

            while self._workers:
                worker = self._workers.pop()
                worker.join()

            # Clear the queues
            self.callback_queue = self.queue_impl()  # pragma: nocover

            atexit.unregister(self.stop)

    def select(self, *args, **kwargs):
        return selector_select(
            *args, selectors_module=gevent.selectors, **kwargs
        )

    def socket(self, *args, **kwargs):
        return utils.create_tcp_socket(socket)

    def create_connection(self, *args, **kwargs):
        return utils.create_tcp_connection(socket, *args, **kwargs)

    def create_socket_pair(self):
        return utils.create_socket_pair(socket)

    def event_object(self):
        """Create an appropriate Event object"""
        return gevent.event.Event()

    def lock_object(self):
        """Create an appropriate Lock object"""
        return gevent.thread.allocate_lock()

    def rlock_object(self):
        """Create an appropriate RLock object"""
        return RLock()

    def async_result(self):
        """Create a :class:`AsyncResult` instance

        The :class:`AsyncResult` instance will have its completion
        callbacks executed in the thread the
        :class:`SequentialGeventHandler` is created in (which should be
        the gevent/main thread).

        """
        return AsyncResult()

    def spawn(self, func, *args, **kwargs):
        """Spawn a function to run asynchronously"""
        return gevent.spawn(func, *args, **kwargs)

    def dispatch_callback(self, callback):
        """Dispatch to the callback object

        The callback is put on separate queues to run depending on the
        type as documented for the :class:`SequentialGeventHandler`.

        """
        self.callback_queue.put(lambda: callback.func(*callback.args))
