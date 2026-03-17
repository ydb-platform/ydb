from __future__ import absolute_import

import sys
from concurrent.futures import Future
from threading import Event, Thread

from tornado import ioloop
from tornado import gen

from .exceptions import ThreadNotStartedError

# Python3's concurrent.futures.Future doesn't allow
# setting exc_info... but exc_info works w/o setting explicitly
_FUTURE_HAS_EXC_INFO = hasattr(Future, "set_exception_info")


class ThreadLoop(object):
    """Tornado IOLoop Backed Concurrent Futures.

    Run Tornado Coroutines from Synchronous Python.

    This is made possible by starting the IOLoop in another thread. When
    coroutines are submitted, they are ran against that loop, and their
    responses are bound to Concurrent Futures.

    .. code-block:: python

        from threadloop import ThreadLoop
        from tornado import gen

        @gen.coroutine
        def coroutine(greeting="Goodbye"):
            yield gen.sleep(1)
            raise gen.Return("%s World" % greeting)

        with ThreadLoop() as threadloop:

            future = threadloop.submit(coroutine, "Hello")

            print future.result() # Hello World

    """
    def __init__(self, io_loop=None):

        self._thread = None
        self._ready = Event()
        self._io_loop = io_loop

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

    def start(self):
        """Start IOLoop in daemonized thread."""
        assert self._thread is None, 'thread already started'

        # configure thread
        self._thread = Thread(target=self._start_io_loop)
        self._thread.daemon = True

        # begin thread and block until ready
        self._thread.start()
        self._ready.wait()

    def _start_io_loop(self):
        """Start IOLoop then set ready threading.Event."""

        def mark_as_ready():
            self._ready.set()

        if not self._io_loop:
            self._io_loop = ioloop.IOLoop()

        self._io_loop.add_callback(mark_as_ready)
        self._io_loop.start()

    def is_ready(self):
        """Is thread & ioloop ready.

        :returns bool:
        """
        if not self._thread:
            return False

        if not self._ready.is_set():
            return False

        return True

    def stop(self):
        """Stop IOLoop & close daemonized thread."""
        self._io_loop.stop()
        self._thread.join()

    def submit(self, fn, *args, **kwargs):
        """Submit Tornado Coroutine to IOLoop in daemonized thread.

        :param fn: Tornado Coroutine to execute
        :param args: Args to pass to coroutine
        :param kwargs: Kwargs to pass to coroutine
        :returns concurrent.futures.Future: future result of coroutine
        """
        if not self.is_ready():
            raise ThreadNotStartedError(
                "The thread has not been started yet, "
                "make sure you call start() first"
            )

        future = Future()

        def execute():
            """Executes fn on the IOLoop."""
            try:
                result = gen.maybe_future(fn(*args, **kwargs))
            except Exception:
                # The function we ran didn't return a future and instead raised
                # an exception. Let's pretend that it returned this dummy
                # future with our stack trace.
                f = gen.Future()
                f.set_exc_info(sys.exc_info())
                on_done(f)
            else:
                result.add_done_callback(on_done)

        def on_done(f):
            """Sets tornado.Future results to the concurrent.Future."""

            if not f.exception():
                future.set_result(f.result())
                return

            # if f is a tornado future, then it has exc_info()
            if hasattr(f, 'exc_info'):
                exception, traceback = f.exc_info()[1:]

            # else it's a concurrent.future
            else:
                # python2's concurrent.future has exception_info()
                if hasattr(f, 'exception_info'):
                    exception, traceback = f.exception_info()

                # python3's concurrent.future just has exception()
                else:
                    exception = f.exception()
                    traceback = None

            # python2 needs exc_info set explicitly
            if _FUTURE_HAS_EXC_INFO:
                future.set_exception_info(exception, traceback)
                return

            # python3 just needs the exception, exc_info works fine
            future.set_exception(exception)

        self._io_loop.add_callback(execute)

        return future
