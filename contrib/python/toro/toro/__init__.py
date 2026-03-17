import contextlib
import heapq
import collections
from functools import partial
try:
    from queue import Full, Empty
except ImportError:
    from Queue import Full, Empty

import tornado
from tornado import ioloop
from tornado import gen
from tornado.concurrent import Future


version_tuple = (1, 0, 1)

version = '.'.join(map(str, version_tuple))
"""Current version of Toro."""


__all__ = [
    # Exceptions
    'NotReady', 'AlreadySet', 'Full', 'Empty', 'Timeout',

    # Primitives
    'AsyncResult', 'Event', 'Condition',  'Semaphore', 'BoundedSemaphore',
    'Lock',

    # Queues
    'Queue', 'PriorityQueue', 'LifoQueue', 'JoinableQueue'
]


class NotReady(Exception):
    """Raised when accessing an :class:`AsyncResult` that has no value yet."""
    pass


class AlreadySet(Exception):
    """Raised when setting a value on an :class:`AsyncResult` that already
    has one."""
    pass


class Timeout(Exception):
    """Raised when a deadline passes before a Future is ready."""

    def __str__(self):
        return "Timeout"


class _TimeoutFuture(Future):

    def __init__(self, deadline, io_loop):
        """Create a Future with optional deadline.

        If deadline is not None, it may be a number denoting a unix timestamp
        (as returned by ``io_loop.time()``) or a ``datetime.timedelta`` object
        for a deadline relative to the current time.

        set_exception(toro.Timeout()) is executed after a timeout.
        """

        super(_TimeoutFuture, self).__init__()
        self.io_loop = io_loop
        if deadline is not None:
            callback = partial(self.set_exception, Timeout())
            self._timeout_handle = io_loop.add_timeout(deadline, callback)
        else:
            self._timeout_handle = None

    def set_result(self, result):
        self._cancel_timeout()
        super(_TimeoutFuture, self).set_result(result)

    def set_exception(self, exception):
        self._cancel_timeout()
        super(_TimeoutFuture, self).set_exception(exception)

    def _cancel_timeout(self):
        if self._timeout_handle:
            self.io_loop.remove_timeout(self._timeout_handle)
            self._timeout_handle = None


class _ContextManagerList(list):
    def __enter__(self, *args, **kwargs):
        for obj in self:
            obj.__enter__(*args, **kwargs)

    def __exit__(self, *args, **kwargs):
        for obj in self:
            obj.__exit__(*args, **kwargs)


class _ContextManagerFuture(Future):
    """A Future that can be used with the "with" statement.

    When a coroutine yields this Future, the return value is a context manager
    that can be used like:

        with (yield future):
            pass

    At the end of the block, the Future's exit callback is run. Used for
    Lock.acquire, Semaphore.acquire, RWLock.acquire_read / acquire_write.
    """
    def __init__(self, wrapped, exit_callback):
        super(_ContextManagerFuture, self).__init__()
        wrapped.add_done_callback(self._done_callback)
        self.exit_callback = exit_callback

    def _done_callback(self, wrapped):
        if wrapped.exception():
            self.set_exception(wrapped.exception())
        else:
            self.set_result(wrapped.result())

    def result(self):
        if self.exception():
            raise self.exception()

        # Otherwise return a context manager that cleans up after the block.
        @contextlib.contextmanager
        def f():
            try:
                yield
            finally:
                self.exit_callback()
        return f()


def _consume_expired_waiters(waiters):
    # Delete waiters at the head of the queue who've timed out
    while waiters and waiters[0].done():
        waiters.popleft()


_null_result = object()


class AsyncResult(object):
    """A one-time event that stores a value or an exception.

    The only distinction between AsyncResult and a simple Future is that
    AsyncResult lets coroutines wait with a deadline. The deadline can be
    configured separately for each waiter.

    An :class:`AsyncResult` instance cannot be reset.

    :Parameters:
      - `io_loop`: Optional custom IOLoop.
    """

    def __init__(self, io_loop=None):
        self.io_loop = io_loop or ioloop.IOLoop.current()
        self.value = _null_result
        self.waiters = []

    def __str__(self):
        result = '<%s ' % (self.__class__.__name__, )
        if self.ready():
            result += 'value=%r' % self.value
        else:
            result += 'unset'
            if self.waiters:
                result += ' waiters[%s]' % len(self.waiters)

        return result + '>'

    def set(self, value):
        """Set a value and wake up all the waiters."""
        if self.ready():
            raise AlreadySet

        self.value = value
        waiters, self.waiters = self.waiters, []
        for waiter in waiters:
            if not waiter.done():  # Might have timed out
                waiter.set_result(value)

    def ready(self):
        return self.value is not _null_result

    def get(self, deadline=None):
        """Get a value once :meth:`set` is called. Returns a Future.

        The Future's result will be the value. The Future raises
        :exc:`toro.Timeout` if no value is set before the deadline.

        :Parameters:
          - `deadline`: Optional timeout, either an absolute timestamp
            (as returned by ``io_loop.time()``) or a ``datetime.timedelta`` for
            a deadline relative to the current time.
        """
        future = _TimeoutFuture(deadline, self.io_loop)
        if self.ready():
            future.set_result(self.value)
        else:
            self.waiters.append(future)

        return future

    def get_nowait(self):
        """Get the value if ready, or raise :class:`NotReady`."""
        if self.ready():
            return self.value
        else:
            raise NotReady


class Condition(object):
    """A condition allows one or more coroutines to wait until notified.

    Like a standard Condition_, but does not need an underlying lock that
    is acquired and released.

    .. _Condition: http://docs.python.org/library/threading.html#threading.Condition

    :Parameters:
      - `io_loop`: Optional custom IOLoop.
    """

    def __init__(self, io_loop=None):
        self.io_loop = io_loop or ioloop.IOLoop.current()
        self.waiters = collections.deque()  # Queue of _Waiter objects

    def __str__(self):
        result = '<%s' % (self.__class__.__name__, )
        if self.waiters:
            result += ' waiters[%s]' % len(self.waiters)
        return result + '>'

    def wait(self, deadline=None):
        """Wait for :meth:`notify`. Returns a Future.

        :exc:`~toro.Timeout` is executed after a timeout.

        :Parameters:
          - `deadline`: Optional timeout, either an absolute timestamp
            (as returned by ``io_loop.time()``) or a ``datetime.timedelta`` for a
            deadline relative to the current time.
        """
        future = _TimeoutFuture(deadline, self.io_loop)
        self.waiters.append(future)
        return future

    def notify(self, n=1):
        """Wake up `n` waiters.

        :Parameters:
          - `n`: The number of waiters to awaken (default: 1)
        """
        waiters = []  # Waiters we plan to run right now
        while n and self.waiters:
            waiter = self.waiters.popleft()
            if not waiter.done():  # Might have timed out
                n -= 1
                waiters.append(waiter)

        for waiter in waiters:
            waiter.set_result(None)

    def notify_all(self):
        """Wake up all waiters."""
        self.notify(len(self.waiters))


# TODO: show correct examples that avoid thread / process issues w/ concurrent.futures.Future
class Event(object):
    """An event blocks coroutines until its internal flag is set to True.

    Similar to threading.Event_.

    .. _threading.Event: http://docs.python.org/library/threading.html#threading.Event

    .. seealso:: :doc:`examples/event_example`

    :Parameters:
      - `io_loop`: Optional custom IOLoop.
    """

    def __init__(self, io_loop=None):
        self.io_loop = io_loop or ioloop.IOLoop.current()
        self.condition = Condition(io_loop=io_loop)
        self._flag = False

    def __str__(self):
        return '<%s %s>' % (
            self.__class__.__name__, 'set' if self._flag else 'clear')

    def is_set(self):
        """Return ``True`` if and only if the internal flag is true."""
        return self._flag

    def set(self):
        """Set the internal flag to ``True``. All waiters are awakened.
        Calling :meth:`wait` once the flag is true will not block.
        """
        self._flag = True
        self.condition.notify_all()

    def clear(self):
        """Reset the internal flag to ``False``. Calls to :meth:`wait`
        will block until :meth:`set` is called.
        """
        self._flag = False

    def wait(self, deadline=None):
        """Block until the internal flag is true. Returns a Future.

        The Future raises :exc:`~toro.Timeout` after a timeout.

        :Parameters:
          - `callback`: Function taking no arguments.
          - `deadline`: Optional timeout, either an absolute timestamp
            (as returned by ``io_loop.time()``) or a ``datetime.timedelta`` for a
            deadline relative to the current time.
        """
        if self._flag:
            future = _TimeoutFuture(None, self.io_loop)
            future.set_result(None)
            return future
        else:
            return self.condition.wait(deadline)


class Queue(object):
    """Create a queue object with a given maximum size.

    If `maxsize` is 0 (the default) the queue size is unbounded.

    Unlike the `standard Queue`_, you can reliably know this Queue's size
    with :meth:`qsize`, since your single-threaded Tornado application won't
    be interrupted between calling :meth:`qsize` and doing an operation on the
    Queue.

    **Examples:**

    :doc:`examples/producer_consumer_example`

    :doc:`examples/web_spider_example`

    :Parameters:
      - `maxsize`: Optional size limit (no limit by default).
      - `io_loop`: Optional custom IOLoop.

    .. _`Gevent's Queue`: http://www.gevent.org/gevent.queue.html

    .. _`standard Queue`: http://docs.python.org/library/queue.html#Queue.Queue
    """
    def __init__(self, maxsize=0, io_loop=None):
        self.io_loop = io_loop or ioloop.IOLoop.current()
        if maxsize is None:
            raise TypeError("maxsize can't be None")

        if maxsize < 0:
            raise ValueError("maxsize can't be negative")

        self._maxsize = maxsize

        # _TimeoutFutures
        self.getters = collections.deque([])
        # Pairs of (item, _TimeoutFuture)
        self.putters = collections.deque([])
        self._init(maxsize)

    # These three are overridable in subclasses.
    def _init(self, maxsize):
        self.queue = collections.deque()

    def _get(self):
        return self.queue.popleft()

    def _put(self, item):
        self.queue.append(item)

    def __repr__(self):
        return '<%s at %s %s>' % (
            type(self).__name__, hex(id(self)), self._format())

    def __str__(self):
        return '<%s %s>' % (type(self).__name__, self._format())

    def _format(self):
        result = 'maxsize=%r' % (self.maxsize, )
        if getattr(self, 'queue', None):
            result += ' queue=%r' % self.queue
        if self.getters:
            result += ' getters[%s]' % len(self.getters)
        if self.putters:
            result += ' putters[%s]' % len(self.putters)
        return result

    def _consume_expired_putters(self):
        # Delete waiters at the head of the queue who've timed out
        while self.putters and self.putters[0][1].done():
            self.putters.popleft()

    def qsize(self):
        """Number of items in the queue"""
        return len(self.queue)

    @property
    def maxsize(self):
        """Number of items allowed in the queue."""
        return self._maxsize

    def empty(self):
        """Return ``True`` if the queue is empty, ``False`` otherwise."""
        return not self.queue

    def full(self):
        """Return ``True`` if there are `maxsize` items in the queue.

        .. note:: if the Queue was initialized with `maxsize=0`
          (the default), then :meth:`full` is never ``True``.
        """
        if self.maxsize == 0:
            return False
        else:
            return self.maxsize <= self.qsize()

    def put(self, item, deadline=None):
        """Put an item into the queue. Returns a Future.

        The Future blocks until a free slot is available for `item`, or raises
        :exc:`toro.Timeout`.

        :Parameters:
          - `deadline`: Optional timeout, either an absolute timestamp
            (as returned by ``io_loop.time()``) or a ``datetime.timedelta`` for a
            deadline relative to the current time.
        """
        _consume_expired_waiters(self.getters)
        future = _TimeoutFuture(deadline, self.io_loop)
        if self.getters:
            assert not self.queue, "queue non-empty, why are getters waiting?"
            getter = self.getters.popleft()

            # Use _put and _get instead of passing item straight to getter, in
            # case a subclass has logic that must run (e.g. JoinableQueue).
            self._put(item)
            getter.set_result(self._get())
            future.set_result(None)
        else:
            if self.maxsize and self.maxsize <= self.qsize():
                self.putters.append((item, future))
            else:
                self._put(item)
                future.set_result(None)

        return future

    def put_nowait(self, item):
        """Put an item into the queue without blocking.

        If no free slot is immediately available, raise queue.Full.
        """
        _consume_expired_waiters(self.getters)
        if self.getters:
            assert not self.queue, "queue non-empty, why are getters waiting?"
            getter = self.getters.popleft()

            self._put(item)
            getter.set_result(self._get())
        elif self.maxsize and self.maxsize <= self.qsize():
            raise Full
        else:
            self._put(item)

    def get(self, deadline=None):
        """Remove and return an item from the queue. Returns a Future.

        The Future blocks until an item is available, or raises
        :exc:`toro.Timeout`.

        :Parameters:
          - `deadline`: Optional timeout, either an absolute timestamp
            (as returned by ``io_loop.time()``) or a ``datetime.timedelta`` for a
            deadline relative to the current time.
        """
        self._consume_expired_putters()
        future = _TimeoutFuture(deadline, self.io_loop)
        if self.putters:
            assert self.full(), "queue not full, why are putters waiting?"
            item, putter = self.putters.popleft()
            self._put(item)
            putter.set_result(None)
            future.set_result(self._get())
        elif self.qsize():
            future.set_result(self._get())
        else:
            self.getters.append(future)

        return future

    def get_nowait(self):
        """Remove and return an item from the queue without blocking.

        Return an item if one is immediately available, else raise
        :exc:`queue.Empty`.
        """
        self._consume_expired_putters()
        if self.putters:
            assert self.full(), "queue not full, why are putters waiting?"
            item, putter = self.putters.popleft()
            self._put(item)
            putter.set_result(None)
            return self._get()
        elif self.qsize():
            return self._get()
        else:
            raise Empty


class PriorityQueue(Queue):
    """A subclass of :class:`Queue` that retrieves entries in priority order
    (lowest first).

    Entries are typically tuples of the form: ``(priority number, data)``.

    :Parameters:
      - `maxsize`: Optional size limit (no limit by default).
      - `initial`: Optional sequence of initial items.
      - `io_loop`: Optional custom IOLoop.
    """
    def _init(self, maxsize):
        self.queue = []

    def _put(self, item, heappush=heapq.heappush):
        heappush(self.queue, item)

    def _get(self, heappop=heapq.heappop):
        return heappop(self.queue)


class LifoQueue(Queue):
    """A subclass of :class:`Queue` that retrieves most recently added entries
    first.

    :Parameters:
      - `maxsize`: Optional size limit (no limit by default).
      - `initial`: Optional sequence of initial items.
      - `io_loop`: Optional custom IOLoop.
    """
    def _init(self, maxsize):
        self.queue = []

    def _put(self, item):
        self.queue.append(item)

    def _get(self):
        return self.queue.pop()


class JoinableQueue(Queue):
    """A subclass of :class:`Queue` that additionally has :meth:`task_done`
    and :meth:`join` methods.

    .. seealso:: :doc:`examples/web_spider_example`

    :Parameters:
      - `maxsize`: Optional size limit (no limit by default).
      - `initial`: Optional sequence of initial items.
      - `io_loop`: Optional custom IOLoop.
    """
    def __init__(self, maxsize=0, io_loop=None):
        Queue.__init__(self, maxsize=maxsize, io_loop=io_loop)
        self.unfinished_tasks = 0
        self._finished = Event(io_loop)
        self._finished.set()

    def _format(self):
        result = Queue._format(self)
        if self.unfinished_tasks:
            result += ' tasks=%s' % self.unfinished_tasks
        return result

    def _put(self, item):
        self.unfinished_tasks += 1
        self._finished.clear()
        Queue._put(self, item)

    def task_done(self):
        """Indicate that a formerly enqueued task is complete.

        Used by queue consumers. For each :meth:`get <Queue.get>` used to
        fetch a task, a subsequent call to :meth:`task_done` tells the queue
        that the processing on the task is complete.

        If a :meth:`join` is currently blocking, it will resume when all
        items have been processed (meaning that a :meth:`task_done` call was
        received for every item that had been :meth:`put <Queue.put>` into the
        queue).

        Raises ``ValueError`` if called more times than there were items
        placed in the queue.
        """
        if self.unfinished_tasks <= 0:
            raise ValueError('task_done() called too many times')
        self.unfinished_tasks -= 1
        if self.unfinished_tasks == 0:
            self._finished.set()

    def join(self, deadline=None):
        """Block until all items in the queue are processed. Returns a Future.

        The count of unfinished tasks goes up whenever an item is added to
        the queue. The count goes down whenever a consumer calls
        :meth:`task_done` to indicate that all work on the item is complete.
        When the count of unfinished tasks drops to zero, :meth:`join`
        unblocks.

        The Future raises :exc:`toro.Timeout` if the count is not zero before
        the deadline.

        :Parameters:
          - `deadline`: Optional timeout, either an absolute timestamp
            (as returned by ``io_loop.time()``) or a ``datetime.timedelta`` for a
            deadline relative to the current time.
        """
        return self._finished.wait(deadline)


class Semaphore(object):
    """A lock that can be acquired a fixed number of times before blocking.

    A Semaphore manages a counter representing the number of release() calls
    minus the number of acquire() calls, plus an initial value. The acquire()
    method blocks if necessary until it can return without making the counter
    negative.

    If not given, value defaults to 1.

    :meth:`acquire` supports the context manager protocol:

    >>> from tornado import gen
    >>> import toro
    >>> semaphore = toro.Semaphore()
    >>>
    >>> @gen.coroutine
    ... def f():
    ...    with (yield semaphore.acquire()):
    ...        assert semaphore.locked()
    ...
    ...    assert not semaphore.locked()

    .. note:: Unlike the standard threading.Semaphore_, a :class:`Semaphore`
      can tell you the current value of its :attr:`counter`, because code in a
      single-threaded Tornado app can check these values and act upon them
      without fear of interruption from another thread.

    .. _threading.Semaphore: http://docs.python.org/library/threading.html#threading.Semaphore

    .. seealso:: :doc:`examples/web_spider_example`

    :Parameters:
      - `value`: An int, the initial value (default 1).
      - `io_loop`: Optional custom IOLoop.
    """
    def __init__(self, value=1, io_loop=None):
        if value < 0:
            raise ValueError('semaphore initial value must be >= 0')

        # The semaphore is implemented as a Queue with 'value' objects
        self.q = Queue(io_loop=io_loop)
        for _ in range(value):
            self.q.put_nowait(None)

        self._unlocked = Event(io_loop=io_loop)
        if value:
            self._unlocked.set()

    def __repr__(self):
        return '<%s at %s%s>' % (
            type(self).__name__, hex(id(self)), self._format())

    def __str__(self):
        return '<%s%s>' % (
            self.__class__.__name__, self._format())

    def _format(self):
        return ' counter=%s' % self.counter

    @property
    def counter(self):
        """An integer, the current semaphore value"""
        return self.q.qsize()

    def locked(self):
        """True if :attr:`counter` is zero"""
        return self.q.empty()

    def release(self):
        """Increment :attr:`counter` and wake one waiter.
        """
        self.q.put(None)
        if not self.locked():
            # No one was waiting on acquire(), so self.q.qsize() is positive
            self._unlocked.set()

    def wait(self, deadline=None):
        """Wait for :attr:`locked` to be False. Returns a Future.

        The Future raises :exc:`toro.Timeout` after the deadline.

        :Parameters:
          - `deadline`: Optional timeout, either an absolute timestamp
            (as returned by ``io_loop.time()``) or a ``datetime.timedelta`` for a
            deadline relative to the current time.
        """
        return self._unlocked.wait(deadline)

    def acquire(self, deadline=None):
        """Decrement :attr:`counter`. Returns a Future.

        Block if the counter is zero and wait for a :meth:`release`. The
        Future raises :exc:`toro.Timeout` after the deadline.

        :Parameters:
          - `deadline`: Optional timeout, either an absolute timestamp
            (as returned by ``io_loop.time()``) or a ``datetime.timedelta`` for a
            deadline relative to the current time.
        """
        queue_future = self.q.get(deadline)
        if self.q.empty():
            self._unlocked.clear()
        future = _ContextManagerFuture(queue_future, self.release)
        return future

    def __enter__(self):
        raise RuntimeError(
            "Use Semaphore like 'with (yield semaphore)', not like"
            " 'with semaphore'")

    __exit__ = __enter__


class BoundedSemaphore(Semaphore):
    """A semaphore that prevents release() being called too often.

    A bounded semaphore checks to make sure its current value doesn't exceed
    its initial value. If it does, ``ValueError`` is raised. In most
    situations semaphores are used to guard resources with limited capacity.
    If the semaphore is released too many times it's a sign of a bug.

    If not given, *value* defaults to 1.

    .. seealso:: :doc:`examples/web_spider_example`
    """
    def __init__(self, value=1, io_loop=None):
        super(BoundedSemaphore, self).__init__(value=value, io_loop=io_loop)
        self._initial_value = value

    def release(self):
        if self.counter >= self._initial_value:
            raise ValueError("Semaphore released too many times")
        return super(BoundedSemaphore, self).release()


class Lock(object):
    """A lock for coroutines.

    It is created unlocked. When unlocked, :meth:`acquire` changes the state
    to locked. When the state is locked, yielding :meth:`acquire` waits until
    a call to :meth:`release`.

    The :meth:`release` method should only be called in the locked state;
    an attempt to release an unlocked lock raises RuntimeError.

    When more than one coroutine is waiting for the lock, the first one
    registered is awakened by :meth:`release`.

    :meth:`acquire` supports the context manager protocol:

    >>> from tornado import gen
    >>> import toro
    >>> lock = toro.Lock()
    >>>
    >>> @gen.coroutine
    ... def f():
    ...    with (yield lock.acquire()):
    ...        assert lock.locked()
    ...
    ...    assert not lock.locked()

    .. note:: Unlike with the standard threading.Lock_, code in a
      single-threaded Tornado application can check if a :class:`Lock`
      is :meth:`locked`, and act on that information without fear that another
      thread has grabbed the lock, provided you do not yield to the IOLoop
      between checking :meth:`locked` and using a protected resource.

    .. _threading.Lock: http://docs.python.org/2/library/threading.html#lock-objects

    .. seealso:: :doc:`examples/lock_example`

    :Parameters:
      - `io_loop`: Optional custom IOLoop.
    """
    def __init__(self, io_loop=None):
        self._block = BoundedSemaphore(value=1, io_loop=io_loop)

    def __str__(self):
        return "<%s _block=%s>" % (
            self.__class__.__name__,
            self._block)

    def acquire(self, deadline=None):
        """Attempt to lock. Returns a Future.

        The Future raises :exc:`toro.Timeout` if the deadline passes.

        :Parameters:
          - `deadline`: Optional timeout, either an absolute timestamp
            (as returned by ``io_loop.time()``) or a ``datetime.timedelta`` for a
            deadline relative to the current time.
        """
        return self._block.acquire(deadline)

    def release(self):
        """Unlock.

        If any coroutines are waiting for :meth:`acquire`,
        the first in line is awakened.

        If not locked, raise a RuntimeError.
        """
        if not self.locked():
            raise RuntimeError('release unlocked lock')
        self._block.release()

    def locked(self):
        """``True`` if the lock has been acquired"""
        return self._block.locked()

    def __enter__(self):
        raise RuntimeError(
            "Use Lock like 'with (yield lock)', not like"
            " 'with lock'")

    __exit__ = __enter__


if tornado.version_info[:2] >= (4, 2):
    tornado_multi_future = gen.multi_future
else:
    tornado_multi_future = lambda futures, quiet_exceptions: futures


class RWLock(object):
    """A reader-writer lock for coroutines.

    It is created unlocked. When unlocked, :meth:`acquire_write` always changes
    the state to locked. When unlocked, :meth:`acquire_read` can changed the
    state to locked, if :meth:`acquire_read` was called max_readers times. When
    the state is locked, yielding :meth:`acquire_read`/meth:`acquire_write`
    waits until a call to :meth:`release_write` in case of locking on write, or
    :meth:`release_read` in case of locking on read.

    The :meth:`release_read` method should only be called in the locked-on-read
    state; an attempt to release an unlocked lock raises RuntimeError.

    The :meth:`release_write` method should only be called in the locked on
    write state; an attempt to release an unlocked lock raises RuntimeError.

    When more than one coroutine is waiting for the lock, the first one
    registered is awakened by :meth:`release_read`/:meth:`release_write`.

    :meth:`acquire_read`/:meth:`acquire_write` support the context manager
    protocol:

    >>> from tornado import gen
    >>> import toro
    >>> lock = toro.RWLock(max_readers=10)
    >>>
    >>> @gen.coroutine
    ... def f():
    ...    with (yield lock.acquire_read()):
    ...        assert not lock.locked()
    ...
    ...    with (yield lock.acquire_write()):
    ...        assert lock.locked()
    ...
    ...    assert not lock.locked()

    :Parameters:
      - `max_readers`: Optional max readers value, default 1.
      - `io_loop`: Optional custom IOLoop.
    """
    def __init__(self, max_readers=1, io_loop=None):
        self._max_readers = max_readers
        self._block = BoundedSemaphore(value=max_readers, io_loop=io_loop)

    def __str__(self):
        return "<%s _block=%s>" % (
            self.__class__.__name__,
            self._block)

    def acquire_read(self, deadline=None):
        """Attempt to lock for read. Returns a Future.

        The Future raises :exc:`toro.Timeout` if the deadline passes.

        :Parameters:
          - `deadline`: Optional timeout, either an absolute timestamp
            (as returned by ``io_loop.time()``) or a ``datetime.timedelta`` for
            a deadline relative to the current time.
        """
        return self._block.acquire(deadline)

    @gen.coroutine
    def acquire_write(self, deadline=None):
        """Attempt to lock for write. Returns a Future.

        The Future raises :exc:`toro.Timeout` if the deadline passes.

        :Parameters:
          - `deadline`: Optional timeout, either an absolute timestamp
            (as returned by ``io_loop.time()``) or a ``datetime.timedelta`` for
            a deadline relative to the current time.
        """
        futures = [self._block.acquire(deadline) for _ in
                   range(self._max_readers)]
        try:
            managers = yield tornado_multi_future(futures,
                                                  quiet_exceptions=Timeout)
        except Timeout:
            for f in futures:
                # Avoid traceback logging.
                f.exception()
            raise

        raise gen.Return(_ContextManagerList(managers))

    def release_read(self):
        """Releases one reader.

        If any coroutines are waiting for :meth:`acquire_read` (in case of full
        readers queue), the first in line is awakened.

        If not locked, raise a RuntimeError.
        """
        if self._block.counter == self._max_readers:
            raise RuntimeError('release unlocked lock')
        self._block.release()

    def release_write(self):
        """Releases after write.

        The first in queue will be awakened after release.

        If not locked, raise a RuntimeError.
        """
        if not self.locked():
            raise RuntimeError('release unlocked lock')
        for i in range(self._max_readers):
            self._block.release()

    def locked(self):
        """``True`` if the lock has been acquired"""
        return self._block.locked()

    def __enter__(self):
        raise RuntimeError(
            "Use RWLock like 'with (yield lock)', not like"
            " 'with lock'")

    __exit__ = __enter__
