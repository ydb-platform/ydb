"""Kazoo Interfaces

.. versionchanged:: 1.4

    The classes in this module used to be interface declarations based on
    `zope.interface.Interface`. They were converted to normal classes and
    now serve as documentation only.

"""

# public API


class IHandler(object):
    """A Callback Handler for Zookeeper completion and watch callbacks.

    This object must implement several methods responsible for
    determining how completion / watch callbacks are handled as well as
    the method for calling :class:`IAsyncResult` callback functions.

    These functions are used to abstract differences between a Python
    threading environment and asynchronous single-threaded environments
    like gevent. The minimum functionality needed for Kazoo to handle
    these differences is encompassed in this interface.

    The Handler should document how callbacks are called for:

    * Zookeeper completion events
    * Zookeeper watch events

    .. attribute:: name

        Human readable name of the Handler interface.

    .. attribute:: timeout_exception

        Exception class that should be thrown and captured if a
        result is not available within the given time.

    .. attribute:: sleep_func

        Appropriate sleep function that can be called with a single
        argument and sleep.

    """

    def start(self):
        """Start the handler, used for setting up the handler."""

    def stop(self):
        """Stop the handler. Should block until the handler is safely
        stopped."""

    def select(self):
        """A select method that implements Python's select.select
        API"""

    def socket(self):
        """A socket method that implements Python's socket.socket
        API"""

    def create_connection(self):
        """A socket method that implements Python's
        socket.create_connection API"""

    def event_object(self):
        """Return an appropriate object that implements Python's
        threading.Event API"""

    def lock_object(self):
        """Return an appropriate object that implements Python's
        threading.Lock API"""

    def rlock_object(self):
        """Return an appropriate object that implements Python's
        threading.RLock API"""

    def async_result(self):
        """Return an instance that conforms to the
        :class:`~IAsyncResult` interface appropriate for this
        handler"""

    def spawn(self, func, *args, **kwargs):
        """Spawn a function to run asynchronously

        :param args: args to call the function with.
        :param kwargs: keyword args to call the function with.

        This method should return immediately and execute the function
        with the provided args and kwargs in an asynchronous manner.

        """

    def dispatch_callback(self, callback):
        """Dispatch to the callback object

        :param callback: A :class:`~kazoo.protocol.states.Callback`
                         object to be called.

        """


class IAsyncResult(object):
    """An Async Result object that can be queried for a value that has
    been set asynchronously.

    This object is modeled on the ``gevent`` AsyncResult object.

    The implementation must account for the fact that the :meth:`set`
    and :meth:`set_exception` methods will be called from within the
    Zookeeper thread which may require extra care under asynchronous
    environments.

    .. attribute:: value

        Holds the value passed to :meth:`set` if :meth:`set` was
        called. Otherwise `None`.

    .. attribute:: exception

        Holds the exception instance passed to :meth:`set_exception`
        if :meth:`set_exception` was called. Otherwise `None`.

    """

    def ready(self):
        """Return `True` if and only if it holds a value or an
        exception"""

    def successful(self):
        """Return `True` if and only if it is ready and holds a
        value"""

    def set(self, value=None):
        """Store the value. Wake up the waiters.

        :param value: Value to store as the result.

        Any waiters blocking on :meth:`get` or :meth:`wait` are woken
        up. Sequential calls to :meth:`wait` and :meth:`get` will not
        block at all."""

    def set_exception(self, exception):
        """Store the exception. Wake up the waiters.

        :param exception: Exception to raise when fetching the value.

        Any waiters blocking on :meth:`get` or :meth:`wait` are woken
        up. Sequential calls to :meth:`wait` and :meth:`get` will not
        block at all."""

    def get(self, block=True, timeout=None):
        """Return the stored value or raise the exception

        :param block: Whether this method should block or return
                      immediately.
        :type block: bool
        :param timeout: How long to wait for a value when `block` is
                        `True`.
        :type timeout: float

        If this instance already holds a value / an exception, return /
        raise it immediately. Otherwise, block until :meth:`set` or
        :meth:`set_exception` has been called or until the optional
        timeout occurs."""

    def get_nowait(self):
        """Return the value or raise the exception without blocking.

        If nothing is available, raise the Timeout exception class on
        the associated :class:`IHandler` interface."""

    def wait(self, timeout=None):
        """Block until the instance is ready.

        :param timeout: How long to wait for a value when `block` is
                        `True`.
        :type timeout: float

        If this instance already holds a value / an exception, return /
        raise it immediately. Otherwise, block until :meth:`set` or
        :meth:`set_exception` has been called or until the optional
        timeout occurs."""

    def rawlink(self, callback):
        """Register a callback to call when a value or an exception is
        set

        :param callback:
            A callback function to call after :meth:`set` or
            :meth:`set_exception` has been called. This function will
            be passed a single argument, this instance.
        :type callback: func

        """

    def unlink(self, callback):
        """Remove the callback set by :meth:`rawlink`

        :param callback: A callback function to remove.
        :type callback: func

        """
