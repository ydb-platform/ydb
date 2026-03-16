"""Zookeeper Locking Implementations

:Maintainer: Ben Bangert <ben@groovie.org>
:Status: Production

Error Handling
==============

It's highly recommended to add a state listener with
:meth:`~KazooClient.add_listener` and watch for
:attr:`~KazooState.LOST` and :attr:`~KazooState.SUSPENDED` state
changes and re-act appropriately. In the event that a
:attr:`~KazooState.LOST` state occurs, its certain that the lock
and/or the lease has been lost.

"""
import re
import time
import uuid

from kazoo.exceptions import (
    CancelledError,
    KazooException,
    LockTimeout,
    NoNodeError,
)
from kazoo.protocol.states import KazooState
from kazoo.retry import (
    ForceRetryError,
    KazooRetry,
    RetryFailedError,
)


class _Watch(object):
    def __init__(self, duration=None):
        self.duration = duration
        self.started_at = None

    def start(self):
        self.started_at = time.monotonic()

    def leftover(self):
        if self.duration is None:
            return None
        else:
            elapsed = time.monotonic() - self.started_at
            return max(0, self.duration - elapsed)


class Lock(object):
    """Kazoo Lock

    Example usage with a :class:`~kazoo.client.KazooClient` instance:

    .. code-block:: python

        zk = KazooClient()
        zk.start()
        lock = zk.Lock("/lockpath", "my-identifier")
        with lock:  # blocks waiting for lock acquisition
            # do something with the lock

    Note: This lock is not *re-entrant*. Repeated calls after already
    acquired will block.

    This is an exclusive lock. For a read/write lock, see :class:`WriteLock`
    and :class:`ReadLock`.

    """

    # Node name, after the contender UUID, before the sequence
    # number. Involved in read/write locks.
    _NODE_NAME = "__lock__"

    # Node names which exclude this contender when present at a lower
    # sequence number. Involved in read/write locks.
    _EXCLUDE_NAMES = ["__lock__"]

    def __init__(self, client, path, identifier=None, extra_lock_patterns=()):
        """Create a Kazoo lock.

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The lock path to use.
        :param identifier: Name to use for this lock contender. This can be
                          useful for querying to see who the current lock
                          contenders are.
        :param extra_lock_patterns: Strings that will be used to
                                    identify other znode in the path
                                    that should be considered contenders
                                    for this lock.
                                    Use this for cross-implementation
                                    compatibility.

        .. versionadded:: 2.7.1
            The extra_lock_patterns option.
        """
        self.client = client
        self.path = path
        self._exclude_names = set(
            self._EXCLUDE_NAMES + list(extra_lock_patterns)
        )
        self._contenders_re = re.compile(
            r"(?:{patterns})(-?\d{{10}})$".format(
                patterns="|".join(self._exclude_names)
            )
        )

        # some data is written to the node. this can be queried via
        # contenders() to see who is contending for the lock
        self.data = str(identifier or "").encode("utf-8")
        self.node = None

        self.wake_event = client.handler.event_object()

        # props to Netflix Curator for this trick. It is possible for our
        # create request to succeed on the server, but for a failure to
        # prevent us from getting back the full path name. We prefix our
        # lock name with a uuid and can check for its presence on retry.
        self.prefix = uuid.uuid4().hex + self._NODE_NAME
        self.create_path = self.path + "/" + self.prefix

        self.create_tried = False
        self.is_acquired = False
        self.assured_path = False
        self.cancelled = False
        self._retry = KazooRetry(
            max_tries=None, sleep_func=client.handler.sleep_func
        )
        self._acquire_method_lock = client.handler.lock_object()

    def _ensure_path(self):
        self.client.ensure_path(self.path)
        self.assured_path = True

    def cancel(self):
        """Cancel a pending lock acquire."""
        self.cancelled = True
        self.wake_event.set()

    def acquire(self, blocking=True, timeout=None, ephemeral=True):
        """
        Acquire the lock. By defaults blocks and waits forever.

        :param blocking: Block until lock is obtained or return immediately.
        :type blocking: bool
        :param timeout: Don't wait forever to acquire the lock.
        :type timeout: float or None
        :param ephemeral: Don't use ephemeral znode for the lock.
        :type ephemeral: bool

        :returns: Was the lock acquired?
        :rtype: bool

        :raises: :exc:`~kazoo.exceptions.LockTimeout` if the lock
                 wasn't acquired within `timeout` seconds.

        .. warning::

            When :attr:`ephemeral` is set to False session expiration
            will not release the lock and must be handled separately.

        .. versionadded:: 1.1
            The timeout option.

        .. versionadded:: 2.4.1
            The ephemeral option.
        """

        retry = self._retry.copy()
        retry.deadline = timeout

        # Ensure we are locked so that we avoid multiple threads in
        # this acquistion routine at the same time...
        method_locked = self._acquire_method_lock.acquire(
            blocking=blocking, timeout=timeout if timeout is not None else -1
        )
        if not method_locked:
            return False

        already_acquired = self.is_acquired
        try:
            gotten = False
            try:
                gotten = retry(
                    self._inner_acquire,
                    blocking=blocking,
                    timeout=timeout,
                    ephemeral=ephemeral,
                )
            except RetryFailedError:
                pass
            except KazooException:
                # if we did ultimately fail, attempt to clean up
                if not already_acquired:
                    self._best_effort_cleanup()
                    self.cancelled = False
                raise
            if gotten:
                self.is_acquired = gotten
            if not gotten and not already_acquired:
                self._best_effort_cleanup()
            return gotten
        finally:
            self._acquire_method_lock.release()

    def _watch_session(self, state):
        self.wake_event.set()
        return True

    def _inner_acquire(self, blocking, timeout, ephemeral=True):
        # wait until it's our chance to get it..
        if self.is_acquired:
            if not blocking:
                return False
            raise ForceRetryError()

        # make sure our election parent node exists
        if not self.assured_path:
            self._ensure_path()

        node = None
        if self.create_tried:
            node = self._find_node()
        else:
            self.create_tried = True

        if not node:
            node = self.client.create(
                self.create_path, self.data, ephemeral=ephemeral, sequence=True
            )
            # strip off path to node
            node = node[len(self.path) + 1 :]

        self.node = node

        while True:
            self.wake_event.clear()

            # bail out with an exception if cancellation has been requested
            if self.cancelled:
                raise CancelledError()

            predecessor = self._get_predecessor(node)
            if predecessor is None:
                return True

            if not blocking:
                return False

            # otherwise we are in the mix. watch predecessor and bide our time
            predecessor = self.path + "/" + predecessor
            self.client.add_listener(self._watch_session)
            try:
                self.client.get(predecessor, self._watch_predecessor)
            except NoNodeError:
                pass  # predecessor has already been deleted
            else:
                self.wake_event.wait(timeout)
                if not self.wake_event.is_set():
                    raise LockTimeout(
                        "Failed to acquire lock on %s after %s seconds"
                        % (self.path, timeout)
                    )
            finally:
                self.client.remove_listener(self._watch_session)

    def _watch_predecessor(self, event):
        self.wake_event.set()

    def _get_predecessor(self, node):
        """returns `node`'s predecessor or None

        Note: This handle the case where the current lock is not a contender
        (e.g. rlock), this and also edge cases where the lock's ephemeral node
        is gone.
        """
        node_sequence = node[len(self.prefix) :]
        children = self.client.get_children(self.path)
        found_self = False
        # Filter out the contenders using the computed regex
        contender_matches = []
        for child in children:
            match = self._contenders_re.search(child)
            if match is not None:
                contender_sequence = match.group(1)
                # Only consider contenders with a smaller sequence number.
                # A contender with a smaller sequence number has a higher
                # priority.
                if contender_sequence < node_sequence:
                    contender_matches.append(match)
            if child == node:
                # Remember the node's match object so we can short circuit
                # below.
                found_self = match

        if found_self is False:  # pragma: nocover
            # somehow we aren't in the childrens -- probably we are
            # recovering from a session failure and our ephemeral
            # node was removed.
            raise ForceRetryError()

        if not contender_matches:
            return None

        # Sort the contenders using the sequence number extracted by the regex
        # and return the original string of the predecessor.
        sorted_matches = sorted(contender_matches, key=lambda m: m.groups())
        return sorted_matches[-1].string

    def _find_node(self):
        children = self.client.get_children(self.path)
        for child in children:
            if child.startswith(self.prefix):
                return child
        return None

    def _delete_node(self, node):
        self.client.delete(self.path + "/" + node)

    def _best_effort_cleanup(self):
        try:
            node = self.node or self._find_node()
            if node:
                self._delete_node(node)
        except KazooException:  # pragma: nocover
            pass

    def release(self):
        """Release the lock immediately."""
        return self.client.retry(self._inner_release)

    def _inner_release(self):
        if not self.is_acquired:
            return False

        try:
            self._delete_node(self.node)
        except NoNodeError:  # pragma: nocover
            pass

        self.is_acquired = False
        self.node = None
        return True

    def contenders(self):
        """Return an ordered list of the current contenders for the
        lock.

        .. note::

            If the contenders did not set an identifier, it will appear
            as a blank string.

        """
        # make sure our election parent node exists
        if not self.assured_path:
            self._ensure_path()

        children = self.client.get_children(self.path)
        # We want all contenders, including self (this is especially important
        # for r/w locks). This is similar to the logic of `_get_predecessor`
        # except we include our own pattern.
        all_contenders_re = re.compile(
            r"(?:{patterns})(-?\d{{10}})$".format(
                patterns="|".join(self._exclude_names | {self._NODE_NAME})
            )
        )
        # Filter out the contenders using the computed regex
        contender_matches = []
        for child in children:
            match = all_contenders_re.search(child)
            if match is not None:
                contender_matches.append(match)
        # Sort the contenders using the sequence number extracted by the regex,
        # then extract the original string.
        contender_nodes = [
            match.string
            for match in sorted(contender_matches, key=lambda m: m.groups())
        ]
        # Retrieve all the contender nodes data (preserving order).
        contenders = []
        for node in contender_nodes:
            try:
                data, stat = self.client.get(self.path + "/" + node)
                if data is not None:
                    contenders.append(data.decode("utf-8"))
            except NoNodeError:  # pragma: nocover
                pass

        return contenders

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


class WriteLock(Lock):
    """Kazoo Write Lock

    Example usage with a :class:`~kazoo.client.KazooClient` instance:

    .. code-block:: python

        zk = KazooClient()
        zk.start()
        lock = zk.WriteLock("/lockpath", "my-identifier")
        with lock:  # blocks waiting for lock acquisition
            # do something with the lock

    The lock path passed to WriteLock and ReadLock must match for them to
    communicate.  The write lock can not be acquired if it is held by
    any readers or writers.

    Note: This lock is not *re-entrant*. Repeated calls after already
    acquired will block.

    This is the write-side of a shared lock.  See :class:`Lock` for a
    standard exclusive lock and :class:`ReadLock` for the read-side of a
    shared lock.

    """

    _NODE_NAME = "__lock__"
    _EXCLUDE_NAMES = ["__lock__", "__rlock__"]


class ReadLock(Lock):
    """Kazoo Read Lock

    Example usage with a :class:`~kazoo.client.KazooClient` instance:

    .. code-block:: python

        zk = KazooClient()
        zk.start()
        lock = zk.ReadLock("/lockpath", "my-identifier")
        with lock:  # blocks waiting for outstanding writers
            # do something with the lock

    The lock path passed to WriteLock and ReadLock must match for them to
    communicate.  The read lock blocks if it is held by any writers,
    but multiple readers may hold the lock.

    Note: This lock is not *re-entrant*. Repeated calls after already
    acquired will block.

    This is the read-side of a shared lock.  See :class:`Lock` for a
    standard exclusive lock and :class:`WriteLock` for the write-side of a
    shared lock.

    """

    _NODE_NAME = "__rlock__"
    _EXCLUDE_NAMES = ["__lock__"]


class Semaphore(object):
    """A Zookeeper-based Semaphore

    This synchronization primitive operates in the same manner as the
    Python threading version only uses the concept of leases to
    indicate how many available leases are available for the lock
    rather than counting.

    Note: This lock is not meant to be *re-entrant*.

    Example:

    .. code-block:: python

        zk = KazooClient()
        semaphore = zk.Semaphore("/leasepath", "my-identifier")
        with semaphore:  # blocks waiting for lock acquisition
            # do something with the semaphore

    .. warning::

        This class stores the allowed max_leases as the data on the
        top-level semaphore node. The stored value is checked once
        against the max_leases of each instance. This check is
        performed when acquire is called the first time. The semaphore
        node needs to be deleted to change the allowed leases.

    .. versionadded:: 0.6
        The Semaphore class.

    .. versionadded:: 1.1
        The max_leases check.

    """

    def __init__(self, client, path, identifier=None, max_leases=1):
        """Create a Kazoo Lock

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The semaphore path to use.
        :param identifier: Name to use for this lock contender. This
                           can be useful for querying to see who the
                           current lock contenders are.
        :param max_leases: The maximum amount of leases available for
                           the semaphore.

        """
        # Implementation notes about how excessive thundering herd
        # and watches are avoided
        # - A node (lease pool) holds children for each lease in use
        # - A lock is acquired for a process attempting to acquire a
        #   lease. If a lease is available, the ephemeral node is
        #   created in the lease pool and the lock is released.
        # - Only the lock holder watches for children changes in the
        #   lease pool
        self.client = client
        self.path = path

        # some data is written to the node. this can be queried via
        # contenders() to see who is contending for the lock
        self.data = str(identifier or "").encode("utf-8")
        self.max_leases = max_leases
        self.wake_event = client.handler.event_object()

        self.create_path = self.path + "/" + uuid.uuid4().hex
        self.lock_path = path + "-" + "__lock__"
        self.is_acquired = False
        self.assured_path = False
        self.cancelled = False
        self._session_expired = False

    def _ensure_path(self):
        result = self.client.ensure_path(self.path)
        self.assured_path = True
        if result is True:
            # node did already exist
            data, _ = self.client.get(self.path)
            try:
                leases = int(data.decode("utf-8"))
            except (ValueError, TypeError):
                # ignore non-numeric data, maybe the node data is used
                # for other purposes
                pass
            else:
                if leases != self.max_leases:
                    raise ValueError(
                        "Inconsistent max leases: %s, expected: %s"
                        % (leases, self.max_leases)
                    )
        else:
            self.client.set(self.path, str(self.max_leases).encode("utf-8"))

    def cancel(self):
        """Cancel a pending semaphore acquire."""
        self.cancelled = True
        self.wake_event.set()

    def acquire(self, blocking=True, timeout=None):
        """Acquire the semaphore. By defaults blocks and waits forever.

        :param blocking: Block until semaphore is obtained or
                         return immediately.
        :type blocking: bool
        :param timeout: Don't wait forever to acquire the semaphore.
        :type timeout: float or None

        :returns: Was the semaphore acquired?
        :rtype: bool

        :raises:
            ValueError if the max_leases value doesn't match the
            stored value.

            :exc:`~kazoo.exceptions.LockTimeout` if the semaphore
            wasn't acquired within `timeout` seconds.

        .. versionadded:: 1.1
            The blocking, timeout arguments and the max_leases check.
        """
        # If the semaphore had previously been canceled, make sure to
        # reset that state.
        self.cancelled = False

        try:
            self.is_acquired = self.client.retry(
                self._inner_acquire, blocking=blocking, timeout=timeout
            )
        except KazooException:
            # if we did ultimately fail, attempt to clean up
            self._best_effort_cleanup()
            self.cancelled = False
            raise

        return self.is_acquired

    def _inner_acquire(self, blocking, timeout=None):
        """Inner loop that runs from the top anytime a command hits a
        retryable Zookeeper exception."""
        self._session_expired = False
        self.client.add_listener(self._watch_session)

        if not self.assured_path:
            self._ensure_path()

        # Do we already have a lease?
        if self.client.exists(self.create_path):
            return True

        w = _Watch(duration=timeout)
        w.start()
        lock = self.client.Lock(self.lock_path, self.data)
        try:
            gotten = lock.acquire(blocking=blocking, timeout=w.leftover())
            if not gotten:
                return False
            while True:
                self.wake_event.clear()

                # Attempt to grab our lease...
                if self._get_lease():
                    return True

                if blocking:
                    # If blocking, wait until self._watch_lease_change() is
                    # called before returning
                    self.wake_event.wait(w.leftover())
                    if not self.wake_event.is_set():
                        raise LockTimeout(
                            "Failed to acquire semaphore on %s"
                            " after %s seconds" % (self.path, timeout)
                        )
                else:
                    return False
        finally:
            lock.release()

    def _watch_lease_change(self, event):
        self.wake_event.set()

    def _get_lease(self, data=None):
        # Make sure the session is still valid
        if self._session_expired:
            raise ForceRetryError("Retry on session loss at top")

        # Make sure that the request hasn't been canceled
        if self.cancelled:
            raise CancelledError("Semaphore cancelled")

        # Get a list of the current potential lock holders. If they change,
        # notify our wake_event object. This is used to unblock a blocking
        # self._inner_acquire call.
        children = self.client.get_children(
            self.path, self._watch_lease_change
        )

        # If there are leases available, acquire one
        if len(children) < self.max_leases:
            self.client.create(self.create_path, self.data, ephemeral=True)

        # Check if our acquisition was successful or not. Update our state.
        if self.client.exists(self.create_path):
            self.is_acquired = True
        else:
            self.is_acquired = False

        # Return current state
        return self.is_acquired

    def _watch_session(self, state):
        if state == KazooState.LOST:
            self._session_expired = True
            self.wake_event.set()

            # Return true to de-register
            return True

    def _best_effort_cleanup(self):
        try:
            self.client.delete(self.create_path)
        except KazooException:  # pragma: nocover
            pass

    def release(self):
        """Release the lease immediately."""
        return self.client.retry(self._inner_release)

    def _inner_release(self):
        if not self.is_acquired:
            return False
        try:
            self.client.delete(self.create_path)
        except NoNodeError:  # pragma: nocover
            pass
        self.is_acquired = False
        return True

    def lease_holders(self):
        """Return an unordered list of the current lease holders.

        .. note::

            If the lease holder did not set an identifier, it will
            appear as a blank string.

        """
        if not self.client.exists(self.path):
            return []

        children = self.client.get_children(self.path)

        lease_holders = []
        for child in children:
            try:
                data, stat = self.client.get(self.path + "/" + child)
                lease_holders.append(data.decode("utf-8"))
            except NoNodeError:  # pragma: nocover
                pass
        return lease_holders

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
