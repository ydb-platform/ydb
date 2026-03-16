"""Zookeeper based queue implementations.

:Maintainer: None
:Status: Possibly Buggy

.. note::

    This queue was reported to cause memory leaks over long running periods.
    See: https://github.com/python-zk/kazoo/issues/175

"""
import uuid

from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.protocol.states import EventType
from kazoo.retry import ForceRetryError


class BaseQueue(object):
    """A common base class for queue implementations."""

    def __init__(self, client, path):
        """
        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The queue path to use in ZooKeeper.
        """
        self.client = client
        self.path = path
        self._entries_path = path
        self.structure_paths = (self.path,)
        self.ensured_path = False

    def _check_put_arguments(self, value, priority=100):
        if not isinstance(value, bytes):
            raise TypeError("value must be a byte string")
        if not isinstance(priority, int):
            raise TypeError("priority must be an int")
        elif priority < 0 or priority > 999:
            raise ValueError("priority must be between 0 and 999")

    def _ensure_paths(self):
        if not self.ensured_path:
            # make sure our parent / internal structure nodes exists
            for path in self.structure_paths:
                self.client.ensure_path(path)
            self.ensured_path = True

    def __len__(self):
        self._ensure_paths()
        _, stat = self.client.retry(self.client.get, self._entries_path)
        return stat.children_count


class Queue(BaseQueue):
    """A distributed queue with optional priority support.

    This queue does not offer reliable consumption. An entry is removed
    from the queue prior to being processed. So if an error occurs, the
    consumer has to re-queue the item or it will be lost.

    """

    prefix = "entry-"

    def __init__(self, client, path):
        """
        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The queue path to use in ZooKeeper.
        """
        super(Queue, self).__init__(client, path)
        self._children = []

    def __len__(self):
        """Return queue size."""
        return super(Queue, self).__len__()

    def get(self):
        """
        Get item data and remove an item from the queue.

        :returns: Item data or None.
        :rtype: bytes
        """
        self._ensure_paths()
        return self.client.retry(self._inner_get)

    def _inner_get(self):
        if not self._children:
            self._children = self.client.retry(
                self.client.get_children, self.path
            )
            self._children = sorted(self._children)
        if not self._children:
            return None
        name = self._children[0]
        try:
            data, stat = self.client.get(self.path + "/" + name)
            self.client.delete(self.path + "/" + name)
        except NoNodeError:  # pragma: nocover
            # the first node has vanished in the meantime, try to
            # get another one
            self._children = []
            raise ForceRetryError()

        self._children.pop(0)
        return data

    def put(self, value, priority=100):
        """Put an item into the queue.

        :param value: Byte string to put into the queue.
        :param priority:
            An optional priority as an integer with at most 3 digits.
            Lower values signify higher priority.
        """
        self._check_put_arguments(value, priority)
        self._ensure_paths()
        path = "{path}/{prefix}{priority:03d}-".format(
            path=self.path, prefix=self.prefix, priority=priority
        )
        self.client.create(path, value, sequence=True)


class LockingQueue(BaseQueue):
    """A distributed queue with priority and locking support.

    Upon retrieving an entry from the queue, the entry gets locked with an
    ephemeral node (instead of deleted). If an error occurs, this lock gets
    released so that others could retake the entry. This adds a little penalty
    as compared to :class:`Queue` implementation.

    The user should call the :meth:`LockingQueue.get` method first to lock and
    retrieve the next entry. When finished processing the entry, a user should
    call the :meth:`LockingQueue.consume` method that will remove the entry
    from the queue.

    This queue will not track connection status with ZooKeeper. If a node locks
    an element, then loses connection with ZooKeeper and later reconnects, the
    lock will probably be removed by Zookeeper in the meantime, but a node
    would still think that it holds a lock. The user should check the
    connection status with Zookeeper or call :meth:`LockingQueue.holds_lock`
    method that will check if a node still holds the lock.

    .. note::
        :class:`LockingQueue` requires ZooKeeper 3.4 or above, since it is
        using transactions.
    """

    lock = "/taken"
    entries = "/entries"
    entry = "entry"

    def __init__(self, client, path):
        """
        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The queue path to use in ZooKeeper.
        """
        super(LockingQueue, self).__init__(client, path)
        self.id = uuid.uuid4().hex.encode()
        self.processing_element = None
        self._lock_path = self.path + self.lock
        self._entries_path = self.path + self.entries
        self.structure_paths = (self._lock_path, self._entries_path)

    def __len__(self):
        """Returns the current length of the queue.

        :returns: queue size (includes locked entries count).
        """
        return super(LockingQueue, self).__len__()

    def put(self, value, priority=100):
        """Put an entry into the queue.

        :param value: Byte string to put into the queue.
        :param priority:
            An optional priority as an integer with at most 3 digits.
            Lower values signify higher priority.

        """
        self._check_put_arguments(value, priority)
        self._ensure_paths()

        self.client.create(
            "{path}/{prefix}-{priority:03d}-".format(
                path=self._entries_path, prefix=self.entry, priority=priority
            ),
            value,
            sequence=True,
        )

    def put_all(self, values, priority=100):
        """Put several entries into the queue. The action only succeeds
        if all entries where put into the queue.

        :param values: A list of values to put into the queue.
        :param priority:
            An optional priority as an integer with at most 3 digits.
            Lower values signify higher priority.

        """
        if not isinstance(values, list):
            raise TypeError("values must be a list of byte strings")
        if not isinstance(priority, int):
            raise TypeError("priority must be an int")
        elif priority < 0 or priority > 999:
            raise ValueError("priority must be between 0 and 999")
        self._ensure_paths()

        with self.client.transaction() as transaction:
            for value in values:
                if not isinstance(value, bytes):
                    raise TypeError("value must be a byte string")
                transaction.create(
                    "{path}/{prefix}-{priority:03d}-".format(
                        path=self._entries_path,
                        prefix=self.entry,
                        priority=priority,
                    ),
                    value,
                    sequence=True,
                )

    def get(self, timeout=None):
        """Locks and gets an entry from the queue. If a previously got entry
        was not consumed, this method will return that entry.

        :param timeout:
            Maximum waiting time in seconds. If None then it will wait
            until an entry appears in the queue.
        :returns: A locked entry value or None if the timeout was reached.
        :rtype: bytes
        """
        self._ensure_paths()
        if self.processing_element is not None:
            return self.processing_element[1]
        else:
            return self._inner_get(timeout)

    def holds_lock(self):
        """Checks if a node still holds the lock.

        :returns: True if a node still holds the lock, False otherwise.
        :rtype: bool
        """
        if self.processing_element is None:
            return False
        lock_id, _ = self.processing_element
        lock_path = "{path}/{id}".format(path=self._lock_path, id=lock_id)
        self.client.sync(lock_path)
        value, stat = self.client.retry(self.client.get, lock_path)
        return value == self.id

    def consume(self):
        """Removes a currently processing entry from the queue.

        :returns: True if element was removed successfully, False otherwise.
        :rtype: bool
        """
        if self.processing_element is not None and self.holds_lock():
            id_, value = self.processing_element
            with self.client.transaction() as transaction:
                transaction.delete(
                    "{path}/{id}".format(path=self._entries_path, id=id_)
                )
                transaction.delete(
                    "{path}/{id}".format(path=self._lock_path, id=id_)
                )
            self.processing_element = None
            return True
        else:
            return False

    def release(self):
        """Removes the lock from currently processed item without consuming it.

        :returns: True if the lock was removed successfully, False otherwise.
        :rtype: bool

        """
        if self.processing_element is not None and self.holds_lock():
            id_, value = self.processing_element
            with self.client.transaction() as transaction:
                transaction.delete(
                    "{path}/{id}".format(path=self._lock_path, id=id_)
                )
            self.processing_element = None
            return True
        else:
            return False

    def _inner_get(self, timeout):
        flag = self.client.handler.event_object()
        lock = self.client.handler.lock_object()
        canceled = False
        value = []

        def check_for_updates(event):
            if event is not None and event.type != EventType.CHILD:
                return
            with lock:
                if canceled or flag.is_set():
                    return
                values = self.client.retry(
                    self.client.get_children,
                    self._entries_path,
                    check_for_updates,
                )
                taken = self.client.retry(
                    self.client.get_children,
                    self._lock_path,
                    check_for_updates,
                )
                available = self._filter_locked(values, taken)
                if len(available) > 0:
                    ret = self._take(available[0])
                    if ret is not None:
                        # By this time, no one took the task
                        value.append(ret)
                        flag.set()

        check_for_updates(None)
        retVal = None
        flag.wait(timeout)
        with lock:
            canceled = True
            if len(value) > 0:
                # We successfully locked an entry
                self.processing_element = value[0]
                retVal = value[0][1]
        return retVal

    def _filter_locked(self, values, taken):
        taken = set(taken)
        available = sorted(values)
        return (
            available
            if len(taken) == 0
            else [x for x in available if x not in taken]
        )

    def _take(self, id_):
        try:
            self.client.create(
                "{path}/{id}".format(path=self._lock_path, id=id_),
                self.id,
                ephemeral=True,
            )
        except NodeExistsError:
            # Item is already locked
            return None

        try:
            value, stat = self.client.retry(
                self.client.get,
                "{path}/{id}".format(path=self._entries_path, id=id_),
            )
        except NoNodeError:
            # Item is already consumed
            self.client.delete(
                "{path}/{id}".format(path=self._lock_path, id=id_)
            )
            return None
        return (id_, value)
