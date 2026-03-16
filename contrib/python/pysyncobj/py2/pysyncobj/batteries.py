import threading
import weakref
import time
import socket
import os
import collections
import heapq
from .syncobj import SyncObjConsumer, replicated


class ReplCounter(SyncObjConsumer):
    def __init__(self):
        """
        Simple distributed counter. You can set, add, sub and inc counter value.
        """
        super(ReplCounter, self).__init__()
        self.__counter = int()

    @replicated
    def set(self, newValue):
        """
        Set new value to a counter.

        :param newValue: new value
        :return: new counter value
        """
        self.__counter = newValue
        return self.__counter

    @replicated
    def add(self, value):
        """
        Adds value to a counter.

        :param value: value to add
        :return: new counter value
        """
        self.__counter += value
        return self.__counter

    @replicated
    def sub(self, value):
        """
        Subtracts a value from counter.

        :param value: value to subtract
        :return: new counter value
        """
        self.__counter -= value
        return self.__counter

    @replicated
    def inc(self):
        """
        Increments counter value by one.

        :return: new counter value
        """
        self.__counter += 1
        return self.__counter

    def get(self):
        """
        :return: current counter value
        """
        return self.__counter


class ReplList(SyncObjConsumer):
    def __init__(self):
        """
        Distributed list - it has an interface similar to a regular list.
        """
        super(ReplList, self).__init__()
        self.__data = []

    @replicated
    def reset(self, newData):
        """Replace list with a new one"""
        assert isinstance(newData, list)
        self.__data = newData

    @replicated
    def set(self, position, newValue):
        """Update value at given position."""
        self.__data[position] = newValue

    @replicated
    def append(self, item):
        """Append item to end"""
        self.__data.append(item)

    @replicated
    def extend(self, other):
        """Extend list by appending elements from the iterable"""
        self.__data.extend(other)

    @replicated
    def insert(self, position, element):
        """Insert object before position"""
        self.__data.insert(position, element)

    @replicated
    def remove(self, element):
        """
        Remove first occurrence of element.
        Raises ValueError if the value is not present.
        """
        self.__data.remove(element)

    @replicated
    def pop(self, position=None):
        """
        Remove and return item at position (default last).
        Raises IndexError if list is empty or index is out of range.
        """
        return self.__data.pop(position)

    @replicated
    def sort(self, reverse=False):
        """Stable sort *IN PLACE*"""
        self.__data.sort(reverse=reverse)

    def index(self, element):
        """
        Return first position of element.
        Raises ValueError if the value is not present.
        """
        return self.__data.index(element)

    def count(self, element):
        """ Return number of occurrences of element """
        return self.__data.count(element)

    def get(self, position):
        """ Return value at given position"""
        return self.__data[position]

    def __getitem__(self, position):
        """ Return value at given position"""
        return self.__data[position]

    @replicated(ver=1)
    def __setitem__(self, position, element):
        """Update value at given position."""
        self.__data[position] = element

    def __len__(self):
        """Return the number of items of a sequence or collection."""
        return len(self.__data)

    def rawData(self):
        """Return internal list - use it carefully"""
        return self.__data


class ReplDict(SyncObjConsumer):
    def __init__(self):
        """
        Distributed dict - it has an interface similar to a regular dict.
        """
        super(ReplDict, self).__init__()
        self.__data = {}

    @replicated
    def reset(self, newData):
        """Replace dict with a new one"""
        assert isinstance(newData, dict)
        self.__data = newData

    @replicated
    def __setitem__(self, key, value):
        """Set value for specified key"""
        self.__data[key] = value

    @replicated
    def set(self, key, value):
        """Set value for specified key"""
        self.__data[key] = value

    @replicated
    def setdefault(self, key, default):
        """Return value for specified key, set default value if key not exist"""
        return self.__data.setdefault(key, default)

    @replicated
    def update(self, other):
        """Adds all values from the other dict"""
        self.__data.update(other)

    @replicated
    def pop(self, key, default=None):
        """Remove and return value for given key, return default if key not exist"""
        return self.__data.pop(key, default)

    @replicated
    def clear(self):
        """Remove all items from dict"""
        self.__data.clear()

    def __getitem__(self, key):
        """Return value for given key"""
        return self.__data[key]

    def get(self, key, default=None):
        """Return value for given key, return default if key not exist"""
        return self.__data.get(key, default)

    def __len__(self):
        """Return size of dict"""
        return len(self.__data)

    def __contains__(self, key):
        """True if key exists"""
        return key in self.__data

    def keys(self):
        """Return all keys"""
        return self.__data.keys()

    def values(self):
        """Return all values"""
        return self.__data.values()

    def items(self):
        """Return all items"""
        return self.__data.items()

    def rawData(self):
        """Return internal dict - use it carefully"""
        return self.__data


class ReplSet(SyncObjConsumer):
    def __init__(self):
        """
        Distributed set - it has an interface similar to a regular set.
        """
        super(ReplSet, self).__init__()
        self.__data = set()

    @replicated
    def reset(self, newData):
        """Replace set with a new one"""
        assert isinstance(newData, set)
        self.__data = newData

    @replicated
    def add(self, item):
        """Add an element to a set"""
        self.__data.add(item)

    @replicated
    def remove(self, item):
        """
        Remove an element from a set; it must be a member.
        If the element is not a member, raise a KeyError.
        """
        self.__data.remove(item)

    @replicated
    def discard(self, item):
        """
        Remove an element from a set if it is a member.
        If the element is not a member, do nothing.
        """
        self.__data.discard(item)

    @replicated
    def pop(self):
        """
        Remove and return an arbitrary set element.
        Raises KeyError if the set is empty.
        """
        return self.__data.pop()

    @replicated
    def clear(self):
        """ Remove all elements from this set. """
        self.__data.clear()

    @replicated
    def update(self, other):
        """ Update a set with the union of itself and others. """
        self.__data.update(other)

    def rawData(self):
        """Return internal dict - use it carefully"""
        return self.__data

    def __len__(self):
        """Return size of set"""
        return len(self.__data)

    def __contains__(self, item):
        """True if item exists"""
        return item in self.__data


class ReplQueue(SyncObjConsumer):
    def __init__(self, maxsize=0):
        """
        Replicated FIFO queue. Based on collections.deque.
        Has an interface similar to Queue.
        
        :param maxsize: Max queue size.
        :type maxsize: int
        """
        super(ReplQueue, self).__init__()
        self.__maxsize = maxsize
        self.__data = collections.deque()

    def qsize(self):
        """Return size of queue"""
        return len(self.__data)

    def empty(self):
        """True if queue is empty"""
        return len(self.__data) == 0

    def __len__(self):
        """Return size of queue"""
        return len(self.__data)

    def full(self):
        """True if queue is full"""
        return len(self.__data) == self.__maxsize

    @replicated
    def put(self, item):
        """Put an item into the queue.
        True - if item placed in queue.
        False - if queue is full and item can not be placed."""
        if self.__maxsize and len(self.__data) >= self.__maxsize:
            return False
        self.__data.append(item)
        return True

    @replicated
    def get(self, default=None):
        """Extract item from queue.
        Return default if queue is empty."""
        try:
            return self.__data.popleft()
        except:
            return default


class ReplPriorityQueue(SyncObjConsumer):
    def __init__(self, maxsize=0):
        """
        Replicated priority queue. Based on heapq.
        Has an interface similar to Queue.

        :param maxsize: Max queue size.
        :type maxsize: int
        """
        super(ReplPriorityQueue, self).__init__()
        self.__maxsize = maxsize
        self.__data = []

    def qsize(self):
        """Return size of queue"""
        return len(self.__data)

    def empty(self):
        """True if queue is empty"""
        return len(self.__data) == 0

    def __len__(self):
        """Return size of queue"""
        return len(self.__data)

    def full(self):
        """True if queue is full"""
        return len(self.__data) == self.__maxsize

    @replicated
    def put(self, item):
        """Put an item into the queue. Items should be comparable, eg. tuples.
        True - if item placed in queue.
        False - if queue is full and item can not be placed."""
        if self.__maxsize and len(self.__data) >= self.__maxsize:
            return False
        heapq.heappush(self.__data, item)
        return True

    @replicated
    def get(self, default=None):
        """Extract the smallest item from queue.
        Return default if queue is empty."""
        if not self.__data:
            return default
        return heapq.heappop(self.__data)


class _ReplLockManagerImpl(SyncObjConsumer):
    def __init__(self, autoUnlockTime):
        super(_ReplLockManagerImpl, self).__init__()
        self.__locks = {}
        self.__autoUnlockTime = autoUnlockTime

    @replicated
    def acquire(self, lockID, clientID, currentTime):
        existingLock = self.__locks.get(lockID, None)
        # Auto-unlock old lock
        if existingLock is not None:
            if currentTime - existingLock[1] > self.__autoUnlockTime:
                existingLock = None
        # Acquire lock if possible
        if existingLock is None or existingLock[0] == clientID:
            self.__locks[lockID] = (clientID, currentTime)
            return True
        # Lock already acquired by someone else
        return False

    @replicated
    def prolongate(self, clientID, currentTime):
        for lockID in list(self.__locks):
            lockClientID, lockTime = self.__locks[lockID]

            if currentTime - lockTime > self.__autoUnlockTime:
                del self.__locks[lockID]
                continue

            if lockClientID == clientID:
                self.__locks[lockID] = (clientID, currentTime)

    @replicated
    def release(self, lockID, clientID):
        existingLock = self.__locks.get(lockID, None)
        if existingLock is not None and existingLock[0] == clientID:
            del self.__locks[lockID]

    def isAcquired(self, lockID, clientID, currentTime):
        existingLock = self.__locks.get(lockID, None)
        if existingLock is not None:
            if existingLock[0] == clientID:
                if currentTime - existingLock[1] < self.__autoUnlockTime:
                    return True
        return False


class ReplLockManager(object):

    def __init__(self, autoUnlockTime, selfID = None):
        """Replicated Lock Manager. Allow to acquire / release distributed locks.

        :param autoUnlockTime: lock will be released automatically
            if no response from holder for more than autoUnlockTime seconds
        :type autoUnlockTime: float
        :param selfID: (optional) - unique id of current lock holder.
        :type selfID: str
        """
        self.__lockImpl = _ReplLockManagerImpl(autoUnlockTime)
        if selfID is None:
            selfID = '%s:%d:%d' % (socket.gethostname(), os.getpid(), id(self))
        self.__selfID = selfID
        self.__autoUnlockTime = autoUnlockTime
        self.__mainThread = threading.current_thread()
        self.__initialised = threading.Event()
        self.__destroying = False
        self.__lastProlongateTime = 0
        self.__thread = threading.Thread(target=ReplLockManager._autoAcquireThread, args=(weakref.proxy(self),))
        self.__thread.start()
        while not self.__initialised.is_set():
            pass

    def _consumer(self):
        return self.__lockImpl

    def destroy(self):
        """Destroy should be called before destroying ReplLockManager"""
        self.__destroying = True

    def _autoAcquireThread(self):
        self.__initialised.set()
        try:
            while True:
                if not self.__mainThread.is_alive():
                    break
                if self.__destroying:
                    break
                time.sleep(0.1)
                if time.time() - self.__lastProlongateTime < float(self.__autoUnlockTime) / 4.0:
                    continue
                syncObj = self.__lockImpl._syncObj
                if syncObj is None:
                    continue
                if syncObj._getLeader() is not None:
                    self.__lastProlongateTime = time.time()
                    self.__lockImpl.prolongate(self.__selfID, time.time())
        except ReferenceError:
            pass

    def tryAcquire(self, lockID, callback=None, sync=False, timeout=None):
        """Attempt to acquire lock.

        :param lockID: unique lock identifier.
        :type lockID: str
        :param sync: True - to wait until lock is acquired or failed to acquire.
        :type sync: bool
        :param callback: if sync is False - callback will be called with operation result.
        :type callback: func(opResult, error)
        :param timeout: max operation time (default - unlimited)
        :type timeout: float
        :return True if acquired, False - somebody else already acquired lock
        """
        attemptTime = time.time()
        if sync:
            acquireRes = self.__lockImpl.acquire(lockID, self.__selfID, attemptTime, callback=callback, sync=sync, timeout=timeout)
            acquireTime = time.time()
            if acquireRes:
                if acquireTime - attemptTime > self.__autoUnlockTime / 2.0:
                    acquireRes = False
                    self.__lockImpl.release(lockID, self.__selfID, sync=sync)
            return acquireRes

        def asyncCallback(acquireRes, errCode):
            if acquireRes:
                acquireTime = time.time()
                if acquireTime - attemptTime > self.__autoUnlockTime / 2.0:
                    acquireRes = False
                    self.__lockImpl.release(lockID, self.__selfID, sync=False)
            callback(acquireRes, errCode)

        self.__lockImpl.acquire(lockID, self.__selfID, attemptTime, callback=asyncCallback, sync=sync, timeout=timeout)

    def isAcquired(self, lockID):
        """Check if lock is acquired by ourselves.

        :param lockID: unique lock identifier.
        :type lockID: str
        :return True if lock is acquired by ourselves.
         """
        return self.__lockImpl.isAcquired(lockID, self.__selfID, time.time())

    def release(self, lockID, callback=None, sync=False, timeout=None):
        """
        Release previously-acquired lock.

        :param lockID:  unique lock identifier.
        :type lockID: str
        :param sync: True - to wait until lock is released or failed to release.
        :type sync: bool
        :param callback: if sync is False - callback will be called with operation result.
        :type callback: func(opResult, error)
        :param timeout: max operation time (default - unlimited)
        :type timeout: float
        """
        self.__lockImpl.release(lockID, self.__selfID, callback=callback, sync=sync, timeout=timeout)
