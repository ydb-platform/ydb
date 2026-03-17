"""Zookeeper Partitioner Implementation

:Maintainer: None
:Status: Unknown

:class:`SetPartitioner` implements a partitioning scheme using
Zookeeper for dividing up resources amongst members of a party.

This is useful when there is a set of resources that should only be
accessed by a single process at a time that multiple processes
across a cluster might want to divide up.

Example Use-Case
----------------

- Multiple workers across a cluster need to divide up a list of queues
  so that no two workers own the same queue.

"""
from functools import partial
import logging
import os
import socket

from kazoo.exceptions import KazooException, LockTimeout
from kazoo.protocol.states import KazooState
from kazoo.recipe.watchers import PatientChildrenWatch


log = logging.getLogger(__name__)


class PartitionState(object):
    """High level partition state values

    .. attribute:: ALLOCATING

        The set needs to be partitioned, and may require an existing
        partition set to be released before acquiring a new partition
        of the set.

    .. attribute:: ACQUIRED

        The set has been partitioned and acquired.

    .. attribute:: RELEASE

        The set needs to be repartitioned, and the current partitions
        must be released before a new allocation can be made.

    .. attribute:: FAILURE

        The set partition has failed. This occurs when the maximum
        time to partition the set is exceeded or the Zookeeper session
        is lost. The partitioner is unusable after this state and must
        be recreated.

    """
    ALLOCATING = "ALLOCATING"
    ACQUIRED = "ACQUIRED"
    RELEASE = "RELEASE"
    FAILURE = "FAILURE"


class SetPartitioner(object):
    """Partitions a set amongst members of a party

    This class will partition a set amongst members of a party such
    that each member will be given zero or more items of the set and
    each set item will be given to a single member. When new members
    enter or leave the party, the set will be re-partitioned amongst
    the members.

    When the :class:`SetPartitioner` enters the
    :attr:`~PartitionState.FAILURE` state, it is unrecoverable
    and a new :class:`SetPartitioner` should be created.

    Example:

    .. code-block:: python

        from kazoo.client import KazooClient
        client = KazooClient()
        client.start()

        qp = client.SetPartitioner(
            path='/work_queues', set=('queue-1', 'queue-2', 'queue-3'))

        while 1:
            if qp.failed:
                raise Exception("Lost or unable to acquire partition")
            elif qp.release:
                qp.release_set()
            elif qp.acquired:
                for partition in qp:
                    # Do something with each partition
            elif qp.allocating:
                qp.wait_for_acquire()

    **State Transitions**

    When created, the :class:`SetPartitioner` enters the
    :attr:`PartitionState.ALLOCATING` state.

    :attr:`~PartitionState.ALLOCATING` ->
    :attr:`~PartitionState.ACQUIRED`

        Set was partitioned successfully, the partition list assigned
        is accessible via list/iter methods or calling list() on the
        :class:`SetPartitioner` instance.

    :attr:`~PartitionState.ALLOCATING` ->
    :attr:`~PartitionState.FAILURE`

        Allocating the set failed either due to a Zookeeper session
        expiration, or failure to acquire the items of the set within
        the timeout period.

    :attr:`~PartitionState.ACQUIRED` ->
    :attr:`~PartitionState.RELEASE`

        The members of the party have changed, and the set needs to be
        repartitioned. :meth:`SetPartitioner.release` should be called
        as soon as possible.

    :attr:`~PartitionState.ACQUIRED` ->
    :attr:`~PartitionState.FAILURE`

        The current partition was lost due to a Zookeeper session
        expiration.

    :attr:`~PartitionState.RELEASE` ->
    :attr:`~PartitionState.ALLOCATING`

        The current partition was released and is being re-allocated.

    """
    def __init__(self, client, path, set, partition_func=None,
                 identifier=None, time_boundary=30, max_reaction_time=1,
                 state_change_event=None):
        """Create a :class:`~SetPartitioner` instance

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The partition path to use.
        :param set: The set of items to partition.
        :param partition_func: A function to use to decide how to
                               partition the set.
        :param identifier: An identifier to use for this member of the
                           party when participating. Defaults to the
                           hostname + process id.
        :param time_boundary: How long the party members must be stable
                              before allocation can complete.
        :param max_reaction_time: Maximum reaction time for party members
                                  change.
        :param state_change_event: An optional Event object that will be set
                                   on every state change.

        """
        # Used to differentiate two states with the same names in time
        self.state_id = 0
        self.state = PartitionState.ALLOCATING
        self.state_change_event = state_change_event or \
            client.handler.event_object()

        self._client = client
        self._path = path
        self._set = set
        self._partition_set = []
        self._partition_func = partition_func or self._partitioner
        self._identifier = identifier or '%s-%s' % (
            socket.getfqdn(), os.getpid())
        self._locks = []
        self._lock_path = '/'.join([path, 'locks'])
        self._party_path = '/'.join([path, 'party'])
        self._time_boundary = time_boundary
        self._max_reaction_time = max_reaction_time

        self._acquire_event = client.handler.event_object()

        # Create basic path nodes
        client.ensure_path(path)
        client.ensure_path(self._lock_path)
        client.ensure_path(self._party_path)

        # Join the party
        self._party = client.ShallowParty(self._party_path,
                                          identifier=self._identifier)
        self._party.join()

        self._state_change = client.handler.rlock_object()
        client.add_listener(self._establish_sessionwatch)

        # Now watch the party and set the callback on the async result
        # so we know when we're ready
        self._child_watching(self._allocate_transition, client_handler=True)

    def __iter__(self):
        """Return the partitions in this partition set"""
        for partition in self._partition_set:
            yield partition

    @property
    def failed(self):
        """Corresponds to the :attr:`PartitionState.FAILURE` state"""
        return self.state == PartitionState.FAILURE

    @property
    def release(self):
        """Corresponds to the :attr:`PartitionState.RELEASE` state"""
        return self.state == PartitionState.RELEASE

    @property
    def allocating(self):
        """Corresponds to the :attr:`PartitionState.ALLOCATING`
        state"""
        return self.state == PartitionState.ALLOCATING

    @property
    def acquired(self):
        """Corresponds to the :attr:`PartitionState.ACQUIRED` state"""
        return self.state == PartitionState.ACQUIRED

    def wait_for_acquire(self, timeout=30):
        """Wait for the set to be partitioned and acquired

        :param timeout: How long to wait before returning.
        :type timeout: int

        """
        self._acquire_event.wait(timeout)

    def release_set(self):
        """Call to release the set

        This method begins the step of allocating once the set has
        been released.

        """
        self._release_locks()
        if self._locks:  # pragma: nocover
            # This shouldn't happen, it means we couldn't release our
            # locks, abort
            self._fail_out()
            return
        else:
            with self._state_change:
                if self.failed:
                    return
                self._set_state(PartitionState.ALLOCATING)
        self._child_watching(self._allocate_transition, client_handler=True)

    def finish(self):
        """Call to release the set and leave the party"""
        self._release_locks()
        self._fail_out()

    def _fail_out(self):
        with self._state_change:
            self._set_state(PartitionState.FAILURE)
        if self._party.participating:
            try:
                self._party.leave()
            except KazooException:  # pragma: nocover
                pass

    def _allocate_transition(self, result):
        """Called when in allocating mode, and the children settled"""

        # Did we get an exception waiting for children to settle?
        if result.exception:  # pragma: nocover
            self._fail_out()
            return

        children, async_result = result.get()
        children_changed = self._client.handler.event_object()

        def updated(result):
            with self._state_change:
                children_changed.set()
                if self.acquired:
                    self._set_state(PartitionState.RELEASE)

        with self._state_change:
            # We can lose connection during processing the event
            if not self.allocating:
                return

            # Remember the state ID to check later for race conditions
            state_id = self.state_id

            # updated() will be called when children change
            async_result.rawlink(updated)

        # Check whether the state has changed during the lock acquisition
        # and abort the process if so.
        def abort_if_needed():
            if self.state_id == state_id:
                if children_changed.is_set():
                    # The party has changed. Repartitioning...
                    self._abort_lock_acquisition()
                    return True
                else:
                    return False
            else:
                if self.allocating or self.acquired:
                    # The connection was lost and user initiated a new
                    # allocation process. Abort it to eliminate race
                    # conditions with locks.
                    with self._state_change:
                        self._set_state(PartitionState.RELEASE)

                return True

        # Split up the set
        partition_set = self._partition_func(
            self._identifier, list(self._party), self._set)

        # Proceed to acquire locks for the working set as needed
        for member in partition_set:
            lock = self._client.Lock(self._lock_path + '/' + str(member))

            while True:
                try:
                    # We mustn't lock without timeout because in that case we
                    # can get a deadlock if the party state will change during
                    # lock acquisition.
                    lock.acquire(timeout=self._max_reaction_time)
                except LockTimeout:
                    if abort_if_needed():
                        return
                except KazooException:
                    return self.finish()
                else:
                    break

            self._locks.append(lock)

            if abort_if_needed():
                return

        # All locks acquired. Time for state transition.
        with self._state_change:
            if self.state_id == state_id and not children_changed.is_set():
                self._partition_set = partition_set
                self._set_state(PartitionState.ACQUIRED)
                self._acquire_event.set()
                return

        if not abort_if_needed():
            # This mustn't happen. Means a logical error.
            self._fail_out()

    def _release_locks(self):
        """Attempt to completely remove all the locks"""
        self._acquire_event.clear()
        for lock in self._locks[:]:
            try:
                lock.release()
            except KazooException:  # pragma: nocover
                # We proceed to remove as many as possible, and leave
                # the ones we couldn't remove
                pass
            else:
                self._locks.remove(lock)

    def _abort_lock_acquisition(self):
        """Called during lock acquisition if a party change occurs"""

        self._release_locks()

        if self._locks:
            # This shouldn't happen, it means we couldn't release our
            # locks, abort
            self._fail_out()
            return

        self._child_watching(self._allocate_transition, client_handler=True)

    def _child_watching(self, func=None, client_handler=False):
        """Called when children are being watched to stabilize

        This actually returns immediately, child watcher spins up a
        new thread/greenlet and waits for it to stabilize before
        any callbacks might run.

        :param client_handler: If True, deliver the result using the
                               client's event handler.
        """
        watcher = PatientChildrenWatch(self._client, self._party_path,
                                       self._time_boundary)
        asy = watcher.start()
        if func is not None:
            # We spin up the function in a separate thread/greenlet
            # to ensure that the rawlink's it might use won't be
            # blocked
            if client_handler:
                func = partial(self._client.handler.spawn, func)
            asy.rawlink(func)
        return asy

    def _establish_sessionwatch(self, state):
        """Register ourself to listen for session events, we shut down
        if we become lost"""
        with self._state_change:
            if self.failed:
                pass
            elif state == KazooState.LOST:
                self._client.handler.spawn(self._fail_out)
            elif not self.release:
                self._set_state(PartitionState.RELEASE)

        return state == KazooState.LOST

    def _partitioner(self, identifier, members, partitions):
        # Ensure consistent order of partitions/members
        all_partitions = sorted(partitions)
        workers = sorted(members)

        i = workers.index(identifier)
        # Now return the partition list starting at our location and
        # skipping the other workers
        return all_partitions[i::len(workers)]

    def _set_state(self, state):
        self.state = state
        self.state_id += 1
        self.state_change_event.set()
