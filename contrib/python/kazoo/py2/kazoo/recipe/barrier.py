"""Zookeeper Barriers

:Maintainer: None
:Status: Unknown

"""
import os
import socket
import uuid

from kazoo.exceptions import KazooException, NoNodeError, NodeExistsError
from kazoo.protocol.states import EventType


class Barrier(object):
    """Kazoo Barrier

    Implements a barrier to block processing of a set of nodes until
    a condition is met at which point the nodes will be allowed to
    proceed. The barrier is in place if its node exists.

    .. warning::

        The :meth:`wait` function does not handle connection loss and
        may raise :exc:`~kazoo.exceptions.ConnectionLossException` if
        the connection is lost while waiting.

    """
    def __init__(self, client, path):
        """Create a Kazoo Barrier

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The barrier path to use.

        """
        self.client = client
        self.path = path

    def create(self):
        """Establish the barrier if it doesn't exist already"""
        self.client.retry(self.client.ensure_path, self.path)

    def remove(self):
        """Remove the barrier

        :returns: Whether the barrier actually needed to be removed.
        :rtype: bool

        """
        try:
            self.client.retry(self.client.delete, self.path)
            return True
        except NoNodeError:
            return False

    def wait(self, timeout=None):
        """Wait on the barrier to be cleared

        :returns: True if the barrier has been cleared, otherwise
                  False.
        :rtype: bool

        """
        cleared = self.client.handler.event_object()

        def wait_for_clear(event):
            if event.type == EventType.DELETED:
                cleared.set()

        exists = self.client.exists(self.path, watch=wait_for_clear)
        if not exists:
            return True

        cleared.wait(timeout)
        return cleared.is_set()


class DoubleBarrier(object):
    """Kazoo Double Barrier

    Double barriers are used to synchronize the beginning and end of
    a distributed task. The barrier blocks when entering it until all
    the members have joined, and blocks when leaving until all the
    members have left.

    .. note::

        You should register a listener for session loss as the process
        will no longer be part of the barrier once the session is
        gone. Connection losses will be retried with the default retry
        policy.

    """
    def __init__(self, client, path, num_clients, identifier=None):
        """Create a Double Barrier

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The barrier path to use.
        :param num_clients: How many clients must enter the barrier to
                            proceed.
        :type num_clients: int
        :param identifier: An identifier to use for this member of the
                           barrier when participating. Defaults to the
                           hostname + process id.

        """
        self.client = client
        self.path = path
        self.num_clients = num_clients
        self._identifier = identifier or '%s-%s' % (
            socket.getfqdn(), os.getpid())
        self.participating = False
        self.assured_path = False
        self.node_name = uuid.uuid4().hex
        self.create_path = self.path + "/" + self.node_name

    def enter(self):
        """Enter the barrier, blocks until all nodes have entered"""
        try:
            self.client.retry(self._inner_enter)
            self.participating = True
        except KazooException:
            # We failed to enter, best effort cleanup
            self._best_effort_cleanup()
            self.participating = False

    def _inner_enter(self):
        # make sure our barrier parent node exists
        if not self.assured_path:
            self.client.ensure_path(self.path)
            self.assured_path = True

        ready = self.client.handler.event_object()

        try:
            self.client.create(
                self.create_path,
                self._identifier.encode('utf-8'), ephemeral=True)
        except NodeExistsError:
            pass

        def created(event):
            if event.type == EventType.CREATED:
                ready.set()

        self.client.exists(self.path + '/' + 'ready', watch=created)

        children = self.client.get_children(self.path)

        if len(children) < self.num_clients:
            ready.wait()
        else:
            self.client.ensure_path(self.path + '/ready')
        return True

    def leave(self):
        """Leave the barrier, blocks until all nodes have left"""
        try:
            self.client.retry(self._inner_leave)
        except KazooException:  # pragma: nocover
            # Failed to cleanly leave
            self._best_effort_cleanup()
        self.participating = False

    def _inner_leave(self):
        # Delete the ready node if its around
        try:
            self.client.delete(self.path + '/ready')
        except NoNodeError:
            pass

        while True:
            children = self.client.get_children(self.path)
            if not children:
                return True

            if len(children) == 1 and children[0] == self.node_name:
                self.client.delete(self.create_path)
                return True

            children.sort()

            ready = self.client.handler.event_object()

            def deleted(event):
                if event.type == EventType.DELETED:
                    ready.set()

            if self.node_name == children[0]:
                # We're first, wait on the highest to leave
                if not self.client.exists(self.path + '/' + children[-1],
                                          watch=deleted):
                    continue

                ready.wait()
                continue

            # Delete our node
            self.client.delete(self.create_path)

            # Wait on the first
            if not self.client.exists(self.path + '/' + children[0],
                                      watch=deleted):
                continue

            # Wait for the lowest to be deleted
            ready.wait()

    def _best_effort_cleanup(self):
        try:
            self.client.retry(self.client.delete, self.create_path)
        except NoNodeError:
            pass
