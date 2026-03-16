# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.


import logging
import random
import threading
import time
from queue import Empty, PriorityQueue
from typing import Any, Dict, Optional, Sequence, Tuple, Type

from .connection import Connection
from .exceptions import ImproperlyConfigured

logger: logging.Logger = logging.getLogger("opensearch")


class ConnectionSelector:
    """
    Simple class used to select a connection from a list of currently live
    connection instances. In init time it is passed a dictionary containing all
    the connections' options which it can then use during the selection
    process. When the `select` method is called it is given a list of
    *currently* live connections to choose from.

    The options dictionary is the one that has been passed to
    :class:`~opensearchpy.Transport` as `hosts` param and the same that is
    used to construct the Connection object itself. When the Connection was
    created from information retrieved from the cluster via the sniffing
    process it will be the dictionary returned by the `host_info_callback`.

    Example of where this would be useful is a zone-aware selector that would
    only select connections from its own zones and only fall back to other
    connections where there would be none in its zones.
    """

    def __init__(self, opts: Sequence[Tuple[Connection, Any]]) -> None:
        """
        :arg opts: dictionary of connection instances and their options
        """
        self.connection_opts = opts

    def select(self, connections: Sequence[Connection]) -> None:
        """
        Select a connection from the given list.

        :arg connections: list of live connections to choose from
        """
        pass


class RandomSelector(ConnectionSelector):
    """
    Select a connection at random
    """

    def select(self, connections: Sequence[Connection]) -> Any:
        return random.choice(connections)


class RoundRobinSelector(ConnectionSelector):
    """
    Selector using round-robin.
    """

    def __init__(self, opts: Sequence[Tuple[Connection, Any]]) -> None:
        super().__init__(opts)
        self.data = threading.local()

    def select(self, connections: Sequence[Connection]) -> Any:
        self.data.rr = getattr(self.data, "rr", -1) + 1
        self.data.rr %= len(connections)
        return connections[self.data.rr]


class ConnectionPool:
    """
    Container holding the :class:`~opensearchpy.Connection` instances,
    managing the selection process (via a
    :class:`~opensearchpy.ConnectionSelector`) and dead connections.

    It's only interactions are with the :class:`~opensearchpy.Transport` class
    that drives all the actions within `ConnectionPool`.

    Initially connections are stored on the class as a list and, along with the
    connection options, get passed to the `ConnectionSelector` instance for
    future reference.

    Upon each request the `Transport` will ask for a `Connection` via the
    `get_connection` method. If the connection fails (its `perform_request`
    raises a `ConnectionError`) it will be marked as dead (via `mark_dead`) and
    put on a timeout (if it fails N times in a row the timeout is exponentially
    longer - the formula is `default_timeout * 2 ** (fail_count - 1)`). When
    the timeout is over the connection will be resurrected and returned to the
    live pool. A connection that has been previously marked as dead and
    succeeds will be marked as live (its fail count will be deleted).
    """

    connections_opts: Sequence[Tuple[Connection, Any]]
    connections: Any
    orig_connections: Tuple[Connection, ...]
    dead: Any
    dead_count: Dict[Any, int]
    dead_timeout: float
    timeout_cutoff: int
    selector: Any

    def __init__(
        self,
        connections: Any,
        dead_timeout: float = 60,
        timeout_cutoff: int = 5,
        selector_class: Type[ConnectionSelector] = RoundRobinSelector,
        randomize_hosts: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        :arg connections: list of tuples containing the
            :class:`~opensearchpy.Connection` instance and its options
        :arg dead_timeout: number of seconds a connection should be retired for
            after a failure, increases on consecutive failures
        :arg timeout_cutoff: number of consecutive failures after which the
            timeout doesn't increase
        :arg selector_class: :class:`~opensearchpy.ConnectionSelector`
            subclass to use if more than one connection is live
        :arg randomize_hosts: shuffle the list of connections upon arrival to
            avoid dog piling effect across processes
        """
        if not connections:
            raise ImproperlyConfigured(
                "No defined connections, you need to " "specify at least one host."
            )
        self.connection_opts = connections
        self.connections = [c for (c, opts) in connections]
        # remember original connection list for resurrect(force=True)
        self.orig_connections = tuple(self.connections)
        # PriorityQueue for thread safety and ease of timeout management
        self.dead = PriorityQueue(len(self.connections))
        self.dead_count = {}

        if randomize_hosts:
            # randomize the connection list to avoid all clients hitting same node
            # after startup/restart
            random.shuffle(self.connections)

        # default timeout after which to try resurrecting a connection
        self.dead_timeout = dead_timeout
        self.timeout_cutoff = timeout_cutoff

        self.selector = selector_class(dict(connections))  # type: ignore

    def mark_dead(self, connection: Any, now: Optional[float] = None) -> None:
        """
        Mark the connection as dead (failed). Remove it from the live pool and
        put it on a timeout.

        :arg connection: the failed instance
        """
        # allow inject for testing purposes
        now = now if now else time.time()
        try:
            self.connections.remove(connection)
        except ValueError:
            logger.info(
                "Attempted to remove %r, but it does not exist in the connection pool.",
                connection,
            )
            # connection not alive or another thread marked it already, ignore
            return
        else:
            dead_count = self.dead_count.get(connection, 0) + 1
            self.dead_count[connection] = dead_count
            timeout = self.dead_timeout * 2 ** min(dead_count - 1, self.timeout_cutoff)
            self.dead.put((now + timeout, connection))
            logger.warning(
                "Connection %r has failed for %i times in a row, putting on %i second timeout.",
                connection,
                dead_count,
                timeout,
            )

    def mark_live(self, connection: Any) -> None:
        """
        Mark connection as healthy after a resurrection. Resets the fail
        counter for the connection.

        :arg connection: the connection to redeem
        """
        try:
            del self.dead_count[connection]
        except KeyError:
            # race condition, safe to ignore
            pass

    def resurrect(self, force: bool = False) -> Any:
        """
        Attempt to resurrect a connection from the dead pool. It will try to
        locate one (not all) eligible (its timeout is over) connection to
        return to the live pool. Any resurrected connection is also returned.

        :arg force: resurrect a connection even if there is none eligible (used
            when we have no live connections). If force is specified resurrect
            always returns a connection.

        """
        # no dead connections
        if self.dead.empty():
            # we are forced to return a connection, take one from the original
            # list. This is to avoid a race condition where get_connection can
            # see no live connections but when it calls resurrect self.dead is
            # also empty. We assume that other threat has resurrected all
            # available connections so we can safely return one at random.
            if force:
                return random.choice(self.orig_connections)
            return

        try:
            # retrieve a connection to check
            timeout, connection = self.dead.get(block=False)
        except Empty:
            # other thread has been faster and the queue is now empty. If we
            # are forced, return a connection at random again.
            if force:
                return random.choice(self.orig_connections)
            return

        if not force and timeout > time.time():
            # return it back if not eligible and not forced
            self.dead.put((timeout, connection))
            return

        # either we were forced or the connection is eligible to be retried
        self.connections.append(connection)
        logger.info("Resurrecting connection %r (force=%s).", connection, force)
        return connection

    def get_connection(self) -> Any:
        """
        Return a connection from the pool using the `ConnectionSelector`
        instance.

        It tries to resurrect eligible connections, forces a resurrection when
        no connections are available and passes the list of live connections to
        the selector instance to choose from.

        Returns a connection instance and its current fail count.
        """
        self.resurrect()
        connections = self.connections[:]

        # no live nodes, resurrect one by force and return it
        if not connections:
            return self.resurrect(True)

        # only call selector if we have a selection
        if len(connections) > 1:
            return self.selector.select(connections)

        # only one connection, no need for a selector
        return connections[0]

    def close(self) -> Any:
        """
        Explicitly closes connections
        """
        for conn in self.connections:
            conn.close()

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.connections!r}>"


class DummyConnectionPool(ConnectionPool):
    def __init__(self, connections: Any, **kwargs: Any) -> None:
        if len(connections) != 1:
            raise ImproperlyConfigured(
                "DummyConnectionPool needs exactly one " "connection defined."
            )
        # we need connection opts for sniffing logic
        self.connection_opts = connections
        self.connection: Any = connections[0][0]
        self.connections = (self.connection,)

    def get_connection(self) -> Any:
        return self.connection

    def close(self) -> None:
        """
        Explicitly closes connections
        """
        self.connection.close()

    def _noop(self, *args: Any, **kwargs: Any) -> Any:
        pass

    mark_dead = mark_live = resurrect = _noop


class EmptyConnectionPool(ConnectionPool):
    """A connection pool that is empty. Errors out if used."""

    def __init__(self, *_: Any, **__: Any) -> None:
        self.connections = []
        self.connection_opts = []

    def get_connection(self) -> Connection:
        raise ImproperlyConfigured("No connections were configured")

    def _noop(self, *args: Any, **kwargs: Any) -> Any:
        pass

    close = mark_dead = mark_live = resurrect = _noop
