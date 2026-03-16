# -*- coding: utf-8 -*-
"""
momoko.connection
=================

Connection handling.

Copyright 2011-2014, Frank Smit & Zaar Hai.
MIT, see LICENSE for more details.
"""

from __future__ import print_function

import sys
if sys.version_info[0] >= 3:
    basestring = str

import logging
from functools import partial
from collections import deque
import time
import datetime
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import register_hstore as _psy_register_hstore
from psycopg2.extras import register_json as _psy_register_json
from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE

import tornado
from tornado.ioloop import IOLoop
from tornado.concurrent import chain_future, Future

from .exceptions import PoolError, PartiallyConnectedError

# Backfill for tornado 5 compatability
# https://www.tornadoweb.org/en/stable/concurrent.html#tornado.concurrent.future_set_exc_info
if tornado.version_info[0] < 5:
    def future_set_exc_info(future, exc_info):
        future.set_exc_info(exc_info)
else:
    from tornado.concurrent import future_set_exc_info

log = logging.getLogger('momoko')


class ConnectionContainer(object):
    """
    Helper class that stores connections according to their state
    """
    def __init__(self):
        self.empty()

    def __repr__(self):
        return ('<%s at %x: %d free, %d busy, %d dead, %d pending, %d waiting>'
                % (self.__class__.__name__,
                   id(self),
                   len(self.free),
                   len(self.busy),
                   len(self.dead),
                   len(self.pending),
                   len(self.waiting_queue)))

    def empty(self):
        self.free = deque()
        self.busy = set()
        self.dead = set()
        self.pending = set()
        self.waiting_queue = deque()

    def add_free(self, conn):
        self.pending.discard(conn)
        log.debug("Handling free connection %s", conn.fileno)

        if not self.waiting_queue:
            log.debug("No outstanding requests - adding to free pool")
            conn.last_used_time = time.time()
            self.free.append(conn)
            return

        log.debug("There are outstanding requests - resumed future from waiting queue")
        self.busy.add(conn)
        future = self.waiting_queue.pop()
        future.set_result(conn)

    def add_dead(self, conn):
        log.debug("Adding dead connection")
        self.pending.discard(conn)
        self.dead.add(conn)

        # If everything is dead, abort anything pending.
        if not self.pending:
            self.abort_waiting_queue(Pool.DatabaseNotAvailable("No database connection available"))

    def acquire(self):
        """Occupy free connection"""
        future = Future()
        if self.free:
            conn = self.free.pop()
            self.busy.add(conn)
            future.set_result(conn)
            log.debug("Acquired free connection %s", conn.fileno)
            return future
        elif self.busy:
            log.debug("No free connections, and some are busy - put in waiting queue")
            self.waiting_queue.appendleft(future)
            return future
        elif self.pending:
            log.debug("No free connections, but some are pending - put in waiting queue")
            self.waiting_queue.appendleft(future)
            return future
        else:
            log.debug("All connections are dead")
            return None

    def release(self, conn):
        log.debug("About to release connection %s", conn.fileno)
        assert conn in self.busy, "Tried to release non-busy connection"
        self.busy.remove(conn)
        if conn.closed:
            log.debug("The connection is dead")
            self.dead.add(conn)
        else:
            log.debug("The connection is alive")
            self.add_free(conn)

    def abort_waiting_queue(self, error):
        while self.waiting_queue:
            future = self.waiting_queue.pop()
            future.set_exception(error)

    def close_alive(self):
        for conn in self.busy.union(self.free):
            if not conn.closed:
                conn.close()

    def shrink(self, target_size, delay_in_seconds):
        now = time.time()
        while len(self.free) > target_size and now - self.free[0].last_used_time > delay_in_seconds:
            conn = self.free.popleft()
            conn.close()

    @property
    def all_dead(self):
        return not (self.free or self.busy or self.pending)

    @property
    def total(self):
        return len(self.free) + len(self.busy) + len(self.dead) + len(self.pending)


class Pool(object):
    """
    Asynchronous connection pool object. All its methods are
    asynchronous unless stated otherwide in method description.

    :param string dsn:
        A `Data Source Name`_ string containing one of the following values:

        * **dbname** - the database name
        * **user** - user name used to authenticate
        * **password** - password used to authenticate
        * **host** - database host address (defaults to UNIX socket if not provided)
        * **port** - connection port number (defaults to 5432 if not provided)

        Or any other parameter supported by PostgreSQL. See the PostgreSQL
        documentation for a complete list of supported parameters_.

    :param connection_factory:
        The ``connection_factory`` argument can be used to create non-standard
        connections. The class returned should be a subclass of `psycopg2.extensions.connection`_.
        See `Connection and cursor factories`_ for details. Defaults to ``None``.

    :param cursor_factory:
        The ``cursor_factory`` argument can be used to return non-standart cursor class
        The class returned should be a subclass of `psycopg2.extensions.cursor`_.
        See `Connection and cursor factories`_ for details. Defaults to ``None``.

    :param int size:
        Minimal number of connections to maintain. ``size`` connections will be opened
        and maintained after calling :py:meth:`momoko.Pool.connect`.

    :param max_size:
        if not ``None``, the pool size will dynamically grow on demand up to ``max_size``
        open connections. By default the connections will still be maintained even if
        when the pool load decreases. See also ``auto_shrink`` parameter.
    :type max_size: int or None

    :param ioloop:
        Tornado IOloop instance to use. Defaults to Tornado's ``IOLoop.instance()``.

    :param bool raise_connect_errors:
        Whether to raise :py:meth:`momoko.PartiallyConnectedError` when failing to
        connect to database during :py:meth:`momoko.Pool.connect`.

    :param int reconnect_interval:
        If database server becomes unavailable, the pool will try to reestablish
        the connection. The attempt frequency is ``reconnect_interval``
        milliseconds.

    :param list setsession:
        List of intial sql commands to be executed once connection is established.
        If any of the commands fails, the connection will be closed.
        **NOTE:** The commands will be executed as one transaction block.

    :param bool auto_shrink:
        Garbage-collect idle connections. Only applicable if ``max_size`` was specified.
        Nevertheless, the pool will mainatain at least ``size`` connections.

    :param shrink_delay:
        A connection is declared idle if it was not used for ``shrink_delay`` time period.
        Idle connections will be garbage-collected if ``auto_shrink`` is set to ``True``.
    :type shrink_delay: :py:meth:`datetime.timedelta`

    :param shrink_period:
        If ``auto_shink`` is enabled, this parameter defines how the pool will check for
        idle connections.
    :type shrink_period: :py:meth:`datetime.timedelta`

    .. _Data Source Name: http://en.wikipedia.org/wiki/Data_Source_Name
    .. _parameters: http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS
    .. _psycopg2.extensions.connection: http://initd.org/psycopg/docs/connection.html#connection
    .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
    """

    class DatabaseNotAvailable(psycopg2.DatabaseError):
        """Raised when Pool can not connect to database server"""

    def __init__(self,
                 dsn,
                 connection_factory=None,
                 cursor_factory=None,
                 size=1,
                 max_size=None,
                 ioloop=None,
                 raise_connect_errors=True,
                 reconnect_interval=500,
                 setsession=(),
                 auto_shrink=False,
                 shrink_delay=datetime.timedelta(minutes=2),
                 shrink_period=datetime.timedelta(minutes=2)
                 ):

        assert size > 0, "The connection pool size must be a number above 0."

        self.size = size
        self.max_size = max_size or size
        assert self.size <= self.max_size, "The connection pool max size must be of at least 'size'."

        self.dsn = dsn
        self.connection_factory = connection_factory
        self.cursor_factory = cursor_factory
        self.raise_connect_errors = raise_connect_errors
        self.reconnect_interval = float(reconnect_interval)/1000  # the parameter is in milliseconds
        self.setsession = setsession

        self.connected = False
        self.closed = False
        self.server_version = None

        self.ioloop = ioloop or IOLoop.instance()

        self.conns = ConnectionContainer()

        self._last_connect_time = 0
        self._no_conn_available_error = self.DatabaseNotAvailable("No database connection available")
        self.shrink_period = shrink_period
        self.shrink_delay = shrink_delay
        self.auto_shrink = auto_shrink
        if auto_shrink:
            self._auto_shrink()

    def _auto_shrink(self):
        self.conns.shrink(self.size, self.shrink_delay.seconds)
        self.ioloop.add_timeout(self.shrink_period, self._auto_shrink)

    def connect(self):
        """
        Returns future that resolves to this Pool object.

        If some connection failed to connect *and* self.raise_connect_errors
        is true, raises :py:meth:`momoko.PartiallyConnectedError`.
        """
        future = Future()
        pending = [self.size-1]

        def on_connect(fut):
            if pending[0]:
                pending[0] -= 1
                return
            # all connection attempts are complete
            if self.conns.dead and self.raise_connect_errors:
                ecp = PartiallyConnectedError("%s connection(s) failed to connect" % len(self.conns.dead))
                future.set_exception(ecp)
            else:
                future.set_result(self)
            log.debug("All initial connection requests complete")

        for i in range(self.size):
            self.ioloop.add_future(self._new_connection(), on_connect)

        return future

    def getconn(self, ping=True):
        """
        Acquire connection from the pool.

        You can then use this connection for subsequent queries.
        Just use ``connection.execute`` instead of ``Pool.execute``.

        Make sure to return connection to the pool by calling :py:meth:`momoko.Pool.putconn`,
        otherwise the connection will remain forever busy and you'll starve your pool.

        Returns a future that resolves to the acquired connection object.

        :param boolean ping:
            Whether to ping the connection before returning it by executing :py:meth:`momoko.Connection.ping`.
        """
        rv = self.conns.acquire()
        if isinstance(rv, Future):
            self._reanimate_and_stretch_if_needed()
            future = rv
        else:
            # Else, all connections are dead
            future = Future()

            def on_reanimate_done(fut):
                if self.conns.all_dead:
                    log.debug("all connections are still dead")
                    future.set_exception(self._no_conn_available_error)
                    return
                f = self.conns.acquire()
                assert isinstance(f, Future)
                chain_future(f, future)

            self.ioloop.add_future(self._reanimate(), on_reanimate_done)

        if not ping:
            return future
        else:
            return self._ping_future_connection(future)

    def putconn(self, connection):
        """
        Return busy connection back to the pool.

        **NOTE:** This is a synchronous method.

        :param Connection connection:
            Connection object previously returned by :py:meth:`momoko.Pool.getconn`.
        """

        self.conns.release(connection)

        if self.conns.all_dead:
            log.debug("All connections are dead - aborting waiting queue")
            self.conns.abort_waiting_queue(self._no_conn_available_error)

    @contextmanager
    def manage(self, connection):
        """
        Context manager that automatically returns connection to the pool.
        You can use it instead of :py:meth:`momoko.Pool.putconn`::

            connection = yield self.db.getconn()
            with self.db.manage(connection):
                cursor = yield connection.execute("BEGIN")
                ...
        """
        assert connection in self.conns.busy, "Can not manage non-busy connection. Where did you get it from?"
        try:
            yield connection
        finally:
            self.putconn(connection)

    def ping(self):
        """
        Make sure this connection is alive by executing SELECT 1 statement -
        i.e. roundtrip to the database.

        See :py:meth:`momoko.Connection.ping` for documentation about the
        parameters.
        """
        return self._operate(Connection.ping)

    def execute(self, *args, **kwargs):
        """
        Prepare and execute a database operation (query or command).

        See :py:meth:`momoko.Connection.execute` for documentation about the
        parameters.
        """
        return self._operate(Connection.execute, args, kwargs)

    def callproc(self, *args, **kwargs):
        """
        Call a stored database procedure with the given name.

        See :py:meth:`momoko.Connection.callproc` for documentation about the
        parameters.
        """
        return self._operate(Connection.callproc, args, kwargs)

    def transaction(self, *args, **kwargs):
        """
        Run a sequence of SQL queries in a database transaction.

        See :py:meth:`momoko.Connection.transaction` for documentation about the
        parameters.
        """
        return self._operate(Connection.transaction, args, kwargs)

    def mogrify(self, *args, **kwargs):
        """
        Return a query string after arguments binding.

        **NOTE:** This is NOT a synchronous method (contary to `momoko.Connection.mogrify`)
        - it asynchronously waits for available connection. For performance
        reasons, its better to create dedicated :py:meth:`momoko.Connection`
        object and use it directly for mogrification, this operation does not
        imply any real operation on the database server.

        See :py:meth:`momoko.Connection.mogrify` for documentation about the
        parameters.
        """
        return self._operate(Connection.mogrify, args, kwargs, async_=False)

    def register_hstore(self, *args, **kwargs):
        """
        Register adapter and typecaster for ``dict-hstore`` conversions.

        See :py:meth:`momoko.Connection.register_hstore` for documentation about
        the parameters. This method has no ``globally`` parameter, because it
        already registers hstore to all the connections in the pool.
        """
        kwargs["globally"] = True
        return self._operate(Connection.register_hstore, args, kwargs)

    def register_json(self, *args, **kwargs):
        """
        Create and register typecasters converting ``json`` type to Python objects.

        See :py:meth:`momoko.Connection.register_json` for documentation about
        the parameters. This method has no ``globally`` parameter, because it
        already registers json to all the connections in the pool.
        """
        kwargs["globally"] = True
        return self._operate(Connection.register_json, args, kwargs)

    def close(self):
        """
        Close the connection pool.

        **NOTE:** This is a synchronous method.
        """
        if self.closed:
            raise PoolError('connection pool is already closed')

        self.conns.close_alive()
        self.conns.empty()
        self.closed = True

    def _operate(self, method, args=(), kwargs=None, async_=True, keep=False, connection=None):
        kwargs = kwargs or {}
        future = Future()

        retry = []

        def when_available(fut):
            try:
                conn = fut.result()
            except psycopg2.Error:
                future_set_exc_info(future, sys.exc_info())
                if retry and not keep:
                    self.putconn(retry[0])
                return

            log.debug("Obtained connection: %s", conn.fileno)
            try:
                future_or_result = method(conn, *args, **kwargs)
            except Exception:
                log.debug("Method failed synchronously")
                return self._retry(retry, when_available, conn, keep, future)

            if not async_:
                future.set_result(future_or_result)
                if not keep:
                    self.putconn(conn)
                return

            def when_done(rfut):
                try:
                    result = rfut.result()
                except psycopg2.Error:
                    log.debug("Method failed Asynchronously")
                    return self._retry(retry, when_available, conn, keep, future)

                future.set_result(result)
                if not keep:
                    self.putconn(conn)

            self.ioloop.add_future(future_or_result, when_done)

        if not connection:
            self.ioloop.add_future(self.getconn(ping=False), when_available)
        else:
            f = Future()
            f.set_result(connection)
            when_available(f)
        return future

    def _retry(self, retry, what, conn, keep, future):
        if conn.closed:
            if not retry:
                retry.append(conn)
                self.ioloop.add_future(conn.connect(), what)
                return
            else:
                future.set_exception(self._no_conn_available_error)
        else:
            future_set_exc_info(future, sys.exc_info())
        if not keep:
            self.putconn(conn)
        return

    def _reanimate(self):
        assert self.conns.dead, "BUG: don't call reanimate when there is no one to reanimate"

        future = Future()

        if self.ioloop.time() - self._last_connect_time < self.reconnect_interval:
            log.debug("Not reconnecting - too soon")
            future.set_result(None)
            return future

        pending = [len(self.conns.dead)-1]

        def on_connect(fut):
            if pending[0]:
                pending[0] -= 1
                return
            future.set_result(None)

        while self.conns.dead:
            conn = self.conns.dead.pop()
            self.ioloop.add_future(self._connect_one(conn), on_connect)

        return future

    def _reanimate_and_stretch_if_needed(self):
        if self.conns.dead:
            self._reanimate()
            return

        if self.conns.total == self.max_size:
            return  # max size reached
        if self.conns.free:
            return  # no point in stretching if there are free connections
        if self.conns.pending:
            if len(self.conns.pending) >= len(self.conns.waiting_queue):
                return  # there are enough outstanding connection requests

        log.debug("Stretching pool")
        self._new_connection()

    def _new_connection(self):
        log.debug("Opening new connection")
        conn = Connection(self.dsn,
                          connection_factory=self.connection_factory,
                          cursor_factory=self.cursor_factory,
                          ioloop=self.ioloop,
                          setsession=self.setsession)
        return self._connect_one(conn)

    def _connect_one(self, conn):
        future = Future()
        self.conns.pending.add(conn)

        def on_connect(fut):
            try:
                fut.result()
            except psycopg2.Error:
                self.conns.add_dead(conn)
            else:
                self.conns.add_free(conn)
                self.server_version = conn.server_version
            self._last_connect_time = self.ioloop.time()
            future.set_result(conn)

        self.ioloop.add_future(conn.connect(), on_connect)
        return future

    def _ping_future_connection(self, conn_future):
        ping_future = Future()

        def on_connection_available(fut):
            try:
                conn = fut.result()
            except psycopg2.Error:
                log.debug("Aborting ping - failed to obtain connection")
                ping_future.set_exception(self._no_conn_available_error)
                return

            def on_ping_done(ping_fut):
                try:
                    ping_fut.result()
                except psycopg2.Error:
                    if conn.closed:
                        ping_future.set_exception(self._no_conn_available_error)
                    else:
                        future_set_exc_info(ping_future, sys.exc_info())
                    self.putconn(conn)
                else:
                    ping_future.set_result(conn)

            f = self._operate(Connection.ping, keep=True, connection=conn)
            self.ioloop.add_future(f, on_ping_done)

        self.ioloop.add_future(conn_future, on_connection_available)

        return ping_future


class Connection(object):
    """
    Asynchronous connection object. All its methods are
    asynchronous unless stated otherwide in method description.

    :param string dsn:
        A `Data Source Name`_ string containing one of the following values:

        * **dbname** - the database name
        * **user** - user name used to authenticate
        * **password** - password used to authenticate
        * **host** - database host address (defaults to UNIX socket if not provided)
        * **port** - connection port number (defaults to 5432 if not provided)

        Or any other parameter supported by PostgreSQL. See the PostgreSQL
        documentation for a complete list of supported parameters_.

    :param connection_factory:
        The ``connection_factory`` argument can be used to create non-standard
        connections. The class returned should be a subclass of `psycopg2.extensions.connection`_.
        See `Connection and cursor factories`_ for details. Defaults to ``None``.

    :param cursor_factory:
        The ``cursor_factory`` argument can be used to return non-standart cursor class
        The class returned should be a subclass of `psycopg2.extensions.cursor`_.
        See `Connection and cursor factories`_ for details. Defaults to ``None``.

    :param list setsession:
        List of intial sql commands to be executed once connection is established.
        If any of the commands fails, the connection will be closed.
        **NOTE:** The commands will be executed as one transaction block.

    .. _Data Source Name: http://en.wikipedia.org/wiki/Data_Source_Name
    .. _parameters: http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS
    .. _psycopg2.extensions.connection: http://initd.org/psycopg/docs/connection.html#connection
    .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
    """
    def __init__(self,
                 dsn,
                 connection_factory=None,
                 cursor_factory=None,
                 ioloop=None,
                 setsession=()):

        self.dsn = dsn
        self.connection_factory = connection_factory
        self.cursor_factory = cursor_factory
        self.ioloop = ioloop or IOLoop.instance()
        self.setsession = setsession

    def connect(self):
        """
        Initiate asynchronous connect.
        Returns future that resolves to this connection object.
        """
        kwargs = {"async_": True}
        if self.connection_factory:
            kwargs["connection_factory"] = self.connection_factory
        if self.cursor_factory:
            kwargs["cursor_factory"] = self.cursor_factory

        future = Future()

        self.connection = None
        try:
            self.connection = psycopg2.connect(self.dsn, **kwargs)
        except psycopg2.Error:
            self.connection = None
            future_set_exc_info(future, sys.exc_info())
            return future

        self.fileno = self.connection.fileno()

        if self.setsession:
            on_connect_future = Future()

            def on_connect(on_connect_future):
                self.ioloop.add_future(self.transaction(self.setsession), lambda x: future.set_result(self))

            self.ioloop.add_future(on_connect_future, on_connect)
            callback = partial(self._io_callback, on_connect_future, self)
        else:
            callback = partial(self._io_callback, future, self)

        self.ioloop.add_handler(self.fileno, callback, IOLoop.WRITE)
        self.ioloop.add_future(future, self._set_server_version)
        self.ioloop.add_future(future, self._close_on_fail)

        return future

    def _set_server_version(self, future):
        if future.exception():
            return
        self.server_version = self.connection.server_version

    def _close_on_fail(self, future):
        # If connection attempt evetually fails - marks connection as closed by ourselves
        # since psycopg2 does not do that for us (on connection attempts)
        if future.exception():
            self.connection = None

    def _io_callback(self, future, result, fd=None, events=None):
        try:
            state = self.connection.poll()
        except (psycopg2.Warning, psycopg2.Error) as err:
            self.ioloop.remove_handler(self.fileno)
            future_set_exc_info(future, sys.exc_info())
        else:
            try:
                if state == POLL_OK:
                    self.ioloop.remove_handler(self.fileno)
                    future.set_result(result)
                elif state == POLL_READ:
                    self.ioloop.update_handler(self.fileno, IOLoop.READ)
                elif state == POLL_WRITE:
                    self.ioloop.update_handler(self.fileno, IOLoop.WRITE)
                else:
                    future.set_exception(psycopg2.OperationalError("poll() returned %s" % state))
            except IOError:
                # Can happen when there are quite a lof of outstanding
                # requests. See https://github.com/FSX/momoko/issues/127
                self.ioloop.remove_handler(self.fileno)
                future.set_exception(psycopg2.OperationalError("IOError on socket"))

    def ping(self):
        """
        Make sure this connection is alive by executing SELECT 1 statement -
        i.e. roundtrip to the database.

        Returns future. If it resolves sucessfully - the connection is alive (or dead otherwise).
        """
        return self.execute("SELECT 1 AS ping")

    def execute(self,
                operation,
                parameters=(),
                cursor_factory=None):
        """
        Prepare and execute a database operation (query or command).

        :param string operation: An SQL query.
        :param tuple/list/dict parameters:
            A list, tuple or dict with query parameters. See `Passing parameters to SQL queries`_
            for more information. Defaults to an empty tuple.
        :param cursor_factory:
            The ``cursor_factory`` argument can be used to create non-standard cursors.
            The class returned must be a subclass of `psycopg2.extensions.cursor`_.
            See `Connection and cursor factories`_ for details. Defaults to ``None``.

        Returns future that resolves to cursor object containing result.

        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _psycopg2.extensions.cursor: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.cursor
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        kwargs = {"cursor_factory": cursor_factory} if cursor_factory else {}
        cursor = self.connection.cursor(**kwargs)
        if parameters:
            cursor.execute(operation, parameters)
        else:
            cursor.execute(operation)

        future = Future()
        callback = partial(self._io_callback, future, cursor)
        self.ioloop.add_handler(self.fileno, callback, IOLoop.WRITE)
        return future

    def callproc(self,
                 procname,
                 parameters=(),
                 cursor_factory=None):
        """
        Call a stored database procedure with the given name.

        The sequence of parameters must contain one entry for each argument that
        the procedure expects. The result of the call is returned as modified copy
        of the input sequence. Input parameters are left untouched, output and
        input/output parameters replaced with possibly new values.

        The procedure may also provide a result set as output. This must then be
        made available through the standard `fetch*()`_ methods.

        :param string procname: The name of the database procedure.
        :param tuple/list parameters:
            A list or tuple with query parameters. See `Passing parameters to SQL queries`_
            for more information. Defaults to an empty tuple.
        :param cursor_factory:
            The ``cursor_factory`` argument can be used to create non-standard cursors.
            The class returned must be a subclass of `psycopg2.extensions.cursor`_.
            See `Connection and cursor factories`_ for details. Defaults to ``None``.

        Returns future that resolves to cursor object containing result.

        .. _fetch*(): http://initd.org/psycopg/docs/cursor.html#fetch
        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _psycopg2.extensions.cursor: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.cursor
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        kwargs = {"cursor_factory": cursor_factory} if cursor_factory else {}
        cursor = self.connection.cursor(**kwargs)
        cursor.callproc(procname, parameters)

        future = Future()
        callback = partial(self._io_callback, future, cursor)
        self.ioloop.add_handler(self.fileno, callback, IOLoop.WRITE)
        return future

    def mogrify(self, operation, parameters=()):
        """
        Return a query string after arguments binding.

        The string returned is exactly the one that would be sent to the database
        running the execute() method or similar.

        **NOTE:** This is a synchronous method.

        :param string operation: An SQL query.
        :param tuple/list parameters:
            A list or tuple with query parameters. See `Passing parameters to SQL queries`_
            for more information. Defaults to an empty tuple.

        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        cursor = self.connection.cursor()
        return cursor.mogrify(operation, parameters)

    def transaction(self,
                    statements,
                    cursor_factory=None,
                    auto_rollback=True):
        """
        Run a sequence of SQL queries in a database transaction.

        :param tuple/list statements:
            List or tuple containing SQL queries with or without parameters. An item
            can be a string (SQL query without parameters) or a tuple/list with two items,
            an SQL query and a tuple/list/dict with parameters.

            See `Passing parameters to SQL queries`_ for more information.
        :param cursor_factory:
            The ``cursor_factory`` argument can be used to create non-standard cursors.
            The class returned must be a subclass of `psycopg2.extensions.cursor`_.
            See `Connection and cursor factories`_ for details. Defaults to ``None``.
        :param bool auto_rollback:
            If one of the transaction statements fails, try to automatically
            execute ROLLBACK to abort the transaction. If ROLLBACK fails, it would
            not be raised, but only logged.

        Returns future that resolves to ``list`` of cursors. Each cursor contains the result
        of the corresponding transaction statement.

        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        .. _psycopg2.extensions.cursor: http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.cursor
        .. _Connection and cursor factories: http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
        """
        cursors = []
        transaction_future = Future()

        queue = self._statement_generator(statements)

        def exec_statements(future):
            try:
                cursor = future.result()
                cursors.append(cursor)
            except Exception as error:
                if auto_rollback and not self.closed:
                    self._rollback(transaction_future, error)
                else:
                    future_set_exc_info(transaction_future, sys.exc_info())
                return

            try:
                operation, parameters = next(queue)
            except StopIteration:
                transaction_future.set_result(cursors[1:-1])
                return

            f = self.execute(operation, parameters, cursor_factory)
            self.ioloop.add_future(f, exec_statements)

        self.ioloop.add_future(self.execute("BEGIN;"), exec_statements)
        return transaction_future

    def _statement_generator(self, statements):
        for statement in statements:
            if isinstance(statement, basestring):
                yield (statement, ())
            else:
                yield statement[:2]
        yield ('COMMIT;', ())

    def _rollback(self, transaction_future, error):
        def rollback_callback(rb_future):
            try:
                rb_future.result()
            except Exception as rb_error:
                log.warn("Failed to ROLLBACK transaction %s", rb_error)
            transaction_future.set_exception(error)
        self.ioloop.add_future(self.execute("ROLLBACK;"), rollback_callback)

    def _register(self, future, registrator, fut):
        try:
            cursor = fut.result()
        except Exception:
            future_set_exc_info(future, sys.exc_info())
            return

        oid, array_oid = cursor.fetchone()
        registrator(oid, array_oid)
        future.set_result(None)

    def register_hstore(self, globally=False, unicode=False):
        """
        Register adapter and typecaster for ``dict-hstore`` conversions.

        More information on the hstore datatype can be found on the
        Psycopg2 |hstoredoc|_.

        :param boolean globally:
            Register the adapter globally, not only on this connection.
        :param boolean unicode:
            If ``True``, keys and values returned from the database will be ``unicode``
            instead of ``str``. The option is not available on Python 3.

        Returns future that resolves to ``None``.

        .. |hstoredoc| replace:: documentation

        .. _hstoredoc: http://initd.org/psycopg/docs/extras.html#hstore-data-type
        """
        future = Future()
        registrator = partial(_psy_register_hstore, None, globally, unicode)
        callback = partial(self._register, future, registrator)
        self.ioloop.add_future(self.execute(
            "SELECT 'hstore'::regtype::oid AS hstore_oid, 'hstore[]'::regtype::oid AS hstore_arr_oid",
        ), callback)

        return future

    def register_json(self, globally=False, loads=None):
        """
        Create and register typecasters converting ``json`` type to Python objects.

        More information on the json datatype can be found on the Psycopg2 |regjsondoc|_.

        :param boolean globally:
            Register the adapter globally, not only on this connection.
        :param function loads:
            The function used to parse the data into a Python object.  If ``None``
            use ``json.loads()``, where ``json`` is the module chosen according to
            the Python version.  See psycopg2.extra docs.

        Returns future that resolves to ``None``.

        .. |regjsondoc| replace:: documentation

        .. _regjsondoc: http://initd.org/psycopg/docs/extras.html#json-adaptation
        """
        future = Future()
        registrator = partial(_psy_register_json, None, globally, loads)
        callback = partial(self._register, future, registrator)
        self.ioloop.add_future(self.execute(
            "SELECT 'json'::regtype::oid AS json_oid, 'json[]'::regtype::oid AS json_arr_oid"
        ), callback)

        return future

    @property
    def closed(self):
        """
        Indicates whether the connection is closed or not.
        """
        # 0 = open, 1 = closed, 2 = 'something horrible happened'
        return self.connection.closed > 0 if self.connection else True

    def close(self):
        """
        Closes the connection.

        **NOTE:** This is a synchronous method.
        """
        if self.connection:
            self.connection.close()


def connect(*args, **kwargs):
    """
    Connection factory.
    See :py:meth:`momoko.Connection` for documentation about the parameters.

    Returns future that resolves to :py:meth:`momoko.Connection` object or raises exception.
    """
    return Connection(*args, **kwargs).connect()
