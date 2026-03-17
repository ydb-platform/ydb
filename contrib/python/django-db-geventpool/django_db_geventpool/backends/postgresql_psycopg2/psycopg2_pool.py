# coding=utf-8

# this file is a modified version of the psycopg2 used at gevent examples
# to be compatible with django, also checks if
# DB connection is closed and reopen it:
# https://github.com/surfly/gevent/blob/master/examples/psycopg2_pool.py
import logging
import sys
import weakref
logger = logging.getLogger('django.geventpool')

try:
    from gevent import queue
    from gevent.lock import RLock
except ImportError:
    from eventlet import queue
    from ...utils import NullContextRLock as RLock

try:
    from psycopg2 import connect, DatabaseError
    import psycopg2.extras
except ImportError as e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading psycopg2 module: %s" % e)

if sys.version_info[0] >= 3:
    integer_types = int,
else:
    import __builtin__
    integer_types = int, __builtin__.long


class DatabaseConnectionPool(object):
    def __init__(self, maxsize=100, reuse=100):
        if not isinstance(maxsize, integer_types):
            raise TypeError('Expected integer, got %r' % (maxsize,))
        if not isinstance(reuse, integer_types):
            raise TypeError('Expected integer, got %r' % (reuse,))

        # Use a WeakSet here so, even if we fail to discard the connection
        # when it is being closed, or it is closed outside of here, the item
        # will be removed automatically
        self._conns = weakref.WeakSet()
        self.maxsize = maxsize
        self.pool = queue.Queue(maxsize=max(reuse, 1))
        self.lock = RLock()

    @property
    def size(self):
        with self.lock:
            return len(self._conns)

    def get(self):
        try:
            if self.size >= self.maxsize or self.pool.qsize():
                conn = self.pool.get()
            else:
                conn = self.pool.get_nowait()

            try:
                # check connection is still valid
                self.check_usable(conn)
                logger.debug("DB connection reused")
            except DatabaseError:
                logger.debug("DB connection was closed, creating a new one")
                conn = None
        except queue.Empty:
            conn = None
            logger.debug("DB connection queue empty, creating a new one")

        if conn is None:
            try:
                conn = self.create_connection()
            except Exception:
                raise
            else:
                self._conns.add(conn)

        return conn

    def put(self, item):
        try:
            self.pool.put_nowait(item)
            logger.debug("DB connection returned to the pool")
        except queue.Full:
            item.close()
            self._conns.discard(item)

    def closeall(self):
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
            except queue.Empty:
                continue
            try:
                conn.close()
            except Exception:
                continue
            finally:
                self._conns.discard(conn)

        logger.debug("DB connections all closed")


class PostgresConnectionPool(DatabaseConnectionPool):
    def __init__(self, *args, **kwargs):
        self.connect = kwargs.pop('connect', connect)
        self.connection = None
        maxsize = kwargs.pop('MAX_CONNS', 4)
        reuse = kwargs.pop('REUSE_CONNS', maxsize)
        self.args = args
        self.kwargs = kwargs
        super(PostgresConnectionPool, self).__init__(maxsize, reuse)

    def create_connection(self):
        conn = self.connect(*self.args, **self.kwargs)
        # set correct encoding
        conn.set_client_encoding('UTF8')
        psycopg2.extras.register_default_jsonb(conn_or_curs=conn, loads=lambda x: x)
        return conn

    def check_usable(self, connection):
        connection.cursor().execute('SELECT 1')
