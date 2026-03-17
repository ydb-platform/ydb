# coding=utf-8

import logging
import sys

try:
    import psycopg2.extensions
except ImportError as e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading psycopg2 module: %s" % e)

try:
    from gevent.lock import Semaphore
except ImportError:
    from eventlet.semaphore import Semaphore

from django.db.backends.postgresql.base import DatabaseWrapper as OriginalDatabaseWrapper

from . import creation, psycopg2_pool

logger = logging.getLogger('django.geventpool')

connection_pools = {}
connection_pools_lock = Semaphore(value=1)


class DatabaseWrapperMixin(object):
    def __init__(self, *args, **kwargs):
        self._pool = None
        super(DatabaseWrapperMixin, self).__init__(*args, **kwargs)
        self.creation = creation.DatabaseCreation(self)

    @property
    def pool(self):
        if self._pool is not None:
            return self._pool
        connection_pools_lock.acquire()
        if self.alias not in connection_pools:
            self._pool = psycopg2_pool.PostgresConnectionPool(
                **self.get_connection_params())
            connection_pools[self.alias] = self._pool
        else:
            self._pool = connection_pools[self.alias]
        connection_pools_lock.release()
        return self._pool

    def get_new_connection(self, conn_params):
        if self.connection is None:
            self.connection = self.pool.get()
            self.closed_in_transaction = False
        return self.connection

    def get_connection_params(self):
        conn_params = super(DatabaseWrapperMixin, self).get_connection_params()
        for attr in ['MAX_CONNS', 'REUSE_CONNS']:
            if attr in self.settings_dict['OPTIONS']:
                conn_params[attr] = self.settings_dict['OPTIONS'][attr]
        return conn_params

    def close(self):
        self.validate_thread_sharing()
        if self.closed_in_transaction or self.connection is None:
            return  # no need to close anything
        try:
            self._close()
        except:
            # In some cases (database restart, network connection lost etc...)
            # the connection to the database is lost without giving Django a
            # notification. If we don't set self.connection to None, the error
            # will occur at every request.
            self.connection = None
            logger.warning(
                'psycopg2 error while closing the connection.',
                exc_info=sys.exc_info())
            raise
        finally:
            self.set_clean()

    def close_if_unusable_or_obsolete(self):
        # Always close the connection because it's not (usually) really being closed.
        self.close()

    def _close(self):
        if self.connection.closed:
            self.pool.closeall()
        else:
            if self.connection.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_INTRANS:
                self.connection.rollback()
                self.connection.autocommit = True
            with self.wrap_database_errors:
                self.pool.put(self.connection)
        self.connection = None

    def closeall(self):
        for pool in connection_pools.values():
            pool.closeall()

    def set_clean(self):
        if self.in_atomic_block:
            self.closed_in_transaction = True
            self.needs_rollback = True


class DatabaseWrapper(DatabaseWrapperMixin, OriginalDatabaseWrapper):
    pass
