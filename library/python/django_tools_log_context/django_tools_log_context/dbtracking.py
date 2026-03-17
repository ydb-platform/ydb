# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import logging
import six
import traceback
from django.db import connections
from django.db.backends import utils
from django.db.backends.base import base
from django.conf import settings
from django.utils.encoding import force_str
from monotonic import monotonic
from ylog.context import log_context
from .state import get_state


def wrap_cursor(connection):
    if not hasattr(connection, '_orig_cursor'):
        connection._orig_cursor = connection.cursor
        def cursor():
            return YlogCursorWrapper(connection._orig_cursor(), connection)
        connection.cursor = cursor


def unwrap_cursor(connection):
    if hasattr(connection, '_orig_cursor'):
        del connection._orig_cursor
        del connection.cursor


def enable_instrumentation():
    for connection in connections.all():
        wrap_cursor(connection)


def disable_instrumentation():
    for connection in connections.all():
       unwrap_cursor(connection)


class YlogCursorWrapper(object):
    def __init__(self, cursor, db):
        self.cursor = cursor
        self.db = db
        self.state = get_state()
        self.logger = logging.getLogger(__name__)

    def _quote_expr(self, element):
        if isinstance(element, six.string_types):
            return "'%s'" % force_str(element, errors='ignore').replace("'", "''")
        else:
            return repr(element)

    def _quote_params(self, params):
        if not params:
            return params
        if isinstance(params, dict):
            return {
                key: self._quote_expr(value)
                for key, value in params.items()
            }
        return list(map(self._quote_expr, params))

    def _record(self, method, raw_sql, params):
        start_time = monotonic()
        try:
            return method(raw_sql, params)
        finally:
            duration = (monotonic() - start_time) * 1000
            conn = self.db.connection
            vendor = getattr(self.db, 'vendor', 'unknown')

            self.state.add_sql_time(duration)

            if settings.TOOLS_LOG_CONTEXT_ENABLE_DB_TRACKING and self.state.is_enabled():
                prefix = 'select' if not isinstance(raw_sql, bytes) else b'select'
                is_select = raw_sql.lower().strip().startswith(prefix)
                if is_select or settings.TOOLS_LOG_CONTEXT_ALWAYS_SUBSTITUTE_SQL_PARAMS:
                    sql = self.db.ops.last_executed_query(
                        self.cursor, raw_sql, self._quote_params(params))
                else:
                    sql = raw_sql
                profiling = {
                    'is_select': is_select,
                    'is_slow': duration > settings.TOOLS_LOG_CONTEXT_SQL_WARNING_THRESHOLD,
                    'vendor': 'database',
                }

                if vendor == 'postgresql':
                    # If an erroneous query was ran on the connection, it might
                    # be in a state where checking isolation_level raises an
                    # exception.
                    try:
                        iso_level = conn.isolation_level
                    except conn.InternalError:
                        iso_level = 'unknown'
                    profiling.update({
                        'encoding': conn.encoding,
                        'iso_level': iso_level,
                        'trans_status': conn.get_transaction_status(),
                    })

                if settings.TOOLS_LOG_CONTEXT_QUERY_TO_ANALYSE:
                    profiling['query_to_analyse'] = raw_sql

                if settings.TOOLS_LOG_CONTEXT_ENABLE_STACKTRACES:
                    profiling['stacktrace'] = ''.join(i.decode('utf-8') if six.PY2 else i for i in traceback.format_stack()[:-1])

                with log_context(execution_time=int(duration), profiling=profiling):
                    self.logger.info('(%.3f msec) %s', duration, sql)

    def execute(self, sql, params=None):
        return self._record(self.cursor.execute, sql, params)

    def executemany(self, sql, param_list):
        return self._record(self.cursor.executemany, sql, param_list)

    def __getattr__(self, attr):
        return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
