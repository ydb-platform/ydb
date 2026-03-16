import copy
import logging
import importlib

from django.db import connections

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.ext.dbapi2 import XRayTracedCursor

log = logging.getLogger(__name__)


def patch_db():
    for conn in connections.all():
        module = importlib.import_module(conn.__module__)
        _patch_conn(getattr(module, conn.__class__.__name__))


class DjangoXRayTracedCursor(XRayTracedCursor):
    def execute(self, query, *args, **kwargs):
        if xray_recorder.stream_sql:
            _previous_meta = copy.copy(self._xray_meta)
            self._xray_meta['sanitized_query'] = query
        result = super().execute(query, *args, **kwargs)
        if xray_recorder.stream_sql:
            self._xray_meta = _previous_meta
        return result

    def executemany(self, query, *args, **kwargs):
        if xray_recorder.stream_sql:
            _previous_meta = copy.copy(self._xray_meta)
            self._xray_meta['sanitized_query'] = query
        result = super().executemany(query, *args, **kwargs)
        if xray_recorder.stream_sql:
            self._xray_meta = _previous_meta
        return result

    def callproc(self, proc, args):
        if xray_recorder.stream_sql:
            _previous_meta = copy.copy(self._xray_meta)
            self._xray_meta['sanitized_query'] = proc
        result = super().callproc(proc, args)
        if xray_recorder.stream_sql:
            self._xray_meta = _previous_meta
        return result


def _patch_cursor(cursor_name, conn):
    attr = '_xray_original_{}'.format(cursor_name)

    if hasattr(conn, attr):
        log.debug('django built-in db {} already patched'.format(cursor_name))
        return

    if not hasattr(conn, cursor_name):
        log.debug('django built-in db does not have {}'.format(cursor_name))
        return

    setattr(conn, attr, getattr(conn, cursor_name))

    meta = {}

    if hasattr(conn, 'vendor'):
        meta['database_type'] = conn.vendor

    def cursor(self, *args, **kwargs):

        host = None
        user = None

        if hasattr(self, 'settings_dict'):
            settings = self.settings_dict
            host = settings.get('HOST', None)
            user = settings.get('USER', None)

        if host:
            meta['name'] = host
        if user:
            meta['user'] = user

        original_cursor = getattr(self, attr)(*args, **kwargs)
        return DjangoXRayTracedCursor(original_cursor, meta)

    setattr(conn, cursor_name, cursor)


def _patch_conn(conn):
    _patch_cursor('cursor', conn)
    _patch_cursor('chunked_cursor', conn)
