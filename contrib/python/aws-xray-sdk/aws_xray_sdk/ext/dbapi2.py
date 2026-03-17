import copy
import wrapt

from aws_xray_sdk.core import xray_recorder


class XRayTracedConn(wrapt.ObjectProxy):

    _xray_meta = None

    def __init__(self, conn, meta={}):

        super().__init__(conn)
        self._xray_meta = meta

    def cursor(self, *args, **kwargs):

        cursor = self.__wrapped__.cursor(*args, **kwargs)
        return XRayTracedCursor(cursor, self._xray_meta)


class XRayTracedCursor(wrapt.ObjectProxy):

    _xray_meta = None

    def __init__(self, cursor, meta={}):

        super().__init__(cursor)
        self._xray_meta = meta

        # we preset database type if db is framework built-in
        if not self._xray_meta.get('database_type'):
            db_type = cursor.__class__.__module__.split('.')[0]
            self._xray_meta['database_type'] = db_type

    def __enter__(self):

        value = self.__wrapped__.__enter__()
        if value is not self.__wrapped__:
            return value
        return self

    @xray_recorder.capture()
    def execute(self, query, *args, **kwargs):

        add_sql_meta(self._xray_meta)
        return self.__wrapped__.execute(query, *args, **kwargs)

    @xray_recorder.capture()
    def executemany(self, query, *args, **kwargs):

        add_sql_meta(self._xray_meta)
        return self.__wrapped__.executemany(query, *args, **kwargs)

    @xray_recorder.capture()
    def callproc(self, proc, args):

        add_sql_meta(self._xray_meta)
        return self.__wrapped__.callproc(proc, args)


def add_sql_meta(meta):

    subsegment = xray_recorder.current_subsegment()

    if not subsegment:
        return

    if meta.get('name', None):
        subsegment.name = meta['name']

    sql_meta = copy.copy(meta)
    if sql_meta.get('name', None):
        del sql_meta['name']
    subsegment.set_sql(sql_meta)
    subsegment.namespace = 'remote'
