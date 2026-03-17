import wrapt
import sqlite3

from aws_xray_sdk.ext.dbapi2 import XRayTracedConn


def patch():

    wrapt.wrap_function_wrapper(
        'sqlite3',
        'connect',
        _xray_traced_connect
    )


def _xray_traced_connect(wrapped, instance, args, kwargs):

    conn = wrapped(*args, **kwargs)

    meta = {}
    meta['name'] = args[0]
    meta['database_version'] = sqlite3.sqlite_version

    traced_conn = XRayTracedSQLite(conn, meta)

    return traced_conn


class XRayTracedSQLite(XRayTracedConn):

    def execute(self, *args, **kwargs):
        return self.cursor().execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        return self.cursor().executemany(*args, **kwargs)
