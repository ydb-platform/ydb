import pymysql
import wrapt

from aws_xray_sdk.ext.dbapi2 import XRayTracedConn
from aws_xray_sdk.core.patcher import _PATCHED_MODULES
from aws_xray_sdk.ext.util import unwrap


def patch():

    wrapt.wrap_function_wrapper(
        'pymysql',
        'connect',
        _xray_traced_connect
    )

    # patch alias
    if hasattr(pymysql, 'Connect'):
        pymysql.Connect = pymysql.connect


def _xray_traced_connect(wrapped, instance, args, kwargs):

    conn = wrapped(*args, **kwargs)
    meta = {
        'database_type': 'MySQL',
        'user': conn.user.decode('utf-8'),
        'driver_version': 'PyMySQL'
    }

    if hasattr(conn, 'server_version'):
        version = sanitize_db_ver(getattr(conn, 'server_version'))
        if version:
            meta['database_version'] = version

    return XRayTracedConn(conn, meta)


def sanitize_db_ver(raw):

    if not raw or not isinstance(raw, tuple):
        return raw

    return '.'.join(str(num) for num in raw)


def unpatch():
    """
    Unpatch any previously patched modules.
    This operation is idempotent.
    """
    _PATCHED_MODULES.discard('pymysql')
    unwrap(pymysql, 'connect')
