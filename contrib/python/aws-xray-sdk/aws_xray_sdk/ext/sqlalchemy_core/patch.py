import logging
import sys
from urllib.parse import urlparse, uses_netloc, quote_plus

import wrapt
from sqlalchemy.sql.expression import ClauseElement

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.patcher import _PATCHED_MODULES
from aws_xray_sdk.core.utils import stacktrace
from aws_xray_sdk.ext.util import unwrap


def _sql_meta(engine_instance, args):
    try:
        metadata = {}
        # Workaround for https://github.com/sqlalchemy/sqlalchemy/issues/10662
        # sqlalchemy.engine.url.URL's __repr__ does not url encode username nor password.
        # This will continue to work once sqlalchemy fixes the bug.
        sa_url = engine_instance.engine.url
        username = sa_url.username
        sa_url = sa_url._replace(username=None, password=None)
        url = urlparse(str(sa_url))
        name = url.netloc
        if username:
            # Restore url encoded username
            quoted_username = quote_plus(username)
            url = url._replace(netloc='{}@{}'.format(quoted_username, url.netloc))
        # Add Scheme to uses_netloc or // will be missing from url.
        uses_netloc.append(url.scheme)
        metadata['url'] = url.geturl()
        metadata['user'] = url.username
        metadata['database_type'] = engine_instance.engine.name
        try:
            version = getattr(engine_instance.dialect, '{}_version'.format(engine_instance.engine.driver))
            version_str = '.'.join(map(str, version))
            metadata['driver_version'] = "{}-{}".format(engine_instance.engine.driver, version_str)
        except AttributeError:
            metadata['driver_version'] = engine_instance.engine.driver
        if engine_instance.dialect.server_version_info is not None:
            metadata['database_version'] = '.'.join(map(str, engine_instance.dialect.server_version_info))
        if xray_recorder.stream_sql:
            try:
                if isinstance(args[0], ClauseElement):
                    metadata['sanitized_query'] = str(args[0].compile(engine_instance.engine))
                else:
                    metadata['sanitized_query'] = str(args[0])
            except Exception:
                logging.getLogger(__name__).exception('Error getting the sanitized query')
    except Exception:
        metadata = None
        name = None
        logging.getLogger(__name__).exception('Error parsing sql metadata.')
    return name, metadata


def _xray_traced_sqlalchemy_execute(wrapped, instance, args, kwargs):
    return _process_request(wrapped, instance, args, kwargs)


def _xray_traced_sqlalchemy_session(wrapped, instance, args, kwargs):
    return _process_request(wrapped, instance.bind, args, kwargs)


def _process_request(wrapped, engine_instance, args, kwargs):
    name, sql = _sql_meta(engine_instance, args)
    if sql is not None:
        subsegment = xray_recorder.begin_subsegment(name, namespace='remote')
    else:
        subsegment = None
    try:
        res = wrapped(*args, **kwargs)
    except Exception:
        if subsegment is not None:
            exception = sys.exc_info()[1]
            stack = stacktrace.get_stacktrace(limit=xray_recorder._max_trace_back)
            subsegment.add_exception(exception, stack)
        raise
    finally:
        if subsegment is not None:
            subsegment.set_sql(sql)
            xray_recorder.end_subsegment()
    return res


def patch():
    wrapt.wrap_function_wrapper(
        'sqlalchemy.engine.base',
        'Connection.execute',
        _xray_traced_sqlalchemy_execute
    )

    wrapt.wrap_function_wrapper(
        'sqlalchemy.orm.session',
        'Session.execute',
        _xray_traced_sqlalchemy_session
    )


def unpatch():
    """
    Unpatch any previously patched modules.
    This operation is idempotent.
    """
    _PATCHED_MODULES.discard('sqlalchemy_core')
    import sqlalchemy
    unwrap(sqlalchemy.engine.base.Connection, 'execute')
    unwrap(sqlalchemy.orm.session.Session, 'execute')
