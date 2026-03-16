import re
import types
from urllib.parse import urlparse, uses_netloc

from sqlalchemy.engine.base import Connection

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.ext.util import strip_url


def decorate_all_functions(function_decorator):
    def decorator(cls):
        for c in cls.__bases__:
            for name, obj in vars(c).items():
                if name.startswith("_"):
                    continue
                if isinstance(obj, types.FunctionType):
                    try:
                        obj = obj.__func__  # unwrap Python 2 unbound method
                    except AttributeError:
                        pass  # not needed in Python 3
                    setattr(c, name, function_decorator(c, obj))
        return cls
    return decorator


def xray_on_call(cls, func):
    def wrapper(*args, **kw):
        from ..query import XRayQuery, XRaySession
        try:
            from ...flask_sqlalchemy.query import XRaySignallingSession
            has_sql_alchemy = True
        except ImportError:
            has_sql_alchemy = False

        class_name = str(cls.__module__)
        c = xray_recorder._context
        sql = None
        subsegment = None
        if class_name == "sqlalchemy.orm.session":
            for arg in args:
                if isinstance(arg, XRaySession):
                    sql = parse_bind(arg.bind)
                if has_sql_alchemy and isinstance(arg, XRaySignallingSession):
                    sql = parse_bind(arg.bind)
        if class_name == 'sqlalchemy.orm.query':
            for arg in args:
                if isinstance(arg, XRayQuery):
                    try:
                        sql = parse_bind(arg.session.bind)
                        if xray_recorder.stream_sql:
                            sql['sanitized_query'] = str(arg)
                    except Exception:
                        sql = None
        if sql is not None:
            if getattr(c._local, 'entities', None) is not None:
                # Strip URL of ? and following text
                sub_name = strip_url(sql['url'])
                subsegment = xray_recorder.begin_subsegment(sub_name, namespace='remote')
            else:
                subsegment = None

        try:
            res = func(*args, **kw)
        finally:
            if subsegment is not None:
                subsegment.set_sql(sql)
                subsegment.put_annotation("sqlalchemy", class_name+'.'+func.__name__)
                xray_recorder.end_subsegment()
        return res
    return wrapper
# URL Parse output
# scheme	0	URL scheme specifier	scheme parameter
# netloc	1	Network location part	empty string
# path	2	Hierarchical path	empty string
# query	3	Query component	empty string
# fragment	4	Fragment identifier	empty string
# username	 	User name	None
# password	 	Password	None
# hostname	 	Host name (lower case)	None
# port	 	Port number as integer, if present	None
#
# XRAY Trace SQL metaData Sample
# "sql" : {
#     "url": "jdbc:postgresql://aawijb5u25wdoy.cpamxznpdoq8.us-west-2.rds.amazonaws.com:5432/ebdb",
#     "preparation": "statement",
#     "database_type": "PostgreSQL",
#     "database_version": "9.5.4",
#     "driver_version": "PostgreSQL 9.4.1211.jre7",
#     "user" : "dbuser",
#     "sanitized_query" : "SELECT  *  FROM  customers  WHERE  customer_id=?;"
#   }
def parse_bind(bind):
    """Parses a connection string and creates SQL trace metadata"""
    if isinstance(bind, Connection):
        engine = bind.engine
    else:
        engine = bind
    m = re.match(r"Engine\((.*?)\)", str(engine))
    if m is not None:
        u = urlparse(m.group(1))
        # Add Scheme to uses_netloc or // will be missing from url.
        uses_netloc.append(u.scheme)
        safe_url = ""
        if u.password is None:
            safe_url = u.geturl()
        else:
            # Strip password from URL
            host_info = u.netloc.rpartition('@')[-1]
            parts = u._replace(netloc='{}@{}'.format(u.username, host_info))
            safe_url = parts.geturl()
        sql = {}
        sql['database_type'] = u.scheme
        sql['url'] = safe_url
        if u.username is not None:
            sql['user'] = "{}".format(u.username)
    return sql
