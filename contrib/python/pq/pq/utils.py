# -*- coding: utf-8 -*-
import re
import sys

from contextlib import contextmanager
from weakref import WeakKeyDictionary
from functools import wraps
from textwrap import dedent
from logging import getLogger
from datetime import datetime, timedelta
from hashlib import sha256
from base64 import b32encode

PY2 = bool(sys.version_info[0] == 2)


logger = getLogger("pq")

_re_format = re.compile(r'%\(([a-z]+)\)[a-z]')
_re_timedelta = re.compile(r'(\d+)([smhd])')
_re_identifier = re.compile(r'^[_a-z][_a-z0-9]*$', re.IGNORECASE)
_timedelta_table = dict(s='seconds', m='minutes', h='hours', d='days')
_statements = WeakKeyDictionary()
_notices = WeakKeyDictionary()


def prepared(f):
    """Decorator that prepares an SQL statement.

    The statement must be present in the function's docstring,
    indented with four spaces, i.e.

        SELECT * FROM %(name)s WHERE value > $1

    The formatting variables are pulled from the function bind's
    instance dictionary.

    The placeholders (with dollar sign) are positional arguments to
    the decorated function. These are automatically applied.
    """

    d = dedent(f.__doc__.split('\n', 1)[1])
    query = "\n".join(
        line for line in d.split('\n')
        if line.startswith("    ")
    ) or d

    arg_count = query.count("$")
    if arg_count:
        fargs = "(" + ", ".join(["%s"] * arg_count) + ")"
    else:
        fargs = None

    fname = f.__name__
    for m in _re_format.finditer(query):
        fname += "_%%(%s)s" % m.group(1)

    @wraps(f)
    def wrapper(self, cursor, *args):
        conn = cursor.connection
        name = fname % self.__dict__
        identifier = unique_identifier(name)
        key = "_prepared_%s" % name

        try:
            prepared = _statements[conn]
        except KeyError:
            prepared = _statements[conn] = set()

        if key not in prepared:
            prepared.add(key)
            d = self.__dict__.copy()
            cursor.execute("PREPARE %s AS\n%s" % (identifier, query), d)

        params = args[:arg_count]
        statement = "EXECUTE " + identifier
        if fargs is not None:
            statement += " " + fargs
        cursor.execute(statement, params or None)
        return f(self, cursor, *args[arg_count:])

    return wrapper


def convert_time_spec(spec):
    if spec is None:
        return

    if isinstance(spec, str) or (PY2 and isinstance(spec, unicode)):
        m = _re_timedelta.match(spec)
        if m is None:
            raise ValueError(spec)

        g = m.groups()
        val = int(g[0])
        key = _timedelta_table.get(g[1])
        spec = timedelta(**{key: val})

    if isinstance(spec, timedelta):
        spec = datetime.utcnow() + spec

    return spec


def utc_format(dt):
    dt = dt.replace(tzinfo=None) - (
        dt.tzinfo.utcoffset(dt) if dt.tzinfo else timedelta()
    )
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@contextmanager
def transaction(conn, **kwargs):
    """Context manager.

    Execute statements within a transaction.

    >>> with transaction(conn) as cursor:
    ...     cursor.execute(...)
    ...     return cursor.fetchall()

    """

    cursor = conn.cursor(**kwargs)

    try:
        yield cursor
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        cursor.close()

    try:
        seen = _notices[cursor.connection]
    except KeyError:
        seen = _notices[cursor.connection] = set()

    for notice in cursor.connection.notices:
        if notice in seen:
            continue
        seen.add(notice)
        logger.warning(notice)


class Literal(object):
    """String wrapper to make a query parameter literal."""

    __slots__ = "s",

    def __init__(self, s):
        self.s = str(s).encode('utf-8')

    def __conform__(self, quote):
        return self

    def __str__(self):
        return self.s.decode('utf-8')

    @classmethod
    def mro(cls):
        return (object, )

    def getquoted(self):
        return self.s


def unique_identifier(name, prefix='pq_'):
    """Create a unique identifier from a name and a (non-empty) prefix.
    ValueError is raised if prefix is not a valid postgresql identifier of maximum length 11 characters.
    """

    if len(prefix) > 11 or not _re_identifier.match(prefix):
        raise ValueError(prefix)

    # b32encode of 256 bits (32 bytes) is 56 bytes, but the final 4 are always padding ('====')
    b32_digest = b32encode(sha256(name.encode()).digest())[:52]

    return prefix + b32_digest.decode()
