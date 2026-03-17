import inspect
import itertools
import time
import warnings

import asyncpg
from sqlalchemy import util, exc, sql
from sqlalchemy.dialects.postgresql import (  # noqa: F401
    ARRAY,
    CreateEnumType,
    DropEnumType,
    JSON,
    JSONB,
    json,
)
from sqlalchemy.dialects.postgresql.base import (
    ENUM,
    PGCompiler,
    PGDialect,
    PGExecutionContext,
)
from sqlalchemy.sql import sqltypes
from pyscopg2.asyncpg import PoolManager

from . import base

try:
    import click
except ImportError:
    click = None
JSON_COLTYPE = 114
JSONB_COLTYPE = 3802


class AsyncpgDBAPI(base.BaseDBAPI):
    Error = asyncpg.PostgresError, asyncpg.InterfaceError


class AsyncpgCompiler(PGCompiler):
    @property
    def bindtemplate(self):
        return self._bindtemplate

    @bindtemplate.setter
    def bindtemplate(self, val):
        # noinspection PyAttributeOutsideInit
        self._bindtemplate = val.replace(":", "$")

    def _apply_numbered_params(self):
        if hasattr(self, "string"):
            return super()._apply_numbered_params()


# noinspection PyAbstractClass
class AsyncpgExecutionContext(base.ExecutionContextOverride, PGExecutionContext):
    async def _execute_scalar(self, stmt, type_):
        conn = self.root_connection
        if (
            isinstance(stmt, util.text_type)
            and not self.dialect.supports_unicode_statements
        ):
            stmt = self.dialect._encoder(stmt)[0]

        if self.dialect.positional:
            default_params = self.dialect.execute_sequence_format()
        else:
            default_params = {}

        conn._cursor_execute(self.cursor, stmt, default_params, context=self)
        r = await self.cursor.async_execute(stmt, None, default_params, 1)
        r = r[0][0]
        if type_ is not None:
            # apply type post processors to the result
            proc = type_._cached_result_processor(
                self.dialect, self.cursor.description[0][1]
            )
            if proc:
                return proc(r)
        return r


class AsyncpgIterator:
    def __init__(self, context, iterator):
        self._context = context
        self._iterator = iterator

    async def __anext__(self):
        row = await self._iterator.__anext__()
        return self._context.process_rows([row])[0]


class AsyncpgCursor(base.Cursor):
    def __init__(self, context, cursor):
        self._context = context
        self._cursor = cursor

    async def many(self, n, *, timeout=base.DEFAULT):
        if timeout is base.DEFAULT:
            timeout = self._context.timeout
        rows = await self._cursor.fetch(n, timeout=timeout)
        return self._context.process_rows(rows)

    async def next(self, *, timeout=base.DEFAULT):
        if timeout is base.DEFAULT:
            timeout = self._context.timeout
        row = await self._cursor.fetchrow(timeout=timeout)
        if not row:
            return None
        return self._context.process_rows([row])[0]

    async def forward(self, n, *, timeout=base.DEFAULT):
        if timeout is base.DEFAULT:
            timeout = self._context.timeout
        await self._cursor.forward(n, timeout=timeout)


class PreparedStatement(base.PreparedStatement):
    def __init__(self, prepared, clause=None):
        super().__init__(clause)
        self._prepared = prepared

    def _get_iterator(self, *params, **kwargs):
        return AsyncpgIterator(
            self.context, self._prepared.cursor(*params, **kwargs).__aiter__()
        )

    async def _get_cursor(self, *params, **kwargs):
        iterator = await self._prepared.cursor(*params, **kwargs)
        return AsyncpgCursor(self.context, iterator)

    async def _execute(self, params, one):
        if one:
            rv = await self._prepared.fetchrow(*params)
            if rv is None:
                rv = []
            else:
                rv = [rv]
        else:
            rv = await self._prepared.fetch(*params)
        return self._prepared.get_statusmsg(), rv


class DBAPICursor(base.DBAPICursor):
    def __init__(self, dbapi_conn):
        self._conn = dbapi_conn
        self._attributes = None
        self._status = None

    async def prepare(self, context, clause=None):
        timeout = context.timeout
        if timeout is None:
            conn = await self._conn.acquire(timeout=timeout)
        else:
            before = time.monotonic()
            conn = await self._conn.acquire(timeout=timeout)
            after = time.monotonic()
            timeout -= after - before
        prepared = await conn.prepare(context.statement, timeout=timeout)
        try:
            self._attributes = prepared.get_attributes()
        except TypeError:  # asyncpg <= 0.12.0
            self._attributes = []
        rv = PreparedStatement(prepared, clause)
        rv.context = context
        return rv

    async def async_execute(self, query, timeout, args, limit=0, many=False):
        if timeout is None:
            conn = await self._conn.acquire(timeout=timeout)
        else:
            before = time.monotonic()
            conn = await self._conn.acquire(timeout=timeout)
            after = time.monotonic()
            timeout -= after - before
        _protocol = getattr(conn, "_protocol")
        timeout = getattr(_protocol, "_get_timeout")(timeout)

        def executor(state, timeout_):
            if many:
                return _protocol.bind_execute_many(state, args, "", timeout_, return_rows=False)
            else:
                return _protocol.bind_execute(state, args, "", limit, True, timeout_)

        with getattr(conn, "_stmt_exclusive_section"):
            result, stmt = await getattr(conn, "_do_execute")(query, executor, timeout)
            try:
                self._attributes = getattr(stmt, "_get_attributes")()
            except TypeError:  # asyncpg <= 0.12.0
                self._attributes = []
            if not many:
                result, self._status = result[:2]
            return result

    @property
    def description(self):
        return [((a[0], a[1][0]) + (None,) * 5) for a in self._attributes]

    def get_statusmsg(self):
        return self._status.decode()


class Pool(base.Pool):
    def __init__(self, url, loop, **kwargs):
        self._url = url
        self._loop = loop
        self._kwargs = kwargs
        self._pool = None

    async def _init(self):
        args = self._kwargs.copy()
        args.update(
            loop=self._loop,
            host=self._url.host,
            port=self._url.port,
            user=self._url.username,
            database=self._url.database,
            password=self._url.password,
        )
        self._pool = await asyncpg.create_pool(**args)
        return self

    def __await__(self):
        return self._init().__await__()

    @property
    def raw_pool(self):
        return self._pool

    async def acquire(self, *, timeout=None, read_only=False):
        return await self._pool.acquire(timeout=timeout)

    async def release(self, conn):
        await self._pool.release(conn)

    async def close(self):
        await self._pool.close()

    def repr(self, color):
        if color and not click:
            warnings.warn("Install click to get colorful repr.", ImportWarning)

        if color and click:
            # noinspection PyProtectedMember
            return "<{classname} max={max} min={min} cur={cur} use={use}>".format(
                classname=click.style(
                    self._pool.__class__.__module__
                    + "."
                    + self._pool.__class__.__name__,
                    fg="green",
                ),
                max=click.style(repr(self._pool._maxsize), fg="cyan"),
                min=click.style(repr(self._pool._minsize), fg="cyan"),
                cur=click.style(
                    repr(
                        len(
                            [
                                0
                                for con in self._pool._holders
                                if con._con and not con._con.is_closed()
                            ]
                        )
                    ),
                    fg="cyan",
                ),
                use=click.style(
                    repr(len([0 for con in self._pool._holders if con._in_use])),
                    fg="cyan",
                ),
            )
        else:
            # noinspection PyProtectedMember
            return "<{classname} max={max} min={min} cur={cur} use={use}>".format(
                classname=self._pool.__class__.__module__
                + "."
                + self._pool.__class__.__name__,
                max=self._pool._maxsize,
                min=self._pool._minsize,
                cur=len(
                    [
                        0
                        for con in self._pool._holders
                        if con._con and not con._con.is_closed()
                    ]
                ),
                use=len([0 for con in self._pool._holders if con._in_use]),
            )


class NullPool(base.Pool):
    # TODO: generic NullPool, abstracting connection part
    def __init__(self, url, loop, **kwargs):
        self._loop = loop
        self._kwargs = dict()
        for k in inspect.getfullargspec(asyncpg.connect).kwonlyargs:
            if k in kwargs:
                self._kwargs[k] = kwargs[k]
        self._kwargs.update(
            dict(
                host=url.host,
                port=url.port,
                user=url.username,
                database=url.database,
                password=url.password,
            )
        )
        self._count = 0

    def __await__(self):
        async def return_self():
            return self

        return return_self().__await__()

    @property
    def raw_pool(self):
        return self

    async def acquire(self, *, timeout=None, read_only=False):
        args = self._kwargs.copy()
        if timeout is not None:
            args.update(timeout=timeout)
        rv = await asyncpg.connect(loop=self._loop, **args)
        self._count += 1
        return rv

    async def release(self, conn):
        self._count -= 1
        await conn.close()

    async def close(self):
        pass

    def repr(self, color):
        if color and not click:
            warnings.warn("Install click to get colorful repr.", ImportWarning)

        if color and click:
            return "<{classname} max={max} min={min} cur={cur} use={cur}>".format(
                classname=click.style("NullPool", fg="green"),
                max=click.style("inf", fg="cyan"),
                min=click.style("0", fg="cyan"),
                cur=click.style(repr(self._count), fg="cyan"),
            )
        else:
            return "<NullPool max=inf min=0 cur={cur} use={cur}>".format(
                cur=self._count
            )


class Transaction(base.Transaction):
    def __init__(self, tx):
        self._tx = tx

    @property
    def raw_transaction(self):
        return self._tx

    async def begin(self):
        await self._tx.start()

    async def commit(self):
        await self._tx.commit()

    async def rollback(self):
        await self._tx.rollback()


class AsyncEnum(ENUM):
    async def create_async(self, bind=None, checkfirst=True):
        if not checkfirst or not await bind.dialect.has_type(
            bind, self.name, schema=self.schema
        ):
            await bind.status(CreateEnumType(self))

    async def drop_async(self, bind=None, checkfirst=True):
        if not checkfirst or await bind.dialect.has_type(
            bind, self.name, schema=self.schema
        ):
            await bind.status(DropEnumType(self))

    async def _on_table_create_async(self, target, bind, checkfirst=False, **kw):
        if (
            checkfirst
            or (not self.metadata and not kw.get("_is_metadata_operation", False))
            and not self._check_for_name_in_memos(checkfirst, kw)
        ):
            await self.create_async(bind=bind, checkfirst=checkfirst)

    async def _on_table_drop_async(self, target, bind, checkfirst=False, **kw):
        if (
            not self.metadata
            and not kw.get("_is_metadata_operation", False)
            and not self._check_for_name_in_memos(checkfirst, kw)
        ):
            await self.drop_async(bind=bind, checkfirst=checkfirst)

    async def _on_metadata_create_async(self, target, bind, checkfirst=False, **kw):
        if not self._check_for_name_in_memos(checkfirst, kw):
            await self.create_async(bind=bind, checkfirst=checkfirst)

    async def _on_metadata_drop_async(self, target, bind, checkfirst=False, **kw):
        if not self._check_for_name_in_memos(checkfirst, kw):
            await self.drop_async(bind=bind, checkfirst=checkfirst)


class GinoNullType(sqltypes.NullType):
    def result_processor(self, dialect, coltype):
        if coltype == JSON_COLTYPE:
            return JSON().result_processor(dialect, coltype)
        if coltype == JSONB_COLTYPE:
            return JSONB().result_processor(dialect, coltype)
        return super().result_processor(dialect, coltype)


class AsyncpgJSONPathType(json.JSONPathType):
    def bind_processor(self, dialect):
        super_proc = self.string_bind_processor(dialect)

        def process(value):
            assert isinstance(value, util.collections_abc.Sequence)
            if super_proc:
                return [super_proc(util.text_type(elem)) for elem in value]
            else:
                return [util.text_type(elem) for elem in value]

        return process


# noinspection PyAbstractClass
class AsyncpgDialect(PGDialect, base.AsyncDialectMixin):
    driver = "asyncpg"
    supports_native_decimal = True
    dbapi_class = AsyncpgDBAPI
    statement_compiler = AsyncpgCompiler
    execution_ctx_cls = AsyncpgExecutionContext
    cursor_cls = DBAPICursor
    init_kwargs = set(
        itertools.chain(
            *[
                inspect.getfullargspec(f).kwonlydefaults.keys()
                for f in [asyncpg.create_pool, asyncpg.connect]
            ]
        )
    )
    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            ENUM: AsyncEnum,
            sqltypes.Enum: AsyncEnum,
            sqltypes.NullType: GinoNullType,
            sqltypes.JSON.JSONPathType: AsyncpgJSONPathType,
        },
    )

    def __init__(self, *args, **kwargs):
        self._pool_kwargs = {}
        for k in self.init_kwargs:
            if k in kwargs:
                self._pool_kwargs[k] = kwargs.pop(k)
        super().__init__(*args, **kwargs)
        self._init_mixin()

    async def init_pool(self, url, loop, pool_class=None):
        if pool_class is None:
            pool_class = Pool
        return await pool_class(url, loop, init=self.on_connect(), **self._pool_kwargs)

    # noinspection PyMethodMayBeStatic
    def transaction(self, raw_conn, args, kwargs):
        return Transaction(raw_conn.transaction(*args, **kwargs))

    def on_connect(self):
        if self.isolation_level is not None:

            async def connect(conn):
                await self.set_isolation_level(conn, self.isolation_level)

            return connect
        else:
            return None

    async def set_isolation_level(self, connection, level):
        """
        Given an asyncpg connection, set its isolation level.

        """
        level = level.replace("_", " ")
        if level not in self._isolation_lookup:
            raise exc.ArgumentError(
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s"
                % (level, self.name, ", ".join(self._isolation_lookup))
            )
        await connection.execute(
            "SET SESSION CHARACTERISTICS AS TRANSACTION " "ISOLATION LEVEL %s" % level
        )
        await connection.execute("COMMIT")

    async def get_isolation_level(self, connection):
        """
        Given an asyncpg connection, return its isolation level.

        """
        val = await connection.fetchval("show transaction isolation level")
        return val.upper()

    async def has_schema(self, connection, schema):
        row = await connection.first(
            sql.text(
                "select nspname from pg_namespace " "where lower(nspname)=:schema"
            ).bindparams(
                sql.bindparam(
                    "schema", util.text_type(schema.lower()), type_=sqltypes.Unicode,
                )
            )
        )

        return bool(row)

    async def has_table(self, connection, table_name, schema=None):
        # seems like case gets folded in pg_class...
        if schema is None:
            row = await connection.first(
                sql.text(
                    "select relname from pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where "
                    "pg_catalog.pg_table_is_visible(c.oid) "
                    "and relname=:name"
                ).bindparams(
                    sql.bindparam(
                        "name", util.text_type(table_name), type_=sqltypes.Unicode
                    ),
                )
            )
        else:
            row = await connection.first(
                sql.text(
                    "select relname from pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where n.nspname=:schema and "
                    "relname=:name"
                ).bindparams(
                    sql.bindparam(
                        "name", util.text_type(table_name), type_=sqltypes.Unicode,
                    ),
                    sql.bindparam(
                        "schema", util.text_type(schema), type_=sqltypes.Unicode,
                    ),
                )
            )
        return bool(row)

    async def has_sequence(self, connection, sequence_name, schema=None):
        if schema is None:
            row = await connection.first(
                sql.text(
                    "SELECT relname FROM pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where relkind='S' and "
                    "n.nspname=current_schema() "
                    "and relname=:name"
                ).bindparams(
                    sql.bindparam(
                        "name", util.text_type(sequence_name), type_=sqltypes.Unicode,
                    )
                )
            )
        else:
            row = await connection.first(
                sql.text(
                    "SELECT relname FROM pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where relkind='S' and "
                    "n.nspname=:schema and relname=:name"
                ).bindparams(
                    sql.bindparam(
                        "name", util.text_type(sequence_name), type_=sqltypes.Unicode,
                    ),
                    sql.bindparam(
                        "schema", util.text_type(schema), type_=sqltypes.Unicode,
                    ),
                )
            )

        return bool(row)

    async def has_type(self, connection, type_name, schema=None):
        if schema is not None:
            query = """
            SELECT EXISTS (
                SELECT * FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n
                WHERE t.typnamespace = n.oid
                AND t.typname = :typname
                AND n.nspname = :nspname
                )
                """
            query = sql.text(query)
        else:
            query = """
            SELECT EXISTS (
                SELECT * FROM pg_catalog.pg_type t
                WHERE t.typname = :typname
                AND pg_type_is_visible(t.oid)
                )
                """
            query = sql.text(query)
        query = query.bindparams(
            sql.bindparam(
                "typname", util.text_type(type_name), type_=sqltypes.Unicode,
            ),
        )
        if schema is not None:
            query = query.bindparams(
                sql.bindparam(
                    "nspname", util.text_type(schema), type_=sqltypes.Unicode,
                ),
            )
        return bool(await connection.scalar(query))


class Pyscopg2Pool(Pool):
    async def _init(self):
        pool_config = {
            'ssl': self._kwargs.get('ssl', False),
            'max_size': self._kwargs.get('max_size', 10),
            'min_size': self._kwargs.get('min_size', 5),
            'loop': self._loop,
            'init': None,
            'fallback_master': self._kwargs.get('fallback_master', True),
        }
        host_str = ','.join([f'{host}:{self._url.port}' for host in self._url.host.split(',')])
        dsn = f'postgresql://{self._url.username}:{self._url.password}@{host_str}/{self._url.database}'

        self._pool = await self.init_pool(dsn, pool_config)
        return self

    async def acquire(self, *, timeout=None, read_only=False):
        return await self._pool.acquire(timeout=timeout, read_only=read_only)

    async def init_pool(self, dsn, pool_config) -> PoolManager:
        return PoolManager(dsn, fallback_master=pool_config.pop('fallback_master'), pool_factory_kwargs=pool_config)

    def repr(self, color=None):
        return "<{classname} max={max} min={min} cur={cur}>".format(
            classname=self._pool.__class__.__module__
                      + "."
                      + self._pool.__class__.__name__,
            max=self._pool._pool_factory_kwargs['max_size'],
            min=self._pool._pool_factory_kwargs['min_size'],
            cur=len(self._pool._pools)
        )
