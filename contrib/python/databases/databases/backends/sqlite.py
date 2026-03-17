import logging
import sqlite3
import typing
import uuid
from urllib.parse import urlencode

import aiosqlite
from sqlalchemy.dialects.sqlite import pysqlite
from sqlalchemy.engine.cursor import CursorResultMetaData
from sqlalchemy.engine.interfaces import Dialect, ExecutionContext
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.ddl import DDLElement

from databases.backends.common.records import Record, Row, create_column_maps
from databases.core import LOG_EXTRA, DatabaseURL
from databases.interfaces import ConnectionBackend, DatabaseBackend, TransactionBackend

logger = logging.getLogger("databases")


class SQLiteBackend(DatabaseBackend):
    def __init__(
        self, database_url: typing.Union[DatabaseURL, str], **options: typing.Any
    ) -> None:
        self._database_url = DatabaseURL(database_url)
        self._options = options
        self._dialect = pysqlite.dialect(paramstyle="qmark")
        # aiosqlite does not support decimals
        self._dialect.supports_native_decimal = False
        self._pool = SQLitePool(self._database_url, **self._options)

    async def connect(self) -> None:
        ...

    async def disconnect(self) -> None:
        # if it extsis, remove reference to connection to cached in-memory database on disconnect
        if self._pool._memref:
            self._pool._memref = None
        # assert self._pool is not None, "DatabaseBackend is not running"
        # self._pool.close()
        # await self._pool.wait_closed()
        # self._pool = None

    def connection(self) -> "SQLiteConnection":
        return SQLiteConnection(self._pool, self._dialect)


class SQLitePool:
    def __init__(self, url: DatabaseURL, **options: typing.Any) -> None:
        self._database = url.database
        self._memref = None
        # add query params to database connection string
        if url.options:
            self._database += "?" + urlencode(url.options)
        self._options = options

        if url.options and "cache" in url.options:
            # reference to a connection to the cached in-memory database must be held to keep it from being deleted
            self._memref = sqlite3.connect(self._database, **self._options)

    async def acquire(self) -> aiosqlite.Connection:
        connection = aiosqlite.connect(
            database=self._database, isolation_level=None, **self._options
        )
        await connection.__aenter__()
        return connection

    async def release(self, connection: aiosqlite.Connection) -> None:
        await connection.__aexit__(None, None, None)


class CompilationContext:
    def __init__(self, context: ExecutionContext):
        self.context = context


class SQLiteConnection(ConnectionBackend):
    def __init__(self, pool: SQLitePool, dialect: Dialect):
        self._pool = pool
        self._dialect = dialect
        self._connection: typing.Optional[aiosqlite.Connection] = None

    async def acquire(self) -> None:
        assert self._connection is None, "Connection is already acquired"
        self._connection = await self._pool.acquire()

    async def release(self) -> None:
        assert self._connection is not None, "Connection is not acquired"
        await self._pool.release(self._connection)
        self._connection = None

    async def fetch_all(self, query: ClauseElement) -> typing.List[Record]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns, context = self._compile(query)
        column_maps = create_column_maps(result_columns)
        dialect = self._dialect

        async with self._connection.execute(query_str, args) as cursor:
            rows = await cursor.fetchall()
            metadata = CursorResultMetaData(context, cursor.description)
            rows = [
                Row(
                    metadata,
                    metadata._processors,
                    metadata._keymap,
                    row,
                )
                for row in rows
            ]
            return [Record(row, result_columns, dialect, column_maps) for row in rows]

    async def fetch_one(self, query: ClauseElement) -> typing.Optional[Record]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns, context = self._compile(query)
        column_maps = create_column_maps(result_columns)
        dialect = self._dialect

        async with self._connection.execute(query_str, args) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            metadata = CursorResultMetaData(context, cursor.description)
            row = Row(
                metadata,
                metadata._processors,
                metadata._keymap,
                row,
            )
            return Record(row, result_columns, dialect, column_maps)

    async def execute(self, query: ClauseElement) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns, context = self._compile(query)
        async with self._connection.cursor() as cursor:
            await cursor.execute(query_str, args)
            if cursor.lastrowid == 0:
                return cursor.rowcount
            return cursor.lastrowid

    async def execute_many(self, queries: typing.List[ClauseElement]) -> None:
        assert self._connection is not None, "Connection is not acquired"
        for single_query in queries:
            await self.execute(single_query)

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns, context = self._compile(query)
        column_maps = create_column_maps(result_columns)
        dialect = self._dialect

        async with self._connection.execute(query_str, args) as cursor:
            metadata = CursorResultMetaData(context, cursor.description)
            async for row in cursor:
                record = Row(
                    metadata,
                    metadata._processors,
                    metadata._keymap,
                    row,
                )
                yield Record(record, result_columns, dialect, column_maps)

    def transaction(self) -> TransactionBackend:
        return SQLiteTransaction(self)

    def _compile(self, query: ClauseElement) -> typing.Tuple[str, list, tuple]:
        compiled = query.compile(
            dialect=self._dialect, compile_kwargs={"render_postcompile": True}
        )
        execution_context = self._dialect.execution_ctx_cls()
        execution_context.dialect = self._dialect

        args = []
        result_map = None

        if not isinstance(query, DDLElement):
            compiled_params = sorted(compiled.params.items())

            params = compiled.construct_params()
            for key in compiled.positiontup:
                raw_val = params[key]
                if key in compiled._bind_processors:
                    val = compiled._bind_processors[key](raw_val)
                else:
                    val = raw_val
                args.append(val)

            execution_context.result_column_struct = (
                compiled._result_columns,
                compiled._ordered_columns,
                compiled._textual_ordered_columns,
                compiled._ad_hoc_textual,
                compiled._loose_column_name_matching,
            )

            mapping = {
                key: "$" + str(i) for i, (key, _) in enumerate(compiled_params, start=1)
            }
            compiled_query = compiled.string % mapping
            result_map = compiled._result_columns

        else:
            compiled_query = compiled.string

        query_message = compiled_query.replace(" \n", " ").replace("\n", " ")
        logger.debug(
            "Query: %s Args: %s", query_message, repr(tuple(args)), extra=LOG_EXTRA
        )
        return compiled.string, args, result_map, CompilationContext(execution_context)

    @property
    def raw_connection(self) -> aiosqlite.core.Connection:
        assert self._connection is not None, "Connection is not acquired"
        return self._connection


class SQLiteTransaction(TransactionBackend):
    def __init__(self, connection: SQLiteConnection):
        self._connection = connection
        self._is_root = False
        self._savepoint_name = ""

    async def start(
        self, is_root: bool, extra_options: typing.Dict[typing.Any, typing.Any]
    ) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        self._is_root = is_root
        if self._is_root:
            async with self._connection._connection.execute("BEGIN") as cursor:
                await cursor.close()
        else:
            id = str(uuid.uuid4()).replace("-", "_")
            self._savepoint_name = f"STARLETTE_SAVEPOINT_{id}"
            async with self._connection._connection.execute(
                f"SAVEPOINT {self._savepoint_name}"
            ) as cursor:
                await cursor.close()

    async def commit(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        if self._is_root:
            async with self._connection._connection.execute("COMMIT") as cursor:
                await cursor.close()
        else:
            async with self._connection._connection.execute(
                f"RELEASE SAVEPOINT {self._savepoint_name}"
            ) as cursor:
                await cursor.close()

    async def rollback(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        if self._is_root:
            async with self._connection._connection.execute("ROLLBACK") as cursor:
                await cursor.close()
        else:
            async with self._connection._connection.execute(
                f"ROLLBACK TO SAVEPOINT {self._savepoint_name}"
            ) as cursor:
                await cursor.close()
