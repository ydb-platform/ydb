from sqlalchemy.engine.interfaces import AdaptedConnection

from sqlalchemy.util.concurrency import await_only
from ydb_dbapi import AsyncConnection, AsyncCursor
import ydb


class AdaptedAsyncConnection(AdaptedConnection):
    def __init__(self, connection: AsyncConnection):
        self._connection: AsyncConnection = connection

    @property
    def _driver(self):
        return self._connection._driver

    @property
    def _session_pool(self):
        return self._connection._session_pool

    @property
    def _tx_context(self):
        return self._connection._tx_context

    @property
    def _tx_mode(self):
        return self._connection._tx_mode

    @property
    def interactive_transaction(self):
        return self._connection.interactive_transaction

    def cursor(self):
        return AdaptedAsyncCursor(self._connection.cursor())

    def begin(self):
        return await_only(self._connection.begin())

    def commit(self):
        return await_only(self._connection.commit())

    def rollback(self):
        return await_only(self._connection.rollback())

    def close(self):
        return await_only(self._connection.close())

    def set_isolation_level(self, level):
        return self._connection.set_isolation_level(level)

    def get_isolation_level(self):
        return self._connection.get_isolation_level()

    def set_ydb_request_settings(self, value: ydb.BaseRequestSettings) -> None:
        self._connection.set_ydb_request_settings(value)

    def get_ydb_request_settings(self) -> ydb.BaseRequestSettings:
        return self._connection.get_ydb_request_settings()

    def set_ydb_retry_settings(self, value: ydb.RetrySettings) -> None:
        self._connection.set_ydb_retry_settings(value)

    def get_ydb_retry_settings(self) -> ydb.RetrySettings:
        return self._connection.get_ydb_retry_settings()

    def describe(self, table_path: str):
        return await_only(self._connection.describe(table_path))

    def check_exists(self, table_path: str):
        return await_only(self._connection.check_exists(table_path))

    def get_table_names(self):
        return await_only(self._connection.get_table_names())


# TODO(vgvoleg): Migrate to AsyncAdapt_dbapi_cursor and AsyncAdapt_dbapi_connection
class AdaptedAsyncCursor:
    _awaitable_cursor_close: bool = False

    def __init__(self, cursor: AsyncCursor):
        self._cursor = cursor

    @property
    def description(self):
        return self._cursor.description

    @property
    def arraysize(self):
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        self._cursor.arraysize = size

    @property
    def rowcount(self):
        return self._cursor.rowcount

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchmany(self, size=None):
        return self._cursor.fetchmany(size=size)

    def fetchall(self):
        return self._cursor.fetchall()

    def execute_scheme(self, sql, parameters=None):
        return await_only(self._cursor.execute_scheme(sql, parameters))

    def execute(self, sql, parameters=None):
        return await_only(self._cursor.execute(sql, parameters))

    def executemany(self, sql, parameters=None):
        return await_only(self._cursor.executemany(sql, parameters))

    def close(self):
        return self._cursor.close()

    def setinputsizes(self, *args):
        pass

    def setoutputsizes(self, *args):
        pass

    async def _async_soft_close(self) -> None:
        pass
