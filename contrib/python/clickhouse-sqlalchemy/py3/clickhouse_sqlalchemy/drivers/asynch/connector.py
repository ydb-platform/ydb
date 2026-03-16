import asyncio

from sqlalchemy.engine.interfaces import AdaptedConnection
from sqlalchemy.util.concurrency import await_only


class AsyncAdapt_asynch_cursor:
    __slots__ = (
        '_adapt_connection',
        '_connection',
        'await_',
        '_cursor',
        '_rows'
    )

    def __init__(self, adapt_connection):
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection  # noqa
        self.await_ = adapt_connection.await_

        cursor = self._connection.cursor()

        self._cursor = self.await_(cursor.__aenter__())
        self._rows = []

    @property
    def _execute_mutex(self):
        return self._adapt_connection._execute_mutex  # noqa

    @property
    def description(self):
        return self._cursor.description

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def arraysize(self):
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, value):
        self._cursor.arraysize = value

    @property
    def lastrowid(self):
        return self._cursor.lastrowid

    def close(self):
        # note we aren't actually closing the cursor here,
        # we are just letting GC do it.   to allow this to be async
        # we would need the Result to change how it does "Safe close cursor".
        self._rows[:] = []  # noqa

    def execute(self, operation, params=None, context=None):
        return self.await_(self._execute_async(operation, params, context))

    async def _execute_async(self, operation, params, context):
        async with self._execute_mutex:
            result = await self._cursor.execute(
                operation,
                args=params,
                context=context
            )

            self._rows = list(await self._cursor.fetchall())
            return result

    def executemany(self, operation, params=None, context=None):
        return self.await_(self._executemany_async(operation, params, context))

    async def _executemany_async(self, operation, params, context):
        async with self._execute_mutex:
            return await self._cursor.executemany(
                operation,
                args=params,
                context=context
            )

    def setinputsizes(self, *args):
        pass

    def setoutputsizes(self, *args):
        pass

    def __iter__(self):
        while self._rows:
            yield self._rows.pop(0)

    def fetchone(self):
        if self._rows:
            return self._rows.pop(0)
        else:
            return None

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize

        retval = self._rows[0:size]
        self._rows[:] = self._rows[size:]
        return retval

    def fetchall(self):
        retval = self._rows[:]
        self._rows[:] = []
        return retval


class AsyncAdapt_asynch_dbapi:
    def __init__(self, asynch):
        self.asynch = asynch
        self.paramstyle = 'pyformat'
        self._init_dbapi_attributes()

    class Error(Exception):
        pass

    def _init_dbapi_attributes(self):
        for name in (
                'ServerException',
                'UnexpectedPacketFromServerError',
                'LogicalError',
                'UnknownTypeError',
                'ChecksumDoesntMatchError',
                'TypeMismatchError',
                'UnknownCompressionMethod',
                'TooLargeStringSize',
                'NetworkError',
                'SocketTimeoutError',
                'UnknownPacketFromServerError',
                'CannotParseUuidError',
                'CannotParseDomainError',
                'PartiallyConsumedQueryError',
                'ColumnException',
                'ColumnTypeMismatchException',
                'StructPackException',
                'InterfaceError',
                'DatabaseError',
                'ProgrammingError',
                'NotSupportedError',
        ):
            setattr(self, name, getattr(self.asynch.errors, name))

    def connect(self, *args, **kwargs) -> 'AsyncAdapt_asynch_connection':
        return AsyncAdapt_asynch_connection(
            self,
            await_only(self.asynch.connect(*args, **kwargs))
        )


class AsyncAdapt_asynch_connection(AdaptedConnection):
    await_ = staticmethod(await_only)
    __slots__ = ('dbapi', '_execute_mutex')

    def __init__(self, dbapi, connection):
        self.dbapi = dbapi
        self._connection = connection
        self._execute_mutex = asyncio.Lock()

    def ping(self, reconnect):
        return self.await_(self._ping_async())

    async def _ping_async(self):
        async with self._execute_mutex:
            return await self._connection.ping()

    def character_set_name(self):
        return self._connection.character_set_name()

    def autocommit(self, value):
        self.await_(self._connection.autocommit(value))

    def cursor(self, server_side=False):
        return AsyncAdapt_asynch_cursor(self)

    def rollback(self):
        self.await_(self._connection.rollback())

    def commit(self):
        self.await_(self._connection.commit())

    def close(self):
        self.await_(self._connection.close())
