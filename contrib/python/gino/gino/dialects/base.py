import asyncio
import weakref

from sqlalchemy import util

# noinspection PyProtectedMember
from ..engine import _SAConnection, _SAEngine, _DBAPIConnection
from ..loader import Loader

DEFAULT = object()


class BaseDBAPI:
    paramstyle = "numeric"
    Error = Exception

    # noinspection PyPep8Naming
    @staticmethod
    def Binary(x):
        return x


class DBAPICursor:
    def execute(self, statement, parameters):
        pass

    def executemany(self, statement, parameters):
        pass

    @property
    def description(self):
        raise NotImplementedError

    async def prepare(self, context, clause=None):
        raise NotImplementedError

    async def async_execute(self, query, timeout, args, limit=0, many=False):
        raise NotImplementedError

    def get_statusmsg(self):
        raise NotImplementedError


class Pool:
    @property
    def raw_pool(self):
        raise NotImplementedError

    async def acquire(self, *, timeout=None):
        raise NotImplementedError

    async def release(self, conn):
        raise NotImplementedError

    async def close(self):
        raise NotImplementedError

    def repr(self, color):
        return repr(self)


class Transaction:
    @property
    def raw_transaction(self):
        raise NotImplementedError

    async def begin(self):
        raise NotImplementedError

    async def commit(self):
        raise NotImplementedError

    async def rollback(self):
        raise NotImplementedError


class PreparedStatement:
    def __init__(self, clause=None):
        self.context = None
        self.clause = clause

    def iterate(self, *params, **kwargs):
        return _PreparedIterableCursor(self, params, kwargs)

    async def _do_execute(
        self, multiparams, params, one=False, return_model=True, status=False
    ):
        ctx = self.context.connection.execute(
            self.clause, *multiparams, **params
        ).context
        if ctx.executemany:
            raise ValueError(
                "PreparedStatement does not support multiple " "parameters."
            )
        if ctx.statement != self.context.statement:
            raise AssertionError(
                "Prepared statement generated different SQL with parameters"
            )
        params = []
        for val in ctx.parameters[0]:
            params.append(val)
        msg, rows = await self._execute(params, one)
        if status:
            return msg
        item = self.context.process_rows(rows, return_model=return_model)
        if one:
            if item:
                item = item[0]
            else:
                item = None
        return item

    async def all(self, *multiparams, **params):
        return await self._do_execute(multiparams, params)

    async def first(self, *multiparams, **params):
        return await self._do_execute(multiparams, params, one=True)

    async def scalar(self, *multiparams, **params):
        rv = await self._do_execute(multiparams, params, one=True, return_model=False)
        if rv:
            return rv[0]
        else:
            return None

    async def status(self, *multiparams, **params):
        return await self._do_execute(multiparams, params, status=True)

    def _get_iterator(self, *params, **kwargs):
        raise NotImplementedError

    async def _get_cursor(self, *params, **kwargs):
        raise NotImplementedError

    async def _execute(self, params, one):
        raise NotImplementedError


class _PreparedIterableCursor:
    def __init__(self, prepared, params, kwargs):
        self._prepared = prepared
        self._params = params
        self._kwargs = kwargs

    def __aiter__(self):
        return getattr(self._prepared, "_get_iterator")(*self._params, **self._kwargs)

    def __await__(self):
        return getattr(self._prepared, "_get_cursor")(
            *self._params, **self._kwargs
        ).__await__()


class _IterableCursor:
    def __init__(self, context):
        self._context = context

    async def _iterate(self):
        prepared = await self._context.cursor.prepare(self._context)
        return prepared.iterate(
            *self._context.parameters[0], timeout=self._context.timeout
        )

    async def _get_cursor(self):
        return await (await self._iterate())

    def __aiter__(self):
        return _LazyIterator(self._iterate)

    def __await__(self):
        return self._get_cursor().__await__()


class _LazyIterator:
    def __init__(self, init):
        self._init = init
        self._iter = None

    async def __anext__(self):
        if self._iter is None:
            self._iter = (await self._init()).__aiter__()
        return await self._iter.__anext__()


class _ResultProxy:
    _metadata = True

    def __init__(self, context):
        self._context = context

    @property
    def context(self):
        return self._context

    async def execute(self, one=False, return_model=True, status=False):
        context = self._context

        param_groups = []
        for params in context.parameters:
            replace_params = []
            for val in params:
                if asyncio.iscoroutine(val):
                    val = await val
                replace_params.append(val)
            param_groups.append(replace_params)

        cursor = context.cursor
        if context.executemany:
            return await cursor.async_execute(
                context.statement, context.timeout, param_groups, many=True
            )
        else:
            args = param_groups[0]
            rows = await cursor.async_execute(
                context.statement, context.timeout, args, 1 if one else 0
            )
            item = context.process_rows(rows, return_model=return_model)
            if one:
                if item:
                    item = item[0]
                else:
                    item = None
            if status:
                item = cursor.get_statusmsg(), item
            return item

    def iterate(self):
        if self._context.executemany:
            raise ValueError("too many multiparams")
        return _IterableCursor(self._context)

    async def prepare(self, clause):
        return await self._context.cursor.prepare(self._context, clause)

    def _soft_close(self):
        pass


class Cursor:
    async def many(self, n, *, timeout=DEFAULT):
        raise NotImplementedError

    async def next(self, *, timeout=DEFAULT):
        raise NotImplementedError

    async def forward(self, n, *, timeout=DEFAULT):
        raise NotImplementedError


class ExecutionContextOverride:
    def _compiled_first_opt(self, key, default=DEFAULT):
        rv = DEFAULT
        opts = getattr(getattr(self, "compiled", None), "execution_options", None)
        if opts:
            rv = opts.get(key, DEFAULT)
        if rv is DEFAULT:
            # noinspection PyUnresolvedReferences
            rv = self.execution_options.get(key, default)
        if rv is DEFAULT:
            raise LookupError("No such execution option!")
        return rv

    @util.memoized_property
    def return_model(self):
        return self._compiled_first_opt("return_model", True)

    @util.memoized_property
    def model(self):
        rv = self._compiled_first_opt("model", None)
        if isinstance(rv, weakref.ref):
            rv = rv()
        return rv

    @util.memoized_property
    def timeout(self):
        return self._compiled_first_opt("timeout", None)

    @util.memoized_property
    def loader(self):
        return self._compiled_first_opt("loader", None)

    def process_rows(self, rows, return_model=True):
        # noinspection PyUnresolvedReferences
        rv = rows = super().get_result_proxy().process_rows(rows)
        loader = self.loader
        if loader is None and self.model is not None:
            loader = Loader.get(self.model)
        if loader is not None and return_model and self.return_model:
            ctx = {}
            rv = []
            loader = Loader.get(loader)
            for row in rows:
                obj, distinct = loader.do_load(row, ctx)
                if distinct:
                    rv.append(obj)
        return rv

    def get_result_proxy(self):
        return _ResultProxy(self)

    @classmethod
    def _init_compiled_prepared(
        cls, dialect, connection, dbapi_connection, compiled, parameters
    ):
        self = cls.__new__(cls)
        self.root_connection = connection
        self._dbapi_connection = dbapi_connection
        self.dialect = connection.dialect

        self.compiled = compiled

        # this should be caught in the engine before
        # we get here
        assert compiled.can_execute

        self.execution_options = compiled.execution_options.union(
            connection._execution_options
        )

        self.result_column_struct = (
            compiled._result_columns,
            compiled._ordered_columns,
            compiled._textual_ordered_columns,
        )

        self.unicode_statement = util.text_type(compiled)
        if not dialect.supports_unicode_statements:
            self.statement = self.unicode_statement.encode(self.dialect.encoding)
        else:
            self.statement = self.unicode_statement

        self.isinsert = compiled.isinsert
        self.isupdate = compiled.isupdate
        self.isdelete = compiled.isdelete
        self.is_text = compiled.isplaintext

        self.executemany = False

        self.cursor = self.create_cursor()

        if self.isinsert or self.isupdate or self.isdelete:
            self.is_crud = True
            self._is_explicit_returning = bool(compiled.statement._returning)
            self._is_implicit_returning = bool(
                compiled.returning and not compiled.statement._returning
            )

        if self.dialect.positional:
            self.parameters = [dialect.execute_sequence_format()]
        else:
            self.parameters = [{}]
        self.compiled_parameters = [{}]

        return self

    @classmethod
    def _init_statement_prepared(
        cls, dialect, connection, dbapi_connection, statement, parameters
    ):
        """Initialize execution context for a string SQL statement."""

        self = cls.__new__(cls)
        self.root_connection = connection
        self._dbapi_connection = dbapi_connection
        self.dialect = connection.dialect
        self.is_text = True

        # plain text statement
        self.execution_options = connection._execution_options

        if self.dialect.positional:
            self.parameters = [dialect.execute_sequence_format()]
        else:
            self.parameters = [{}]

        self.executemany = False

        if not dialect.supports_unicode_statements and isinstance(
            statement, util.text_type
        ):
            self.unicode_statement = statement
            self.statement = dialect._encoder(statement)[0]
        else:
            self.statement = self.unicode_statement = statement

        self.cursor = self.create_cursor()
        return self


class AsyncDialectMixin:
    cursor_cls = DBAPICursor
    dbapi_class = BaseDBAPI

    def _init_mixin(self):
        self._sa_conn = _SAConnection(
            _SAEngine(self), _DBAPIConnection(self.cursor_cls)
        )

    @classmethod
    def dbapi(cls):
        return cls.dbapi_class

    def compile(self, elem, *multiparams, **params):
        context = self._sa_conn.execute(elem, *multiparams, **params).context
        if context.executemany:
            return context.statement, context.parameters
        else:
            return context.statement, context.parameters[0]

    async def init_pool(self, url, loop):
        raise NotImplementedError

    def transaction(self, raw_conn, args, kwargs):
        raise NotImplementedError
