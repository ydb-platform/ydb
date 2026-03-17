from sqlalchemy.util import asbool

from . import connector
from ..base import (
    ClickHouseDialect, ClickHouseExecutionContextBase, ClickHouseCompiler,
)

# Export connector version
VERSION = (0, 0, 2, None)


class ClickHouseExecutionContext(ClickHouseExecutionContextBase):
    def pre_exec(self):
        # Always do executemany on INSERT with VALUES clause.
        if self.isinsert and self.compiled.statement.select is None:
            self.executemany = True


class ClickHouseNativeCompiler(ClickHouseCompiler):

    def visit_insert(self, insert_stmt, asfrom=False, **kw):
        rv = super(ClickHouseNativeCompiler, self).visit_insert(
            insert_stmt, asfrom=asfrom, **kw)

        if kw.get('literal_binds') or insert_stmt.parameters:
            return rv

        pos = rv.lower().rfind('values (')
        # Remove (%s)-templates from VALUES clause if exists.
        # ClickHouse server since version 19.3.3 parse query after VALUES and
        # allows inplace parameters.
        # Example: INSERT INTO test (x) VALUES (1), (2).
        if pos != -1:
            rv = rv[:pos + 6]
        return rv


class ClickHouseDialect_native(ClickHouseDialect):
    driver = 'native'
    execution_ctx_cls = ClickHouseExecutionContext
    statement_compiler = ClickHouseNativeCompiler

    @classmethod
    def dbapi(cls):
        return connector

    def create_connect_args(self, url):
        url.drivername = 'clickhouse'

        self.engine_reflection = asbool(
            url.query.pop('engine_reflection', 'true')
        )

        return (str(url), ), {}

    def _execute(self, connection, sql, scalar=False, **kwargs):
        f = connection.scalar if scalar else connection.execute
        return f(sql, **kwargs)


dialect = ClickHouseDialect_native
