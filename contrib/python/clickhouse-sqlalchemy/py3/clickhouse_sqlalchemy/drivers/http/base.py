import sqlalchemy as sa
from sqlalchemy.util import asbool

from ..base import ClickHouseDialect, ClickHouseExecutionContextBase
from . import connector


# Export connector version
VERSION = (0, 0, 2, None)


class ClickHouseExecutionContext(ClickHouseExecutionContextBase):
    def pre_exec(self):
        pass


class ClickHouseDialect_http(ClickHouseDialect):
    driver = 'http'
    execution_ctx_cls = ClickHouseExecutionContext

    supports_statement_cache = True

    @classmethod
    def dbapi(cls):
        return connector

    def create_connect_args(self, url):
        kwargs = {}
        protocol = url.query.get('protocol', 'http')
        port = url.port or 8123
        db_name = url.database or 'default'
        endpoint = url.query.get('endpoint', '')

        self.engine_reflection = asbool(
            url.query.get('engine_reflection', 'true')
        )

        kwargs.update(url.query)
        if kwargs.get('verify') and kwargs['verify'] in ('False', 'false'):
            kwargs['verify'] = False

        db_url = '%s://%s:%d/%s' % (protocol, url.host, port, endpoint)

        return (db_url, db_name, url.username, url.password), kwargs

    def _execute(self, connection, sql, scalar=False, **kwargs):
        if isinstance(sql, str):
            # Makes sure the query will go through the
            # `ClickHouseExecutionContext` logic.
            sql = sa.sql.elements.TextClause(sql)
        f = connection.scalar if scalar else connection.execute
        return f(sql, **kwargs)


dialect = ClickHouseDialect_http
