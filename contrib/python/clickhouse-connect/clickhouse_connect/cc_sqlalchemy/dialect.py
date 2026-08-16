from typing import Any, cast

import sqlalchemy.schema as sa_schema
from sqlalchemy import text
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import NoResultFound, NoSuchTableError

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy import dialect_name, ischema_names
from clickhouse_connect.cc_sqlalchemy.inspector import ChInspector, get_columns, get_table_metadata
from clickhouse_connect.cc_sqlalchemy.sql import full_table
from clickhouse_connect.cc_sqlalchemy.sql.compiler import ChStatementCompiler
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import ChDDLCompiler
from clickhouse_connect.cc_sqlalchemy.sql.preparer import ChIdentifierPreparer
from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.driver.binding import quote_identifier


class ClickHouseDialect(DefaultDialect):
    """
    See :py:class:`sqlalchemy.engine.interfaces`
    """

    name = dialect_name
    driver = "connect"

    default_schema_name = "default"
    supports_native_decimal = True
    supports_native_boolean = True
    supports_statement_cache = False
    supports_comments = True
    inline_comments = True
    returns_unicode_strings = True
    postfetch_lastrowid = False
    ddl_compiler = ChDDLCompiler
    statement_compiler = ChStatementCompiler
    preparer = ChIdentifierPreparer  # type: ignore[assignment]
    description_encoding = None
    max_identifier_length = 127
    ischema_names = ischema_names
    inspector = ChInspector
    construct_arguments = [
        (
            sa_schema.Table,
            {
                "engine": None,
                "table_type": None,
                "dictionary_source": None,
                "dictionary_layout": None,
                "dictionary_lifetime": None,
                "dictionary_primary_key": None,
            },
        ),
        (
            sa_schema.Column,
            {
                "materialized": None,
                "alias": None,
                "codec": None,
                "ttl": None,
                "after": None,
                "settings": None,
            },
        ),
    ]

    def __init__(self, server_side_params: bool = False, **kwargs):
        # Set before super().__init__() so ChIdentifierPreparer can read it when built.
        self.server_side_params = server_side_params
        super().__init__(**kwargs)

    @staticmethod
    def _ch_query_settings(context: Any) -> dict[str, Any] | None:
        # Deep-merge one level of execution_options["settings"], statement wins per key.
        if context is None:
            return None
        merged = context.execution_options.get("settings")
        stmt = getattr(context, "invoked_statement", None)
        stmt_settings = stmt.get_execution_options().get("settings") if stmt is not None else None
        if not stmt_settings:
            return merged
        if not merged:
            return dict(stmt_settings)
        return {**merged, **stmt_settings}

    def do_execute(self, cursor, statement, parameters, context=None):
        cast(Cursor, cursor).execute(statement, parameters, settings=self._ch_query_settings(context))

    def do_executemany(self, cursor, statement, parameters, context=None):
        cast(Cursor, cursor).executemany(statement, parameters, settings=self._ch_query_settings(context))

    def do_execute_no_params(self, cursor, statement, context=None):
        cast(Cursor, cursor).execute(statement, settings=self._ch_query_settings(context))

    # SQA 1 compatibility

    @classmethod
    def dbapi(cls):
        return dbapi

    # SQA 2 compatibility

    @classmethod
    def import_dbapi(cls):
        return dbapi

    def _get_default_schema_name(self, connection):
        return connection.execute(text("SELECT currentDatabase()")).scalar()

    def get_schema_names(self, connection, **_):
        return [row.name for row in connection.execute(text("SHOW DATABASES"))]

    @staticmethod
    def has_database(connection, db_name):
        # EXISTS DATABASE consults DatabaseCatalog directly, so it sees DataLakeCatalog
        # and other remote databases that system.databases omitted by default before server 26.5.
        result = connection.execute(text(f"EXISTS DATABASE {quote_identifier(db_name)}"))
        row = result.fetchone()
        return row[0] == 1

    def get_table_names(self, connection, schema=None, **kw):
        cmd = "SHOW TABLES"
        if schema:
            cmd += " FROM " + quote_identifier(schema)
        return [row.name for row in connection.execute(text(cmd))]

    def get_columns(self, connection, table_name, schema=None, **kw):
        return get_columns(connection, table_name, schema)

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_temp_table_names(self, connection, schema=None, **kw):
        return []

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def get_temp_view_names(self, connection, schema=None, **kw):
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        raise NoSuchTableError(f"{schema}.{view_name}" if schema else view_name)

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        try:
            table_metadata = get_table_metadata(connection, table_name, schema)
        except NoResultFound:
            raise NoSuchTableError(f"{schema}.{table_name}" if schema else table_name) from None
        return {"text": table_metadata.comment or None}

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def has_table(self, connection, table_name, schema=None, **_kw):
        result = connection.execute(text(f"EXISTS TABLE {full_table(table_name, schema)}"))
        row = result.fetchone()
        return row[0] == 1

    def has_sequence(self, connection, sequence_name, schema=None, **_kw):
        return False

    def do_begin_twophase(self, connection, xid):
        raise NotImplementedError

    def do_prepare_twophase(self, connection, xid):
        raise NotImplementedError

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_recover_twophase(self, connection):
        raise NotImplementedError

    def set_isolation_level(self, dbapi_conn, level):
        pass

    def get_isolation_level(self, dbapi_conn):
        return "AUTOCOMMIT"
