from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from alembic.ddl.impl import DefaultImpl
from alembic.util import CommandError
from sqlalchemy import Column, MetaData, String, Table, text
from sqlalchemy.sql.dml import Delete, Update

from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType, sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Array as ChSqlaArray
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Enum as ChSqlaEnum
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Map as ChSqlaMap
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Nullable
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Tuple as ChSqlaTuple
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
from clickhouse_connect.cc_sqlalchemy.sql import full_table
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import (
    ClickHouseDDLHelper,
    column_specification,
)
from clickhouse_connect.driver.binding import quote_identifier


def _render_ch_type(type_obj):
    """Render a ChSqlaType as valid Python source for autogen migrations"""
    wrappers = type_obj.type_def.wrappers
    if isinstance(type_obj, ChSqlaEnum):
        keys = list(type_obj.type_def.keys)
        values = list(type_obj.type_def.values)
        rendered = f"{type_obj.__class__.__name__}(keys={keys!r}, values={values!r})"
    elif isinstance(type_obj, ChSqlaArray):
        rendered = f"Array({_render_inner(type_obj.type_def.values[0])})"
    elif isinstance(type_obj, ChSqlaMap):
        key, value = type_obj.type_def.values
        rendered = f"Map({_render_inner(key)}, {_render_inner(value)})"
    elif isinstance(type_obj, ChSqlaTuple):
        elements = ", ".join(_render_inner(v) for v in type_obj.type_def.values)
        rendered = f"Tuple({elements})"
    else:
        return str(type_obj.name)
    for wrapper in reversed(wrappers):
        rendered = f"{wrapper}({rendered})"
    return rendered


def _render_inner(name):
    return _render_ch_type(sqla_type_from_name(name))


class ClickHouseImpl(DefaultImpl):
    __dialect__ = "clickhousedb"
    transactional_ddl = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.context_opts.get("include_schemas") and not self.context_opts.get("version_table_schema") and self.connection is not None:
            current_database = self.connection.execute(text("SELECT currentDatabase()")).scalar()
            if current_database:
                self.context_opts["version_table_schema"] = current_database

    def version_table_impl(
        self,
        *,
        version_table: str,
        version_table_schema: str | None,
        version_table_pk: bool,
        **_kw: Any,
    ) -> Table:
        return Table(
            version_table,
            MetaData(),
            Column("version_num", String(32), nullable=False),
            MergeTree(order_by="version_num"),
            schema=version_table_schema,
        )

    def _exec(
        self,
        construct,
        execution_options=None,
        multiparams=None,
        params=None,
    ) -> Any:
        if isinstance(construct, Update) and self._is_version_table_construct(construct):
            return self._exec_version_update(construct, execution_options)
        if isinstance(construct, Delete) and self._is_version_table_construct(construct):
            return self._exec_version_delete(construct, execution_options)
        return super()._exec(
            construct,
            execution_options=execution_options,
            multiparams=multiparams,
            params=params or {},
        )

    def add_column(
        self,
        table_name: str,
        column: Column,
        *,
        schema: str | None = None,
        if_not_exists: bool | None = None,
        **kw: Any,
    ) -> None:
        sql = [
            "ALTER TABLE",
            full_table(table_name, schema),
            "ADD COLUMN",
        ]
        if if_not_exists:
            sql.append("IF NOT EXISTS")
        sql.append(column_specification(self.dialect, column))
        after = kw.get("after") or ClickHouseDDLHelper.get_option(column, "after")
        if after:
            sql.extend(["AFTER", quote_identifier(after)])
        settings = ClickHouseDDLHelper.render_settings(kw.get("clickhouse_settings"))
        if settings:
            sql.extend(["SETTINGS", settings])
        self._exec(text(" ".join(sql)))

    def drop_column(
        self,
        table_name: str,
        column: Column,
        *,
        schema: str | None = None,
        if_exists: bool | None = None,
        **kw: Any,
    ) -> None:
        sql = ["ALTER TABLE", full_table(table_name, schema), "DROP COLUMN"]
        if if_exists:
            sql.append("IF EXISTS")
        sql.append(quote_identifier(column.name))
        settings = ClickHouseDDLHelper.render_settings(kw.get("clickhouse_settings"))
        if settings:
            sql.extend(["SETTINGS", settings])
        self._exec(text(" ".join(sql)))

    def create_table_comment(self, table: Table) -> None:
        self._exec(text(self._comment_table_sql(table, table.comment)))

    def drop_table_comment(self, table: Table) -> None:
        self._exec(text(self._comment_table_sql(table, None)))

    def alter_column(
        self,
        table_name: str,
        column_name: str,
        *,
        nullable: bool | None = None,
        server_default=False,
        name: str | None = None,
        type_=None,
        schema: str | None = None,
        autoincrement: bool | None = None,
        comment=False,
        existing_comment: str | None = None,
        existing_type=None,
        existing_server_default=None,
        existing_nullable: bool | None = None,
        existing_autoincrement: bool | None = None,
        if_exists: bool | None = None,
        **kw: Any,
    ) -> None:
        if autoincrement is not None or existing_autoincrement is not None:
            return
        if name is not None:
            rename_sql = ["ALTER TABLE", full_table(table_name, schema), "RENAME COLUMN"]
            if if_exists:
                rename_sql.append("IF EXISTS")
            rename_sql.extend([quote_identifier(column_name), "TO", quote_identifier(name)])
            self._exec(text(" ".join(rename_sql)))
            column_name = name

        settings = ClickHouseDDLHelper.render_settings(kw.get("clickhouse_settings"))
        will_modify = nullable is not None or server_default is not False or type_ is not None

        if comment is not False and not will_modify:
            self._exec(text(self._comment_column_sql(table_name, column_name, comment, schema, settings)))

        if not will_modify:
            return

        if type_ is not None:
            effective_type = type_
        else:
            effective_type = existing_type
        if effective_type is None:
            raise CommandError(f"ClickHouse alter_column requires existing_type for {table_name}.{column_name}")
        if nullable is not None:
            effective_type = self._set_type_nullable(effective_type, nullable)

        sql = [
            "ALTER TABLE",
            full_table(table_name, schema),
            "MODIFY COLUMN",
        ]
        if if_exists:
            sql.append("IF EXISTS")
        sql.append(
            column_specification(
                self.dialect,
                Column(
                    column_name,
                    effective_type,
                    server_default=None if server_default is False else server_default,
                    comment=existing_comment if comment is False else comment,
                ),
            )
        )
        if settings:
            sql.extend(["SETTINGS", settings])
        self._exec(text(" ".join(sql)))

    def compare_type(self, inspector_column, metadata_column) -> bool:
        inspector_type = inspector_column.type
        metadata_type = metadata_column.type
        explicit_nullable = ClickHouseDDLHelper.explicit_column_nullable(metadata_column)
        if explicit_nullable is None and isinstance(inspector_type, ChSqlaType) and isinstance(metadata_type, ChSqlaType):
            inspector_type = ClickHouseDDLHelper.without_nullable(inspector_type)
            metadata_type = ClickHouseDDLHelper.without_nullable(metadata_type)
        else:
            metadata_type = ClickHouseDDLHelper.effective_column_type(metadata_column)
        inspector_type = self._normalize_type_name(inspector_type)
        metadata_type = self._normalize_type_name(metadata_type)
        return inspector_type != metadata_type

    def compare_server_default(
        self,
        inspector_column,
        metadata_column,
        rendered_metadata_default,
        rendered_inspector_default,
    ):
        return self._normalize_default(rendered_inspector_default) != self._normalize_default(rendered_metadata_default)

    def render_type(self, type_obj, autogen_context):
        if not isinstance(type_obj, ChSqlaType):
            return False
        return _render_ch_type(type_obj)

    def _exec_version_update(self, construct: Update, execution_options=None):
        # Alembic emits a normal SQLAlchemy Update here, but ClickHouse version tracking
        # needs insert + mutation delete semantics. SQLAlchemy does not expose a stable
        # public API for these values across versions, so this depends on the current
        # Update internals.
        values = construct._values
        if not values:
            raise CommandError("ClickHouse Alembic version update is missing values")
        version_value = self._compile_clause(list(values.values())[0])
        where_clause = self._compile_version_where(construct)
        self._exec(text(f"INSERT INTO {self._version_table_name} (version_num) VALUES ({version_value})"))
        self._exec(text(f"ALTER TABLE {self._version_table_name} DELETE WHERE {where_clause} SETTINGS mutations_sync = 2"))
        return SimpleNamespace(rowcount=1)

    def _exec_version_delete(self, construct: Delete, execution_options=None):
        where_clause = self._compile_version_where(construct)
        return super()._exec(
            text(f"ALTER TABLE {self._version_table_name} DELETE WHERE {where_clause} SETTINGS mutations_sync = 2"),
            execution_options=execution_options,
        )

    @property
    def _version_table_name(self) -> str:
        schema = self.context_opts.get("version_table_schema")
        table = self.context_opts.get("version_table", "alembic_version")
        if schema:
            return f"{quote_identifier(schema)}.{quote_identifier(table)}"
        return quote_identifier(table)

    def _is_version_table_construct(self, construct) -> bool:
        table = getattr(construct, "table", None)
        if table is None:
            return False
        if table.name != self.context_opts.get("version_table", "alembic_version"):
            return False
        expected_schema = self.context_opts.get("version_table_schema")
        # Alembic captures version_table_schema before ClickHouseImpl.__init__
        # has a chance to set it, so the _version Table may have schema=None
        # while context_opts has the auto-detected database name.
        if table.schema == expected_schema:
            return True
        if table.schema is None and expected_schema is not None:
            return True
        return False

    def _compile_version_where(self, construct) -> str:
        predicates = []
        for expression in construct._where_criteria:
            # SQLAlchemy does not provide a public helper for pulling these predicates
            # back apart, so this relies on the current binary expression structure.
            column_name = getattr(getattr(expression, "left", None), "name", None)
            if not column_name:
                predicates.append(self._compile_clause(expression))
                continue
            right = self._compile_clause(expression.right)
            predicates.append(f"{quote_identifier(column_name)} = {right}")
        return " AND ".join(predicates)

    def _compile_clause(self, clause) -> str:
        return str(
            clause.compile(
                dialect=self.dialect,
                compile_kwargs={"literal_binds": True},
            )
        )

    def _comment_column_sql(
        self,
        table_name: str,
        column_name: str,
        comment: str | None,
        schema: str | None,
        settings: str,
    ) -> str:
        sql = [
            "ALTER TABLE",
            full_table(table_name, schema),
            "COMMENT COLUMN",
            quote_identifier(column_name),
            ClickHouseDDLHelper.render_comment(comment),
        ]
        if settings:
            sql.extend(["SETTINGS", settings])
        return " ".join(sql)

    @staticmethod
    def _comment_table_sql(table: Table, comment: str | None) -> str:
        return " ".join(
            [
                "ALTER TABLE",
                full_table(table.name, table.schema),
                "MODIFY COMMENT",
                ClickHouseDDLHelper.render_comment(comment),
            ]
        )

    @staticmethod
    def _normalize_default(default: str | None) -> str | None:
        if default is None:
            return None
        return default.strip()

    @staticmethod
    def _normalize_type_name(type_: Any) -> str:
        if hasattr(type_, "name"):
            return str(type_.name).replace(" ", "")
        return str(type_).replace(" ", "")

    @staticmethod
    def _set_type_nullable(type_: Any, nullable: bool):
        if isinstance(type_, type) and issubclass(type_, ChSqlaType):
            type_ = type_()
        if not isinstance(type_, ChSqlaType):
            return type_
        if nullable:
            if type_.nullable:
                return type_

            return Nullable(type_)
        if not type_.nullable:
            return type_
        wrappers = tuple(wrapper for wrapper in type_.type_def.wrappers if wrapper != "Nullable")
        return type_.__class__(type_def=type_.type_def.__class__(wrappers, type_.type_def.keys, type_.type_def.values))
