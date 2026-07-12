from __future__ import annotations

from typing import Any

from sqlalchemy import Column
from sqlalchemy.exc import CompileError
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.compiler import DDLCompiler

from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Nullable
from clickhouse_connect.cc_sqlalchemy.sql import format_table
from clickhouse_connect.datatypes.base import TypeDef
from clickhouse_connect.driver.binding import format_str, quote_identifier


class ClickHouseDDLHelper:
    dialect_names = ("clickhousedb", "clickhouse")

    @classmethod
    def get_option(cls, obj: Any, name: str) -> Any:
        kwargs = getattr(obj, "kwargs", None)
        if kwargs is not None:
            for prefix in cls.dialect_names:
                key = f"{prefix}_{name}"
                if key in kwargs and kwargs[key] is not None:
                    return kwargs[key]
        dialect_options = getattr(obj, "dialect_options", None)
        if dialect_options:
            for prefix in cls.dialect_names:
                options = dialect_options.get(prefix)
                if options and options.get(name) is not None:
                    return options.get(name)
        return None

    @classmethod
    def is_dictionary(cls, table: Any) -> bool:
        return getattr(table, "__visit_name__", None) == "dictionary" or cls.get_option(table, "table_type") == "dictionary"

    @classmethod
    def dictionary_option(cls, table: Any, name: str) -> Any:
        attr_name = "primary_key_def" if name == "primary_key" else name
        value = getattr(table, attr_name, None)
        if value is not None:
            return value
        return cls.get_option(table, f"dictionary_{name}")

    @staticmethod
    def explicit_column_nullable(column: Column) -> bool | None:
        user_defined = getattr(column, "_user_defined_nullable", None)
        if isinstance(user_defined, bool):
            return user_defined
        return None

    @staticmethod
    def column_nullable(column: Column) -> bool:
        column_type = getattr(column, "type", None)
        if isinstance(column_type, ChSqlaType) and column_type.nullable:
            return True
        explicit_nullable = ClickHouseDDLHelper.explicit_column_nullable(column)
        if explicit_nullable is not None:
            return explicit_nullable
        return False

    @staticmethod
    def effective_column_type(column: Column):
        column_type = column.type
        if not isinstance(column_type, ChSqlaType):
            return column_type
        if column_type.nullable:
            return column_type
        explicit_nullable = ClickHouseDDLHelper.explicit_column_nullable(column)
        if not explicit_nullable:
            return column_type

        return Nullable(column_type)

    @staticmethod
    def without_nullable(type_):
        if not isinstance(type_, ChSqlaType) or not type_.nullable:
            return type_
        type_def = type_.type_def
        wrappers = tuple(wrapper for wrapper in type_def.wrappers if wrapper != "Nullable")
        return type_.__class__(type_def=TypeDef(wrappers, type_def.keys, type_def.values))

    @staticmethod
    def render_settings(settings: dict[str, Any] | None) -> str:
        if not settings:
            return ""
        return ", ".join(f"{key} = {ClickHouseDDLHelper._render_setting_value(value)}" for key, value in settings.items())

    @staticmethod
    def render_comment(comment: str | None) -> str:
        if comment is None:
            return "''"
        escaped = comment.replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _render_setting_value(value: Any) -> str:
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        return format_str(str(value))


def column_specification(dialect, column: Column) -> str:
    compiler = dialect.ddl_compiler(dialect, None)
    return compiler.get_column_specification(column)


class ChDDLCompiler(DDLCompiler):
    def visit_create_schema(self, create, **_):
        return f"CREATE DATABASE {quote_identifier(create.element)}"

    def visit_drop_schema(self, drop, **_):
        return f"DROP DATABASE {quote_identifier(drop.element)}"

    def visit_create_table(self, create, **_):
        table = create.element
        if_not_exists = " IF NOT EXISTS" if getattr(create, "if_not_exists", False) else ""

        if ClickHouseDDLHelper.is_dictionary(table):
            return self._visit_create_dictionary(create, table, if_not_exists)

        engine = getattr(table, "engine", None) or ClickHouseDDLHelper.get_option(table, "engine")
        if engine is None:
            raise CompileError(
                f"ClickHouse table '{table.name}' requires an engine — specify e.g. MergeTree(order_by='id') as a table argument"
            )
        text = f"CREATE TABLE{if_not_exists} {format_table(table)} ("
        text += ", ".join([self.get_column_specification(c.element) for c in create.columns])
        text += ") " + engine.compile()
        if table.comment:
            text += f" COMMENT {self.sql_compiler.render_literal_value(table.comment, sqltypes.STRINGTYPE)}"
        return text

    def _visit_create_dictionary(self, create, dictionary, if_not_exists: str):
        text = f"CREATE DICTIONARY{if_not_exists} {format_table(dictionary)} ("
        text += ", ".join([self.get_column_specification(c.element) for c in create.columns])
        text += ")"

        primary_key = ClickHouseDDLHelper.dictionary_option(dictionary, "primary_key")
        if primary_key:
            text += f" PRIMARY KEY {primary_key}"

        source = ClickHouseDDLHelper.dictionary_option(dictionary, "source")
        if source:
            text += f" SOURCE({source})"

        layout = ClickHouseDDLHelper.dictionary_option(dictionary, "layout")
        if layout:
            layout = layout if "(" in layout else f"{layout}()"
            text += f" LAYOUT({layout})"

        lifetime = ClickHouseDDLHelper.dictionary_option(dictionary, "lifetime")
        if lifetime:
            text += f" LIFETIME({lifetime})"

        if dictionary.comment:
            text += f" COMMENT {self.sql_compiler.render_literal_value(dictionary.comment, sqltypes.STRINGTYPE)}"

        return text

    def visit_drop_table(self, drop, **_):
        table = drop.element
        if_exists = " IF EXISTS" if getattr(drop, "if_exists", False) else ""
        if ClickHouseDDLHelper.is_dictionary(table):
            return f"DROP DICTIONARY{if_exists} {format_table(table)}"
        return f"DROP TABLE{if_exists} {format_table(table)}"

    def visit_add_column(self, create, **_):
        return f"ALTER TABLE {format_table(create.element)} ADD COLUMN {self.get_column_specification(create.column)}"

    def visit_drop_column(self, drop, **_):
        return f"ALTER TABLE {format_table(drop.element)} DROP COLUMN {quote_identifier(drop.column.name)}"

    def get_column_specification(self, column: Column, **_):
        text = f"{quote_identifier(column.name)} {ClickHouseDDLHelper.effective_column_type(column).compile()}"
        materialized = ClickHouseDDLHelper.get_option(column, "materialized")
        alias = ClickHouseDDLHelper.get_option(column, "alias")
        if materialized is not None:
            text += f" MATERIALIZED {self.render_default_string(materialized)}"
            return text
        if alias is not None:
            text += f" ALIAS {self.render_default_string(alias)}"
            return text
        default = self.get_column_default_string(column)
        if default is not None:
            text += f" DEFAULT {default}"
        codec = ClickHouseDDLHelper.get_option(column, "codec")
        if codec is not None:
            codec_sql = codec if isinstance(codec, str) else ", ".join(str(item) for item in codec)
            text += f" CODEC({codec_sql})"
        ttl = ClickHouseDDLHelper.get_option(column, "ttl")
        if ttl is not None:
            text += f" TTL {self.render_default_string(ttl)}"
        if column.comment:
            text += f" COMMENT {self.sql_compiler.render_literal_value(column.comment, sqltypes.STRINGTYPE)}"
        return text
