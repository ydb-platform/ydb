import ast
import re
from collections.abc import Collection
from typing import Any

import sqlalchemy.schema as sa_schema
from sqlalchemy import text
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import NoResultFound

from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import build_engine
from clickhouse_connect.cc_sqlalchemy.sql import full_table
from clickhouse_connect.cc_sqlalchemy.sql.sqlparse import (
    extract_parenthesized_block,
    find_top_level_clause,
    split_top_level,
)


def _database_name(connection, schema: str | None) -> str:
    if schema:
        return schema
    return connection.execute(text("SELECT currentDatabase()")).scalar()


def get_table_metadata(connection, table_name, schema=None):
    database = _database_name(connection, schema)
    result_set = connection.execute(
        text("SELECT engine, engine_full, comment FROM system.tables WHERE database = :database AND name = :table_name"),
        {"database": database, "table_name": table_name},
    )
    row = next(result_set, None)
    if not row:
        raise NoResultFound(f"Table {database}.{table_name} does not exist")
    return row


def get_engine(connection, table_name, schema=None):
    row = get_table_metadata(connection, table_name, schema)
    return build_engine(row.engine_full)


def get_dictionary_create_sql(connection, table_name: str, schema: str | None = None) -> str:
    create_sql = connection.execute(text(f"SHOW CREATE DICTIONARY {full_table(table_name, schema)}")).scalar()
    return create_sql or ""


def _parse_dictionary_column(definition: str) -> dict[str, Any]:
    match = re.match(r"^`(?P<name>[^`]+)`\s+(?P<rest>.+)$", definition, flags=re.DOTALL)
    if not match:
        match = re.match(r"^(?P<name>\S+)\s+(?P<rest>.+)$", definition, flags=re.DOTALL)
    if not match:
        raise ValueError(f"Could not parse dictionary column definition: {definition}")

    name = match.group("name")
    remainder = match.group("rest").strip()
    type_index, _ = find_top_level_clause(
        remainder,
        (" DEFAULT ", " MATERIALIZED ", " ALIAS ", " TTL ", " COMMENT ", " CODEC("),
    )
    type_name = remainder[:type_index].strip() if type_index != -1 else remainder
    sqla_type = sqla_type_from_name(type_name.replace("\n", " "))
    column = {
        "name": name,
        "type": sqla_type,
        "nullable": sqla_type.nullable,
        "autoincrement": False,
    }

    comment_index, comment_clause = find_top_level_clause(remainder, (" COMMENT ",))
    if comment_clause:
        comment_sql = remainder[comment_index + len(comment_clause) :].strip()
        column["comment"] = ast.literal_eval(comment_sql)
        remainder = remainder[:comment_index].rstrip()

    default_index, default_clause = find_top_level_clause(remainder, (" DEFAULT ", " MATERIALIZED ", " ALIAS "))
    if default_clause:
        default_sql = remainder[default_index + len(default_clause) :].strip()
        if default_clause == " DEFAULT ":
            column["server_default"] = text(default_sql)
        elif default_clause == " MATERIALIZED ":
            column["clickhouse_materialized"] = text(default_sql)
        elif default_clause == " ALIAS ":
            column["clickhouse_alias"] = text(default_sql)
    return column


def get_dictionary_columns(connection, table_name: str, schema: str | None = None) -> list[dict[str, Any]]:
    create_sql = get_dictionary_create_sql(connection, table_name, schema)
    if not create_sql:
        return []
    start = create_sql.find("(")
    if start == -1:
        return []
    column_block, _ = extract_parenthesized_block(create_sql, start)
    return [_parse_dictionary_column(column_sql) for column_sql in split_top_level(column_block)]


def get_dictionary_metadata(connection, table_name: str, schema: str | None = None) -> dict[str, Any]:
    create_sql = get_dictionary_create_sql(connection, table_name, schema)
    if not create_sql:
        return {}

    metadata: dict[str, Any] = {"clickhouse_table_type": "dictionary"}
    for line in (line.strip() for line in create_sql.splitlines()):
        if not line:
            continue
        if line.startswith("PRIMARY KEY "):
            metadata["clickhouse_dictionary_primary_key"] = line[len("PRIMARY KEY ") :]
        elif line.startswith("SOURCE(") and line.endswith(")"):
            metadata["clickhouse_dictionary_source"] = line[len("SOURCE(") : -1]
        elif line.startswith("LIFETIME(") and line.endswith(")"):
            metadata["clickhouse_dictionary_lifetime"] = line[len("LIFETIME(") : -1]
        elif line.startswith("LAYOUT(") and line.endswith(")"):
            metadata["clickhouse_dictionary_layout"] = line[len("LAYOUT(") : -1]
        elif line.startswith("COMMENT "):
            metadata["comment"] = ast.literal_eval(line[len("COMMENT ") :])
    return metadata


class ChInspector(Inspector):
    def reflect_table(
        self,
        table,
        *_args,
        include_columns: Collection[str] | None = None,
        exclude_columns: Collection[str] = (),
        **_kwargs,
    ):
        schema = table.schema
        table_metadata = get_table_metadata(self.bind, table.name, schema)
        if table_metadata.engine == "Dictionary":
            reflected_columns = get_dictionary_columns(self.bind, table.name, schema)
        else:
            reflected_columns = self.get_columns(table.name, schema)

        for col in reflected_columns:
            name = col.pop("name")
            if (include_columns and name not in include_columns) or (exclude_columns and name in exclude_columns):
                continue
            col_type = col.pop("type")
            col_args = {key: value for key, value in col.items() if value is not None}
            table.append_column(sa_schema.Column(name, col_type, **col_args))
        if table_metadata.engine == "Dictionary":
            dictionary_metadata = get_dictionary_metadata(self.bind, table.name, schema)
            table.comment = dictionary_metadata.pop("comment", None)
            for key, value in dictionary_metadata.items():
                table.kwargs[key] = value
            return

        table.engine = build_engine(table_metadata.engine_full)
        table.comment = table_metadata.comment or None
        if table.engine is not None:
            table.kwargs["clickhouse_engine"] = table.engine

    def get_columns(self, table_name, schema=None, **_kwargs):
        table_metadata = get_table_metadata(self.bind, table_name, schema)
        if table_metadata.engine == "Dictionary":
            return get_dictionary_columns(self.bind, table_name, schema)
        table_id = full_table(table_name, schema)
        result_set = self.bind.execute(text(f"DESCRIBE TABLE {table_id}"))
        if not result_set:
            raise NoResultFound(f"Table {table_id} does not exist")
        columns = []
        for row in result_set:
            sqla_type = sqla_type_from_name(row.type.replace("\n", ""))
            col = {
                "name": row.name,
                "type": sqla_type,
                "nullable": sqla_type.nullable,
                "autoincrement": False,
                "comment": row.comment or None,
                "clickhouse_codec": row.codec_expression or None,
                "clickhouse_ttl": text(row.ttl_expression) if row.ttl_expression else None,
            }
            if row.default_type == "DEFAULT" and row.default_expression:
                col["server_default"] = text(row.default_expression)
            elif row.default_type == "MATERIALIZED" and row.default_expression:
                col["clickhouse_materialized"] = text(row.default_expression)
            elif row.default_type == "ALIAS" and row.default_expression:
                col["clickhouse_alias"] = text(row.default_expression)
            columns.append(col)
        return columns
