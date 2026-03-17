from __future__ import annotations

import re
from typing import TYPE_CHECKING

from aerich.inspectdb import Column, FieldMapDict, Inspect

if TYPE_CHECKING:
    from tortoise.backends.base_postgres.client import BasePostgresClient


class InspectPostgres(Inspect):
    def __init__(self, conn: BasePostgresClient, tables: list[str] | None = None) -> None:
        super().__init__(conn, tables)
        self.schema = conn.server_settings.get("schema") or "public"

    @property
    def field_map(self) -> FieldMapDict:
        return {
            "int2": self.smallint_field,
            "int4": self.int_field,
            "int8": self.bigint_field,
            "smallint": self.smallint_field,
            "bigint": self.bigint_field,
            "varchar": self.char_field,
            "text": self.text_field,
            "timestamptz": self.datetime_field,
            "float4": self.float_field,
            "float8": self.float_field,
            "date": self.date_field,
            "time": self.time_field,
            "decimal": self.decimal_field,
            "numeric": self.decimal_field,
            "uuid": self.uuid_field,
            "jsonb": self.json_field,
            "bytea": self.binary_field,
            "bool": self.bool_field,
            "timestamp": self.datetime_field,
        }

    async def get_all_tables(self) -> list[str]:
        sql = "select TABLE_NAME from information_schema.TABLES where table_catalog=$1 and table_schema=$2"
        ret = await self.conn.execute_query_dict(sql, [self.database, self.schema])
        return list(map(lambda x: x["table_name"], ret))

    async def get_columns(self, table: str) -> list[Column]:
        columns = []
        sql = f"""select c.column_name,
       col_description('public.{table}'::regclass, ordinal_position) as column_comment,
       t.constraint_type as column_key,
       udt_name as data_type,
       is_nullable,
       column_default,
       character_maximum_length,
       numeric_precision,
       numeric_scale
from information_schema.constraint_column_usage const
         join information_schema.table_constraints t
              using (table_catalog, table_schema, table_name, constraint_catalog, constraint_schema, constraint_name)
         right join information_schema.columns c using (column_name, table_catalog, table_schema, table_name)
where c.table_catalog = $1
  and c.table_name = $2
  and c.table_schema = $3"""  # nosec:B608
        if "psycopg" in str(type(self.conn)).lower():
            sql = re.sub(r"\$[123]", "%s", sql)
        ret = await self.conn.execute_query_dict(sql, [self.database, table, self.schema])
        for row in ret:
            columns.append(
                Column(
                    name=row["column_name"],
                    data_type=row["data_type"],
                    null=row["is_nullable"] == "YES",
                    default=row["column_default"],
                    length=row["character_maximum_length"],
                    max_digits=row["numeric_precision"],
                    decimal_places=row["numeric_scale"],
                    comment=row["column_comment"],
                    pk=row["column_key"] == "PRIMARY KEY",
                    unique=False,  # can't get this simply
                    index=False,  # can't get this simply
                )
            )
        return columns
