from __future__ import annotations

from aerich.inspectdb import Column, FieldMapDict, Inspect


class InspectMySQL(Inspect):
    @property
    def field_map(self) -> FieldMapDict:
        return {
            "int": self.int_field,
            "smallint": self.smallint_field,
            "tinyint": self.bool_field,
            "bigint": self.bigint_field,
            "varchar": self.char_field,
            "char": self.uuid_field,
            "longtext": self.text_field,
            "text": self.text_field,
            "datetime": self.datetime_field,
            "float": self.float_field,
            "double": self.float_field,
            "date": self.date_field,
            "time": self.time_field,
            "decimal": self.decimal_field,
            "json": self.json_field,
            "longblob": self.binary_field,
        }

    async def get_all_tables(self) -> list[str]:
        sql = "select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA=%s"
        ret = await self.conn.execute_query_dict(sql, [self.database])
        return list(map(lambda x: x["TABLE_NAME"], ret))

    async def get_columns(self, table: str) -> list[Column]:
        columns = []
        sql = """select c.*, s.NON_UNIQUE, s.INDEX_NAME
from information_schema.COLUMNS c
         left join information_schema.STATISTICS s on c.TABLE_NAME = s.TABLE_NAME
    and c.TABLE_SCHEMA = s.TABLE_SCHEMA
    and c.COLUMN_NAME = s.COLUMN_NAME
where c.TABLE_SCHEMA = %s
  and c.TABLE_NAME = %s"""
        ret = await self.conn.execute_query_dict(sql, [self.database, table])
        for row in ret:
            unique = index = False
            if (non_unique := row["NON_UNIQUE"]) is not None:
                unique = not non_unique
            elif row["COLUMN_KEY"] == "UNI":
                unique = True
            if (index_name := row["INDEX_NAME"]) is not None:
                index = index_name != "PRIMARY"
            columns.append(
                Column(
                    name=row["COLUMN_NAME"],
                    data_type=row["DATA_TYPE"],
                    null=row["IS_NULLABLE"] == "YES",
                    default=row["COLUMN_DEFAULT"],
                    pk=row["COLUMN_KEY"] == "PRI",
                    comment=row["COLUMN_COMMENT"],
                    unique=unique,
                    extra=row["EXTRA"],
                    index=index,
                    length=row["CHARACTER_MAXIMUM_LENGTH"],
                    max_digits=row["NUMERIC_PRECISION"],
                    decimal_places=row["NUMERIC_SCALE"],
                )
            )
        return columns
