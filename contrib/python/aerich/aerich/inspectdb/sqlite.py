from __future__ import annotations

from aerich.inspectdb import Column, FieldMapDict, Inspect


class InspectSQLite(Inspect):
    @property
    def field_map(self) -> FieldMapDict:
        return {
            "INTEGER": self.int_field,
            "INT": self.bool_field,
            "SMALLINT": self.smallint_field,
            "VARCHAR": self.char_field,
            "TEXT": self.text_field,
            "TIMESTAMP": self.datetime_field,
            "REAL": self.float_field,
            "BIGINT": self.bigint_field,
            "DATE": self.date_field,
            "TIME": self.time_field,
            "JSON": self.json_field,
            "BLOB": self.binary_field,
        }

    async def get_columns(self, table: str) -> list[Column]:
        columns = []
        sql = f"PRAGMA table_info({table})"
        ret = await self.conn.execute_query_dict(sql)
        columns_index = await self._get_columns_index(table)
        for row in ret:
            try:
                length = row["type"].split("(")[1].split(")")[0]
            except IndexError:
                length = None
            columns.append(
                Column(
                    name=row["name"],
                    data_type=row["type"].split("(")[0],
                    null=row["notnull"] == 0,
                    default=row["dflt_value"],
                    length=length,
                    pk=row["pk"] == 1,
                    unique=columns_index.get(row["name"]) == "unique",
                    index=columns_index.get(row["name"]) == "index",
                )
            )
        return columns

    async def _get_columns_index(self, table: str) -> dict[str, str]:
        sql = f"PRAGMA index_list ({table})"
        indexes = await self.conn.execute_query_dict(sql)
        ret = {}
        for index in indexes:
            sql = f"PRAGMA index_info({index['name']})"
            index_info = (await self.conn.execute_query_dict(sql))[0]
            ret[index_info["name"]] = "unique" if index["unique"] else "index"
        return ret

    async def get_all_tables(self) -> list[str]:
        sql = "select tbl_name from sqlite_master where type='table' and name!='sqlite_sequence'"
        ret = await self.conn.execute_query_dict(sql)
        return list(map(lambda x: x["tbl_name"], ret))
