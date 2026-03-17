from __future__ import annotations

import pathlib
from importlib import resources
from typing import cast

from typing_extensions import LiteralString

from procrastinate import connector as connector_module

migrations_path = pathlib.Path(__file__).parent / "sql" / "migrations"


class SchemaManager:
    def __init__(self, connector: connector_module.BaseConnector):
        self.connector = connector

    @staticmethod
    def get_schema() -> LiteralString:
        # procrastinate takes full responsibility for the queries, we
        # can safely vouch for them being as safe as if they were
        # defined in the code itself.
        schema_sql = (resources.files("procrastinate.sql") / "schema.sql").read_text(
            encoding="utf-8"
        )
        return cast(LiteralString, schema_sql)

    @staticmethod
    def get_migrations_path() -> str:
        return str(migrations_path)

    def apply_schema(self) -> None:
        queries = self.get_schema()
        queries = queries.replace("%", "%%")
        self.connector.get_sync_connector().execute_query(query=queries)

    async def apply_schema_async(self) -> None:
        queries = self.get_schema()
        queries = queries.replace("%", "%%")
        await self.connector.execute_query_async(query=queries)
