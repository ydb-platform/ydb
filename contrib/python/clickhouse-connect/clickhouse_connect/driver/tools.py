import asyncio
from collections.abc import Sequence
from typing import Any

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.binding import quote_identifier
from clickhouse_connect.driver.summary import QuerySummary


def insert_file(
    client: Client,
    table: str,
    file_path: str,
    fmt: str | None = None,
    column_names: Sequence[str] | None = None,
    database: str | None = None,
    settings: dict[str, Any] | None = None,
    compression: str | None = None,
) -> QuerySummary:
    if not database and table[0] not in ("`", "'") and table.find(".") > 0:
        full_table = table
    elif database:
        full_table = f"{quote_identifier(database)}.{quote_identifier(table)}"
    else:
        full_table = quote_identifier(table)
    if not fmt:
        fmt = "CSV" if column_names else "CSVWithNames"
    if compression is None:
        if file_path.endswith(".gzip") or file_path.endswith(".gz"):
            compression = "gzip"
    with open(file_path, "rb") as file:
        return client.raw_insert(
            full_table,
            column_names=column_names,
            insert_block=file,
            fmt=fmt,
            settings=settings,
            compression=compression,
        )


async def insert_file_async(
    client,
    table: str,
    file_path: str,
    fmt: str | None = None,
    column_names: Sequence[str] | None = None,
    database: str | None = None,
    settings: dict[str, Any] | None = None,
    compression: str | None = None,
) -> QuerySummary:

    if not database and table[0] not in ("`", "'") and table.find(".") > 0:
        full_table = table
    elif database:
        full_table = f"{quote_identifier(database)}.{quote_identifier(table)}"
    else:
        full_table = quote_identifier(table)
    if not fmt:
        fmt = "CSV" if column_names else "CSVWithNames"
    if compression is None:
        if file_path.endswith(".gzip") or file_path.endswith(".gz"):
            compression = "gzip"

    def read_file():
        with open(file_path, "rb") as file:
            return file.read()

    file_data = await asyncio.to_thread(read_file)

    return await client.raw_insert(
        full_table,
        column_names=column_names,
        insert_block=file_data,
        fmt=fmt,
        settings=settings,
        compression=compression,
    )
