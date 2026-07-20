from typing import Any

from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.driver import create_client
from clickhouse_connect.driver.query import QueryResult


class Connection:
    """
    See :ref:`https://peps.python.org/pep-0249/`
    """

    def __init__(
        self,
        dsn: str | None = None,
        username: str = "",
        password: str = "",
        host: str | None = None,
        database: str | None = None,
        interface: str | None = None,
        port: int | None = None,
        secure: bool | str = False,
        **kwargs: Any,
    ):
        self.client = create_client(
            host=host,
            username=username,
            password=password,
            database=database,
            interface=interface,
            port=port,
            secure=secure,
            dsn=dsn,
            generic_args=kwargs,
        )

        self.client._add_integration_tag("sqlalchemy")
        self.timezone = self.client.server_tz

    def close(self) -> None:
        self.client.close()

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def command(self, cmd: str) -> Any:
        return self.client.command(cmd)

    def raw_query(self, query: str) -> QueryResult:
        return self.client.query(query)

    def cursor(self) -> Cursor:
        return Cursor(self.client)
