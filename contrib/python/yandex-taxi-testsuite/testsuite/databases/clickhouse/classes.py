import dataclasses
import pathlib


@dataclasses.dataclass(frozen=True)
class ConnectionInfo:
    """Clickhouse connection parameters"""

    host: str
    tcp_port: int
    http_port: int
    dbname: str | None = None

    def replace(self, **kwargs) -> 'ConnectionInfo':
        """Returns new instance with attrs updated"""
        return dataclasses.replace(self, **kwargs)


@dataclasses.dataclass(frozen=True)
class DatabaseConfig:
    dbname: str
    migrations: list[pathlib.Path]


DatabasesDict = dict[str, DatabaseConfig]
