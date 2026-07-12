from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql.ddl import DDL

from clickhouse_connect.driver.binding import format_str, quote_identifier


class CreateDatabase(DDL):
    """
    SqlAlchemy DDL statement that is essentially an alternative to the built in CreateSchema DDL class
    """

    def __init__(
        self,
        name: str,
        engine: str = None,
        zoo_path: str = None,
        shard_name: str = "{shard}",
        replica_name: str = "{replica}",
        exists_ok: bool = False,
    ):
        """
        :param name: Database name
        :param engine: Database ClickHouse engine type
        :param zoo_path: ClickHouse zookeeper path for Replicated database engine
        :param shard_name: Clickhouse shard name for Replicated database engine
        :param replica_name: Replica name for Replicated database engine
        """
        if engine and engine not in ("Ordinary", "Atomic", "Lazy", "Replicated"):
            raise ArgumentError(f"Unrecognized engine type {engine}")
        stmt = f"CREATE DATABASE {'IF NOT EXISTS ' if exists_ok else ''}{quote_identifier(name)}"
        if engine:
            stmt += f" Engine {engine}"
            if engine == "Replicated":
                if not zoo_path:
                    raise ArgumentError("zoo_path is required for Replicated Database Engine")
                stmt += f" ({format_str(zoo_path)}, {format_str(shard_name)}, {format_str(replica_name)})"
        super().__init__(stmt)


class DropDatabase(DDL):
    """
    Alternative DDL statement for built in SqlAlchemy DropSchema DDL class
    """

    def __init__(self, name: str, missing_ok: bool = False):
        super().__init__(f"DROP DATABASE {'IF EXISTS ' if missing_ok else ''}{quote_identifier(name)}")
