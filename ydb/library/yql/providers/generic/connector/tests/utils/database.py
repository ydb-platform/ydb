from dataclasses import dataclass

from yql.essentials.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind


@dataclass
class Database:
    name: str

    def __init__(self, name: str, kind: EGenericDataSourceKind.ValueType):
        self.kind = kind

        match kind:
            case EGenericDataSourceKind.POSTGRESQL:
                # PostgreSQL implicitly converts all identifiers to lowercase,
                # so we'd better make it first on our own
                self.name = name[:63].lower()
            case EGenericDataSourceKind.CLICKHOUSE:
                # We use preinitialized database when working with ClickHouse.
                self.name = "db"
            case EGenericDataSourceKind.MS_SQL_SERVER:
                # For this kind of database this name is provided by the external logic
                self.name = name
            case EGenericDataSourceKind.MYSQL:
                # For this kind of database this name is provided by the external logic
                self.name = name
            case EGenericDataSourceKind.ORACLE:
                # Oracle is not sensitive for identifiers until they are inclosed in quota marks,
                # therefore, we'd better use uppercase for ease of testing
                self.name = name[:127].upper()  # TODO: is it needed? max length of Oracle table name is 128 bytes/chars
            case EGenericDataSourceKind.YDB:
                # We use preinitialized database when working with YDB.
                self.name = "local"
            case _:
                raise Exception(f'invalid data source: {self.kind}')

    def exists(self) -> str:
        match self.kind:
            case EGenericDataSourceKind.POSTGRESQL:
                return f"SELECT 1 FROM pg_database WHERE datname = '{self.name}'"
            case _:
                raise Exception(f'invalid data source: {self.kind}')

    def create(self) -> str:
        match self.kind:
            case EGenericDataSourceKind.POSTGRESQL:
                return f"CREATE DATABASE {self.name}"
            case _:
                raise Exception(f'invalid data source: {self.kind}')

    def sql_table_name(self, table_name: str) -> str:
        return table_name

    def missing_database_msg(self) -> str:
        match self.kind:
            case EGenericDataSourceKind.CLICKHOUSE:
                return f"Database {self.name} doesn't exist"
            case EGenericDataSourceKind.POSTGRESQL:
                return f'database "{self.name}" does not exist'
            case EGenericDataSourceKind.YDB:
                raise Exception("Fix me first in YQ-3315")
            case EGenericDataSourceKind.MS_SQL_SERVER:
                return 'Cannot open database'
            case EGenericDataSourceKind.MYSQL:
                return 'Unknown database'
            case EGenericDataSourceKind.ORACLE:
                raise Exception("Fix me first in YQ-3413")
            case _:
                raise Exception(f'invalid data source: {self.kind}')

    def missing_table_msg(self) -> str:
        match self.kind:
            case EGenericDataSourceKind.CLICKHOUSE:
                return 'table does not exist'
            case EGenericDataSourceKind.POSTGRESQL:
                return 'table does not exist'
            case EGenericDataSourceKind.YDB:
                return 'issues = [{\'Path not found\'}])'
            case EGenericDataSourceKind.MS_SQL_SERVER:
                return 'table does not exist'
            case EGenericDataSourceKind.MYSQL:
                return 'table does not exist'
            case EGenericDataSourceKind.ORACLE:
                return 'table does not exist'
            case _:
                raise Exception(f'invalid data source: {self.kind}')

    def __str__(self) -> str:
        return f'database_{self.name}'

    def __hash__(self) -> int:
        return hash(self.name)
