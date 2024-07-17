from dataclasses import dataclass

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind


@dataclass
class Database:
    name: str

    def __init__(self, name: str, kind: EDataSourceKind.ValueType):
        self.kind = kind

        match kind:
            case EDataSourceKind.POSTGRESQL:
                # PostgreSQL implicitly converts all identifiers to lowercase,
                # so we'd better make it first on our own
                self.name = name[:63].lower()
            case EDataSourceKind.CLICKHOUSE:
                self.name = name[:255]
            case EDataSourceKind.MYSQL:
                self.name = name[:63]
            case EDataSourceKind.YDB:
                self.name = name
            case _:
                raise Exception(f'invalid data source: {self.kind}')

    def query_exists(self) -> str:
        match self.kind:
            case EDataSourceKind.POSTGRESQL:
                return f"SELECT 1 FROM pg_database WHERE datname = '{self.name}'"
            case _:
                raise Exception(f'invalid data source: {self.kind}')

    def query_create(self) -> str:
        match self.kind:
            case EDataSourceKind.CLICKHOUSE:
                return f"CREATE DATABASE IF NOT EXISTS {self.name} ENGINE = Memory"
            case EDataSourceKind.POSTGRESQL:
                return f"CREATE DATABASE {self.name}"
            case _:
                raise Exception(f'invalid data source: {self.kind}')

    def missing_database_msg(self) -> str:
        match self.kind:
            case EDataSourceKind.CLICKHOUSE:
                return f"Database {self.name} does not exist"
            case EDataSourceKind.POSTGRESQL:
                return f'database "{self.name}" does not exist'
            case EDataSourceKind.YDB:
                raise Exception("Fix me first in YQ-3315")
            case EDataSourceKind.MYSQL:
                return 'Unknown database'
            case _:
                raise Exception(f'invalid data source: {self.kind}')

    def missing_table_msg(self) -> str:
        match self.kind:
            case EDataSourceKind.CLICKHOUSE:
                return 'table does not exist'
            case EDataSourceKind.POSTGRESQL:
                return 'table does not exist'
            case EDataSourceKind.YDB:
                return 'issues = [{\'Path not found\'}])'
            case EDataSourceKind.MYSQL:
                return 'table does not exist'
            case _:
                raise Exception(f'invalid data source: {self.kind}')

    def __str__(self) -> str:
        return f'database_{self.name}'

    def __hash__(self) -> int:
        return hash(self.name)
