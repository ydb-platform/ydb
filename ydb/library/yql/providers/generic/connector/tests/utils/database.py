from dataclasses import dataclass
from random import choice
from string import ascii_lowercase, digits

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind


@dataclass
class Database:
    name: str

    def __init__(self):
        self.name = choice(ascii_lowercase)
        self.name += ''.join(choice(ascii_lowercase + digits + '_') for i in range(8))

    @classmethod
    def make_for_data_source_kind(cls, kind: EDataSourceKind.ValueType):
        match kind:
            case EDataSourceKind.CLICKHOUSE:
                return Database()
            case EDataSourceKind.POSTGRESQL:
                return Database()
            case _:
                raise Exception(f'invalid data source: {kind}')

    def exists(self, kind: EDataSourceKind.ValueType) -> str:
        match kind:
            case EDataSourceKind.POSTGRESQL:
                return f"SELECT 1 FROM pg_database WHERE datname = '{self.name}'"
            case _:
                raise Exception(f'invalid data source: {kind}')

    def create(self, kind: EDataSourceKind.ValueType) -> str:
        match kind:
            case EDataSourceKind.CLICKHOUSE:
                return f'CREATE DATABASE IF NOT EXISTS {self.name} ENGINE = Memory'
            case EDataSourceKind.POSTGRESQL:
                return f"CREATE DATABASE {self.name}"
            case _:
                raise Exception(f'invalid data source: {kind}')

    def sql_table_name(self, table_name: str) -> str:
        return table_name

    def missing_database_msg(self, kind: EDataSourceKind.ValueType) -> str:
        match kind:
            case EDataSourceKind.CLICKHOUSE:
                return f"Database {self.name} doesn't exist"
            case EDataSourceKind.POSTGRESQL:
                return f'database "{self.name}" does not exist'
            case _:
                raise Exception(f'invalid data source: {kind}')

    def missing_table_msg(self, kind: EDataSourceKind.ValueType) -> str:
        match kind:
            case EDataSourceKind.CLICKHOUSE:
                return 'table does not exist'
            case EDataSourceKind.POSTGRESQL:
                return 'table does not exist'
            case _:
                raise Exception(f'invalid data source: {kind}')

    def __str__(self) -> str:
        return f'database_{self.name}'

    def __hash__(self) -> int:
        return hash(self.name)
