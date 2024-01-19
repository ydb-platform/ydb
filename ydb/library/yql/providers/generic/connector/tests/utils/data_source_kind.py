from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind


def data_source_kind_alias(kind: EDataSourceKind.ValueType) -> str:
    match (kind):
        case EDataSourceKind.CLICKHOUSE:
            return "ch"
        case EDataSourceKind.POSTGRESQL:
            return "pg"
        case _:
            raise Exception(f'invalid data source: {kind}')
