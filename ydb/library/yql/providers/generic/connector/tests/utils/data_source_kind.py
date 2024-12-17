from yql.essentials.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind


def data_source_kind_alias(kind: EGenericDataSourceKind.ValueType) -> str:
    match (kind):
        case EGenericDataSourceKind.CLICKHOUSE:
            return "ch"
        case EGenericDataSourceKind.POSTGRESQL:
            return "pg"
        case _:
            raise Exception(f'invalid data source: {kind}')
