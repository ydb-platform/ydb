import hashlib

from typing import Sequence

from ydb.library.yql.providers.generic.connector.tests.utils.schema import Schema
from ydb.public.api.protos.ydb_value_pb2 import Type


def generate_table_data(schema: Schema, bytes_soft_limit: int) -> Sequence[Sequence]:
    rows = []

    ix = 0
    actual_size = 0

    while actual_size < bytes_soft_limit:
        row = []

        for col in schema.columns:
            match col.ydb_type:
                case Type.INT64:
                    row.append(ix)
                    actual_size += 8
                case Type.UTF8:
                    value = hashlib.md5(str(ix).encode('ascii')).hexdigest()
                    row.append(value)
                    actual_size += len(value)
                case _:
                    raise ValueError(f'unexpected type {col.ydb_type}')

        rows.append(row)

        ix += 1

    return rows
