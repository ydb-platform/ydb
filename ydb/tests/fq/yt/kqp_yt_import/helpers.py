from typing import Optional

import google.protobuf.text_format as proto
import ydb.public.api.protos.ydb_value_pb2 as ydb

from ydb.tests.fq.tools.kqprun import KqpRun


ValueByTypeExtractors = {
    ydb.Type.PrimitiveTypeId.INT64: lambda x: x.int64_value,
    ydb.Type.PrimitiveTypeId.STRING: lambda x: x.bytes_value,
}


def add_sample_table(kqp_run: KqpRun, table_name: str = 'input', infer_schema: bool = True):
    attrs: Optional[str] = None
    if not infer_schema:
        attrs = """
            {"_yql_row_spec" = {
                "Type" = ["StructType"; [
                    ["key"; ["DataType"; "String"]];
                    ["subkey"; ["DataType"; "Int64"]];
                    ["value"; ["DataType"; "String"]];
                ]]
            }}
        """

    kqp_run.add_table(table_name, [
        '{"key"="075";"subkey"=1;"value"="abc"};',
        '{"key"="800";"subkey"=2;"value"="ddd"};',
        '{"key"="020";"subkey"=3;"value"="q"};',
        '{"key"="150";"subkey"=4;"value"="qzz"};'
    ], attrs)


def validate_sample_result(result: str):
    result_set = ydb.ResultSet()
    proto.Parse(result, result_set)

    columns = [
        ('key', ydb.Type.PrimitiveTypeId.STRING),
        ('subkey', ydb.Type.PrimitiveTypeId.INT64),
        ('value', ydb.Type.PrimitiveTypeId.STRING)
    ]

    assert len(result_set.columns) == len(columns)
    for i, (column_name, column_type_id) in enumerate(columns):
        assert result_set.columns[i].name == column_name

        result_column_type = result_set.columns[i].type.type_id
        assert result_column_type == column_type_id, f'{result_column_type} != {column_type_id}'

    rows = [
        (b'075', 1, b'abc'),
        (b'800', 2, b'ddd'),
        (b'020', 3, b'q'),
        (b'150', 4, b'qzz')
    ]

    assert len(result_set.rows) == len(rows)
    for i, row in enumerate(rows):
        for j, expected_value in enumerate(row):
            value_extractor = ValueByTypeExtractors[result_set.columns[j].type.type_id]
            result_value = value_extractor(result_set.rows[i].items[j])
            assert result_value == expected_value, f'{result_value} != {expected_value}'
