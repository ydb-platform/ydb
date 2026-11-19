import pytest
import ydb.public.api.protos.ydb_value_pb2 as ydb_value


class TestSqlValidation:
    def test_invalid_sql(self, client):
        result = client.execute('SELECT FROM;').check(False)
        assert result.error

    @pytest.mark.parametrize('depth', [1, 8, 16])
    def test_nested_issues(self, client, depth):
        # The nested issue tree must also fit the protobuf recursion limit.
        sql = '$t_0 = SELECT 1 - "f" AS data;\n'
        for i in range(depth):
            sql += f'$t_{i + 1} = SELECT data FROM $t_{i} WHERE data != {i};\n'
        result = client.execute(sql + f'SELECT data FROM $t_{depth};').check(False)
        assert 'Cannot substract' in result.error, result.error
        assert any(issue.issues for issue in result.exception.issues)

    @pytest.mark.parametrize('depth', [1, 8, 16])
    def test_nested_type(self, client, depth):
        # Every struct adds several protobuf messages to its schema. Keep the
        # response below the protobuf decoder's recursion limit.
        sql = '$t_0 = SELECT CAST(1 AS Int32) AS data;\n'
        for i in range(depth):
            sql += f'$t_{i + 1} = SELECT <|data:data|> AS data FROM $t_{i};\n'
        result = client.execute(sql + f'SELECT data FROM $t_{depth};').check().result_set()
        assert len(result.columns) == 1
        assert result.columns[0].name == 'data'
        assert len(result.rows) == 1
        column_type = result.columns[0].type
        value = result.rows[0]['data']
        for _ in range(depth):
            assert column_type.WhichOneof('type') == 'struct_type'
            members = column_type.struct_type.members
            assert len(members) == 1
            assert members[0].name == 'data'
            column_type = members[0].type
            assert list(value) == ['data']
            value = value['data']
        assert column_type.type_id == ydb_value.Type.INT32
        assert value == 1
