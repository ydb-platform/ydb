import logging

import ydb.public.api.protos.ydb_value_pb2 as ydb


class TestMySQL:
    def test_simple(self, client, create_source):
        table_name = 'simple_table'
        conn_name = f'conn_{table_name}'

        create_source(conn_name, "mysql")

        sql = fR'''
            SELECT *
            FROM {conn_name}.{table_name};
            '''

        result_set = client.query(sql)[0]
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "number"
        assert result_set.columns[0].type == ydb.Type(
            optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.INT32))
        )
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == 1
        assert result_set.rows[1][0] == 2
        assert result_set.rows[2][0] == 3
