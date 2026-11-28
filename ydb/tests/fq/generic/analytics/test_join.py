import logging


from ydb.library.yql.providers.generic.connector.tests.utils.one_time_waiter import OneTimeWaiter
from yql.essentials.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind
import conftest

one_time_waiter = OneTimeWaiter(
    data_source_kind=EGenericDataSourceKind.YDB,
    docker_compose_file_path=conftest.docker_compose_file_path,
    expected_tables=["join_table", "dummy_table"],
)


class TestJoinAnalytics:
    def test_simple(self, client, create_source):
        table_name = "join_table"
        ch_conn_name = f"ch_conn_{table_name}"
        gp_conn_name = f"gp_conn_{table_name}"
        pg_conn_name = f"pg_conn_{table_name}"
        my_conn_name = f"my_conn_{table_name}"
        ydb_conn_name = f"ydb_conn_{table_name}"

        create_source(ch_conn_name, "clickhouse")

        create_source(gp_conn_name, "greenplum")

        create_source(pg_conn_name, "postgresql")

        create_source(my_conn_name, "mysql")

        one_time_waiter.wait()

        create_source(ydb_conn_name, "ydb")

        sql = Rf"""
            SELECT pg.data AS data_pg, ch.data AS data_ch, ydb.data AS data_ydb, gp.data AS data_gp, my.data AS data_my
            FROM {pg_conn_name}.{table_name} AS pg
            JOIN {ch_conn_name}.{table_name} AS ch
            ON pg.id = ch.id
            JOIN {ydb_conn_name}.{table_name} AS ydb
            ON pg.id = ydb.id
            JOIN {gp_conn_name}.{table_name} AS gp
            ON pg.id = gp.id
            JOIN {my_conn_name}.{table_name} AS my
            ON pg.id = my.id
            ORDER BY data_pg;
            """

        result_set = client.query(sql)[0]
        logging.debug(str(result_set))
        assert len(result_set.columns) == 5
        assert result_set.columns[0].name == "data_pg"
        assert result_set.columns[1].name == "data_ch"
        assert result_set.columns[2].name == "data_ydb"
        assert result_set.columns[3].name == "data_gp"
        assert result_set.columns[4].name == "data_my"
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == b"pg10"
        assert result_set.rows[0][1] == b"ch10"
        assert result_set.rows[0][2] == b"ydb10"
        assert result_set.rows[0][3] == b"gp10"
        assert result_set.rows[0][4] == b"my10"
        assert result_set.rows[1][0] == b"pg20"
        assert result_set.rows[1][1] == b"ch20"
        assert result_set.rows[1][2] == b"ydb20"
        assert result_set.rows[1][3] == b"gp20"
        assert result_set.rows[1][4] == b"my20"
        assert result_set.rows[2][0] == b"pg30"
        assert result_set.rows[2][1] == b"ch30"
        assert result_set.rows[2][2] == b"ydb30"
        assert result_set.rows[2][3] == b"gp30"
        assert result_set.rows[2][4] == b"my30"
