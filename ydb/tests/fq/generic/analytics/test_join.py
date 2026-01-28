import logging
import pytest

import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all, yq_v1

from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.fq.generic.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.tests.utils.one_time_waiter import OneTimeWaiter
from yql.essentials.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind
import conftest
from google.protobuf.struct_pb2 import NullValue

import boto3
import time

one_time_waiter = OneTimeWaiter(
    data_source_kind=EGenericDataSourceKind.YDB,
    docker_compose_file_path=conftest.docker_compose_file_path,
    expected_tables=["join_table", "dummy_table"],
)


class TestJoinAnalytics:
    @yq_all
    @pytest.mark.parametrize(
        "mvp_external_ydb_endpoint", [{"endpoint": "tests-fq-generic-analytics-ydb:2136"}], indirect=True
    )
    @pytest.mark.parametrize("fq_client", [{"folder_id": "my_folder"}], indirect=True)
    def test_simple(self, fq_client: FederatedQueryClient, settings: Settings):
        table_name = "join_table"
        ch_conn_name = f"ch_conn_{table_name}"
        gp_conn_name = f"gp_conn_{table_name}"
        pg_conn_name = f"pg_conn_{table_name}"
        my_conn_name = f"my_conn_{table_name}"
        ydb_conn_name = f"ydb_conn_{table_name}"
        query_name = f"query_{table_name}"

        fq_client.create_clickhouse_connection(
            name=ch_conn_name,
            database_name=settings.clickhouse.dbname,
            database_id="clickhouse_cluster_id",
            login=settings.clickhouse.username,
            password=settings.clickhouse.password,
        )

        fq_client.create_greenplum_connection(
            name=gp_conn_name,
            database_name=settings.greenplum.dbname,
            database_id="greenplum_cluster_id",
            login=settings.greenplum.username,
            password=settings.greenplum.password,
        )

        fq_client.create_postgresql_connection(
            name=pg_conn_name,
            database_name=settings.postgresql.dbname,
            database_id="postgresql_cluster_id",
            login=settings.postgresql.username,
            password=settings.postgresql.password,
        )

        fq_client.create_mysql_connection(
            name=my_conn_name,
            database_name=settings.mysql.dbname,
            database_id="mysql_cluster_id",
            login=settings.mysql.username,
            password=settings.mysql.password,
        )

        one_time_waiter.wait()

        fq_client.create_ydb_connection(
            name=ydb_conn_name,
            database_id=settings.ydb.dbname,
        )

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

        query_id = fq_client.create_query(query_name, sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        fq_client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = fq_client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 5
        assert result_set.columns[0].name == "data_pg"
        assert result_set.columns[1].name == "data_ch"
        assert result_set.columns[2].name == "data_ydb"
        assert result_set.columns[3].name == "data_gp"
        assert result_set.columns[4].name == "data_my"
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].bytes_value == b"pg10"
        assert result_set.rows[0].items[1].bytes_value == b"ch10"
        assert result_set.rows[0].items[2].bytes_value == b"ydb10"
        assert result_set.rows[0].items[3].bytes_value == b"gp10"
        assert result_set.rows[0].items[4].bytes_value == b"my10"
        assert result_set.rows[1].items[0].bytes_value == b"pg20"
        assert result_set.rows[1].items[1].bytes_value == b"ch20"
        assert result_set.rows[1].items[2].bytes_value == b"ydb20"
        assert result_set.rows[1].items[3].bytes_value == b"gp20"
        assert result_set.rows[1].items[4].bytes_value == b"my20"
        assert result_set.rows[2].items[0].bytes_value == b"pg30"
        assert result_set.rows[2].items[1].bytes_value == b"ch30"
        assert result_set.rows[2].items[2].bytes_value == b"ydb30"
        assert result_set.rows[2].items[3].bytes_value == b"gp30"
        assert result_set.rows[2].items[4].bytes_value == b"my30"

    @yq_v1
    @pytest.mark.parametrize(
        "mvp_external_ydb_endpoint", [{"endpoint": "tests-fq-generic-analytics-ydb:2136"}], indirect=True
    )
    @pytest.mark.parametrize("fq_client", [{"folder_id": "my_folder"}], indirect=True)
    def test_streamlookup(self, fq_client: FederatedQueryClient, settings: Settings):
        table_name = "join_table"
        my_conn_name = f"my_conn_{table_name}"
        ydb_conn_name = f"ydb_conn_{table_name}"
        query_name = f"query_{table_name}"

        fq_client.create_mysql_connection(
            name=my_conn_name,
            database_name=settings.mysql.dbname,
            database_id="mysql_cluster_id",
            login=settings.mysql.username,
            password=settings.mysql.password,
        )

        one_time_waiter.wait()

        fq_client.create_ydb_connection(
            name=ydb_conn_name,
            database_id=settings.ydb.dbname,
        )

        sql = Rf"""
            SELECT ydb.data AS data_ydb, my.data AS data_my
            FROM {my_conn_name}.{table_name} AS my
            LEFT JOIN /*+streamlookup()*/ ANY {ydb_conn_name}.{table_name} AS ydb
            ON my.id = ydb.id
            ORDER BY data_my;
            """

        query_id = fq_client.create_query(query_name, sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        fq_client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = fq_client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "data_ydb"
        assert result_set.columns[1].name == "data_my"
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].bytes_value == b"ydb10"
        assert result_set.rows[0].items[1].bytes_value == b"my10"
        assert result_set.rows[1].items[0].bytes_value == b"ydb20"
        assert result_set.rows[1].items[1].bytes_value == b"my20"
        assert result_set.rows[2].items[0].bytes_value == b"ydb30"
        assert result_set.rows[2].items[1].bytes_value == b"my30"

    @yq_v1
    @pytest.mark.parametrize(
        "mvp_external_ydb_endpoint", [{"endpoint": "tests-fq-generic-analytics-ydb:2136"}], indirect=True
    )
    @pytest.mark.parametrize("fq_client", [{"folder_id": "my_folder"}], indirect=True)
    def test_s3_streamlookup(self, fq_client: FederatedQueryClient, kikimr, s3, settings: Settings, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("wbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        for i in range(5):
            fruit = "id,data\n"
            for k in range(5):
                fruit += str(k) + ",s3_" + str(k) + str(i) + "\n"
            logging.debug(fruit)
            s3_client.put_object(Body=fruit, Bucket='wbucket', Key='fruits' + str(i) + '.csv', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        kikimr.compute_plane.wait_bootstrap()
        storage_connection_name = unique_prefix + "fruitbucket"
        fq_client.create_storage_connection(storage_connection_name, "wbucket")

        time.sleep(10)  # 2 x node info update period

        sql = f'''
            SELECT Fruit, sum(Price) as Price, sum(Weight) as Weight
            FROM `{storage_connection_name}`.`fruits*`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            )) group by Fruit;
            '''

        table_name = "join_table"
        ydb_conn_name = f"ydb_conn_{table_name}"
        query_name = f"query_{table_name}"

        one_time_waiter.wait()

        fq_client.create_ydb_connection(
            name=ydb_conn_name,
            database_id=settings.ydb.dbname,
        )

        sql = Rf"""
            $input = SELECT * FROM `{storage_connection_name}`.`fruits*`
            WITH (format=csv_with_names, SCHEMA (
                id Int NOT NULL,
                data String NOT NULL
            ));

            SELECT ydb.data AS data_ydb, s3.data AS data_s3
            FROM $input AS s3
            LEFT JOIN /*+streamlookup()*/ ANY {ydb_conn_name}.{table_name} AS ydb
            ON s3.id = ydb.id
            ORDER BY data_s3;
            """

        query_id = fq_client.create_query(query_name, sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        fq_client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = fq_client.get_result_data(query_id, limit=1000)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "data_ydb"
        assert result_set.columns[1].name == "data_s3"
        assert len(result_set.rows) == 5*5
        row = 0
        for j in range(5):
            for i in range(5):
                if j >= 1 and j <= 3:
                    assert result_set.rows[row].items[0].bytes_value == b"ydb" + bytes(str(j) + '0', 'utf-8')
                else:
                    assert result_set.rows[row].items[0].null_flag_value == NullValue.NULL_VALUE
                assert result_set.rows[row].items[1].bytes_value == b"s3_" + bytes(str(j) + str(i), 'utf-8')
                row += 1
