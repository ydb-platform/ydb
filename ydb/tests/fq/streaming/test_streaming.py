import logging
import os
import time
import random
import string
import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

logger = logging.getLogger(__name__)


class TestStreamingInYdb(TestYdsBase):

    def create_source(self, kikimr, sourceName, shared=False):
        kikimr.YdbClient.query(f"""
            CREATE EXTERNAL DATA SOURCE `{sourceName}` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION="{os.getenv("YDB_ENDPOINT")}",
                DATABASE_NAME="{os.getenv("YDB_DATABASE")}",
                SHARED_READING="{shared}",
                AUTH_METHOD="NONE");""")

    @pytest.mark.parametrize("kikimr", [{"local_checkpoints": False}, {"local_checkpoints": True}], indirect=True, ids=["sdk_checkpoints", "local_checkpoints"])
    def test_read_topic(self, kikimr):
        sourceName = "test_read_topic" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        self.init_topics(sourceName, create_output=False)

        self.create_source(kikimr, sourceName, False)
        sql = f"""SELECT time FROM {sourceName}.`{self.input_topic}`
            WITH (
                FORMAT="json_each_row",
                SCHEMA=(time String NOT NULL))
            LIMIT 1"""

        future = kikimr.YdbClient.query_async(sql)
        time.sleep(1)
        data = ['{"time": "lunch time"}']
        self.write_stream(data)
        result_sets = future.result()
        assert result_sets[0].rows[0]['time'] == b'lunch time'

    @pytest.mark.parametrize("kikimr", [{"local_checkpoints": False}, {"local_checkpoints": True}], indirect=True, ids=["sdk_checkpoints", "local_checkpoints"])
    def test_read_topic_shared_reading_limit(self, kikimr):
        sourceName = "test_read_topic_shared_reading_limit" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        self.init_topics(sourceName, create_output=False, partitions_count=10)

        self.create_source(kikimr, sourceName, True)
        sql = f"""SELECT time FROM {sourceName}.`{self.input_topic}`
            WITH (
                FORMAT="json_each_row",
                SCHEMA=(time String NOT NULL))
            WHERE time like "%lunch%"
            LIMIT 1"""

        future1 = kikimr.YdbClient.query_async(sql)
        future2 = kikimr.YdbClient.query_async(sql)
        time.sleep(3)
        data = ['{"time": "lunch time"}']
        self.write_stream(data)
        result_sets1 = future1.result()
        result_sets2 = future2.result()
        assert result_sets1[0].rows[0]['time'] == b'lunch time'
        assert result_sets2[0].rows[0]['time'] == b'lunch time'

    @pytest.mark.parametrize("kikimr", [{"local_checkpoints": False}, {"local_checkpoints": True}], indirect=True, ids=["sdk_checkpoints", "local_checkpoints"])
    def test_read_topic_shared_reading_insert_to_topic(self, kikimr):
        sourceName = "source3_" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        self.init_topics(sourceName, partitions_count=10)
        self.create_source(kikimr, sourceName, True)

        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $in = SELECT time FROM {source_name}.`{input_topic}`
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(time String NOT NULL))
                WHERE time like "%lunch%";
                INSERT INTO {source_name}.`{output_topic}` SELECT time FROM $in;
            END DO;'''

        kikimr.YdbClient.query(sql.format(query_name="query1", source_name=sourceName, input_topic=self.input_topic, output_topic=self.output_topic))
        kikimr.YdbClient.query(sql.format(query_name="query2", source_name=sourceName, input_topic=self.input_topic, output_topic=self.output_topic))
        time.sleep(3)
        data = ['{"time": "lunch time"}']
        expected_data = ['lunch time', 'lunch time']
        self.write_stream(data)
        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data

        # TODO
        # sql = R'''ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);'''
        # kikimr.YdbClient.query(sql.format(query_name="query1"))
        # kikimr.YdbClient.query(sql.format(query_name="query2"))

        # time.sleep(4)

        # data = ['{"time": "next lunch time"}']
        # expected_data = ['next lunch time', 'next lunch time']
        # self.write_stream(data)

        # sql = R'''ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);'''
        # kikimr.YdbClient.query(sql.format(query_name="query1"))
        # kikimr.YdbClient.query(sql.format(query_name="query2"))
        # assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data

        # sql = R'''DROP STREAMING QUERY `{query_name}`;'''
        # kikimr.YdbClient.query(sql.format(query_name="query1"))
        # kikimr.YdbClient.query(sql.format(query_name="query2"))

    # TODO
    # @pytest.mark.parametrize("kikimr", [{"local_checkpoints": False}, {"local_checkpoints": True}], indirect=True, ids=["sdk_checkpoints", "local_checkpoints"])
    # def test_read_topic_shared_reading_restart_nodes(self, kikimr):
    #     sourceName = "source4_" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    #     self.init_topics(sourceName, partitions_count=10)
    #     self.create_source(kikimr, sourceName, True)
    #     sql = R'''
    #         CREATE STREAMING QUERY `{query_name}` AS
    #         DO BEGIN
    #             pragma FeatureR010="prototype";
    #             PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;

    #             $in = SELECT * FROM {source_name}.`{input_topic}`
    #                 WITH (
    #                     FORMAT="json_each_row",
    #                     SCHEMA=(dt UINT64, str STRING));
    #             $mr = SELECT * FROM $in
    #                 MATCH_RECOGNIZE(
    #                     MEASURES
    #                     LAST(A.dt) as dt_begin,
    #                     LAST(C.dt) as dt_end,
    #                     LAST(A.str) as a_str,
    #                     LAST(B.str) as b_str,
    #                     LAST(C.str) as c_str
    #                     ONE ROW PER MATCH
    #                     PATTERN ( A B C )
    #                     DEFINE
    #                         A as A.str='A',
    #                         B as B.str='B',
    #                         C as C.str='C');
    #             INSERT INTO {source_name}.`{output_topic}`
    #                 SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $mr;
    #         END DO;'''

    #     kikimr.YdbClient.query(sql.format(query_name="query1", source_name=sourceName, input_topic=self.input_topic, output_topic=self.output_topic))

    #     time.sleep(4)
    #     data = [
    #         '{"dt": 1696849942000001, "str": "A" }',
    #         '{"dt": 1696849942500001, "str": "B" }',
    #         '{"dt": 1696849943000001, "str": "C" }'
    #     ]
    #     self.write_stream(data)
    #     time.sleep(2)
    #     logging.debug("Stop node 1")

    #     node = kikimr.Cluster.nodes[1]
    #     node.kill()
    #     logging.debug("Start node 1")
    #     node.start()
    #     time.sleep(4)

    #     data = [
    #         '{"dt": 1696849942000001, "str": "A" }',
    #         '{"dt": 1696849942500001, "str": "B" }',
    #         '{"dt": 1696849943000001, "str": "C" }'
    #         ]
    #     self.write_stream(data)

    #     time.sleep(30)
