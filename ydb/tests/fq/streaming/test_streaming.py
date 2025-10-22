import logging
import os
import time
import random
import string
import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

logger = logging.getLogger(__name__)


class TestStreamingInYdb(TestYdsBase):

    @pytest.mark.parametrize("kikimr", [{"local_checkpoints": False}, {"local_checkpoints": True}], indirect=True, ids=["sdk_checkpoints", "local_checkpoints"])
    def test_read_topic(self, kikimr):
        sourceName = "test_read_topic" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        self.init_topics(sourceName, create_output=False)

        kikimr.query(f"""
            CREATE EXTERNAL DATA SOURCE `{sourceName}` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION="{os.getenv("YDB_ENDPOINT")}",
                DATABASE_NAME="{os.getenv("YDB_DATABASE")}",
                SHARED_READING="FALSE",
                AUTH_METHOD="NONE");""")

        sql = f"""SELECT time FROM {sourceName}.`{self.input_topic}`
            WITH (
                FORMAT="json_each_row",
                SCHEMA=(time String NOT NULL))
            LIMIT 1"""

        future = kikimr.query_async(sql)
        time.sleep(1)
        data = ['{"time": "lunch time"}']
        self.write_stream(data)
        result_sets = future.result()
        assert result_sets[0].rows[0]['time'] == b'lunch time'

    @pytest.mark.parametrize("kikimr", [{"local_checkpoints": False}, {"local_checkpoints": True}], indirect=True, ids=["sdk_checkpoints", "local_checkpoints"])
    def test_read_topic_shared_reading_limit(self, kikimr):
        sourceName = "test_read_topic_shared_reading_limit" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        self.init_topics(sourceName, create_output=False, partitions_count=10)

        kikimr.query(f"""
            CREATE EXTERNAL DATA SOURCE `{sourceName}` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION="{os.getenv("YDB_ENDPOINT")}",
                DATABASE_NAME="{os.getenv("YDB_DATABASE")}",
                SHARED_READING="TRUE",
                AUTH_METHOD="NONE");""")

        sql = f"""SELECT time FROM {sourceName}.`{self.input_topic}`
            WITH (
                FORMAT="json_each_row",
                SCHEMA=(time String NOT NULL))
            WHERE time like "%lunch%"
            LIMIT 1"""

        future1 = kikimr.query_async(sql)
        future2 = kikimr.query_async(sql)
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

        kikimr.query(f"""
            CREATE EXTERNAL DATA SOURCE `{sourceName}` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION="{os.getenv("YDB_ENDPOINT")}",
                DATABASE_NAME="{os.getenv("YDB_DATABASE")}",
                SHARED_READING="TRUE",
                AUTH_METHOD="NONE");""")

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

        kikimr.query(sql.format(query_name="query1", source_name=sourceName, input_topic=self.input_topic, output_topic=self.output_topic))
        kikimr.query(sql.format(query_name="query2", source_name=sourceName, input_topic=self.input_topic, output_topic=self.output_topic))
        time.sleep(3)
        data = ['{"time": "lunch time"}']
        expected_data = ['lunch time', 'lunch time']
        self.write_stream(data)
        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data

        sql = R'''ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);'''
        kikimr.query(sql.format(query_name="query1"))
        kikimr.query(sql.format(query_name="query2"))

        time.sleep(4)

        data = ['{"time": "next lunch time"}']
        expected_data = ['next lunch time', 'next lunch time']
        self.write_stream(data)

        sql = R'''ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);'''
        kikimr.query(sql.format(query_name="query1"))
        kikimr.query(sql.format(query_name="query2"))
        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data

        sql = R'''DROP STREAMING QUERY `{query_name}`;'''
        kikimr.query(sql.format(query_name="query1"))
        kikimr.query(sql.format(query_name="query2"))
