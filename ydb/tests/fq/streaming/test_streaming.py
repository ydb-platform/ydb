import logging
import os
import time

from ydb.tests.fq.streaming.base import StreamingImportTestBase
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

logger = logging.getLogger(__name__)


class TestStreamingInYdb(StreamingImportTestBase, TestYdsBase):

    def test_read_topic(self):
        self.init_topics("topic_name1", create_output=False)

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `sourceName` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION="{os.getenv("YDB_ENDPOINT")}",
                DATABASE_NAME="{os.getenv("YDB_DATABASE")}",
                SHARED_READING="TRUE",
                AUTH_METHOD="NONE");""")

        sql = f"""SELECT time FROM sourceName.`{self.input_topic}`
            WITH (
                FORMAT="json_each_row",
                SCHEMA=(time String NOT NULL))
            LIMIT 1"""

        future = self.ydb_client.query_async(sql)
        time.sleep(1)
        data = ['{"time": "lunch time"}']
        self.write_stream(data)
        # time.sleep(4)
        result_sets = future.result()
        assert result_sets[0].rows[0]['time'] == b'lunch time'
