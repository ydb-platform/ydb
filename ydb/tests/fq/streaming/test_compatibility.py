import logging
import pytest
import time
from typing import Callable

from ydb.tests.fq.streaming_common.common import StreamingTestBase
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

logger = logging.getLogger(__name__)


class SimpleTest(TestYdsBase):
    Id = 0

    def __init__(self, prefix):
        self.prefix = prefix
        self.id = SimpleTest.Id
        self.filter = self.get_name()
        SimpleTest.Id += 1

    def get_name(self):
        return f"{self.prefix}_test666_{self.id}"

    def get_path(self):
        return f"/Root/{self.get_name()}"

    def get_query_text(self, inp, out):
        name = self.get_name()
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                ''' + R'''$in = SELECT time FROM {inp}
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(time String NOT NULL))
                WHERE time like "%{filter}%";
                INSERT INTO {out} SELECT time FROM $in;
            END DO;'''
        return sql.format(query_name=name, inp=inp, out=out, filter=self.filter)

    def get_test_data(self):
        input_data = [f'{{"time": "{self.filter} time"}}']
        expected_data = [f'{self.filter} time']
        return input_data, expected_data


Queries = [SimpleTest("test_compatibility_")]


class TestStreamingCompatibility(StreamingTestBase):

    @pytest.mark.parametrize("kikimr", [{"is_compatibility_tests": True}], indirect=["kikimr"])
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_compatibility(self: StreamingTestBase, kikimr, entity_name: Callable[[str], str], local_topics: bool) -> None:
        inp, out, endpoint = self.get_io_names(kikimr, "test_compatibility", local_topics, entity_name, partitions_count=10)

        for query in Queries:
            logger.debug(f"Start query {query.get_name()}")
            kikimr.ydb_client.query(query.get_query_text(inp, out))
            self.wait_completed_checkpoints(kikimr, query.get_path())

        for query in Queries:
            logger.debug(f"Test data for query {query.get_name()}")
            data, expected_data = query.get_test_data()
            self.write_stream(data, endpoint=endpoint)
            assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data
            self.wait_completed_checkpoints(kikimr, query.get_path())

        for i, _ in enumerate(kikimr.roll()):
            logger.debug(f"Roll {i}!")
            time.sleep(5)

            for query in Queries:
                logger.debug(f"Test data for query {query.get_name()}")
                data, expected_data = query.get_test_data()
                self.write_stream(data, endpoint=endpoint)
                assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data
                self.wait_completed_checkpoints(kikimr, query.get_path())

            # tmp_queries = [SimpleTest("test_compatibility_")]
            # for query in tmp_queries:
            #     logger.debug(f"Start new query  {query.get_name()}")
            #     kikimr.ydb_client.query(query.get_query_text(inp, out))
            #     self.wait_completed_checkpoints(kikimr, query.get_path())
            #     data, expected_data = query.get_test_data()
            #     self.write_stream(data, endpoint=endpoint)
            #     assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data
            #     logger.debug(f"Stoping {query.get_name()}...")
            #     kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query.get_name()}`;")
            #     logger.debug(f"Query {query.get_name()} no longer exists)")

        for query in Queries:
            logger.debug(f"Stoping {query.get_name()}...")
            kikimr.ydb_client.query(f"ALTER STREAMING QUERY `{query.get_name()}` SET (RUN = FALSE);")
            kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query.get_name()}`;")
            logger.debug(f"Query {query.get_name()} is dropped")
            time.sleep(0.5)
