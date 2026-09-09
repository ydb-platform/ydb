import logging
import pytest
import time
from typing import Callable

from ydb.tests.fq.streaming_common.common import StreamingTestBase
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

logger = logging.getLogger(__name__)


class SimpleTest:
    Id = 0

    def __init__(self, prefix):
        self.prefix = prefix
        self.id = SimpleTest.Id
        self.filter = self.get_name()
        SimpleTest.Id += 1

    def get_name(self):
        return f"{self.prefix}_test666_{self.id}"

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

class StreamingTestBase2(StreamingTestBase):

    def start_query(self, kikimr, query):
        self.current_query_name = query.get_name()
        logger.debug(f"Start query {self.current_query_name}")
        kikimr.ydb_client.query(query.get_query_text(self.inp, self.out))
        self.wait_completed_checkpoints(kikimr, self.current_query_name)

    def check_data(self, kikimr, query):
        self.current_query_name = query.get_name()
        logger.debug(f"Write test data for query {self.current_query_name}")
        data, expected_data = query.get_test_data()
        self.write_stream(data, endpoint=self.topic_endpoint)
        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=self.topic_endpoint, timeout=60) == expected_data
        self.wait_completed_checkpoints(kikimr, self.current_query_name)

    def stop_query(self, kikimr, query):
        self.current_query_name = query.get_name()
        logger.debug(f"Stopping {self.current_query_name}...")
        kikimr.ydb_client.query(f"ALTER STREAMING QUERY `{self.current_query_name}` SET (RUN = FALSE);")
        kikimr.ydb_client.query(f"DROP STREAMING QUERY `{self.current_query_name}`;")
        logger.debug(f"Query {self.current_query_name} is dropped")


class TestStreamingCompatibility(StreamingTestBase2):

    @pytest.mark.parametrize("kikimr", [{"is_compatibility_tests": True}], indirect=["kikimr"])
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_compatibility(self: StreamingTestBase2, kikimr, entity_name: Callable[[str], str], local_topics: bool) -> None:
        self.inp, self.out, self.topic_endpoint = self.get_io_names(kikimr, "test_compatibility", local_topics, entity_name, partitions_count=10)

        try:
            for query in Queries:
                self.start_query(kikimr, query)

            # check input/output data
            for query in Queries:
                self.check_data(kikimr, query)

            for i, _ in enumerate(kikimr.rolling()):
                time.sleep(5)

                for query in Queries:
                    self.check_data(kikimr, query)

                kikimr.recreate_driver()
                tmp_queries = [SimpleTest("test_compatibility_")]
                for query in tmp_queries:
                    self.start_query(kikimr, query)
                    self.check_data(kikimr, query)
                    self.stop_query(kikimr, query)

            for query in Queries:
                self.stop_query(kikimr, query)
                time.sleep(0.5)

        except AssertionError as error:
            path = f"{kikimr.get_database_name()}/{self.current_query_name}"
            raise AssertionError(f"{error}\n{self.get_diagnostics(kikimr, path)}") from error
