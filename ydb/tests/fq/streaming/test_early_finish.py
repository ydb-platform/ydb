import logging
import pytest
import time
from typing import Callable

from contrib.ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase


logger = logging.getLogger(__name__)


class TestEarlyFinish(StreamingTestBase):
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_early_finish_case1(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool) -> None:
        inp1, inp2, endpoint = self.get_io_names(kikimr, f"test_early_finish_case1{local_topics!s:.1}", local_topics, entity_name)

        sql = R'''
            SELECT * FROM (
                SELECT JSON_VALUE(CAST(Data AS Json), "$.key") AS t1 FROM {inp1} WITH (STREAMING = "TRUE") LIMIT 100
            ) AS a
            JOIN (
                SELECT JSON_VALUE(CAST(Data AS Json), "$.key") AS t2 FROM {inp2} WITH (STREAMING = "TRUE") LIMIT 3
            ) AS b
            ON a.t1 = b.t2
            LIMIT 1;
            '''

        future = kikimr.ydb_client.query_async(sql.format(inp1=inp1, inp2=inp2))
        time.sleep(1)

        for i in range(10):
            self.write_stream(['{{"key": "{}"}}'.format(i)], topic_path=self.output_topic, endpoint=endpoint)

        for i in range(105):
            self.write_stream(['{{"key": "{}"}}'.format(i + 100)], topic_path=self.input_topic, endpoint=endpoint)

        future.result()

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_early_finish_case2(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool) -> None:
        inp1, inp2, endpoint = self.get_io_names(kikimr, f"test_early_finish_case2{local_topics!s:.1}", local_topics, entity_name)

        sql = R'''
            SELECT * FROM (
                SELECT JSON_VALUE(CAST(Data AS Json), "$.key") AS t1 FROM {inp1} WITH (STREAMING = "TRUE") LIMIT 100
            ) AS a
            JOIN (
                SELECT JSON_VALUE(CAST(Data AS Json), "$.key") AS t2 FROM {inp2} WITH (STREAMING = "TRUE") LIMIT 3
            ) AS b
            ON a.t1 = b.t2;
            '''

        future = kikimr.ydb_client.query_async(sql.format(inp1=inp1, inp2=inp2))
        time.sleep(1)

        for i in range(10):
            self.write_stream(['{{"key": "{}"}}'.format(i)], topic_path=self.output_topic, endpoint=endpoint)

        for i in range(105):
            self.write_stream(['{{"key": "{}"}}'.format(i + 100)], topic_path=self.input_topic, endpoint=endpoint)

        future.result()

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_early_finish_case3(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool) -> None:
        inp1, inp2, endpoint = self.get_io_names(kikimr, f"test_early_finish_case3{local_topics!s:.1}", local_topics, entity_name)

        sql = R'''
            SELECT * FROM (
                SELECT JSON_VALUE(CAST(Data AS Json), "$.key") AS t1 FROM {inp1} WITH (STREAMING = "TRUE") LIMIT 3
            ) AS a
            JOIN (
                SELECT JSON_VALUE(CAST(Data AS Json), "$.key") AS t2 FROM {inp2} WITH (STREAMING = "TRUE") LIMIT 3
            ) AS b
            ON a.t1 = b.t2
            LIMIT 3;
            '''

        future = kikimr.ydb_client.query_async(sql.format(inp1=inp1, inp2=inp2))
        time.sleep(1)

        for i in range(10):
            self.write_stream(['{{"key": "{}"}}'.format(i)], topic_path=self.output_topic, endpoint=endpoint)

        for i in range(10):
            self.write_stream(['{{"key": "{}"}}'.format(i)], topic_path=self.input_topic, endpoint=endpoint)

        result_sets = future.result()
        rows = [row for result_set in result_sets for row in result_set.rows]
        logger.debug(str(rows))
        assert len(rows) == 3

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_early_finish_case4(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool) -> None:
        inp1, inp2, endpoint = self.get_io_names(kikimr, f"test_early_finish_case4{local_topics!s:.1}", local_topics, entity_name)

        sql = R'''
            SELECT * FROM (
                SELECT JSON_VALUE(CAST(Data AS Json), "$.key") AS t1 FROM {inp1} WITH (STREAMING = "TRUE")
            ) AS a
            LEFT JOIN (
                SELECT JSON_VALUE(CAST(Data AS Json), "$.key") AS t2 FROM {inp2} WITH (STREAMING = "TRUE") LIMIT 10
            ) AS b
            ON a.t1 = b.t2
            LIMIT 3;
            '''

        future = kikimr.ydb_client.query_async(sql.format(inp1=inp1, inp2=inp2))
        time.sleep(1)

        for i in range(10):
            self.write_stream(['{{"key": "{}"}}'.format(i)], topic_path=self.output_topic, endpoint=endpoint)

        for i in range(10):
            self.write_stream(['{{"key": "{}"}}'.format(i)], topic_path=self.input_topic, endpoint=endpoint)

        result_sets = future.result()
        rows = [row for result_set in result_sets for row in result_set.rows]
        logger.debug(str(rows))
        assert len(rows) == 3

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_early_finish_case5(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool) -> None:
        inp1, inp2, endpoint = self.get_io_names(kikimr, f"test_early_finish_case5{local_topics!s:.1}", local_topics, entity_name)

        sql = R'''
            SELECT * FROM (
                SELECT JSON_VALUE(CAST(Data AS Json), "$.key") AS t1 FROM {inp1} WITH (STREAMING = "TRUE")
            ) AS a
            LEFT JOIN (
                SELECT JSON_VALUE(CAST(Data AS Json), "$.key") AS t2 FROM {inp2} WITH (STREAMING = "TRUE") LIMIT 10
            ) AS b
            ON a.t1 = b.t2
            LIMIT 3;
            '''

        future = kikimr.ydb_client.query_async(sql.format(inp1=inp1, inp2=inp2))
        time.sleep(1)

        for i in range(10):
            self.write_stream(['{{"key": "{}"}}'.format(i)], topic_path=self.output_topic, endpoint=endpoint)

        for i in range(20):
            self.write_stream(['{{"key": "{}"}}'.format(i + 20)], topic_path=self.input_topic, endpoint=endpoint)
            time.sleep(1)

        for i in range(10):
            self.write_stream(['{{"key": "{}"}}'.format(i)], topic_path=self.input_topic, endpoint=endpoint)

        result_sets = future.result()
        rows = [row for result_set in result_sets for row in result_set.rows]
        logger.debug(str(rows))
        assert len(rows) == 3
