import logging
import time
import pytest
import random
import string

from ydb.tests.fq.streaming_common.common import StreamingTestBase

logger = logging.getLogger(__name__)


class TestStreamingLarge(StreamingTestBase):

    def do_write_read(self, input, expected_output, endpoint):
        logger.debug("do_write_read")
        time.sleep(2)
        logger.debug("write data to stream")
        self.write_stream(input, endpoint=endpoint)
        logger.debug("read data from stream")
        read_data = self.read_stream(len(expected_output), topic_path=self.output_topic, endpoint=endpoint, timeout=60)
        if len(read_data) != len(expected_output):
            read_data = sorted(read_data)
            read_data = read_data[-len(expected_output) :]  # deduplication disabled
        assert sorted(read_data) == sorted(expected_output)

    @pytest.mark.parametrize("kikimr", [{"rebalancing_timeout_ms": "50000"}], indirect=["kikimr"])
    def test_rolling_restart(self, kikimr, entity_name):

        # local_topics over EDS
        inp, out, endpoint = self.get_io_names(
            kikimr,
            "test_rolling_restart",
            local_topics=False,
            entity_name=entity_name,
            shared=True,
            partitions_count=1
        )

        query_count = 2
        for i in range(query_count):
            name = f"test_shared_reading_{i}"
            sql = f"""
                CREATE STREAMING QUERY `{name}` AS DO BEGIN
                $input = (
                    SELECT * FROM
                        {inp} WITH (
                            FORMAT = 'json_each_row',
                            SCHEMA (time String NOT NULL, level String NOT NULL, host String NOT NULL)
                        )
                );
                $json = (SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow())))) FROM $input);
                INSERT INTO {out} SELECT * FROM $json;
                END DO;"""

            path = f"/Root/{name}"
            kikimr.ydb_client.query(sql.format(query_name=name, inp=inp, out=out))
            self.wait_completed_checkpoints(kikimr, path)

        for i, _ in enumerate(self.roll(kikimr)):
            logger.debug(f"RollingUpgrade {i}")
            input = [f'{{"time": "2025-01-01T00:15:00.000000Z", "level": "error", "host": "host-{i}"}}']
            expected_data = [
                f'{{"host":"host-{i}","level":"error","time":"2025-01-01T00:15:00.000000Z"}}'
            ] * query_count
            self.do_write_read(input, expected_data, endpoint)
            time.sleep(10)

    @pytest.mark.parametrize("kikimr", [{"rebalancing_timeout_ms": "20000"}], indirect=["kikimr"])
    def test_restart_nodes(self, kikimr, entity_name):

        inp, out, endpoint = self.get_io_names(
            kikimr,
            "test_restart_nodes",
            local_topics=False,
            entity_name=entity_name,
            shared=True,
            partitions_count=9
        )

        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.MaxTasksPerStage = "1";
                $in = SELECT value FROM {inp}
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(value String NOT NULL))
                WHERE value like "%value%";
                INSERT INTO {out} SELECT value FROM $in;
            END DO;'''

        query_name1 = "test_restart_nodes1"
        query_name2 = "test_restart_nodes2"
        kikimr.ydb_client.query(sql.format(query_name=query_name1, inp=inp, out=out))
        kikimr.ydb_client.query(sql.format(query_name=query_name2, inp=inp, out=out))
        path1 = f"/Root/{query_name1}"
        path2 = f"/Root/{query_name2}"
        self.wait_completed_checkpoints(kikimr, path1)
        self.wait_completed_checkpoints(kikimr, path2)

        message_count = 9
        for i in range(message_count):
            self.write_stream(['{"value": "value0"}'], partition_key=(''.join(random.choices(string.digits, k=8))), endpoint=endpoint)
        expected_data = ['value0'] * message_count * 2
        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data
        self.wait_completed_checkpoints(kikimr, path1)
        self.wait_completed_checkpoints(kikimr, path2)

        def test(i):
            restart_node_id = random.randint(1, 9)
            logger.debug(f"Restart node {restart_node_id}")
            node = kikimr.cluster.nodes[restart_node_id]
            node.stop()
            node.start()
            value = f"value{i}"
            for i in range(message_count):
                self.write_stream([f'{{"value": "{value}"}}'], partition_key=(''.join(random.choices(string.digits, k=8))), endpoint=endpoint)

            expected_data = [value] * message_count * 2
            self.wait_completed_checkpoints(kikimr, path1)
            self.wait_completed_checkpoints(kikimr, path2)
            assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data
            self.wait_completed_checkpoints(kikimr, path1)
            self.wait_completed_checkpoints(kikimr, path2)

        test(1)
        test(2)
        test(3)

        sql = R'''DROP STREAMING QUERY `{query_name}`;'''
        kikimr.ydb_client.query(sql.format(query_name=query_name1))
        kikimr.ydb_client.query(sql.format(query_name=query_name2))

    @pytest.mark.parametrize("kikimr", [{"rebalancing_timeout_ms": "5000"}], indirect=["kikimr"])
    def test_replace_node(self, kikimr, entity_name):

        inp, out, endpoint = self.get_io_names(
            kikimr,
            "test_replace_node",
            local_topics=False,
            entity_name=entity_name,
            shared=True,
            partitions_count=9
        )
        node1 = kikimr.cluster.nodes[9]
        node1.stop()

        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.MaxTasksPerStage = "1";
                $in = SELECT value FROM {inp}
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(value String NOT NULL))
                WHERE value like "%value%";
                INSERT INTO {out} SELECT value FROM $in;
            END DO;'''

        query_name1 = "test_replace_node1"
        query_name2 = "test_replace_node2"
        kikimr.ydb_client.query(sql.format(query_name=query_name1, inp=inp, out=out))
        kikimr.ydb_client.query(sql.format(query_name=query_name2, inp=inp, out=out))
        time.sleep(2)

        message_count = 9
        for i in range(message_count):
            self.write_stream(['{"value": "value0"}'], partition_key=(''.join(random.choices(string.digits, k=8))), endpoint=endpoint)
        expected_data = ['value0'] * message_count * 2
        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data
        time.sleep(2)

        stop_node_id = random.randint(1, 8)
        logger.debug(f"Stop node {stop_node_id}, start node 1")
        node2 = kikimr.cluster.nodes[stop_node_id]
        node2.stop()
        node1.start()

        def write_read(i):
            value = f"value {i}"
            for i in range(message_count):
                self.write_stream([f'{{"value": "{value}"}}'], partition_key=(''.join(random.choices(string.digits, k=8))), endpoint=endpoint)
            expected_data = [value] * message_count * 2
            assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint, timeout=180) == expected_data
            time.sleep(2)

        write_read(1)
        node1.stop()
        node2.start()
        write_read(2)
        node2.stop()
        node1.start()
        write_read(3)
        node1.stop()
        node2.start()
        write_read(4)
        node2.stop()
        node1.start()
        write_read(5)
        sql = R'''DROP STREAMING QUERY `{query_name}`;'''
        kikimr.ydb_client.query(sql.format(query_name=query_name1))
        kikimr.ydb_client.query(sql.format(query_name=query_name2))
