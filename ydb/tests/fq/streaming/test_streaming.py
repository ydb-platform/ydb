import logging
import os
import time
import random
import string
import pytest

from ydb.tests.tools.fq_runner.kikimr_runner import plain_or_under_sanitizer_wrapper

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_metrics import load_metrics

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

    def monitoring_endpoint(self, kikimr, node_id=None):
        node = kikimr.Cluster.nodes[node_id]
        return f"http://localhost:{node.mon_port}"

    def get_sensors(self, kikimr, node_id, counters):
        url = self.monitoring_endpoint(kikimr, node_id) + "/counters/counters={}/json".format(counters)
        return load_metrics(url)

    def get_checkpoint_coordinator_metric(self, kikimr, query_id, metric_name, expect_counters_exist=False):
        sum = 0
        found = False
        for node_id in kikimr.Cluster.nodes:
            sensor = self.get_sensors(kikimr, node_id, "kqp").find_sensor(
                {
                    # "query_id": query_id,  # TODO
                    "subsystem": "checkpoint_coordinator",
                    "sensor": metric_name
                }
            )
            if sensor is not None:
                found = True
                sum += sensor
        assert found or not expect_counters_exist
        return sum

    def get_completed_checkpoints(self, kikimr, query_id):
        return self.get_checkpoint_coordinator_metric(kikimr, query_id, "CompletedCheckpoints")

    def wait_completed_checkpoints(self, kikimr, query_id,
                                   timeout=plain_or_under_sanitizer_wrapper(120, 150)):
        current = self.get_checkpoint_coordinator_metric(kikimr, query_id, "CompletedCheckpoints")
        checkpoints_count = current + 2
        deadline = time.time() + timeout
        while True:
            completed = self.get_completed_checkpoints(kikimr, query_id)
            if completed >= checkpoints_count:
                break
            assert time.time() < deadline, "Wait checkpoint failed, actual completed: " + str(completed)
            time.sleep(plain_or_under_sanitizer_wrapper(0.5, 2))

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
    def test_restart_query(self, kikimr):
        sourceName = "test_restart_query" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        self.init_topics(sourceName, partitions_count=10)
        self.create_source(kikimr, sourceName, False)

        name = "query1"
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

        query_id = "query_id"  # TODO
        kikimr.YdbClient.query(sql.format(query_name=name, source_name=sourceName, input_topic=self.input_topic, output_topic=self.output_topic))
        self.wait_completed_checkpoints(kikimr, query_id)

        data = ['{"time": "lunch time"}']
        expected_data = ['lunch time']
        self.write_stream(data)

        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data
        self.wait_completed_checkpoints(kikimr, query_id)

        kikimr.YdbClient.query(f"ALTER STREAMING QUERY `{name}` SET (RUN = FALSE);")
        time.sleep(0.5)

        data = ['{"time": "next lunch time"}']
        expected_data = ['next lunch time']
        self.write_stream(data)

        kikimr.YdbClient.query(f"ALTER STREAMING QUERY `{name}` SET (RUN = TRUE);")
        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data

        kikimr.YdbClient.query(f"DROP STREAMING QUERY `{name}`;")

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

        query_id = "query_id"  # TODO
        self.wait_completed_checkpoints(kikimr, query_id)

        data = ['{"time": "lunch time"}']
        expected_data = ['lunch time', 'lunch time']
        self.write_stream(data)
        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data
        self.wait_completed_checkpoints(kikimr, query_id)

        sql = R'''ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);'''
        kikimr.YdbClient.query(sql.format(query_name="query1"))
        kikimr.YdbClient.query(sql.format(query_name="query2"))

        time.sleep(1)

        data = ['{"time": "next lunch time"}']
        expected_data = ['next lunch time', 'next lunch time']
        self.write_stream(data)

        sql = R'''ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);'''
        kikimr.YdbClient.query(sql.format(query_name="query1"))
        kikimr.YdbClient.query(sql.format(query_name="query2"))
        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data

        sql = R'''DROP STREAMING QUERY `{query_name}`;'''
        kikimr.YdbClient.query(sql.format(query_name="query1"))
        kikimr.YdbClient.query(sql.format(query_name="query2"))

    @pytest.mark.parametrize("kikimr", [{"local_checkpoints": False}, {"local_checkpoints": True}], indirect=True, ids=["sdk_checkpoints", "local_checkpoints"])
    def test_read_topic_shared_reading_restart_nodes(self, kikimr):
        sourceName = "source_" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        self.init_topics(sourceName, partitions_count=10)
        self.create_source(kikimr, sourceName, True)

        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $in = SELECT value FROM {source_name}.`{input_topic}`
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(value String NOT NULL))
                WHERE value like "%value%";
                INSERT INTO {source_name}.`{output_topic}` SELECT value FROM $in;
            END DO;'''

        kikimr.YdbClient.query(sql.format(query_name="query1", source_name=sourceName, input_topic=self.input_topic, output_topic=self.output_topic))
        query_id = "query_id"  # TODO
        self.wait_completed_checkpoints(kikimr, query_id)

        self.write_stream(['{"value": "value1"}'])
        expected_data = ['value1']
        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data
        self.wait_completed_checkpoints(kikimr, query_id)

        logging.debug("Restart node 1")
        node = kikimr.Cluster.nodes[1]
        node.stop()
        node.start()

        self.write_stream(['{"value": "value2"}'])
        expected_data = ['value2']
        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data
        self.wait_completed_checkpoints(kikimr, query_id)

        logging.debug("Restart node 2")
        node = kikimr.Cluster.nodes[2]
        node.stop()
        node.start()

        self.write_stream(['{"value": "value3"}'])
        expected_data = ['value3']
        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data
        self.wait_completed_checkpoints(kikimr, query_id)

    @pytest.mark.parametrize("kikimr", [{"local_checkpoints": False}, {"local_checkpoints": True}], indirect=True, ids=["sdk_checkpoints", "local_checkpoints"])
    def test_read_topic_restore_state(self, kikimr):
        sourceName = "source4_" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        self.init_topics(sourceName, partitions_count=1)
        self.create_source(kikimr, sourceName, True)
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                pragma FeatureR010="prototype";
                PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;

                $in = SELECT * FROM {source_name}.`{input_topic}`
                    WITH (
                        FORMAT="json_each_row",
                        SCHEMA=(dt UINT64, str STRING));
                $mr = SELECT * FROM $in
                    MATCH_RECOGNIZE(
                        MEASURES
                        LAST(A.dt) as a_time,
                        LAST(B.dt) as b_time,
                        LAST(C.dt) as c_time
                        ONE ROW PER MATCH
                        AFTER MATCH SKIP TO NEXT ROW
                        PATTERN ( ( A | B ) ( B | C ) )
                        DEFINE
                            A as A.str='A',
                            B as B.str='B',
                            C as C.str='C');
                INSERT INTO {source_name}.`{output_topic}`
                    SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $mr;
            END DO;'''

        kikimr.YdbClient.query(sql.format(query_name="query1", source_name=sourceName, input_topic=self.input_topic, output_topic=self.output_topic))
        query_id = "query_id"  # TODO
        self.wait_completed_checkpoints(kikimr, query_id)

        data = [
            '{"dt": 1696849942000001, "str": "A" }',
            '{"dt": 1696849942500001, "str": "B" }'
        ]
        self.write_stream(data)
        expected_data = ['{"a_time":1696849942000001,"b_time":1696849942500001,"c_time":null}']
        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data
        self.wait_completed_checkpoints(kikimr, query_id)

        logging.debug("Restart node 1")
        node = kikimr.Cluster.nodes[1]
        node.stop()
        node.start()

        data = ['{"dt": 1696849943000001, "str": "C" }']
        self.write_stream(data)
        expected_data = ['{"a_time":null,"b_time":1696849942500001,"c_time":1696849943000001}']
        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data
