import logging
import os
import time
import random
import string

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

    def get_actor_count(self, kikimr, node_id, activity):
        result = self.get_sensors(kikimr, node_id, "utils").find_sensor(
            {"activity": activity, "sensor": "ActorsAliveByActivity", "execpool": "User"})
        return result if result is not None else 0

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

    def test_restart_query(self, kikimr):
        sourceName = "test_restart_query" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        self.init_topics(sourceName, partitions_count=10)
        self.create_source(kikimr, sourceName, False)

        name = "test_restart_query"
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

    def test_read_topic_shared_reading_restart_nodes(self, kikimr):
        sourceName = "source_" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        self.init_topics(sourceName, partitions_count=1)
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

        restart_node_id = None
        for node_id in kikimr.Cluster.nodes:
            count = self.get_actor_count(kikimr, node_id, "DQ_PQ_READ_ACTOR")
            if count:
                restart_node_id = node_id

        logging.debug(f"Restart node {restart_node_id}")
        node = kikimr.Cluster.nodes[restart_node_id]
        node.stop()
        node.start()

        self.write_stream(['{"value": "value2"}'])
        expected_data = ['value2']
        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data
        self.wait_completed_checkpoints(kikimr, query_id)

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

        restart_node_id = None
        for node_id in kikimr.Cluster.nodes:
            count = self.get_actor_count(kikimr, node_id, "DQ_PQ_READ_ACTOR")
            if count:
                restart_node_id = node_id

        logging.debug(f"Restart node {restart_node_id}")
        node = kikimr.Cluster.nodes[restart_node_id]
        node.stop()
        node.start()

        data = ['{"dt": 1696849943000001, "str": "C" }']
        self.write_stream(data)
        expected_data = ['{"a_time":null,"b_time":1696849942500001,"c_time":1696849943000001}']
        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data

    def test_json_errors(self, kikimr):
        sourceName = "test_json_errors" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        self.init_topics(sourceName, partitions_count=10)
        self.create_source(kikimr, sourceName, True)

        name = "test_json_errors"
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $in = SELECT data FROM {source_name}.`{input_topic}`
                WITH (
                    FORMAT="json_each_row",
                    `skip.json.errors` = "true",
                    SCHEMA=(time UINT32 NOT NULL, data String NOT NULL));
                INSERT INTO {source_name}.`{output_topic}` SELECT data FROM $in;
            END DO;'''

        query_id = "query_id"  # TODO
        kikimr.YdbClient.query(sql.format(query_name=name, source_name=sourceName, input_topic=self.input_topic, output_topic=self.output_topic))
        self.wait_completed_checkpoints(kikimr, query_id)

        data = [
            '{"time": 101, "data": "hello1"}',
            '{"time": 102, "data": 7777}',
            '{"time": 103, "data": "hello2"}'
        ]
        self.write_stream(data, partition_key="key")

        expected = ['hello1', 'hello2']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

    def test_restart_query_by_rescaling(self, kikimr):
        sourceName = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        self.init_topics(sourceName, partitions_count=10)
        self.create_source(kikimr, sourceName, True)

        name = "test_restart_query_by_rescaling"
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 2 }}
                ] @@;
                $in = SELECT time FROM {source_name}.`{input_topic}`
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(time String NOT NULL))
                WHERE time like "%time%";
                INSERT INTO `{source_name}`.`{output_topic}` SELECT time FROM $in;
            END DO;'''

        query_id = "query_id"  # TODO
        kikimr.YdbClient.query(sql.format(query_name=name, source_name=sourceName, input_topic=self.input_topic, output_topic=self.output_topic))
        self.wait_completed_checkpoints(kikimr, query_id)

        message_count = 20
        for i in range(message_count):
            self.write_stream(['{"time": "time to do it"}'], topic_path=None, partition_key=(''.join(random.choices(string.digits, k=8))))
        assert self.read_stream(message_count, topic_path=self.output_topic) == ["time to do it" for i in range(message_count)]
        self.wait_completed_checkpoints(kikimr, query_id)

        logging.debug(f"stopping query {name}")
        kikimr.YdbClient.query(f"ALTER STREAMING QUERY `{name}` SET (RUN = FALSE);")

        sql = R'''ALTER STREAMING QUERY `{query_name}` SET (
            RUN = TRUE,
            FORCE = TRUE
            ) AS
            DO BEGIN
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 3 }}
                ] @@;
                $in = SELECT time FROM {source_name}.`{input_topic}`
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(time String NOT NULL))
                WHERE time like "%lunch%";
                INSERT INTO `{source_name}`.`{output_topic}` SELECT time FROM $in;
            END DO;'''

        kikimr.YdbClient.query(sql.format(query_name=name, source_name=sourceName, input_topic=self.input_topic, output_topic=self.output_topic))

        message = '{"time": "time to lunch"}'
        for i in range(message_count):
            self.write_stream([message], topic_path=None, partition_key=(''.join(random.choices(string.digits, k=8))))
        assert self.read_stream(message_count, topic_path=self.output_topic) == ["time to lunch" for i in range(message_count)]

        kikimr.YdbClient.query(f"ALTER STREAMING QUERY `{name}` SET (RUN = FALSE);")

    def test_types(self, kikimr):
        sourceName = "test_types"
        self.init_topics(sourceName, partitions_count=1)
        create_read_rule(self.input_topic, self.consumer_name)

        self.create_source(kikimr, sourceName)

        query_name="test_types1"
        def test_type(self, kikimr, type, input, expected_output):
            sql = R'''
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    $in = SELECT field_name FROM {source_name}.`{input_topic}`
                    WITH (
                        FORMAT="json_each_row",
                        SCHEMA=(field_name {type_name} NOT NULL));
                    INSERT INTO {source_name}.`{output_topic}` SELECT CAST(field_name as String) FROM $in;
                END DO;'''

            kikimr.YdbClient.query(sql.format(query_name=query_name, source_name=sourceName, type_name=type, input_topic=self.input_topic, output_topic=self.output_topic))
            self.write_stream([f"{{\"field_name\": {input}}}"])
            assert self.read_stream(1, topic_path=self.output_topic) == [expected_output]
            kikimr.YdbClient.query(f"DROP STREAMING QUERY `{query_name}`")

        test_type(self, kikimr, type="String", input='"lunch time"', expected_output='lunch time')
        test_type(self, kikimr, type="Utf8", input='"Relativitätstheorie"', expected_output='Relativitätstheorie')
        #test_type(self, kikimr, type="Json", input='{"name": "value"}', expected_output='{"name": "value"}')
        #test_type(self, kikimr, type="JsonDocument", input='{"name": "value"}', expected_output='lunch time')   # Unsupported
        #test_type(self, kikimr, type="Bool", input='True', expected_output='True')
        test_type(self, kikimr, type="Int8", input='42', expected_output='42')
        test_type(self, kikimr, type="Uint64", input='777', expected_output='777')
        test_type(self, kikimr, type="Float", input='1024.1024', expected_output='1024.1024')
        test_type(self, kikimr, type="Double", input='-777.777', expected_output='-777.777')
        test_type(self, kikimr, type="Timestamp", input='"2025-08-25 10:49:00"', expected_output='2025-08-25T10:49:00Z')
        
    def test_raw_format(self, kikimr):
        sourceName = "test_restart_query" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        self.init_topics(sourceName, partitions_count=10)
        self.create_source(kikimr, sourceName, False)

        query_name = "test_restart_query"
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $input = SELECT CAST(data AS Json) as json FROM {source_name}.`{input_topic}`
                WITH (
                    FORMAT="raw",
                    SCHEMA=(data String NOT NULL));
                $parsed = SELECT JSON_VALUE(json, "$.time") as k, JSON_VALUE(json, "$.value") as v FROM $input;
                INSERT INTO {source_name}.`{output_topic}` SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $parsed;
            END DO;'''
        query_id = "query_id"  # TODO
        kikimr.YdbClient.query(sql.format(query_name=query_name, source_name=sourceName, input_topic=self.input_topic, output_topic=self.output_topic))
        self.wait_completed_checkpoints(kikimr, query_id)

        data = ['{"time": "2020-01-01T13:00:00.000000Z", "value": "lunch time"}']
        expected_data = ['{"k":"2020-01-01T13:00:00.000000Z","v":"lunch time"}']
        self.write_stream(data)

        assert self.read_stream(len(expected_data), topic_path=self.output_topic) == expected_data
        kikimr.YdbClient.query(f"DROP STREAMING QUERY `{query_name}`")