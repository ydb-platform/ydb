import logging
import pytest
import random
import string
import time

from ydb.tests.fq.streaming.common import StreamingTestBase
from ydb.tests.tools.datastreams_helpers.control_plane import create_read_rule

logger = logging.getLogger(__name__)


class TestStreamingInYdb(StreamingTestBase):
    def get_input_name(self, kikimr, name, local_topics, entity_name, partitions_count=1, shared=False):
        if local_topics and shared:
            pytest.skip("Shared reading is not supported for local topics: YQ-5036")

        endpoint = self.get_endpoint(kikimr, local_topics)
        source_name = entity_name(name)
        self.init_topics(source_name, create_output=False, partitions_count=partitions_count, endpoint=endpoint)
        self.create_source(kikimr, source_name, shared=shared)

        if local_topics:
            return f"`{self.input_topic}`", endpoint
        else:
            return f"`{source_name}`.`{self.input_topic}`", endpoint

    def get_io_names(self, kikimr, name, local_topics, entity_name, partitions_count=1, shared=False):
        if local_topics and shared:
            pytest.skip("Shared reading is not supported for local topics: YQ-5036")

        endpoint = self.get_endpoint(kikimr, local_topics)
        source_name = entity_name(name)
        self.init_topics(source_name, create_output=True, partitions_count=partitions_count, endpoint=endpoint)
        self.create_source(kikimr, source_name, shared=shared)

        if local_topics:
            return f"`{self.input_topic}`", f"`{self.output_topic}`", endpoint
        else:
            return f"`{source_name}`.`{self.input_topic}`", f"`{source_name}`.`{self.output_topic}`", endpoint

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_read_topic(self, kikimr, entity_name, local_topics):
        input_name, endpoint = self.get_input_name(kikimr, "test_read_topic", local_topics, entity_name)

        sql = f"""SELECT time FROM {input_name}
            WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (time String NOT NULL)
            )
            LIMIT 1"""

        future = kikimr.ydb_client.query_async(sql)
        time.sleep(1)
        data = ['{"time": "lunch time"}']
        self.write_stream(data, endpoint=endpoint)
        result_sets = future.result()
        assert result_sets[0].rows[0]['time'] == b'lunch time'

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_read_topic_shared_reading_limit(self, kikimr, entity_name, local_topics):
        input_name, endpoint = self.get_input_name(kikimr, "test_read_topic_shared_reading_limit", local_topics, entity_name, partitions_count=10, shared=True)

        sql = f"""SELECT time FROM {input_name}
            WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (time String NOT NULL)
            )
            WHERE time like "%lunch%"
            LIMIT 1"""

        future1 = kikimr.ydb_client.query_async(sql)
        future2 = kikimr.ydb_client.query_async(sql)
        time.sleep(3)
        data = ['{"time": "lunch time"}']
        self.write_stream(data, endpoint=endpoint)
        result_sets1 = future1.result()
        result_sets2 = future2.result()
        assert result_sets1[0].rows[0]['time'] == b'lunch time'
        assert result_sets2[0].rows[0]['time'] == b'lunch time'

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_restart_query(self, kikimr, entity_name, local_topics):
        inp, out, endpoint = self.get_io_names(kikimr, "test_restart_query", local_topics, entity_name, partitions_count=10)

        name = "test_restart_query"
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $in = SELECT time FROM {inp}
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(time String NOT NULL))
                WHERE time like "%lunch%";
                INSERT INTO {out} SELECT time FROM $in;
            END DO;'''

        path = f"/Root/{name}"
        kikimr.ydb_client.query(sql.format(query_name=name, inp=inp, out=out))
        self.wait_completed_checkpoints(kikimr, path)

        data = ['{"time": "lunch time"}']
        expected_data = ['lunch time']
        self.write_stream(data, endpoint=endpoint)

        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data
        self.wait_completed_checkpoints(kikimr, path)

        kikimr.ydb_client.query(f"ALTER STREAMING QUERY `{name}` SET (RUN = FALSE);")
        time.sleep(0.5)

        data = ['{"time": "next lunch time"}']
        expected_data = ['next lunch time']
        self.write_stream(data, endpoint=endpoint)

        kikimr.ydb_client.query(f"ALTER STREAMING QUERY `{name}` SET (RUN = TRUE);")
        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data

        kikimr.ydb_client.query(f"DROP STREAMING QUERY `{name}`;")

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_read_topic_shared_reading_insert_to_topic(self, kikimr, entity_name, local_topics):
        inp, out, endpoint = self.get_io_names(kikimr, "shared_reading_insert_to_topic", local_topics, entity_name, partitions_count=10, shared=True)

        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $in = SELECT time FROM {inp}
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(time String NOT NULL))
                WHERE time like "%lunch%";
                INSERT INTO {out} SELECT time FROM $in;
            END DO;'''

        query_name1 = "test_read_topic_shared_reading_insert_to_topic1"
        query_name2 = "test_read_topic_shared_reading_insert_to_topic2"
        kikimr.ydb_client.query(sql.format(query_name=query_name1, inp=inp, out=out))
        kikimr.ydb_client.query(sql.format(query_name=query_name2, inp=inp, out=out))
        path1 = f"/Root/{query_name1}"
        self.wait_completed_checkpoints(kikimr, path1)

        data = ['{"time": "lunch time"}']
        expected_data = ['lunch time', 'lunch time']
        self.write_stream(data, endpoint=endpoint)
        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data
        self.wait_completed_checkpoints(kikimr, path1)

        sql = R'''ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);'''
        kikimr.ydb_client.query(sql.format(query_name=query_name1))
        kikimr.ydb_client.query(sql.format(query_name=query_name2))

        time.sleep(1)

        data = ['{"time": "next lunch time"}']
        expected_data = ['next lunch time', 'next lunch time']
        self.write_stream(data, endpoint=endpoint)

        sql = R'''ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);'''
        kikimr.ydb_client.query(sql.format(query_name=query_name1))
        kikimr.ydb_client.query(sql.format(query_name=query_name2))
        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data

        sql = R'''DROP STREAMING QUERY `{query_name}`;'''
        kikimr.ydb_client.query(sql.format(query_name=query_name1))
        kikimr.ydb_client.query(sql.format(query_name=query_name2))

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_read_topic_shared_reading_restart_nodes(self, kikimr, entity_name, local_topics):
        inp, out, endpoint = self.get_io_names(kikimr, "reading_restart_nodes", local_topics, entity_name, partitions_count=1, shared=True)

        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $in = SELECT value FROM {inp}
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(value String NOT NULL))
                WHERE value like "%value%";
                INSERT INTO {out} SELECT value FROM $in;
            END DO;'''

        query_name = "test_read_topic_shared_reading_restart_nodes"
        kikimr.ydb_client.query(sql.format(query_name=query_name, inp=inp, out=out))
        path = f"/Root/{query_name}"
        self.wait_completed_checkpoints(kikimr, path)

        self.write_stream(['{"value": "value1"}'], endpoint=endpoint)
        expected_data = ['value1']
        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data
        self.wait_completed_checkpoints(kikimr, path)

        restart_node_id = None
        for node_id in kikimr.cluster.nodes:
            count = self.get_actor_count(kikimr, node_id, "DQ_PQ_READ_ACTOR")
            if count:
                restart_node_id = node_id

        logger.debug(f"Restart node {restart_node_id}")
        node = kikimr.cluster.nodes[restart_node_id]
        node.stop()
        node.start()

        self.write_stream(['{"value": "value2"}'], endpoint=endpoint)
        expected_data = ['value2']
        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data
        self.wait_completed_checkpoints(kikimr, path)

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_read_topic_restore_state(self, kikimr, entity_name, local_topics):
        inp, out, endpoint = self.get_io_names(kikimr, "test_read_topic_restore_state", local_topics, entity_name, partitions_count=1, shared=True)

        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                pragma FeatureR010="prototype";
                PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;

                $in = SELECT * FROM {inp}
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
                INSERT INTO {out}
                    SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $mr;
            END DO;'''

        query_name = "test_read_topic_restore_state"
        kikimr.ydb_client.query(sql.format(query_name=query_name, inp=inp, out=out))
        path = f"/Root/{query_name}"
        self.wait_completed_checkpoints(kikimr, path)

        data = [
            '{"dt": 1696849942000001, "str": "A" }',
            '{"dt": 1696849942500001, "str": "B" }'
        ]
        self.write_stream(data, endpoint=endpoint)
        expected_data = ['{"a_time":1696849942000001,"b_time":1696849942500001,"c_time":null}']
        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data
        self.wait_completed_checkpoints(kikimr, path)

        restart_node_id = None
        for node_id in kikimr.cluster.nodes:
            count = self.get_actor_count(kikimr, node_id, "DQ_PQ_READ_ACTOR")
            if count:
                restart_node_id = node_id

        logger.debug(f"Restart node {restart_node_id}")
        node = kikimr.cluster.nodes[restart_node_id]
        node.stop()
        node.start()

        data = ['{"dt": 1696849943000001, "str": "C" }']
        self.write_stream(data, endpoint=endpoint)
        self.wait_completed_checkpoints(kikimr, path)

        expected_data = ['{"a_time":null,"b_time":1696849942500001,"c_time":1696849943000001}']
        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_json_errors(self, kikimr, entity_name, local_topics):
        inp, out, endpoint = self.get_io_names(kikimr, "test_json_errors", local_topics, entity_name, partitions_count=10, shared=True)

        name = "test_json_errors"
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $in = SELECT data FROM {inp}
                WITH (
                    FORMAT="json_each_row",
                    `skip.json.errors` = "true",
                    SCHEMA=(time UINT32 NOT NULL, data String NOT NULL));
                INSERT INTO {out} SELECT data FROM $in;
            END DO;'''

        path = f"/Root/{name}"
        kikimr.ydb_client.query(sql.format(query_name=name, inp=inp, out=out))
        self.wait_completed_checkpoints(kikimr, path)

        data = [
            '{"time": 101, "data": "hello1"}',
            '{"time": 102, "data": 7777}',
            '{"time": 103, "data": "hello2"}'
        ]
        self.write_stream(data, partition_key="key", endpoint=endpoint)

        expected = ['hello1', 'hello2']
        assert self.read_stream(len(expected), topic_path=self.output_topic, endpoint=endpoint) == expected

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_restart_query_by_rescaling(self, kikimr, entity_name, local_topics):
        inp, out, endpoint = self.get_io_names(kikimr, "test_restart_query_by_rescaling", local_topics, entity_name, partitions_count=10, shared=True)

        name = "test_restart_query_by_rescaling"
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 2 }}
                ] @@;
                $in = SELECT time FROM {inp}
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(time String NOT NULL))
                WHERE time like "%time%";
                INSERT INTO {out} SELECT time FROM $in;
            END DO;'''

        path = f"/Root/{name}"
        kikimr.ydb_client.query(sql.format(query_name=name, inp=inp, out=out))
        self.wait_completed_checkpoints(kikimr, path)

        message_count = 20
        for i in range(message_count):
            self.write_stream(['{"time": "time to do it"}'], topic_path=None, partition_key=(''.join(random.choices(string.digits, k=8))), endpoint=endpoint)
        assert self.read_stream(message_count, topic_path=self.output_topic, endpoint=endpoint) == ["time to do it" for i in range(message_count)]
        self.wait_completed_checkpoints(kikimr, path)

        logger.debug(f"stopping query {name}")
        kikimr.ydb_client.query(f"ALTER STREAMING QUERY `{name}` SET (RUN = FALSE);")

        sql = R'''ALTER STREAMING QUERY `{query_name}` SET (
            RUN = TRUE,
            FORCE = TRUE
            ) AS
            DO BEGIN
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 3 }}
                ] @@;
                $in = SELECT time FROM {inp}
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(time String NOT NULL))
                WHERE time like "%lunch%";
                INSERT INTO {out} SELECT time FROM $in;
            END DO;'''

        kikimr.ydb_client.query(sql.format(query_name=name, inp=inp, out=out))

        message = '{"time": "time to lunch"}'
        for i in range(message_count):
            self.write_stream([message], topic_path=None, partition_key=(''.join(random.choices(string.digits, k=8))), endpoint=endpoint)
        assert self.read_stream(message_count, topic_path=self.output_topic, endpoint=endpoint) == ["time to lunch" for i in range(message_count)]

        kikimr.ydb_client.query(f"ALTER STREAMING QUERY `{name}` SET (RUN = FALSE);")

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_pragma(self, kikimr, entity_name, local_topics):
        inp, out, endpoint = self.get_io_names(kikimr, "test_pragma", local_topics, entity_name, partitions_count=10)

        create_read_rule(self.input_topic, self.consumer_name, default_endpoint=endpoint)

        query_name = "test_pragma1"
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.DisableCheckpoints="true";
                PRAGMA ydb.MaxTasksPerStage = "1";
                PRAGMA pq.Consumer = "{consumer_name}";
                $in = SELECT time FROM {inp}
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(time String NOT NULL));
                INSERT INTO {out} SELECT time FROM $in;
            END DO;'''

        kikimr.ydb_client.query(sql.format(query_name=query_name, consumer_name=self.consumer_name, inp=inp, out=out))
        self.write_stream(['{"time": "lunch time"}'], endpoint=endpoint)
        assert self.read_stream(1, topic_path=self.output_topic, endpoint=endpoint) == ['lunch time']

        kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`")

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_types(self, kikimr, entity_name, local_topics):
        inp, out, endpoint = self.get_io_names(kikimr, "test_types", local_topics, entity_name, partitions_count=1)

        query_name = "test_types1"

        def test_type(self, kikimr, type, input, expected_output):
            sql = R'''
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    $in = SELECT field_name FROM {inp}
                    WITH (
                        FORMAT="json_each_row",
                        SCHEMA=(field_name {type_name} NOT NULL));
                    INSERT INTO {out} SELECT CAST(field_name as String) FROM $in;
                END DO;'''

            kikimr.ydb_client.query(sql.format(query_name=query_name, inp=inp, type_name=type, out=out))
            self.write_stream([f"{{\"field_name\": {input}}}"], endpoint=endpoint)
            assert self.read_stream(1, topic_path=self.output_topic, endpoint=endpoint) == [expected_output]
            kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`")

        test_type(self, kikimr, type="String", input='"lunch time"', expected_output='lunch time')
        test_type(self, kikimr, type="Utf8", input='"Relativitätstheorie"', expected_output='Relativitätstheorie')
        test_type(self, kikimr, type="Int8", input='42', expected_output='42')
        test_type(self, kikimr, type="Uint64", input='777', expected_output='777')
        test_type(self, kikimr, type="Float", input='1024.1024', expected_output='1024.1024')
        test_type(self, kikimr, type="Double", input='-777.777', expected_output='-777.777')
        test_type(self, kikimr, type="Bool", input='true', expected_output='true')
        test_type(self, kikimr, type="Uuid", input='"3d6c7233-d082-4b25-83e2-10d271bbc911"', expected_output='3d6c7233-d082-4b25-83e2-10d271bbc911')
        # Unsupported
        # test_type(self, kikimr, type="Timestamp", input='"2025-08-25 10:49:00"', expected_output='2025-08-25T10:49:00Z')
        # test_type(self, kikimr, type="Json", input='{"name": "value"}', expected_output='{"name": "value"}')
        # test_type(self, kikimr, type="JsonDocument", input='{"name": "value"}', expected_output='lunch time')

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_raw_format(self, kikimr, entity_name, local_topics):
        inp, out, endpoint = self.get_io_names(kikimr, "test_raw_format", local_topics, entity_name, partitions_count=10)

        query_name = "test_raw_format_string"
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $input = SELECT CAST(data AS Json) AS json FROM {inp}
                WITH (
                    FORMAT="raw",
                    SCHEMA=(data String));
                $parsed = SELECT JSON_VALUE(json, "$.time") as k, JSON_VALUE(json, "$.value") as v FROM $input;
                INSERT INTO {out} SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $parsed;
            END DO;'''
        path = f"/Root/{query_name}"
        kikimr.ydb_client.query(sql.format(query_name=query_name, inp=inp, out=out))
        self.wait_completed_checkpoints(kikimr, path)

        data = ['{"time": "2020-01-01T13:00:00.000000Z", "value": "lunch time"}']
        expected_data = ['{"k":"2020-01-01T13:00:00.000000Z","v":"lunch time"}']
        self.write_stream(data, endpoint=endpoint)

        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data
        kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`")

        query_name = "test_raw_format_default"
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $input = SELECT CAST(Data AS Json) AS json FROM {inp};
                $parsed = SELECT JSON_VALUE(json, "$.time") as k, JSON_VALUE(json, "$.value") as v FROM $input;
                INSERT INTO {out} SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $parsed;
            END DO;'''
        path = f"/Root/{query_name}"
        kikimr.ydb_client.query(sql.format(query_name=query_name, inp=inp, out=out))
        self.wait_completed_checkpoints(kikimr, path)

        data = ['{"time": "2020-01-01T13:00:00.000000Z", "value": "lunch time"}']
        expected_data = ['{"k":"2020-01-01T13:00:00.000000Z","v":"lunch time"}']
        self.write_stream(data, endpoint=endpoint)

        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data
        kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`")

        query_name = "test_raw_format_json"
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $input = SELECT data AS json FROM {inp}
                WITH (
                    FORMAT="raw",
                    SCHEMA=(data Json));
                $parsed = SELECT JSON_VALUE(json, "$.time") as k, JSON_VALUE(json, "$.value") as v FROM $input;
                INSERT INTO {out} SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $parsed;
            END DO;'''
        path = f"/Root/{query_name}"
        kikimr.ydb_client.query(sql.format(query_name=query_name, inp=inp, out=out))
        self.wait_completed_checkpoints(kikimr, path)

        data = ['{"time": "2020-01-01T13:00:00.000000Z", "value": "lunch time"}']
        expected_data = ['{"k":"2020-01-01T13:00:00.000000Z","v":"lunch time"}']
        self.write_stream(data, endpoint=endpoint)

        assert self.read_stream(len(expected_data), topic_path=self.output_topic, endpoint=endpoint) == expected_data

        kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`")
