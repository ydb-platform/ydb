import os
from typing import Callable

from ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase


class TestKafkaStreamingInYdb(StreamingTestBase):
    def test_read_kafka(self, kikimr: Kikimr, entity_name: Callable[[str], str]) -> None:
        # Prepare the data through Topic API; the query reads it through Kafka API.
        _, output, endpoint = self.get_io_names(kikimr, "kafka_read", False, entity_name)
        source_name = entity_name("kafka_source")
        query_name = entity_name("kafka_query")
        consumer_name = entity_name("kafka_consumer")
        kafka_endpoint = f"localhost:{os.environ['YDB_KAFKA_PROXY_PORT']}"

        kikimr.ydb_client.query(f'''
            CREATE EXTERNAL DATA SOURCE `{source_name}` WITH (
                SOURCE_TYPE = "Kafka",
                LOCATION = "{kafka_endpoint}",
                AUTH_METHOD = "NONE"
            );
        ''')
        try:
            kikimr.ydb_client.query(f'''
                CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                    PRAGMA ydb.MaxTasksPerStage = "1";
                    PRAGMA kafka.Consumer = "{consumer_name}";
                    INSERT INTO {output}
                    SELECT Data FROM `{source_name}`.`{self.input_topic}` WITH (FORMAT = "raw");
                END DO;
            ''')
            self.wait_completed_checkpoints(kikimr, query_name)

            messages = ["first", "second", "third"]
            self.write_stream(messages, endpoint=endpoint)
            assert self.read_stream(len(messages), endpoint=endpoint) == messages
        finally:
            kikimr.ydb_client.query(f"DROP STREAMING QUERY IF EXISTS `{query_name}`;")
            kikimr.ydb_client.query(f"DROP EXTERNAL DATA SOURCE `{source_name}`;")
