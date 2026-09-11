import os

import ydb

from ydb.tests.fq.streaming_common.common import StreamingTestBase, YdbClient


class TestLogbroker(StreamingTestBase):
    def test_read_write(self, kikimr):
        database = "/Root/logbroker-federation/prod"
        endpoint = f"localhost:{os.environ['cluster_a_port']}"
        input_topic = "streaming-input"
        output_topic = "streaming-output"
        consumer = "consumer"
        query_name = "logbroker-copy"

        # Create both topics through the federation's config manager.
        with ydb.Driver(
            endpoint=f"grpc://localhost:{os.environ['CM_PORT']}",
            database="/logbroker-federation/prod",
        ) as driver:
            driver.wait(timeout=10, fail_fast=True)
            for topic in (input_topic, output_topic):
                driver.topic_client.create_topic(
                    topic,
                    min_active_partitions=4,
                    consumers=[f"/logbroker-federation/prod/{consumer}"],
                )

        logbroker = YdbClient.from_driver_config(f"grpc://{endpoint}", database)
        try:
            kikimr.ydb_client.create_external_data_source("logbroker", endpoint, database)
            try:
                kikimr.ydb_client.query(f"""
                    CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                        INSERT INTO `logbroker`.`{output_topic}`
                        SELECT Data FROM `logbroker`.`{input_topic}`;
                    END DO;
                """)
                try:
                    self.wait_completed_checkpoints(kikimr, query_name)
                    messages = ["hello from cluster_a", "hello from cluster_b"]
                    logbroker.topic_write(input_topic, [messages[0]])
                    logbroker_b = YdbClient.from_driver_config(
                        f"grpc://localhost:{os.environ['cluster_b_port']}", database
                    )
                    try:
                        logbroker_b.topic_write(input_topic, [messages[1]])
                    finally:
                        logbroker_b.stop()

                    # Messages from different clusters and partitions may arrive in any order.
                    actual = logbroker.topic_read(output_topic, consumer, len(messages))
                    assert sorted(actual) == sorted(messages)
                finally:
                    kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`;")
            finally:
                kikimr.ydb_client.query("DROP EXTERNAL DATA SOURCE `logbroker`;")
        finally:
            logbroker.stop()
