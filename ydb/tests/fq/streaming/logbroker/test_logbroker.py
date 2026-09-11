import os
import time

import ydb

from ydb.tests.fq.streaming_common.common import StreamingTestBase, YdbClient


def wait_topic_consumer(driver, cluster_name, path, consumer, timeout=60):
    deadline = time.monotonic() + timeout
    consumer_names = []
    last_error = None
    while True:
        try:
            description = driver.topic_client.describe_topic(path)
            consumer_names = [item.name for item in description.consumers]
            last_error = None
            # Federation consumer paths may be returned without the leading slash.
            if consumer.lstrip("/") in {name.lstrip("/") for name in consumer_names}:
                return
        except (ydb.SchemeError, ydb.NotFound) as error:
            last_error = error
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"Consumer {consumer!r} did not appear in topic {path!r} on {cluster_name}; "
                f"last observed consumers: {consumer_names!r}"
            ) from last_error
        time.sleep(1)


class TestLogbroker(StreamingTestBase):
    def test_read_write(self, kikimr):
        database = "/Root/logbroker-federation/prod"
        endpoint = f"localhost:{os.environ['cluster_a_port']}"
        input_topic = "streaming-input"
        output_topic = "streaming-output"
        consumer = "/logbroker-federation/prod/consumer"
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
                    consumers=[consumer],
                )

        # Config manager creation may finish before consumers are visible on the clusters.
        for cluster_name in ("cluster_a", "cluster_b"):
            with ydb.Driver(
                endpoint=f"grpc://localhost:{os.environ[f'{cluster_name}_port']}",
                database=database,
                auth_token="root@builtin",
            ) as driver:
                driver.wait(timeout=10, fail_fast=True)
                for topic in (input_topic, output_topic):
                    wait_topic_consumer(driver, cluster_name, f"{database}/{topic}", consumer)

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
