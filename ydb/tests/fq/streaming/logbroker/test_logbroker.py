import os
import time
from contextlib import ExitStack

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
            if consumer in consumer_names:
                return
        except (ydb.SchemeError, ydb.NotFound) as error:
            last_error = error
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"Consumer {consumer!r} did not appear in topic {path!r} on {cluster_name}; "
                f"last observed consumers: {consumer_names!r}"
            ) from last_error
        time.sleep(0.2)


def wait_topic_messages(logbrokers, path, expected_count, timeout=120):
    deadline = time.monotonic() + timeout
    while True:
        counts = {}
        for cluster_name, logbroker in logbrokers.items():
            description = logbroker.driver.topic_client.describe_topic(path, include_stats=True)
            # Missing statistics are not evidence that a cluster is empty.
            if any(partition.partition_stats is None for partition in description.partitions):
                counts[cluster_name] = None
                continue
            counts[cluster_name] = sum(
                partition.partition_stats.partition_end - partition.partition_stats.partition_start
                for partition in description.partitions
            )
        if all(count is not None for count in counts.values()) and sum(counts.values()) >= expected_count:
            return counts
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"Expected at least {expected_count} messages in topic {path!r}; "
                f"last observed per-cluster counts: {counts!r}"
            )
        time.sleep(0.2)


class TestLogbroker(StreamingTestBase):
    def test_read_write(self, kikimr):
        database = "/logbroker-federation/prod"
        discovery_endpoint = os.environ["FEDERATION_DISCOVERY_ENDPOINT"]
        input_topic = "streaming-input"
        output_topic = "streaming-output"
        consumer = "prod/consumer"
        federation_consumer = f"/logbroker-federation/{consumer}"
        query_name = "sledge_hammer"

        with ydb.Driver(
            endpoint=f"grpc://localhost:{os.environ['CM_PORT']}",
            database=database,
        ) as driver:
            driver.wait(timeout=10, fail_fast=True)
            for topic in (input_topic, output_topic):
                driver.topic_client.create_topic(
                    topic,
                    min_active_partitions=4,
                    consumers=[federation_consumer],
                )

        for cluster_name in ("cluster_a", "cluster_b"):
            with ydb.Driver(
                endpoint=f"grpc://localhost:{os.environ[f'{cluster_name}_port']}",
                database=f"/Root{database}",
                auth_token="root@builtin",
            ) as driver:
                driver.wait(timeout=10, fail_fast=True)
                for topic in (input_topic, output_topic):
                    wait_topic_consumer(driver, cluster_name, f"/Root{database}/{topic}", consumer)

        with ExitStack() as clients:
            logbrokers_client = {}
            for cluster_name in ("cluster_a", "cluster_b"):
                logbroker = YdbClient.from_driver_config(
                    f"grpc://localhost:{os.environ[f'{cluster_name}_port']}", f"/Root{database}"
                )
                clients.callback(logbroker.stop)
                logbrokers_client[cluster_name] = logbroker

            kikimr.ydb_client.create_external_data_source("logbroker", discovery_endpoint, database)
            try:
                kikimr.ydb_client.query(f"""
                    CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                        INSERT INTO `logbroker`.`{output_topic}`
                        SELECT Data FROM `logbroker`.`{input_topic}`;
                    END DO;
                """)
                try:
                    self.wait_completed_checkpoints(kikimr, query_name)
                    messages = []
                    for cluster_name, text in zip(("cluster_a", "cluster_b"), ("Trust me", "I know what I'm doing")):
                        for partition_id in range(4):
                            message = f"{text}, {cluster_name}, partition {partition_id}"
                            logbrokers_client[cluster_name].topic_write(
                                input_topic, [message], partition_id=partition_id
                            )
                            messages.append(message)

                    # The query may write to either cluster, or distribute messages across both.
                    counts = wait_topic_messages(logbrokers_client, output_topic, len(messages))
                    actual = []
                    for cluster_name, count in counts.items():
                        if count:
                            actual.extend(logbrokers_client[cluster_name].topic_read(output_topic, consumer, count))
                    assert sorted(actual) == sorted(messages), counts
                finally:
                    kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`;")
            finally:
                kikimr.ydb_client.query("DROP EXTERNAL DATA SOURCE `logbroker`;")
