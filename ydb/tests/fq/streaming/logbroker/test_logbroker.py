import os
import time
from contextlib import ExitStack

import ydb

from ydb.tests.fq.streaming_common.common import StreamingTestBase, YdbClient



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
        time.sleep(1)


class TestLogbroker(StreamingTestBase):
    def test_read_write(self, kikimr):
        database = "/Root/logbroker-federation/prod"
        cm_endpoint = f"localhost:{os.environ['CM_PORT']}"
        discovery_endpoint = os.environ["FEDERATION_DISCOVERY_ENDPOINT"]
        input_topic = "streaming-input"
        output_topic = "streaming-output"
        # Config manager uses the full path; cluster APIs use the name without the federation prefix.
        consumer = "prod/consumer"
        federation_consumer = f"/logbroker-federation/{consumer}"
        query_name = "sledge_hammer"

        # Create both topics through the federation's config manager.
        with ydb.Driver(
            endpoint=f"grpc://{cm_endpoint}",
            database="/logbroker-federation/prod",
        ) as driver:
            driver.wait(timeout=10, fail_fast=True)
            for topic in (input_topic, output_topic):
                driver.topic_client.create_topic(
                    topic,
                    min_active_partitions=4,
                    consumers=[federation_consumer],
                )

        with ExitStack() as clients:
            logbrokers_client = {}
            for cluster_name in ("cluster_a", "cluster_b"):
                logbroker = YdbClient.from_driver_config(
                    f"grpc://localhost:{os.environ[f'{cluster_name}_port']}", database
                )
                clients.callback(logbroker.stop)
                logbrokers_client[cluster_name] = logbroker

            kikimr.ydb_client.create_external_data_source("logbroker", discovery_endpoint, "/logbroker-federation/prod")
            try:
                kikimr.ydb_client.query(f"""
                    CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                        INSERT INTO `logbroker`.`{output_topic}`
                        SELECT Data FROM `logbroker`.`{input_topic}`;
                    END DO;
                """)
                try:
                    self.wait_completed_checkpoints(kikimr, query_name)
                    messages = ["Trust me", "I know what I'm doing"]
                    for cluster_name, message in zip(("cluster_a", "cluster_b"), messages):
                        logbrokers_client[cluster_name].topic_write(input_topic, [message])

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
