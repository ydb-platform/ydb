import time
from contextlib import ExitStack
from itertools import cycle

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
    def init(self, kikimr, logbroker_federation, cleanup, query_name="sledge_hammer"):
        database = "/logbroker-federation/admin"
        self.input_topic = f"{query_name}-input"
        self.output_topic = f"{query_name}-output"
        self.consumer = "admin/consumer"
        self.query_name = query_name
        federation_consumer = f"/logbroker-federation/{self.consumer}"

        with ydb.Driver(
            endpoint=f"grpc://{logbroker_federation.cm_endpoint}",
            database=database,
        ) as driver:
            driver.wait(timeout=10, fail_fast=True)
            for topic in (self.input_topic, self.output_topic):
                driver.topic_client.create_topic(
                    topic,
                    min_active_partitions=4,
                    consumers=[federation_consumer],
                )

        for cluster_name, endpoint in logbroker_federation.ydb_cluster_endpoints.items():
            with ydb.Driver(
                endpoint=f"grpc://{endpoint}",
                database=f"/Root{database}",
                auth_token="root@builtin",
            ) as driver:
                driver.wait(timeout=10, fail_fast=True)
                for topic in (self.input_topic, self.output_topic):
                    wait_topic_consumer(driver, cluster_name, f"/Root{database}/{topic}", self.consumer)

        self.logbrokers_client = {}
        for cluster_name, endpoint in logbroker_federation.ydb_cluster_endpoints.items():
            logbroker = YdbClient.from_driver_config(
                f"grpc://{endpoint}", f"/Root{database}"
            )
            cleanup.callback(logbroker.stop)
            self.logbrokers_client[cluster_name] = logbroker

        kikimr.ydb_client.create_external_data_source("logbroker", logbroker_federation.discovery_endpoint, database)
        cleanup.callback(kikimr.ydb_client.query, "DROP EXTERNAL DATA SOURCE `logbroker`;")

    def start_query(self, kikimr, cleanup):
        kikimr.ydb_client.query(f"""
            CREATE STREAMING QUERY `{self.query_name}` AS DO BEGIN
                INSERT INTO `logbroker`.`{self.output_topic}`
                SELECT Data FROM `logbroker`.`{self.input_topic}`;
            END DO;
        """)
        cleanup.callback(kikimr.ydb_client.query, f"DROP STREAMING QUERY `{self.query_name}`;")
        self.wait_completed_checkpoints(kikimr, self.query_name)

    def test_read_write(self, kikimr, logbroker_federation):
        with ExitStack() as cleanup:
            self.init(kikimr, logbroker_federation, cleanup)
            self.start_query(kikimr, cleanup)

            messages = []
            for cluster_name, text in zip(
                logbroker_federation.ydb_cluster_endpoints,
                cycle(("Trust me", "I know what I'm doing")),
            ):
                for partition_id in range(4):
                    message = f"{text}, {cluster_name}, partition {partition_id}"
                    self.logbrokers_client[cluster_name].topic_write(
                        self.input_topic, [message], partition_id=partition_id
                    )
                    messages.append(message)

            # The query may write to any cluster, or distribute messages across multiple clusters.
            counts = wait_topic_messages(self.logbrokers_client, self.output_topic, len(messages))
            actual = []
            for cluster_name, count in counts.items():
                if count:
                    actual.extend(self.logbrokers_client[cluster_name].topic_read(self.output_topic, self.consumer, count))
            assert sorted(actual) == sorted(messages), counts

    def test_stop_cluster(self, kikimr, logbroker_federation):
        with ExitStack() as cleanup:
            self.init(kikimr, logbroker_federation, cleanup, query_name="stop_cluster")
            self.start_query(kikimr, cleanup)

            path = f"{kikimr.get_database_name()}/{self.query_name}"

            def get_query_state():
                result_sets = kikimr.ydb_client.query(f"""
                    SELECT Status, RetryCount, Issues
                    FROM `.sys/streaming_queries`
                    WHERE Path = "{path}";
                """, timeout=10)
                assert len(result_sets) == 1, result_sets
                assert len(result_sets[0].rows) == 1, result_sets[0].rows
                return result_sets[0].rows[0]

            initial_state = get_query_state()
            assert initial_state.Status == "RUNNING", initial_state
            retry_count = initial_state.RetryCount

            cluster_names = list(logbroker_federation.ydb_cluster_endpoints)
            assert len(cluster_names) == 3, cluster_names
            # The first cluster hosts CM metadata; stop a data cluster instead.
            stopped_cluster = cluster_names[-1]
            cleanup.callback(logbroker_federation.start_cluster, stopped_cluster)
            logbroker_federation.stop_cluster(stopped_cluster)

            deadline = time.monotonic() + 20
            while True:
                state = get_query_state()
                assert state.RetryCount == retry_count, (
                    f"Query retried after stopping {stopped_cluster}: "
                    f"RetryCount {retry_count} -> {state.RetryCount}; issues: {state.Issues}"
                )
                assert state.Status == "RUNNING", (
                    f"Query stopped running after stopping {stopped_cluster}: "
                    f"status: {state.Status}; issues: {state.Issues}"
                )
                if time.monotonic() >= deadline:
                    break
                time.sleep(1)
            logbroker_federation.start_cluster(stopped_cluster)
            time.sleep(30)
