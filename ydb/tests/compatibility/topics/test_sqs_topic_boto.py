# -*- coding: utf-8 -*-
import base64
import threading
import time
import uuid

import boto3
import pytest
from botocore.config import Config

from ydb.tests.library.compatibility.fixtures import (
    RollingUpgradeAndDowngradeFixture,
    string_version_to_tuple,
    logger,
)
from ydb.tests.oss.ydb_sdk_import import ydb
from test_topic import (
    BATCHING_FLAG,
    CurrentToCurrentVersionFixture,
    OFFSET_DELTA_FLAG,
    STABLE_26_3,
    TOPIC_BATCHING_CODEC,
    read_kafka_batch_payload_values,
    set_feature_flags,
    write_kafka_batch,
)


MIN_SUPPORTED_VERSION = "stable-26-2"
SQS_REGION = "ru-central1"
SECURITY_TOKEN = "root@builtin"
WORKLOAD_DURATION_SECONDS = 10
WORKER_THREADS = 2
# Keep read_timeout short so a stuck call after HTTP proxy restart does not
# consume the whole workload window before workers can retry.
BOTO_CONFIG = Config(
    connect_timeout=5,
    read_timeout=15,
    retries={"max_attempts": 2, "mode": "standard"},
)
DEFAULT_SQS_CONSUMER = "ydb-sqs-consumer"


def skip_if_unsupported(versions):
    if min(versions) < string_version_to_tuple(MIN_SUPPORTED_VERSION):
        pytest.skip(f"Only available since {MIN_SUPPORTED_VERSION}")


def make_sqs_topic_queue_url(endpoint, database_path, topic_name, consumer=DEFAULT_SQS_CONSUMER):
    return (
        f"{endpoint}/v1"
        f"/{len(database_path)}/{database_path}"
        f"/{len(topic_name)}/{topic_name}"
        f"/{len(consumer)}/{consumer}"
    )


def create_sqs_topic(driver, topic_name, consumer=DEFAULT_SQS_CONSUMER):
    with ydb.QuerySessionPool(driver) as session_pool:
        session_pool.execute_with_retries(f"""
            CREATE TOPIC `{topic_name}`
              (CONSUMER `{consumer}`
                WITH (
                  type = 'shared',
                  keep_messages_order = false,
                  default_processing_timeout = Interval('PT20S')
                )
              );
        """)


def send_sqs_messages(client, queue_url, bodies):
    for pos in range(0, len(bodies), 10):
        chunk = bodies[pos:pos + 10]
        response = client.send_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {
                    "Id": str(index),
                    "MessageBody": body,
                }
                for index, body in enumerate(chunk)
            ],
        )
        assert not response.get("Failed"), response
        assert len(response.get("Successful", [])) == len(chunk)


def receive_and_delete_sqs_messages(client, queue_url, expected_count, timeout=120):
    bodies = []
    deadline = time.time() + timeout
    while len(bodies) < expected_count and time.time() < deadline:
        response = client.receive_message(
            QueueUrl=queue_url,
            WaitTimeSeconds=1,
            MaxNumberOfMessages=10,
        )
        for message in response.get("Messages", []):
            bodies.append(message["Body"])
            client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )

    assert len(bodies) == expected_count
    return bodies


class TestTopicSqsBotoRollingUpdate(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        skip_if_unsupported(self.versions)
        yield from self.setup_cluster(
            tenant_db="SqsTopic",
            extra_feature_flags=[
                "enable_topic_message_level_parallelism",
            ],
            http_proxy_config={
                "enabled": True,
                "sqs_topic_enabled": True,
                "ymq_enabled": False,
                "yandex_cloud_service_region": [SQS_REGION, "ru-central-1"],
            },
        )

    @property
    def sqs_endpoint(self):
        return self.http_proxy_endpoint + self.database_path

    def _make_boto_client(self):
        session = boto3.session.Session()
        return session.client(
            service_name="sqs",
            aws_access_key_id="unused",
            aws_secret_access_key="unused",
            aws_session_token=SECURITY_TOKEN,
            endpoint_url=self.sqs_endpoint,
            region_name=SQS_REGION,
            config=BOTO_CONFIG,
        )

    def _receive_and_delete(self, client, queue_url, stats, lock):
        response = client.receive_message(
            QueueUrl=queue_url,
            WaitTimeSeconds=1,
            MaxNumberOfMessages=1,
        )
        messages = response.get("Messages", [])
        if not messages:
            return
        message = messages[0]
        client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message["ReceiptHandle"],
        )
        with lock:
            stats["deleted"] += 1

    def _worker_loop(self, queue_url, stop_event, stats, lock):
        # One client per thread: boto3 clients are not guaranteed thread-safe.
        client = self._make_boto_client()
        iteration = 0
        while not stop_event.is_set():
            try:
                client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=f"msg-{threading.get_ident()}-{iteration}-a",
                )
                client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=f"msg-{threading.get_ident()}-{iteration}-b",
                )
                with lock:
                    stats["sent"] += 2

                self._receive_and_delete(client, queue_url, stats, lock)
                self._receive_and_delete(client, queue_url, stats, lock)

                attrs = client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=[
                        "QueueArn",
                        "ReceiveMessageWaitTimeSeconds",
                        "VisibilityTimeout",
                    ],
                )
                assert "Attributes" in attrs
                with lock:
                    stats["get_attributes"] += 1

                wait_time = "1" if iteration % 2 == 0 else "0"
                client.set_queue_attributes(
                    QueueUrl=queue_url,
                    Attributes={
                        "ReceiveMessageWaitTimeSeconds": wait_time,
                    },
                )
                with lock:
                    stats["set_attributes"] += 1
            except Exception:
                logger.exception("boto SQS worker iteration failed")
                with lock:
                    stats["errors"] += 1
                # Stale connection after HTTP proxy restart: rebuild the client.
                client = self._make_boto_client()
                if stop_event.is_set():
                    break
                time.sleep(0.5)
            iteration += 1

    def _run_boto_cycle(self, step_name):
        client = self._make_boto_client()
        queue_name = f"compat_boto_{uuid.uuid4().hex}"
        logger.info("Step %s: creating queue %s at %s", step_name, queue_name, self.sqs_endpoint)
        # After HTTP proxy restart, the first requests may fail until the listener is ready.
        queue_url = None
        last_error = None
        for attempt in range(10):
            try:
                queue_url = client.create_queue(QueueName=queue_name)["QueueUrl"]
                break
            except Exception as e:
                last_error = e
                logger.warning("create_queue attempt %s failed: %r", attempt + 1, e)
                time.sleep(1)
                client = self._make_boto_client()
        assert queue_url is not None, f"create_queue failed at step {step_name}: {last_error!r}"

        # Warm up control-plane calls: after proxy upgrade GetQueueAttributes may
        # return RequestExpired until the node is fully ready.
        for attempt in range(10):
            try:
                client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=["QueueArn", "ReceiveMessageWaitTimeSeconds"],
                )
                break
            except Exception as e:
                logger.warning("warmup get_queue_attributes attempt %s failed: %r", attempt + 1, e)
                time.sleep(1)
                client = self._make_boto_client()
        else:
            raise AssertionError(f"get_queue_attributes warmup failed at step {step_name}")

        stop_event = threading.Event()
        lock = threading.Lock()
        stats = {
            "sent": 0,
            "deleted": 0,
            "get_attributes": 0,
            "set_attributes": 0,
            "errors": 0,
        }
        threads = [
            threading.Thread(
                target=self._worker_loop,
                args=(queue_url, stop_event, stats, lock),
                name=f"sqs-boto-worker-{i}",
                daemon=True,
            )
            for i in range(WORKER_THREADS)
        ]

        try:
            for thread in threads:
                thread.start()

            time.sleep(WORKLOAD_DURATION_SECONDS)
            stop_event.set()
            for thread in threads:
                thread.join(timeout=90)

            alive = [thread.name for thread in threads if thread.is_alive()]
            logger.info("Step %s stats: %s alive=%s", step_name, stats, alive)
            # Daemon workers may still be blocked in a boto call after node restart;
            # require that useful work was done rather than that every thread exited.
            assert stats["sent"] > 0, f"no messages were sent at step {step_name}: {stats}"
            assert stats["deleted"] > 0, f"no messages were deleted at step {step_name}: {stats}"
            assert stats["get_attributes"] > 0, f"get_queue_attributes was not called at step {step_name}: {stats}"
            assert stats["set_attributes"] > 0, f"set_queue_attributes was not called at step {step_name}: {stats}"
            assert stats["errors"] < stats["sent"], f"too many worker errors at step {step_name}: {stats}"

            client.purge_queue(QueueUrl=queue_url)
        finally:
            stop_event.set()
            for thread in threads:
                thread.join(timeout=5)
            try:
                client.delete_queue(QueueUrl=queue_url)
            except Exception:
                logger.exception("Failed to delete queue %s", queue_url)

    def test_boto_queue_operations(self):
        # roll(): initial yield, then upgrade every node/slot, then downgrade every node/slot.
        for iteration, _ in enumerate(self.roll()):
            logger.info("Running boto SQS cycle after roll iteration #%d", iteration)
            self._run_boto_cycle(f"roll_{iteration}")


class TestTopicSqsBotoMessagesBatchingDisabledRead(CurrentToCurrentVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if self.all_binary_paths[0] != self.all_binary_paths[1]:
            pytest.skip("This test covers disabling batching without changing the binary version")
        if self.versions[0] < STABLE_26_3:
            pytest.skip("Topic message batching is available since stable-26-3")

        yield from self.setup_cluster(
            tenant_db="SqsTopic",
            extra_feature_flags=[
                "enable_topic_message_level_parallelism",
                OFFSET_DELTA_FLAG,
                BATCHING_FLAG,
            ],
            http_proxy_config={
                "enabled": True,
                "sqs_topic_enabled": True,
                "ymq_enabled": False,
                "yandex_cloud_service_region": [SQS_REGION, "ru-central-1"],
            },
        )

    @property
    def sqs_endpoint(self):
        return self.http_proxy_endpoint + self.database_path

    def _make_boto_client(self):
        session = boto3.session.Session()
        return session.client(
            service_name="sqs",
            aws_access_key_id="unused",
            aws_secret_access_key="unused",
            aws_session_token=SECURITY_TOKEN,
            endpoint_url=self.sqs_endpoint,
            region_name=SQS_REGION,
            config=BOTO_CONFIG,
        )

    # Store a physical Kafka batch through the topic protocol, disable batching, and verify that
    # SQS receive returns the physical payload with BodyEncoding instead of losing the batch.
    def test_kafka_batch_written_with_topic_protocol_is_read_by_sqs_after_flag_disable(self):
        topic_name = f"sqs_batch_compat_{uuid.uuid4().hex}"
        create_sqs_topic(self.driver, topic_name)

        batch_values = [
            f"sqs-topic-batch-message-{i}".encode("utf-8")
            for i in range(5)
        ]
        write_kafka_batch(self.driver, topic_name, batch_values)

        set_feature_flags(self.config, **{BATCHING_FLAG: False})
        self.change_cluster_version()

        client = self._make_boto_client()
        queue_url = make_sqs_topic_queue_url(self.http_proxy_endpoint, self.database_path, topic_name)
        response = client.receive_message(
            QueueUrl=queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=10,
            AttributeNames=["All"],
        )
        messages = response.get("Messages", [])

        assert len(messages) == 1
        message = messages[0]
        assert message["Attributes"]["BodyEncoding"] == str(TOPIC_BATCHING_CODEC)
        assert read_kafka_batch_payload_values(base64.b64decode(message["Body"])) == batch_values

        client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message["ReceiptHandle"],
        )
