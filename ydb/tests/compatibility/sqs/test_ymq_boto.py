# -*- coding: utf-8 -*-
import uuid

import boto3
import pytest
from botocore.config import Config

from ydb.tests.library.compatibility.fixtures import (
    current_binary_path,
    current_name,
    logger,
    path_to_version,
    string_version_to_tuple,
)
from ydb.tests.compatibility.sqs.ymq_rolling_base import (
    SQS_REGION,
    YmqRollingUpdateBase,
)


BOTO_CONFIG = Config(
    connect_timeout=5,
    read_timeout=15,
    retries={"max_attempts": 2, "mode": "standard"},
)
# FifoQueue is omitted from YMQ GetQueueAttributesResult before this version.
# Require it for versions newer than 26-4 and for current (+inf).
FIFO_QUEUE_ATTR_MIN_VERSION = string_version_to_tuple("stable-26-4")


class TestYmqHttpProxyBotoRollingUpdate(YmqRollingUpdateBase):
    """HTTP proxy with ymq_enabled: JSON SQS API via boto3 (same as sqs_over_topic)."""

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_ymq_cluster(
            http_proxy_config={
                "enabled": True,
                "ymq_enabled": True,
                "sqs_topic_enabled": False,
                "yandex_cloud_service_region": [SQS_REGION, "ru-central-1"],
            },
        )

    @property
    def sqs_endpoint(self):
        return self.http_proxy_endpoint + self.database_path

    def _make_boto_client(self):
        session = boto3.session.Session()
        # Do not pass aws_session_token: http_proxy would auth as root@builtin and
        # fail YMQ path ACL (DescribePath) on /Root/SQS/<user>/...
        return session.client(
            service_name="sqs",
            aws_access_key_id=self._ymq_username,
            aws_secret_access_key="unused",
            endpoint_url=self.sqs_endpoint,
            region_name=SQS_REGION,
            config=BOTO_CONFIG,
        )

    def _run_boto_cycle(self, step_name, *, is_fifo):
        queue_name = "compat_ymq_{}".format(uuid.uuid4().hex)
        if is_fifo:
            queue_name += ".fifo"
        logger.info(
            "Step %s: creating %s queue %s at %s",
            step_name,
            "fifo" if is_fifo else "std",
            queue_name,
            self.sqs_endpoint,
        )

        create_kwargs = {"QueueName": queue_name}
        if is_fifo:
            create_kwargs["Attributes"] = {"FifoQueue": "true"}

        self._backfill_topic_created()
        queue_url = self._retry(
            step_name,
            "create_queue",
            lambda: self._make_boto_client().create_queue(**create_kwargs)["QueueUrl"],
        )
        self._backfill_topic_created()

        try:
            def exercise():
                client = self._make_boto_client()
                send_kwargs = {
                    "QueueUrl": queue_url,
                    "MessageBody": "msg-{}-{}".format(step_name, uuid.uuid4().hex),
                }
                if is_fifo:
                    send_kwargs["MessageGroupId"] = "group-1"
                    # Unique per attempt: retries must not hit FIFO deduplication.
                    send_kwargs["MessageDeduplicationId"] = uuid.uuid4().hex
                client.send_message(**send_kwargs)

                messages = []
                for _ in range(10):
                    response = client.receive_message(
                        QueueUrl=queue_url,
                        WaitTimeSeconds=1,
                        MaxNumberOfMessages=1,
                    )
                    messages = response.get("Messages", [])
                    if messages:
                        break
                assert messages, "no messages received at step {}".format(step_name)
                client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=messages[0]["ReceiptHandle"],
                )

                attrs = client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=[
                        "QueueArn",
                        "ReceiveMessageWaitTimeSeconds",
                        "VisibilityTimeout",
                    ],
                )
                assert "Attributes" in attrs
                assert "QueueArn" in attrs["Attributes"]

                if is_fifo:
                    fifo_attrs = client.get_queue_attributes(
                        QueueUrl=queue_url,
                        AttributeNames=["FifoQueue"],
                    )
                    fifo_val = fifo_attrs.get("Attributes", {}).get("FifoQueue")
                    proxy_path = self.cluster.nodes[1].binary_path
                    proxy_version = path_to_version.get(proxy_path)
                    # Versions newer than 26-4, plus source-built current.
                    require_fifo = proxy_version is not None and (
                        proxy_version > FIFO_QUEUE_ATTR_MIN_VERSION
                        or (
                            current_name == "current"
                            and proxy_path == current_binary_path
                        )
                    )
                    if fifo_val is not None:
                        assert fifo_val == "true"
                    elif require_fifo:
                        raise AssertionError(
                            "FifoQueue missing from GetQueueAttributes on {}: {!r}".format(
                                proxy_version, fifo_attrs
                            )
                        )

                client.set_queue_attributes(
                    QueueUrl=queue_url,
                    Attributes={"ReceiveMessageWaitTimeSeconds": "1"},
                )
                client.purge_queue(QueueUrl=queue_url)

            self._retry(step_name, "queue operations", exercise)
        finally:
            try:
                self._make_boto_client().delete_queue(QueueUrl=queue_url)
            except Exception:
                logger.exception("Failed to delete queue %s", queue_url)
            self._backfill_topic_created()

    def test_boto_std_queue_operations(self):
        for iteration, _ in enumerate(self.roll()):
            logger.info("Running boto YMQ std cycle after roll iteration #%d", iteration)
            self._run_boto_cycle("roll_{}".format(iteration), is_fifo=False)

    def test_boto_fifo_queue_operations(self):
        for iteration, _ in enumerate(self.roll()):
            logger.info("Running boto YMQ fifo cycle after roll iteration #%d", iteration)
            self._run_boto_cycle("roll_{}".format(iteration), is_fifo=True)
