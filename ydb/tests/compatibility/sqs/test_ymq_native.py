# -*- coding: utf-8 -*-
import uuid

import pytest

from ydb.tests.library.compatibility.fixtures import logger
from ydb.tests.library.sqs.requests_client import SqsHttpApi
from ydb.tests.compatibility.sqs.ymq_rolling_base import YmqRollingUpdateBase


class TestYmqNativeRollingUpdate(YmqRollingUpdateBase):
    """Native sqs_port: query/XML protocol via SqsHttpApi.

    Current botocore speaks JSON-only SQS wire protocol, which native YMQ HTTP
    does not understand (MissingAction). SqsHttpApi matches functional/sqs.
    """

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_ymq_cluster()

    def _make_api(self, raise_on_error=True):
        return SqsHttpApi(
            "localhost",
            self.sqs_port,
            self._ymq_username,
            raise_on_error=raise_on_error,
            timeout=30,
        )

    def _run_cycle(self, step_name):
        self._backfill_topic_created()
        self._retry(step_name, "list_queues readiness", lambda: self._make_api().list_queues())

        queue_name = "compat_ymq_{}".format(uuid.uuid4().hex)
        logger.info("Step %s: creating queue %s at localhost:%s", step_name, queue_name, self.sqs_port)
        queue_url = self._retry(step_name, "create_queue", lambda: self._make_api().create_queue(queue_name))
        self._backfill_topic_created()

        try:
            def exercise():
                api = self._make_api()
                api.send_message(queue_url, "msg-{}".format(step_name))

                messages = api.receive_message(queue_url, max_number_of_messages=1, wait_timeout=5)
                assert messages, "no messages received at step {}".format(step_name)
                api.delete_message(queue_url, messages[0]["ReceiptHandle"])

                attrs = api.get_queue_attributes(
                    queue_url,
                    attributes=["QueueArn", "ReceiveMessageWaitTimeSeconds", "VisibilityTimeout"],
                )
                assert attrs is not None
                assert "QueueArn" in attrs

                api.set_queue_attributes(queue_url, {"ReceiveMessageWaitTimeSeconds": "1"})
                api.purge_queue(queue_url)

            self._retry(step_name, "queue operations", exercise)
        finally:
            try:
                self._retry(
                    step_name,
                    "delete_queue",
                    lambda: self._make_api().delete_queue(queue_url),
                    attempts=10,
                    sleep_sec=1,
                )
            except Exception:
                logger.exception("Failed to delete queue %s", queue_url)
            self._backfill_topic_created()

    def test_queue_operations(self):
        for iteration, _ in enumerate(self.roll()):
            logger.info("Running native YMQ cycle after roll iteration #%d", iteration)
            self._run_cycle("roll_{}".format(iteration))
