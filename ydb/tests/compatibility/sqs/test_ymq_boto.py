# -*- coding: utf-8 -*-
import time
import uuid

import boto3
import pytest
import yatest
from botocore.config import Config

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.compatibility.fixtures import (
    RollingUpgradeAndDowngradeFixture,
    logger,
    prepare_feature_flags,
    prepare_table_service_config,
)
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.sqs.requests_client import SqsHttpApi
from ydb.tests.library.sqs.tables import create_all_tables as create_all_sqs_tables
from ydb.tests.library.sqs.tables import get_table_path
from ydb.tests.oss.ydb_sdk_import import ydb


SQS_REGION = "ru-central1"
SQS_ROOT = "/Root/SQS"
YMQ_USERNAME = "ymqcompat"
BOTO_CONFIG = Config(
    connect_timeout=5,
    read_timeout=15,
    retries={"max_attempts": 2, "mode": "standard"},
)


class YmqRollingUpdateBase(RollingUpgradeAndDowngradeFixture):
    """Shared setup for classic YMQ compatibility rolling tests."""

    @property
    def sqs_port(self):
        return self.cluster.nodes[1].sqs_port

    def setup_ymq_cluster(self, **kwargs):
        """Like setup_cluster, but enables SQS/YMQ and configures sqs_config before start."""
        kwargs.setdefault("enable_sqs", True)
        extra_feature_flags, disabled_feature_flags = prepare_feature_flags(
            kwargs.pop("extra_feature_flags", []),
            kwargs.pop("disabled_feature_flags", []),
        )
        self.config = KikimrConfigGenerator(
            erasure=kwargs.pop("erasure", Erasure.MIRROR_3_DC),
            binary_paths=[self.all_binary_paths[0]],
            use_in_memory_pdisks=kwargs.pop("use_in_memory_pdisks", False),
            extra_feature_flags=extra_feature_flags,
            disabled_feature_flags=disabled_feature_flags,
            table_service_config=prepare_table_service_config(kwargs.pop("table_service_config", {})),
            **kwargs,
        )
        self.config.yaml_config["sqs_config"]["root"] = SQS_ROOT
        self.config.yaml_config["sqs_config"]["enable_queue_master"] = True
        self.config.yaml_config["sqs_config"]["validate_message_body"] = True

        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        self.endpoints = []
        self.http_proxy_endpoints = []
        for i in range(1, len(self.cluster.nodes) + 1):
            self.endpoints.append("grpc://%s:%s" % ("localhost", self.cluster.nodes[i].port))
            self.http_proxy_endpoints.append(
                "http://%s:%s" % ("localhost", self.cluster.nodes[i].http_proxy_port)
            )

        self.endpoint = self.endpoints[0]
        self.http_proxy_endpoint = self.http_proxy_endpoints[0]
        self.database_path = "/Root"
        self.driver = self.create_driver()

        try:
            self._init_ymq()
            yield
        finally:
            self.stop_driver()
            self.cluster.stop()

    def _init_ymq(self):
        self.driver.scheme_client.make_directory(SQS_ROOT)
        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                create_all_sqs_tables(SQS_ROOT, session)

        self._create_sqs_user(YMQ_USERNAME)
        self._ymq_username = YMQ_USERNAME

    def _backfill_topic_created(self):
        # Older YMQ builds leave TopicCreated NULL; newer builds VERIFY on read.
        # Compatibility binaries are prebuilt, so sanitize rows from the test side.
        # Use the same path helper as create_all_tables (may contain a double slash).
        queues_table = get_table_path(SQS_ROOT, ".Queues")
        query = "UPDATE `{}` SET TopicCreated = false WHERE TopicCreated IS NULL".format(queues_table)
        try:
            with ydb.QuerySessionPool(self.driver) as pool:
                pool.execute_with_retries(query)
        except Exception:
            logger.exception("Failed to backfill TopicCreated")

    def _create_sqs_user(self, username, retries_count=20):
        cmd = [
            "curl",
            "-v",
            "localhost:{}?Action=CreateUser&UserName={}".format(self.sqs_port, username),
            "-H",
            "authorization: aaa credential=abacaba/20220830/ec2/aws4_request",
        ]
        while retries_count:
            logger.info("Creating SQS user via %s", " ".join(cmd))
            try:
                yatest.common.execute(cmd)
                return
            except yatest.common.ExecutionError as ex:
                logger.warning("CreateUser failed: %s. Retrying", ex)
                retries_count -= 1
                time.sleep(3)
        raise RuntimeError("Failed to create SQS user {}".format(username))

    def _retry(self, step_name, what, fn, attempts=30, sleep_sec=2):
        last_error = None
        for attempt in range(attempts):
            try:
                return fn()
            except Exception as e:
                last_error = e
                logger.warning("%s attempt %s failed at step %s: %r", what, attempt + 1, step_name, e)
                time.sleep(sleep_sec)
        raise AssertionError("{} failed at step {}: {!r}".format(what, step_name, last_error))


class TestYmqNativeBotoRollingUpdate(YmqRollingUpdateBase):
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

    def test_boto_queue_operations(self):
        for iteration, _ in enumerate(self.roll()):
            logger.info("Running native YMQ cycle after roll iteration #%d", iteration)
            self._run_cycle("roll_{}".format(iteration))


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
