# -*- coding: utf-8 -*-
import time

import yatest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.compatibility.fixtures import (
    RollingUpgradeAndDowngradeFixture,
    logger,
    prepare_feature_flags,
    prepare_table_service_config,
)
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.sqs.tables import create_all_tables as create_all_sqs_tables
from ydb.tests.library.sqs.tables import get_table_path
from ydb.tests.oss.ydb_sdk_import import ydb


SQS_REGION = "ru-central1"
SQS_ROOT = "/Root/SQS"
YMQ_USERNAME = "ymqcompat"


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
