# -*- coding: utf-8 -*-
import json
import time

import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

import ydb


class TestCdcDocumentationRegressions:
    @classmethod
    def setup_class(cls):
        config = KikimrConfigGenerator(
            erasure=Erasure.NONE,
            extra_feature_flags=[
                "enable_changefeeds",
                "enable_changefeed_initial_scan",
                "enable_changefeed_debezium_json_format",
            ],
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database="/Root",
                endpoint=cls.cluster.nodes[1].endpoint,
            )
        )
        cls.driver.wait(timeout=60)

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop()

    def execute(self, query):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(query)

    def create_table(self, name):
        self.execute(
            f"""
                CREATE TABLE `{name}` (
                    key Uint64 NOT NULL,
                    value Utf8,
                    PRIMARY KEY (key)
                );
            """
        )

    def test_subsecond_barrier_interval_emits_barriers(self):
        """A positive Interval must not be silently converted to disabled."""
        self.create_table("subsecond_barrier")
        self.execute(
            """
                ALTER TABLE `subsecond_barrier`
                ADD CHANGEFEED `feed` WITH (
                    MODE = 'KEYS_ONLY',
                    FORMAT = 'JSON',
                    BARRIERS_INTERVAL = Interval('PT0.5S')
                );
            """
        )
        self.execute(
            "ALTER TOPIC `subsecond_barrier/feed` ADD CONSUMER consumer;"
        )

        barriers = []
        with self.driver.topic_client.reader(
            "/Root/subsecond_barrier/feed",
            consumer="consumer",
        ) as reader:
            deadline = time.time() + 3
            while time.time() < deadline and not barriers:
                try:
                    message = reader.receive_message(timeout=0.5)
                except TimeoutError:
                    continue
                body = json.loads(message.data.decode("utf-8"))
                if "resolved" in body:
                    barriers.append(body)

        assert barriers, "positive 500 ms barrier interval was silently disabled"

    def test_max_partition_count_rejects_uint32_overflow(self):
        """Partition limits larger than the protobuf field must be rejected."""
        self.create_table("partition_overflow")

        with pytest.raises(ydb.issues.Error):
            self.execute(
                """
                    ALTER TABLE `partition_overflow`
                    ADD CHANGEFEED `feed` WITH (
                        MODE = 'KEYS_ONLY',
                        FORMAT = 'JSON',
                        TOPIC_AUTO_PARTITIONING = 'ENABLED',
                        TOPIC_MIN_ACTIVE_PARTITIONS = 1,
                        TOPIC_MAX_ACTIVE_PARTITIONS = 4294967297
                    );
                """
            )

    def test_show_create_preserves_changefeed_settings(self):
        """SHOW CREATE must return DDL that recreates CDC topic settings."""
        self.create_table("show_create_settings")
        self.execute(
            """
                ALTER TABLE `show_create_settings`
                ADD CHANGEFEED `feed` WITH (
                    MODE = 'UPDATES',
                    FORMAT = 'JSON',
                    VIRTUAL_TIMESTAMPS = TRUE,
                    BARRIERS_INTERVAL = Interval('PT1S'),
                    RETENTION_PERIOD = Interval('PT12H'),
                    TOPIC_AUTO_PARTITIONING = 'ENABLED',
                    TOPIC_MIN_ACTIVE_PARTITIONS = 2,
                    TOPIC_MAX_ACTIVE_PARTITIONS = 4
                );
            """
        )

        result_sets = self.execute("SHOW CREATE TABLE `show_create_settings`;")
        ddl = result_sets[0].rows[0]["CreateQuery"]
        expected_fragments = (
            "BARRIERS_INTERVAL = INTERVAL('PT1S')",
            "TOPIC_AUTO_PARTITIONING = 'ENABLED'",
            "TOPIC_MAX_ACTIVE_PARTITIONS = 4",
        )
        missing = [fragment for fragment in expected_fragments if fragment not in ddl]
        assert not missing, f"SHOW CREATE omitted CDC settings {missing}:\n{ddl}"
