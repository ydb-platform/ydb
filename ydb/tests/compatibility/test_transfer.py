# -*- coding: utf-8 -*-
import pytest
import time
import uuid

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture, MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class Workload:
    def __init__(self, driver, endpoint):
        self.driver = driver
        self.endpoint = endpoint
        self.id = f"{uuid.uuid1()}".replace("-", "_")
        self.table_name = f"target_table_{self.id}"
        self.topic_name = f"source_topic_{self.id}"
        self.transfer_name = f"transfer_{self.id}"
        self.message_count = 0

    def create_table(self, mode):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                    CREATE TABLE {self.table_name} (
                        partition Uint32 NOT NULL,
                        offset Uint64 NOT NULL,
                        message Utf8,
                        PRIMARY KEY (partition, offset)
                    )  WITH (
                        STORE = {mode}
                    );
                """
            )

    def create_topic(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"CREATE TOPIC {self.topic_name};"
            )

    def create_transfer(self, local):
        lmb = '''
                $l = ($x) -> {
                    return [
                        <|
                            partition:CAST($x._partition AS Uint32),
                            offset:CAST($x._offset AS Uint64),
                            message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
        '''

        connection_string = "" if local else f"CONNECTION_STRING = '{self.endpoint}/?database=/Root',"

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                    {lmb}

                    CREATE TRANSFER {self.transfer_name}
                    FROM {self.topic_name} TO {self.table_name} USING $l
                    WITH (
                        {connection_string}
                        FLUSH_INTERVAL = Interval('PT1S'),
                        BATCH_SIZE_BYTES = 8388608
                    );
                """
            )

    def write_to_topic(self):
        finished_at = time.time() + 5

        with self.driver.topic_client.writer(self.topic_name, producer_id="producer-id") as writer:
            while time.time() < finished_at:
                writer.write(ydb.TopicWriterMessage(f"message-{time.time()}"))
                self.message_count += 1

    def wait_transfer_finished(self):
        iterations = 10
        last_offset = -1

        with ydb.QuerySessionPool(self.driver) as session_pool:
            for i in range(iterations):
                time.sleep(1)

                rss = session_pool.execute_with_retries(
                    f"""
                        SELECT MAX(offset) AS last_offset
                        FROM {self.table_name};
                    """
                )
                rs = rss[0]
                last_offset = rs.rows[0].last_offset

                if last_offset + 1 == self.message_count:
                    return

            raise Exception(f"Transfer still work after {iterations} seconds. Last offset is {last_offset}")


def skip_if_unsupported(versions):
    if min(versions) < (25, 1):
        pytest.skip("Only available since 25-1-2")


class TestTransferMixedClusterFixture(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        skip_if_unsupported(self.versions)

        #
        # Setup cluster
        #
        yield from self.setup_cluster(
            # # Some feature flags can be passed. And other KikimrConfigGenerator options
            extra_feature_flags={
                "enable_column_store": True,
                "enable_topic_transfer": True,
            }
        )

    @pytest.mark.parametrize("store, local", [
        ("row", False),
        # ("column", False) TODO uncomment after OLAP will be enabled
        # ("row", True), TODO uncomment after local topic will be merged
    ])
    def test_transfer(self, store, local):
        utils = Workload(self.driver, self.endpoint)

        #
        # 1. Fill table with data
        #
        utils.create_table(store)
        utils.create_topic()
        utils.create_transfer(local)

        utils.write_to_topic()

        #
        # 2. check written data is correct
        #
        utils.wait_transfer_finished()


class TestTransferRestartToAnotherVersion(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        skip_if_unsupported(self.versions)

        #
        # Setup cluster
        #
        yield from self.setup_cluster(
            # # Some feature flags can be passed. And other KikimrConfigGenerator options
            extra_feature_flags={
                "enable_column_store": True,
                "enable_topic_transfer": True,
            }
        )

    @pytest.mark.parametrize("store, local", [
        ("row", False),
        # ("column", False) TODO uncomment after OLAP will be enabled
        # ("row", True), TODO uncomment after local topic will be merged
    ])
    def test_transfer(self, store, local):
        utils = Workload(self.driver, self.endpoint)

        #
        # 1. Fill table with data
        #
        utils.create_table(store)
        utils.create_topic()
        utils.create_transfer(local)

        utils.write_to_topic()
        self.change_cluster_version()
        utils.write_to_topic()

        #
        # 2. check written data is correct
        #
        utils.wait_transfer_finished()


class TestTransferRollingUpdate(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        skip_if_unsupported(self.versions)

        #
        # Setup cluster
        #
        yield from self.setup_cluster(
            # # Some feature flags can be passed. And other KikimrConfigGenerator options
            extra_feature_flags={
                "enable_column_store": True,
                "enable_topic_transfer": True,
            }
        )

    @pytest.mark.parametrize("store, local", [
        ("row", False),
        # ("column", False) TODO uncomment after OLAP will be enabled
        # ("row", True), TODO uncomment after local topic will be merged
    ])
    def test_transfer(self, store, local):
        utils = Workload(self.driver, self.endpoint)

        #
        # 1. Fill table with data
        #
        utils.create_table(store)
        utils.create_topic()
        utils.create_transfer(local)

        for _ in self.roll():
            utils.write_to_topic()

        #
        # 2. check written data is correct
        #
        utils.wait_transfer_finished()
