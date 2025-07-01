# -*- coding: utf-8 -*-
import pytest
import time

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture, MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb


def create_table(pool, mode):
    pool.execute_with_retries(
        f"""
            CREATE TABLE target_table (
                partition Uint32 NOT NULL,
                offset Uint64 NOT NULL,
                message Utf8,
                PRIMARY KEY (partition, offset)
            )  WITH (
                STORE = {mode}
            );
        """
    )

def create_topic(pool):
    pool.execute_with_retries(
        f"CREATE TOPIC source_topic;"
    )

def create_transfer(pool):
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

    self.pool.execute_with_retries(
        f"""
            {lmb}

            CREATE TRANSFER transfer_name
            FROM source_toic TO target_table USING $l
            WITH (
                FLUSH_INTERVAL = Interval('PT1S'),
                BATCH_SIZE_BYTES = 8388608
            );
        """
    )

def write_to_topic(driver):
    finished_at = time.time() + 15
    message_count = 0

    with driver.topic_client.writer('source_topic', producer_id="producer-id") as writer:
        while time.time() < finished_at:
            writer.write(ydb.TopicWriterMessage(f"message-{time.time()}"))
            message_count += 1
    
    return message_count

def wait_transfer_finished(pool, expected_message_count):
    iterations = 10

    last_offset = -1

    for i in range(iterations):
        time.sleep(1)

        rss = pool.execute_with_retries(
            """
                SELECT MAX(offset) AS last_offset
                FROM target_table;
            """
        )
        rs = rss[0]
        last_offset = rs.rows[0].last_offset

        if last_offset + 1 == expected_message_count:
            return

    raise Exception(f"Transfer still work after {iterations} seconds. Last offset is {last_offset}")

@unittest.skip('uncomment after 24-4 and 25-1 ecxluded. functionality raise from 25-1-2')
class TestTransferMixedClusterFixture(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        #
        # Setup cluster
        #
        yield from self.setup_cluster(
            # # Some feature flags can be passed. And other KikimrConfigGenerator options
            # extra_feature_flags={
            #     "some_feature_flag": True,
            # }
        )

    @pytest.mark.parametrize("store", ["row", "column"])
    def test_transfer(self, store):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            #
            # 1. Fill table with data
            #
            create_table(session_pool, store)
            create_topic(session_pool)
            create_transfer(session_pool)

            message_count = write_to_topic(self.driver)

            #
            # 2. check written data is correct
            #
            wait_transfer_finished(session_pool, message_count)


@unittest.skip('uncomment after 24-4 excluded. functionality raise from 25-1-2')
class TestTransferRestartToAnotherVersion(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        #
        # Setup cluster
        #
        yield from self.setup_cluster(
            # # Some feature flags can be passed. And other KikimrConfigGenerator options
            # extra_feature_flags={
            #     "some_feature_flag": True,
            # }
        )

    @pytest.mark.parametrize("store", ["row", "column"])
    def test_transfer(self, store):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            #
            # 1. Fill table with data
            #
            create_table(session_pool, store)
            create_topic(session_pool)
            create_transfer(session_pool)

            self.change_cluster_version()

            message_count = write_to_topic(self.driver)

            #
            # 2. check written data is correct
            #
            wait_transfer_finished(session_pool, message_count)


@unittest.skip('uncomment after 24-4 excluded. functionality raise from 25-1-2')
class TestTransferRollingUpdate(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        #
        # Setup cluster
        #
        yield from self.setup_cluster(
            # # Some feature flags can be passed. And other KikimrConfigGenerator options
            # extra_feature_flags={
            #     "some_feature_flag": True,
            # }
        )

    @pytest.mark.parametrize("store", ["row", "column"])
    def test_transfer(self, store):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            #
            # 1. Fill table with data
            #
            create_table(session_pool, store)
            create_topic(session_pool)
            create_transfer(session_pool)

            self.change_cluster_version()

            message_count = 0
            for _ in self.roll():
                message_count += write_to_topic(self.driver)

            #
            # 2. check written data is correct
            #
            wait_transfer_finished(session_pool, message_count)

