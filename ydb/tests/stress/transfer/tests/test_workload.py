# -*- coding: utf-8 -*-
import pytest
import time
import ydb

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


class TestYdbTransferWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            extra_feature_flags={
                "enable_parameterized_decimal": True,
                "enable_table_datetime64": True,
                "enable_vector_index": True,
                "enable_topic_transfer": True,
            }
        ))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test(self, store_type):
        self.database = '/Root'
        self.message_count = 10000

        driver_config = ydb.DriverConfig(f'grpc://localhost:{self.cluster.nodes[1].grpc_port}', self.database)

        with ydb.Driver(driver_config) as driver:
            driver.wait(timeout=5)

            with ydb.QuerySessionPool(driver) as pool:
                print(f"CREATE TABLE {store_type}")
                pool.execute_with_retries(
                    f"""
                        CREATE TABLE target_table_{store_type} (
                            partition Uint32 NOT NULL,
                            offset Uint64 NOT NULL,
                            message Utf8,
                            PRIMARY KEY (partition, offset)
                        )  WITH (
                            STORE = {store_type}
                        );
                    """
                )

                print("CREATE TOPIC")
                pool.execute_with_retries(
                    f"""
                        CREATE TOPIC source_topic_{store_type};
                    """
                )

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

                pool.execute_with_retries(
                    """
                        {0}

                        CREATE TRANSFER transfer_{1}
                        FROM source_topic_{1} TO target_table_{1} USING $l
                        WITH (
                            CONNECTION_STRING = 'grpc://localhost:{2}/?database={3}',
                            FLUSH_INTERVAL = Interval('PT1S'),
                            BATCH_SIZE_BYTES = 8388608
                        );
                    """.format(lmb, store_type, self.cluster.nodes[1].grpc_port, self.database)
                )

                with driver.topic_client.writer(f"source_topic_{store_type}", producer_id="producer-id") as writer:
                    for i in range(self.message_count):
                        writer.write(ydb.TopicWriterMessage(f"message-{i}"))

                for i in range(30):
                    time.sleep(1)

                    rss = pool.execute_with_retries(
                        f"""
                            SELECT MAX(offset) AS last_offset
                            FROM target_table_{store_type};
                        """
                    )
                    rs = rss[0]
                    row = rs.rows[0]

                    print("Last offset={row.last_offset}, expected={self.message_count}")
                    if row.last_offset == (self.message_count - 1):
                        return
