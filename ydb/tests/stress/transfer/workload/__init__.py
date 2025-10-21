# -*- coding: utf-8 -*-
import ydb

import time
import unittest
import uuid


class Workload(unittest.TestCase):
    def __init__(self, endpoint, database, duration, mode, topic):
        self.database = database
        self.endpoint = endpoint
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.pool = ydb.QuerySessionPool(self.driver)
        self.duration = duration
        self.mode = mode
        self.topic = topic
        self.id = f"{uuid.uuid1()}".replace("-", "_")
        self.table_name = f"transfer_target_table_{mode}_{self.id}"
        self.topic_name = f"transfer_source_topic_{mode}_{self.id}"
        self.transfer_name = f"transfer_{mode}_{self.id}"

    def create_table(self):
        self.pool.execute_with_retries(
            f"""
                CREATE TABLE {self.table_name} (
                    partition Uint32 NOT NULL,
                    offset Uint64 NOT NULL,
                    message Utf8,
                    PRIMARY KEY (partition, offset)
                )  WITH (
                    STORE = {self.mode}
                );
            """
        )

    def create_topic(self):
        self.pool.execute_with_retries(
            f"CREATE TOPIC {self.topic_name};"
        )

    def create_transfer(self):
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

        cs = "" if self.topic == "local" else f"CONNECTION_STRING = '{self.endpoint}/?database={self.database}',"

        self.pool.execute_with_retries(
            f"""
                {lmb}

                CREATE TRANSFER {self.transfer_name}
                FROM {self.topic_name} TO {self.table_name} USING $l
                WITH (
                    {cs}
                    FLUSH_INTERVAL = Interval('PT1S'),
                    BATCH_SIZE_BYTES = 8388608
                );
            """
        )

    def write_to_topic(self):
        finished_at = time.time() + self.duration
        self.message_count = 0

        with self.driver.topic_client.writer(self.topic_name, producer_id="producer-id") as writer:
            while time.time() < finished_at:
                writer.write(ydb.TopicWriterMessage(f"message-{time.time()}"))
                self.message_count += 1

    def wait_transfer_finished(self):
        iterations = 30

        last_offset = -1

        for i in range(iterations):
            time.sleep(1)

            rss = self.pool.execute_with_retries(
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

    def loop(self):
        self.create_table()
        self.create_topic()
        self.create_transfer()

        self.write_to_topic()

        self.wait_transfer_finished()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.stop()
        self.driver.stop()
