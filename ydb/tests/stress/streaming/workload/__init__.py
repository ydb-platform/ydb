# -*- coding: utf-8 -*-
import ydb
from ydb.tests.stress.common.instrumented_pools import InstrumentedQuerySessionPool
import time
import random
import logging

logger = logging.getLogger(__name__)


class Workload():
    def __init__(self, endpoint: str, database: str, duration: int, partitions_count: int, prefix: str, enable_watermarks: bool):
        self.database = database
        self.endpoint = endpoint
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.pool = InstrumentedQuerySessionPool(self.driver)
        self.duration = duration
        self.prefix = prefix
        self.input_topic = f'{prefix}/input_topic'
        self.output_topic = f'{prefix}/output_topic'
        self.query_name = f'{prefix}/query_name'
        self.consumer_name = 'consumer_name'
        self.partitions_count = partitions_count
        self.receive_message_timeout_sec = 1
        self.enable_watermarks = enable_watermarks
        logger.info("Workload::init")

    def create_topics(self):
        logger.info("Workload::create_topics")
        self.pool.execute_with_retries(
            f"""
                CREATE TOPIC `{self.input_topic}` WITH (min_active_partitions = {self.partitions_count}, retention_period = Interval('PT1H'));
                CREATE TOPIC `{self.output_topic}` (CONSUMER {self.consumer_name}) WITH (retention_period = Interval('PT12H'));
            """
        )

    def drop_topics(self):
        logger.info("Workload::drop_topics")

        self.pool.execute_with_retries(
            f"""
                DROP TOPIC  `{self.input_topic}`;
                DROP TOPIC  `{self.output_topic}`;
            """
        )

    def create_external_data_source(self):
        logger.info("Workload::create_external_data_source")
        shared_reading = str(self.enable_watermarks).lower()
        query = f"""
                CREATE EXTERNAL DATA SOURCE `{self.prefix}/source_name` WITH (
                    SOURCE_TYPE="Ydb",
                    LOCATION="{self.endpoint}",
                    DATABASE_NAME="{self.database}",
                    SHARED_READING="{shared_reading}",
                    AUTH_METHOD="NONE"
                );
            """
        self.pool.execute_with_retries(query)

    def create_streaming_query(self):
        logger.info("Workload::create_streaming_query")
        max_tasks_per_stage = 'PRAGMA ydb.MaxTasksPerStage = "1";' if self.enable_watermarks else ""
        watermarks = ', WATERMARK AS (CAST(time AS Timestamp) - Interval("PT1M"))' if self.enable_watermarks else ""
        query = f"""
                CREATE STREAMING QUERY `{self.prefix}/query_name` AS DO BEGIN
                {max_tasks_per_stage}
                $input = (
                    SELECT * FROM
                        `{self.prefix}/source_name`.`{self.input_topic}` WITH (
                            FORMAT = 'json_each_row',
                            SCHEMA (time Uint64 NOT NULL, level String NOT NULL)
                            {watermarks}
                        )
                );
                $filtered = (SELECT * FROM $input WHERE level = 'error');

                $number_errors = (
                    SELECT COUNT(*) AS error_count, CAST(HOP_START() AS String) AS ts
                    FROM $filtered
                    GROUP BY
                        HoppingWindow(CAST(time AS Timestamp), 'PT1S', 'PT1S')
                );

                $json = (SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
                    FROM $number_errors
                );

                INSERT INTO `{self.prefix}/source_name`.`{self.output_topic}`
                SELECT * FROM $json;
                END DO;
            """
        self.pool.execute_with_retries(query)

    def check_status(self):
        result_sets = self.pool.execute_with_retries(f"SELECT Status FROM `.sys/streaming_queries` WHERE Path = '{self.database}/{self.prefix}/query_name'")
        status = result_sets[0].rows[0].Status
        if status != 'RUNNING':
            raise Exception(f"Unexpected query status: expected 'RUNNING', got '{status}'")

    def write_to_input_topic(self):
        logger.info("Workload::write_to_input_topic")

        writers = []
        for i in range(self.partitions_count):
            writers.append(self.driver.topic_client.writer(self.input_topic, partition_id=i))

        finished_at = time.time() + self.duration
        while time.time() < finished_at:
            for writer in writers:
                messages = []
                for i in range(100):
                    level = "error" if random.choice([True, False]) else "warn"
                    messages.append(f'{{"time": {int(time.time() * 1000000)}, "level": "{level}"}}')
                try:
                    writer.write(messages, timeout=0.5)
                    writer.flush()
                except Exception:
                    pass

        for writer in writers:
            writer.close(flush=False)

        logger.info("Workload::write_to_input_topic end")

    def read_from_output_topic(self):
        logger.info("Workload::read_from_output_topic")
        count = 0
        with self.driver.topic_client.reader(self.output_topic, self.consumer_name) as reader:
            while True:
                try:
                    reader.receive_message(timeout=self.receive_message_timeout_sec)
                    count += 1
                except TimeoutError:
                    break
        expected = self.duration  # Group by HOP 1s
        if count < expected * 0.7:
            raise Exception(f"Insufficient data in output topic: expected ~{expected} messages, got {count}")

    def loop(self):
        self.create_topics()
        self.create_external_data_source()
        self.create_streaming_query()
        self.check_status()
        self.write_to_input_topic()
        self.read_from_output_topic()
        self.drop_topics()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.stop()
        self.driver.stop()
