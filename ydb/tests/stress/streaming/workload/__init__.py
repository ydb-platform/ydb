# -*- coding: utf-8 -*-
import ydb
import time
import unittest
import uuid
import random
from collections import defaultdict

class Workload(unittest.TestCase):
    def __init__(self, endpoint, database, duration):
        self.database = database
        self.endpoint = endpoint
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.pool = ydb.QuerySessionPool(self.driver)
        self.duration = duration
        self.input_topic = 'streaming_recipe/input_topic'
        self.output_topic = 'streaming_recipe/output_topic'
        self.query_name = 'my_queries/query_name'
        self.consumer_name = 'consumer_name'

    def create_topics(self):
        self.pool.execute_with_retries(
            f"""
                CREATE TOPIC `{self.input_topic}`;
                CREATE TOPIC `{self.output_topic}` (CONSUMER {self.consumer_name});
            """
        )

    def create_external_data_source(self):
        self.pool.execute_with_retries(
            f"""
                CREATE EXTERNAL DATA SOURCE source_name WITH (
                    SOURCE_TYPE="Ydb",
                    LOCATION="{self.endpoint}",
                    DATABASE_NAME="{self.database}",
                    SHARED_READING="false",
                    AUTH_METHOD="NONE");
            """
        )

    def create_streaming_query(self):
        self.pool.execute_with_retries(
            f"""
                CREATE STREAMING QUERY `my_queries/query_name` AS DO BEGIN
                $input = (
                    SELECT * FROM
                        source_name.`{self.input_topic}` WITH (
                            FORMAT = 'json_each_row',
                            SCHEMA (time Uint64 NOT NULL, level String NOT NULL)
                        )
                );
                $filtered = (SELECT * FROM $input WHERE level == 'error');

                $number_errors = (
                    SELECT COUNT(*) AS error_count, CAST(HOP_START() AS String) AS ts
                    FROM $filtered
                    GROUP BY
                        HoppingWindow(CAST(time AS Timestamp), 'PT1S', 'PT1S')
                );

                $json = (SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
                    FROM $number_errors
                );

                INSERT INTO source_name.`{self.output_topic}`
                SELECT * FROM $json;
                END DO;
            """
        )

    def check_status(self):
        result_sets = self.pool.execute_with_retries("SELECT Status FROM `.sys/streaming_queries`")
        status = result_sets[0].rows[0].Status
        if status != 'RUNNING':
            raise Exception(f"Unexpected query status, expected 'RUNNING', actual {status}")


    def write_to_input_topic(self):
        finished_at = time.time() + self.duration
        self.message_count = 0

        with self.driver.topic_client.writer(self.input_topic, producer_id="producer-id") as writer:
            while time.time() < finished_at:
                level = "error" if random.choice([True, False]) else "warn"
                writer.write(ydb.TopicWriterMessage(f'{{"time": {int(time.time() * 1000000)}, "level": "{level}"}}'))
                self.message_count += 1

    def read_from_output_topic(self):
        with self.driver.topic_client.reader(self.output_topic, self.consumer_name) as reader:
            messages_info = defaultdict(list)
            count = 0
            while True:
                try:
                    mess = reader.receive_message(timeout=1)
                    count += 1
                except TimeoutError:
                    break
            if count < self.duration / 2:
                raise Exception(f"There is not enough data in the output topic, actual {count}, expected ~{self.duration}")

    def loop(self):
        self.pool.execute_with_retries("GRANT ALL ON `/Root` TO ``;")
        self.create_topics()
        self.create_external_data_source()
        self.create_streaming_query()
        self.check_status()
        self.write_to_input_topic()
        self.read_from_output_topic()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.stop()
        self.driver.stop()
