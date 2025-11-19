# -*- coding: utf-8 -*-
import ydb

import time
import unittest
import uuid


class Workload(unittest.TestCase):
    def __init__(self, endpoint, database, duration):
        self.database = database
        self.endpoint = endpoint
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.pool = ydb.QuerySessionPool(self.driver)
        self.duration = duration
  #      self.mode = mode
 #       self.topic = topic
#        self.id = f"{uuid.uuid1()}".replace("-", "_")

        self.input_topic = 'streaming_recipe/input_topic'
        self.output_topic = 'streaming_recipe/output_topic'
        self.query_name = 'my_queries/query_name'
        self.consumer_name = 'consumer_name'

    # def create_table(self):
    #     self.pool.execute_with_retries(
    #         f"""
    #             CREATE TABLE {self.table_name} (
    #                 partition Uint32 NOT NULL,
    #                 offset Uint64 NOT NULL,
    #                 message Utf8,
    #                 PRIMARY KEY (partition, offset)
    #             )  WITH (
    #                 STORE = {self.mode}
    #             );
    #         """
    #     )

    def create_topics(self):
        self.pool.execute_with_retries(
            f"""
                CREATE TOPIC `{self.input_topic}`;
                CREATE TOPIC `{self.output_topic}` (CONSUMER {self.consumer_name});
            """
        )

    def create_external_data_source(self):
        #logger.debug("create_external_data_source")
        endpoint = self.endpoint # f"localhost:{self.cluster.nodes[1].port}"
        self.pool.execute_with_retries(
            f"""
                CREATE EXTERNAL DATA SOURCE source_name WITH (
                    SOURCE_TYPE="Ydb",
                    LOCATION="{endpoint}",
                    DATABASE_NAME="{self.database}",
                    SHARED_READING="false",
                    AUTH_METHOD="NONE");
            """
        )

    def create_streaming_query(self):
        #logger.debug("create_streaming_query")
        self.pool.execute_with_retries(
            f"""
                CREATE STREAMING QUERY `my_queries/query_name` AS DO BEGIN
                $input = (
                    SELECT * FROM
                        source_name.`{self.input_topic}` WITH (
                            FORMAT = 'json_each_row',
                            SCHEMA (time String NOT NULL, level String NOT NULL, host String NOT NULL)
                        )
                );
                $filtered = (SELECT * FROM $input WHERE level == 'error');

                $number_errors = (
                    SELECT host, COUNT(*) AS error_count, CAST(HOP_START() AS String) AS ts
                    FROM $filtered
                    GROUP BY
                        HoppingWindow(CAST(time AS Timestamp), 'PT600S', 'PT600S'),
                        host
                );

                $json = (SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
                    FROM $number_errors
                );

                INSERT INTO source_name.`{self.output_topic}`
                SELECT * FROM $json;
                END DO;

            """
        )

    def write_to_topic(self):
        finished_at = time.time() + self.duration
        self.message_count = 0

        with self.driver.topic_client.writer(self.input_topic, producer_id="producer-id") as writer:
            while time.time() < finished_at:
                writer.write(ydb.TopicWriterMessage(f"message-{time.time()}"))
                self.message_count += 1

    

    def loop(self):
        self.create_topics()
        self.create_external_data_source()
        self.create_streaming_query()

        self.write_to_topic()

     #   self.wait_transfer_finished()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.stop()
        self.driver.stop()
