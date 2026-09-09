import logging
import random
import string
import time
from typing import Callable

import pytest

from ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.tools.datastreams_helpers.control_plane import create_read_rule


logger = logging.getLogger(__name__)


class TestStreamingPartitions(StreamingTestBase):
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_restart_query_after_partition_increase(
        self: StreamingTestBase,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        local_topics: bool,
    ) -> None:
        inp, out, endpoint = self.get_io_names(
            kikimr,
            f"test_restart_after_part_inc{local_topics!s:.1}",
            local_topics,
            entity_name,
            partitions_count=1,
        )

        name = f"test_restart_after_part_inc_{local_topics!s:.1}"
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $in = SELECT value FROM {inp}
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(value String NOT NULL))
                WHERE value LIKE "%data%";
                INSERT INTO {out} SELECT value FROM $in;
            END DO;'''

        kikimr.ydb_client.query(sql.format(query_name=name, inp=inp, out=out))
        self.wait_completed_checkpoints(kikimr, name)

        # Stop the query before altering the topic partition count
        logger.debug(f"stopping query {name}")
        kikimr.ydb_client.query(f"ALTER STREAMING QUERY `{name}` SET (RUN = FALSE);")
        time.sleep(0.5)

        logger.debug(f"altering topic {self.input_topic} partition count to 20")
        self.get_ydb_client(kikimr, local_topics).driver.topic_client.alter_topic(
            self.input_topic, set_min_active_partitions=20
        )

        logger.debug(f"restarting query {name} without recompilation")
        kikimr.ydb_client.query(f"ALTER STREAMING QUERY `{name}` SET (RUN = TRUE);")
        self.wait_completed_checkpoints(kikimr, name, timeout=30)

        # Write data with random partition keys so messages land on different partitions
        message_count = 20
        for _ in range(message_count):
            self.write_stream(
                ['{"value": "my_data"}'],
                topic_path=None,
                partition_key=''.join(random.choices(string.digits, k=8)),
                endpoint=endpoint,
            )

        expected_data = ["my_data" for _ in range(message_count)]
        assert self.read_stream(message_count, topic_path=self.output_topic, endpoint=endpoint) == expected_data

        kikimr.ydb_client.query(f"DROP STREAMING QUERY `{name}`;")

    def test_streaming_query_reads_auto_partitioned_topic(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str]) -> None:
        input_topic = entity_name("auto_partitioned_input")
        output_topic = entity_name("auto_partitioned_output")
        query_name = entity_name("auto_partitioned_query")
        consumer_name = "auto_partitioned_consumer"

        kikimr.ydb_client.query(f"""
            CREATE TOPIC `{input_topic}`
            WITH (
                AUTO_PARTITIONING_STRATEGY = 'SCALE_UP',
                MIN_ACTIVE_PARTITIONS = 1,
                MAX_ACTIVE_PARTITIONS = 3,
                AUTO_PARTITIONING_STABILIZATION_WINDOW = Interval('PT10S'),
                AUTO_PARTITIONING_UP_UTILIZATION_PERCENT = 2,
                AUTO_PARTITIONING_DOWN_UTILIZATION_PERCENT = 1
            );
            CREATE TOPIC `{output_topic}`;
        """)
        create_read_rule(output_topic, consumer_name, default_endpoint=kikimr.endpoint)

        topic_client = kikimr.ydb_client.driver.topic_client
        load_message_payload = "x" * 1000 * 1000
        for message_index in range(10):
            for producer_id in ("auto-split-producer-1", "auto-split-producer-2"):
                load_message = f"{producer_id}-{message_index}-{load_message_payload}"
                for attempt in range(5):
                    try:
                        kikimr.ydb_client.topic_write(
                            input_topic,
                            [load_message],
                            producer_id=producer_id,
                        )
                        break
                    except RuntimeError as error:
                        if (
                            str(error) != "StopIteration interacts badly with generators and cannot be raised into a Future"
                            or attempt == 4
                        ):
                            raise
                        logger.warning("Write stream closed during auto-partitioning; retrying with a new writer")
                        time.sleep(1)

        def has_real_split() -> bool:
            partitions = {
                partition.partition_id: partition
                for partition in topic_client.describe_topic(input_topic).partitions
            }
            parent = partitions.get(0)
            if parent is None or parent.active or len(parent.child_partition_ids) != 2:
                return False

            return all(
                child_id in partitions
                and partitions[child_id].active
                and 0 in partitions[child_id].parent_partition_ids
                for child_id in parent.child_partition_ids
            )

        assert wait_for(
            has_real_split,
            timeout_seconds=120,
            step_seconds=1,
        ), "The input topic did not perform a real auto-split of partition 0"

        partitions = {
            partition.partition_id: partition
            for partition in topic_client.describe_topic(input_topic).partitions
        }
        child_partition_ids = sorted(partitions[0].child_partition_ids)

        kikimr.ydb_client.query(f"""
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{output_topic}`
                SELECT Data FROM `{input_topic}`;
            END DO;
        """)
        self.wait_completed_checkpoints(kikimr, query_name)

        child_partition_messages = []
        for message_index, partition_id in enumerate(child_partition_ids):
            message = f"partition-{partition_id}-{message_index}"
            child_partition_messages.append(message)
            kikimr.ydb_client.topic_write(
                input_topic,
                [message],
                partition_id=partition_id,
            )

        assert sorted(kikimr.ydb_client.topic_read(output_topic, consumer_name, len(child_partition_ids))) == sorted(child_partition_messages)

        kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`;")

    def test_streaming_query_restarts_after_auto_partitioning(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str]) -> None:
        input_topic = entity_name("auto_partitioned_restart_input")
        output_topic = entity_name("auto_partitioned_restart_output")
        query_name = entity_name("auto_partitioned_restart_query")
        consumer_name = "auto_partitioned_restart_consumer"

        kikimr.ydb_client.query(f"""
            CREATE TOPIC `{input_topic}`
            WITH (
                AUTO_PARTITIONING_STRATEGY = 'SCALE_UP',
                MIN_ACTIVE_PARTITIONS = 1,
                MAX_ACTIVE_PARTITIONS = 3,
                AUTO_PARTITIONING_STABILIZATION_WINDOW = Interval('PT10S'),
                AUTO_PARTITIONING_UP_UTILIZATION_PERCENT = 2,
                AUTO_PARTITIONING_DOWN_UTILIZATION_PERCENT = 1
            );
            CREATE TOPIC `{output_topic}`;
        """)
        create_read_rule(output_topic, consumer_name, default_endpoint=kikimr.endpoint)

        kikimr.ydb_client.query(f"""
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{output_topic}`
                SELECT Data FROM `{input_topic}`;
            END DO;
        """)
        self.wait_completed_checkpoints(kikimr, query_name)

        kikimr.ydb_client.topic_write(input_topic, ["before-split"], partition_id=0)
        assert sorted(kikimr.ydb_client.topic_read(output_topic, consumer_name, 1)) == ["before-split"]

        topic_client = kikimr.ydb_client.driver.topic_client
        load_message_payload = "x" * 1000 * 1000
        load_messages_count = 20
        load_messages = []
        for message_index in range(load_messages_count):
            for producer_id in ("auto-split-restart-producer-1", "auto-split-restart-producer-2"):
                load_message = f"{producer_id}-{message_index}-{load_message_payload}"
                load_messages.append(load_message)
                for attempt in range(5):
                    try:
                        kikimr.ydb_client.topic_write(
                            input_topic,
                            [load_message],
                            producer_id=producer_id,
                        )
                        break
                    except RuntimeError as error:
                        if (
                            str(error) != "StopIteration interacts badly with generators and cannot be raised into a Future"
                            or attempt == 4
                        ):
                            raise
                        logger.warning("Write stream closed during auto-partitioning; retrying with a new writer")
                        time.sleep(1)

        def has_real_split() -> bool:
            partitions = {
                partition.partition_id: partition
                for partition in topic_client.describe_topic(input_topic).partitions
            }
            parent = partitions.get(0)
            if parent is None or parent.active or len(parent.child_partition_ids) != 2:
                return False

            return all(
                child_id in partitions
                and partitions[child_id].active
                and 0 in partitions[child_id].parent_partition_ids
                for child_id in parent.child_partition_ids
            )

        assert wait_for(
            has_real_split,
            timeout_seconds=120,
            step_seconds=1,
        ), "The input topic did not perform a real auto-split of partition 0"

        # The partition-count checker restarts the query after the split so it
        # can create read sessions for the new active partitions.
        self.wait_completed_checkpoints(kikimr, query_name)
        assert sorted(kikimr.ydb_client.topic_read(output_topic, consumer_name, len(load_messages))) == sorted(load_messages)

        partitions = topic_client.describe_topic(input_topic).partitions
        active_partition_ids = sorted(partition.partition_id for partition in partitions if partition.active)
        active_partition_messages = []
        for message_index, partition_id in enumerate(active_partition_ids):
            message = f"partition-{partition_id}-{message_index}"
            active_partition_messages.append(message)
            kikimr.ydb_client.topic_write(
                input_topic,
                [message],
                partition_id=partition_id,
            )

        assert sorted(kikimr.ydb_client.topic_read(output_topic, consumer_name, len(active_partition_ids))) == sorted(active_partition_messages)

        kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`;")
