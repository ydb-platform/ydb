import datetime
import json
import time
from typing import Callable

import pytest

from ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase, counter_nodes
from ydb.tests.library.common.wait_for import wait_for


class TestRescaling(StreamingTestBase):

    def _reader_count(self, kikimr: Kikimr) -> int:
        return sum(
            self.get_actor_count(kikimr, node_id, "DQ_MESSAGE_STREAM_READ_ACTOR")
            for node_id in counter_nodes(kikimr.cluster)
        )

    def _wait_started(self, kikimr: Kikimr, query_name: str):
        self.wait_completed_checkpoints(kikimr, query_name)

    def _wait_reader_count(self, kikimr: Kikimr, query_name: str, expected_readers=6) -> int:
        assert wait_for(
            lambda: self._reader_count(kikimr) == expected_readers,
            timeout_seconds=60,
            step_seconds=1,
        ), f"Expected {expected_readers} PQ readers after CREATE, got {self._reader_count(kikimr)}"

    def _stop_query(self, kikimr: Kikimr, query_name: str) -> None:
        kikimr.ydb_client.query(f"ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);")
        assert wait_for(
            lambda: self._reader_count(kikimr) == 0, timeout_seconds=120, step_seconds=1
        ), "PQ readers did not stop"

    def _resume_query(
        self,
        kikimr: Kikimr,
        query_name: str,
        readers_before: int,
        expect_growth: bool,
    ) -> None:
        # Resume the saved graph without recompiling the query.
        kikimr.ydb_client.query(f"ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);")
        assert wait_for(
            lambda: (
                self._reader_count(kikimr) > readers_before
                if expect_growth
                else self._reader_count(kikimr) == readers_before
            ),
            timeout_seconds=120,
            step_seconds=1,
        ), f"Unexpected reader count: before={readers_before}, after={self._reader_count(kikimr)}"
        self.wait_completed_checkpoints(kikimr, query_name)

    def _restart_query(
        self,
        kikimr: Kikimr,
        query_name: str,
        readers_before: int,
        scale_up: bool,
        added_slots: list,
    ) -> None:
        self._stop_query(kikimr, query_name)
        if scale_up:
            added_slots.extend(kikimr.cluster.register_and_start_slots(kikimr.get_database_name(), count=3))
            kikimr.cluster.wait_tenant_up(kikimr.get_database_name(), token="root@builtin")
            time.sleep(1)
        self._resume_query(kikimr, query_name, readers_before, expect_growth=scale_up)

    def _cleanup_query(self, kikimr: Kikimr, query_name: str, added_slots: list) -> None:
        try:
            kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`;")
        finally:
            if added_slots:
                kikimr.cluster.unregister_and_stop_slots(added_slots)

    def _write_and_checkpoint(self, kikimr: Kikimr, query_name: str, batches: dict[int, list[str]]) -> None:
        before = self.get_streaming_query_metric(kikimr, query_name, "streaming.query.input.bytes")
        for partition_id, messages in batches.items():
            kikimr.ydb_client.topic_write(self.input_topic, messages, partition_id=partition_id)
        self.wait_streaming_query_metric(
            kikimr,
            query_name,
            "streaming.query.input.bytes",
            expected_value=before + sum(len(message.encode()) for messages in batches.values() for message in messages),
        )
        # A full-graph barrier after ingestion includes operator state, not just offsets.
        self.wait_completed_checkpoints(kikimr, query_name)

    def _check_downstream_tasks(
        self,
        kikimr: Kikimr,
        query_name: str,
        readers_before: int,
        tasks_before: int,
    ) -> None:
        readers_after = self._reader_count(kikimr)
        self.wait_streaming_query_metric(
            kikimr,
            query_name,
            "streaming.query.tasks.count",
            expected_value=tasks_before - readers_before + readers_after,
        )
        tasks_after = self.get_streaming_query_metric(kikimr, query_name, "streaming.query.tasks.count")
        assert (
            tasks_after - readers_after == tasks_before - readers_before
        ), "Downstream task count changed during PQ source rescaling"

    @pytest.mark.parametrize(
        "max_tasks_per_stage",
        [1, 5, 30],
        ids=["max_tasks_1", "max_tasks_5", "max_tasks_30"],
    )
    def test_pq_source_restart_preserves_max_tasks_per_stage(
        self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        max_tasks_per_stage: int,
    ) -> None:
        """Restarting the saved graph must preserve the user-specified reader limit."""
        query_name = entity_name("pq_source_restart_preserves_max_tasks_per_stage")
        inp, out, _ = self.get_io_names(
            kikimr,
            query_name,
            True,
            entity_name,
            partitions_count=100,
        )
        kikimr.ydb_client.query(f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                PRAGMA ydb.MaxTasksPerStage = "{max_tasks_per_stage}";
                INSERT INTO {out} SELECT Data FROM {inp};
            END DO;
        ''')
        try:
            expected_tasks = min(max_tasks_per_stage, 6)
            self._wait_started(kikimr, query_name)
            self._wait_reader_count(kikimr, query_name, expected_tasks)
            self._stop_query(kikimr, query_name)
            self._resume_query(kikimr, query_name, expected_tasks, expect_growth=False)
            readers_after = self._reader_count(kikimr)
            assert (
                readers_after == expected_tasks
            ), f"Expected {expected_tasks} readers after STOP/START, got {readers_after}"
        finally:
            self._cleanup_query(kikimr, query_name, [])

    @pytest.mark.parametrize("kikimr", [{"enable_exactly_once_topics_writing": True}], indirect=True)
    def test_pq_source_rescaling_skips_deferred_publication(
        self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
    ) -> None:
        """Keep the saved graph and sink state when deferred publication is enabled."""
        partitions_count = 100
        query_name = entity_name("pq_source_rescaling_skips_deferred_publication")
        inp, out, _ = self.get_io_names(
            kikimr,
            query_name,
            True,
            entity_name,
            partitions_count=partitions_count,
        )

        def check_reading(phase: str) -> None:
            expected = [f"{phase}_{partition}" for partition in range(partitions_count)]
            for partition, message in enumerate(expected):
                kikimr.ydb_client.topic_write(self.input_topic, [message], partition_id=partition)
            actual = kikimr.ydb_client.topic_read(self.output_topic, self.consumer_name, len(expected))
            assert sorted(actual) == sorted(expected), (actual, expected)
            self.wait_completed_checkpoints(kikimr, query_name)

        added_slots = []
        kikimr.ydb_client.query(f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                INSERT INTO {out} WITH (DELIVERY_GUARANTEE = "exactly_once")
                SELECT Data FROM {inp};
            END DO;
        ''')
        try:
            self._wait_started(kikimr, query_name)
            readers_before = self._reader_count(kikimr)
            check_reading("before")
            assert 0 < readers_before < (partitions_count + 4) // 5, readers_before
            self._stop_query(kikimr, query_name)
            added_slots.extend(kikimr.cluster.register_and_start_slots(kikimr.get_database_name(), count=3))
            kikimr.cluster.wait_tenant_up(kikimr.get_database_name(), token="root@builtin")
            self._resume_query(kikimr, query_name, readers_before, expect_growth=False)
            check_reading("after")
            assert self._reader_count(kikimr) == readers_before, "Deferred-publication query was rescaled"
        finally:
            self._cleanup_query(kikimr, query_name, added_slots)

    @pytest.mark.parametrize("scale_up", [False, True], ids=["restart", "scale_up"])
    def test_pq_source_rescaling_insert_select(
        self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        scale_up: bool,
    ) -> None:
        """Resume a plain topic-to-topic query and read every partition after rescaling."""
        partitions_count = 100
        query_name = entity_name("pq_source_rescaling_insert_select")
        inp, out, _ = self.get_io_names(
            kikimr,
            query_name,
            True,
            entity_name,
            partitions_count=partitions_count,
        )
        client = kikimr.ydb_client

        def check_reading(phase: str) -> None:
            expected = []
            for partition_id in range(partitions_count):
                messages = [f"{phase}_{partition_id}_{index}" for index in range(2)]
                client.topic_write(self.input_topic, messages, partition_id=partition_id)
                expected.extend(messages)
            actual = client.topic_read(self.output_topic, self.consumer_name, len(expected))
            # Ordering across partitions is unspecified; compare multisets to
            # detect missing messages, duplicates and replay of the earlier phase.
            assert sorted(actual) == sorted(expected), (actual, expected)
            self.wait_completed_checkpoints(kikimr, query_name)

        added_slots = []
        kikimr.ydb_client.query(f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                INSERT INTO {out} SELECT Data FROM {inp};
            END DO;
        ''')
        try:
            expected_readers_before = 6
            self._wait_started(kikimr, query_name)
            self._wait_reader_count(kikimr, query_name, expected_readers_before)
            # Enough partitions for genuine scale-up despite default grouping.
            assert 0 < expected_readers_before < (partitions_count + 4) // 5, expected_readers_before
            check_reading("before")

            self._restart_query(kikimr, query_name, expected_readers_before, scale_up, added_slots)
            check_reading("after")
        finally:
            self._cleanup_query(kikimr, query_name, added_slots)

    @pytest.mark.parametrize(
        "scale_up",
        [
            False,
            pytest.param(True, marks=pytest.mark.skip(reason="Rescaling queries with aggregations is not supported")),
        ],
        ids=["restart", "scale_up"],
    )
    def test_pq_source_rescaling_hopping_closed_window(
        self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        scale_up: bool,
    ) -> None:
        """Resume after checkpointing a closed target window and process a new window when readers scale up."""
        partitions_count = 100
        query_name = entity_name("pq_source_rescaling_hopping_closed_window")
        inp, out, _ = self.get_io_names(
            kikimr,
            query_name,
            True,
            entity_name,
            partitions_count=partitions_count,
        )
        client = kikimr.ydb_client
        # Stay ahead of write-time watermarks but within the five-minute
        # future limit of the event-time watermark generator.
        base = (int(time.time()) // 20) * 20 + 120

        def timestamp(seconds: int) -> str:
            return (
                datetime.datetime.fromtimestamp(base + seconds, datetime.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )

        def event(seconds: int, value: int = 0, key: str = "clock") -> str:
            return json.dumps({"ts": timestamp(seconds), "key": key, "value": value})

        added_slots = []
        kikimr.ydb_client.query(f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                $input = SELECT CAST(ts AS Timestamp) AS event_time, key, value
                FROM {inp} WITH (
                    FORMAT = json_each_row,
                    SCHEMA (ts String NOT NULL, key String NOT NULL, value Uint64 NOT NULL),
                    WATERMARK = CAST(ts AS Timestamp) - Interval('PT1S'),
                    WATERMARK_IDLE_TIMEOUT = 'PT5S'
                );
                $windows = SELECT HOP_END() AS window_end, SUM(value) AS total
                FROM $input
                GROUP BY HoppingWindow(event_time, 'PT10S', 'PT20S');
                INSERT INTO {out}
                SELECT Unwrap(CAST(total AS String)) FROM $windows
                WHERE window_end IN (Timestamp('{timestamp(20)}'), Timestamp('{timestamp(80)}'));
            END DO;
        ''')
        try:
            expected_readers_before = 6
            self._wait_started(kikimr, query_name)
            assert 0 < expected_readers_before < (partitions_count + 4) // 5, expected_readers_before
            self.wait_streaming_query_metric(
                kikimr,
                query_name,
                "streaming.query.tasks.count",
                expected_value=expected_readers_before + 1,
            )
            tasks_before = self.get_streaming_query_metric(kikimr, query_name, "streaming.query.tasks.count")
            assert tasks_before > expected_readers_before

            batches = {partition_id: [event(1)] for partition_id in range(partitions_count)}
            batches[0].extend([event(2, 10, "target"), event(3, 20, "target")])
            self._write_and_checkpoint(kikimr, query_name, batches)
            # Advance all partitions beyond both overlapping windows and save
            # their offsets before stopping. The target window is already closed.
            self._write_and_checkpoint(
                kikimr, query_name, {partition_id: [event(40)] for partition_id in range(partitions_count)}
            )
            assert client.topic_read(self.output_topic, self.consumer_name, 1) == ["30"]
            self.wait_completed_checkpoints(kikimr, query_name)

            self._restart_query(kikimr, query_name, expected_readers_before, scale_up, added_slots)
            self._check_downstream_tasks(kikimr, query_name, expected_readers_before, tasks_before)

            # Exercise every partition, including those moved to new readers,
            # in a disjoint window after recovery.
            self._write_and_checkpoint(
                kikimr,
                query_name,
                {
                    partition_id: [event(62, 1, "target"), event(63, 2, "target")]
                    for partition_id in range(partitions_count)
                },
            )
            self._write_and_checkpoint(
                kikimr, query_name, {partition_id: [event(100)] for partition_id in range(partitions_count)}
            )
            actual = client.topic_read(self.output_topic, self.consumer_name, 1)
            assert actual == [str(3 * partitions_count)], actual
            self.wait_completed_checkpoints(kikimr, query_name)
            self._check_downstream_tasks(kikimr, query_name, expected_readers_before, tasks_before)
        finally:
            self._cleanup_query(kikimr, query_name, added_slots)

    def test_pq_source_rescaling_twice(
        self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
    ) -> None:
        """Restore partition offsets across consecutive 1 -> 2 -> 4 task graphs."""
        query_name = entity_name("pq_source_rescaling_twice")
        inp, out, _ = self.get_io_names(
            kikimr,
            query_name,
            True,
            entity_name,
            partitions_count=1,
        )
        client = kikimr.ydb_client
        kikimr.ydb_client.query(f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                INSERT INTO {out} SELECT Data FROM {inp};
            END DO;
        ''')
        try:
            for phase, (partitions_count, expected_readers) in enumerate([(1, 1), (10, 2), (20, 4)]):
                if phase:
                    self._stop_query(kikimr, query_name)
                    # Default grouping is five partitions per task. Growing the
                    # topic controls parallelism without imposing a saved task cap.
                    client.driver.topic_client.alter_topic(
                        self.input_topic,
                        set_min_active_partitions=partitions_count,
                    )
                    self._resume_query(kikimr, query_name, expected_readers, expect_growth=False)
                else:
                    self._wait_started(kikimr, query_name)
                    self._wait_reader_count(kikimr, query_name, expected_readers)

                assert wait_for(
                    lambda: self._reader_count(kikimr) == expected_readers,
                    timeout_seconds=60,
                    step_seconds=1,
                ), f"Phase {phase}: expected {expected_readers} readers, got {self._reader_count(kikimr)}"
                self.wait_streaming_query_metric(
                    kikimr,
                    query_name,
                    "streaming.query.tasks.count",
                    expected_value=expected_readers,
                )

                expected = []
                for partition_id in range(partitions_count):
                    messages = [f"phase_{phase}_partition_{partition_id}_message_{i}" for i in range(2)]
                    client.topic_write(self.input_topic, messages, partition_id=partition_id)
                    expected.extend(messages)
                actual = client.topic_read(self.output_topic, self.consumer_name, len(expected))
                # Read with the same consumer across all phases: replayed messages
                # from an earlier graph must not replace the new phase's output.
                assert sorted(actual) == sorted(expected), (phase, actual, expected)
                # The next restart must restore a checkpoint of this graph, not
                # just the original single-task checkpoint.
                self.wait_completed_checkpoints(kikimr, query_name)
        finally:
            self._cleanup_query(kikimr, query_name, [])

    @pytest.mark.parametrize("local_topics", [True, False], ids=["local", "external"])
    def test_pq_source_rescaling_partition_increase(
        self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        local_topics: bool,
    ) -> None:
        """Read newly added topic partitions after resuming the saved query."""
        partitions_count = 20
        query_name = entity_name("pq_source_rescaling_partition_increase")
        inp, out, _ = self.get_io_names(
            kikimr,
            query_name,
            local_topics,
            entity_name,
            partitions_count=1,
        )
        client = self.get_ydb_client(kikimr, local_topics)
        added_slots = []
        kikimr.ydb_client.query(f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                $input = SELECT value FROM {inp} WITH (
                    FORMAT = json_each_row,
                    SCHEMA (value String NOT NULL)
                )
                WHERE value LIKE "%data%";
                INSERT INTO {out} SELECT value FROM $input;
            END DO;
        ''')
        try:
            expected_readers_before = 1
            self._wait_started(kikimr, query_name)
            self._wait_reader_count(kikimr, query_name, expected_readers_before)
            assert expected_readers_before == 1, expected_readers_before
            self._stop_query(kikimr, query_name)
            client.driver.topic_client.alter_topic(
                self.input_topic,
                set_min_active_partitions=partitions_count,
            )
            self._resume_query(kikimr, query_name, expected_readers_before, expect_growth=True)

            expected = [f"data_{partition_id}" for partition_id in range(partitions_count)]
            for partition_id, value in enumerate(expected):
                client.topic_write(
                    self.input_topic,
                    [json.dumps({"value": value})],
                    partition_id=partition_id,
                )
            actual = client.topic_read(self.output_topic, self.consumer_name, len(expected))
            assert sorted(actual) == sorted(expected), (actual, expected)
            self.wait_completed_checkpoints(kikimr, query_name)
        finally:
            self._cleanup_query(kikimr, query_name, added_slots)

    @pytest.mark.parametrize("scale_up", [False, True], ids=["restart", "scale_up"])
    def test_pq_source_rescaling_partition_predicate(
        self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        scale_up: bool,
    ) -> None:
        """Rescaling must redistribute selected partition IDs, not their dense indices."""
        # Keep sparse IDs and enough selected partitions for real scale-up
        # with the default grouping of five partitions per reader task.
        partitions_count = 208
        selected = list(range(7, 207, 2))
        query_name = entity_name("pq_source_rescaling_partition_predicate")
        inp, out, _ = self.get_io_names(
            kikimr,
            query_name,
            True,
            entity_name,
            partitions_count=partitions_count,
        )
        client = kikimr.ydb_client

        def check_reading(phase: str) -> None:
            # Excluded partitions must not even reach the JSON parser. This also
            # detects pruning being disabled while a residual filter hides it.
            for partition_id in range(partitions_count):
                if partition_id not in selected:
                    client.topic_write(self.input_topic, ["not valid json"], partition_id=partition_id)
            expected = [f"{phase}_{partition_id}" for partition_id in selected]
            for partition_id, value in zip(selected, expected):
                client.topic_write(
                    self.input_topic,
                    [json.dumps({"value": value})],
                    partition_id=partition_id,
                )
            actual = client.topic_read(self.output_topic, self.consumer_name, len(expected))
            assert sorted(actual) == sorted(expected), (actual, expected)
            self.wait_completed_checkpoints(kikimr, query_name)

        added_slots = []
        kikimr.ydb_client.query(f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                $input = SELECT value FROM {inp} WITH (
                    FORMAT = json_each_row,
                    SCHEMA (value String NOT NULL)
                )
                WHERE __ydb_partition_id IN ({", ".join(map(str, selected))});
                INSERT INTO {out} SELECT value FROM $input;
            END DO;
        ''')
        try:
            expected_readers_before = 6
            self._wait_started(kikimr, query_name)
            self._wait_reader_count(kikimr, query_name, expected_readers_before)
            assert 0 < expected_readers_before < (len(selected) + 4) // 5, expected_readers_before
            check_reading("before")

            self._restart_query(kikimr, query_name, expected_readers_before, scale_up, added_slots)
            check_reading("after")
        finally:
            self._cleanup_query(kikimr, query_name, added_slots)

    @pytest.mark.parametrize(
        "scale_up",
        [
            False,
            pytest.param(True, marks=pytest.mark.skip(reason="Rescaling queries with aggregations is not supported")),
        ],
        ids=["restart", "scale_up"],
    )
    def test_pq_source_rescaling_hopping_open_window(
        self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        scale_up: bool,
    ) -> None:
        """An unchanged hopping stage must retain its open window when readers scale up."""
        partitions_count = 100
        query_name = entity_name("pq_source_rescaling_hopping_open_window")
        inp, out, _ = self.get_io_names(
            kikimr,
            query_name,
            True,
            entity_name,
            partitions_count=partitions_count,
        )
        client = kikimr.ydb_client

        # Keep event time ahead of source watermarks and within the future limit.
        # The target window is [base, base + 20), with another overlapping window
        # ending at base + 10. Only the target window is written to the output.
        base = (int(time.time()) // 20) * 20 + 120

        def timestamp(seconds: int) -> str:
            return (
                datetime.datetime.fromtimestamp(base + seconds, datetime.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )

        def event(seconds: int, value: int = 0, key: str = "clock") -> str:
            return json.dumps({"ts": timestamp(seconds), "key": key, "value": value})

        added_slots = []
        kikimr.ydb_client.query(f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                $input = SELECT CAST(ts AS Timestamp) AS event_time, key, value
                FROM {inp} WITH (
                    FORMAT = json_each_row,
                    SCHEMA (ts String NOT NULL, key String NOT NULL, value Uint64 NOT NULL),
                    WATERMARK = CAST(ts AS Timestamp) - Interval('PT1S'),
                    WATERMARK_IDLE_TIMEOUT = 'PT5S'
                );

                $windows = SELECT key, HOP_END() AS window_end, SUM(value) AS total
                FROM $input
                WHERE key = 'target'
                GROUP BY key, HoppingWindow(event_time, 'PT10S', 'PT20S');

                INSERT INTO {out}
                SELECT Unwrap(CAST(total AS String)) FROM $windows
                WHERE window_end = Timestamp('{timestamp(20)}');
            END DO;
        ''')
        try:
            self._wait_started(kikimr, query_name)
            readers_before = self._reader_count(kikimr)
            self.wait_streaming_query_metric(
                kikimr,
                query_name,
                "streaming.query.tasks.count",
                expected_value=readers_before + 1,
            )
            tasks_before = self.get_streaming_query_metric(kikimr, query_name, "streaming.query.tasks.count")
            assert 0 < readers_before < (partitions_count + 4) // 5, readers_before
            assert tasks_before > readers_before, (tasks_before, readers_before)

            # Seed every partition, so no idle partition can hold back the final
            # watermark. Clock records advance watermarks but do not aggregate.
            batches = {partition_id: [event(1)] for partition_id in range(partitions_count)}
            batches[0].extend([event(2, 10, "target"), event(3, 20, "target")])
            self._write_and_checkpoint(kikimr, query_name, batches)
            assert (
                self.get_streaming_query_metric(kikimr, query_name, "streaming.query.output.bytes") == 0
            ), "The target window closed before the restart"

            self._restart_query(kikimr, query_name, readers_before, scale_up, added_slots)
            # Only the reader count should change, not downstream parallelism.
            self._check_downstream_tasks(kikimr, query_name, readers_before, tasks_before)

            self._write_and_checkpoint(kikimr, query_name, {0: [event(4, 5, "target")]})
            self._write_and_checkpoint(
                kikimr, query_name, {partition_id: [event(40)] for partition_id in range(partitions_count)}
            )
            actual = client.topic_read(self.output_topic, self.consumer_name, 1)
            assert actual == ["35"], (
                f"Expected checkpointed sum 30 plus new value 5, got {actual}; "
                "a sum of 5 means the hopping state was lost while PQ offsets were restored"
            )
            self.wait_completed_checkpoints(kikimr, query_name)
            self._check_downstream_tasks(kikimr, query_name, readers_before, tasks_before)
        finally:
            self._cleanup_query(kikimr, query_name, added_slots)
