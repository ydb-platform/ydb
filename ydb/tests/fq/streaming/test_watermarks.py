import datetime
import json
import logging
import pytest
import time
from typing import Callable, Self

from ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase, YdbClient
from ydb.tests.library.test_meta import link_test_case

logger = logging.getLogger(__name__)
DEFAULT_INITIAL_TS = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


class TestWatermarksInYdb(StreamingTestBase):
    idle_timeout_seconds = 5

    @staticmethod
    def _event(
        seconds: int,
        event_id: str,
        filter: bool = False,
        initial_ts: datetime.datetime = DEFAULT_INITIAL_TS,
    ) -> str:
        event_time = initial_ts + datetime.timedelta(seconds=seconds)
        return json.dumps({
            "ts": event_time.isoformat().replace("+00:00", "Z"),
            "pass": 0 if filter else 1,
            "id": event_id,
        })

    def _create_query(
        self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        scenario: str,
        local_topics: bool,
        shared_reading: bool,
        tasks: int = 2,
        partitions_count: int | None = None,
        idle_timeout_seconds: int | None = None,
        settings: dict[str, str] = {},
        input_parsing: bool = False,
        replicate_after_parsing: bool = False,
        cascade_hopping: bool = False,
    ) -> str:
        query_name = entity_name(scenario)
        partitions_count = partitions_count or tasks
        idle_timeout_seconds = idle_timeout_seconds or self.idle_timeout_seconds
        input_name, output_name, _ = self.get_io_names(
            kikimr, query_name, local_topics, entity_name, partitions_count=partitions_count, shared=shared_reading
        )

        settings_str = f"WITH ({', '.join(f'{k} = {v}' for k, v in settings.items())})" if settings else ""
        idleness_clause = f', WATERMARK_IDLE_TIMEOUT = "PT{idle_timeout_seconds}S"' if partitions_count > 1 else ''

        suffixes = ["0", "1"] if replicate_after_parsing else ["0"]
        input = (
            f'''
            $input = (
                SELECT
                    Yson::ConvertTo(Yson::ParseJson(line), Struct<ts: String, pass: Uint64, id: String>) AS row
                FROM
                    {input_name}
                    FLATTEN LIST BY (
                        String::SplitToList(Data, '.') AS line
                    )
                WHERE
                    line != ''
            );

            $input = (
                SELECT
                    ts,
                    pass,
                    id
                FROM
                    $input
                    FLATTEN COLUMNS
            );
            ''' + ''.join(
                f'''
            $input{suffix} = (
                SELECT
                    CAST(ts AS Timestamp) AS event_time,
                    pass,
                    id
                FROM
                    $input WITH (
                        WATERMARK = CAST(ts AS Timestamp) - Interval('PT5S')
                        {idleness_clause}
                    ) AS input
            );
        '''
                for suffix in suffixes
            )
            if input_parsing
            else f'''
            $input0 = (
                SELECT
                    CAST(ts AS Timestamp) AS event_time,
                    pass,
                    id
                FROM
                    {input_name} WITH (
                        FORMAT = json_each_row,
                        SCHEMA (ts String, pass Uint64, id String),
                        WATERMARK = CAST(ts AS Timestamp) - Interval('PT5S')
                        {idleness_clause}
                    )
            );
        '''
        )

        build_process: Callable[[str], str] = lambda suffix: (f'''
            $process{suffix} = (
                SELECT
                    HOP_END() AS event_time,
                    AGGREGATE_LIST(id) AS id
                FROM
                    $input{suffix}
                WHERE
                    pass > 0
                GROUP BY
                    HoppingWindow(event_time, 'PT1S', 'PT1S')
            );
        ''' if cascade_hopping else f'''
            $process{suffix} = (
                SELECT
                    event_time,
                    id
                FROM
                    $input{suffix}
                WHERE
                    pass > 0
            );
        ''') + f'''
            $output{suffix} = (
                SELECT
                    HOP_END() AS event_time,
                    AGGREGATE_LIST(id) AS id
                FROM
                    $process{suffix}
                GROUP BY
                    HoppingWindow(event_time, 'PT1S', 'PT1S')
            );
        '''

        process = f'''
        {''.join(build_process(suffix) for suffix in suffixes)}
        $output = (
            {' UNION ALL '.join(f'SELECT * FROM $output{suffix}' for suffix in suffixes)}
        );
        '''

        sql = f'''
            CREATE STREAMING QUERY `{query_name}` {settings_str} AS DO BEGIN
            PRAGMA ydb.MaxTasksPerStage = '{tasks}';

            {input}

            {process}

            INSERT INTO {output_name}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(id))))
            FROM $output;
            END DO;
        '''
        kikimr.ydb_client.query(sql)
        self.wait_completed_checkpoints(kikimr, query_name)
        return query_name

    def _write_topic(
        self,
        ydb_client: YdbClient,
        messages: list[str],
        partition_id: int = 0,
    ) -> None:
        ydb_client.topic_write(self.input_topic, messages, partition_id=partition_id)

    def _write_topic_and_wait(
        self,
        ydb_client: YdbClient,
        kikimr: Kikimr,
        query_name: str,
        message: str,
        partition_id: int = 0,
    ) -> None:
        """Write a single event and wait until it is buffered in the aggregation state.

        The PQ source actor pushes per-partition watermarks based on
        message.GetWriteTime() (wall-clock, ~now). A new source batch from any
        partition causes a write_time watermark (~2026) to be delivered to the
        compute actor. This advances HoppingWindow's HopIndex far beyond the
        historical event_time values used in tests (1970), making any not-yet-
        buffered 1970-timestamp event appear "late" and be dropped.

        This helper ensures the event is actually ingested (input.bytes advances)
        and a full-graph checkpoint barrier has passed before any subsequent write
        delivers a new source batch.  That way the event is in the aggregation
        state before the next write_time watermark can close its window.
        """
        input_bytes_before = self.get_streaming_query_metric(
            kikimr, query_name, "streaming.query.input.bytes"
        )
        self._write_topic(ydb_client, [message], partition_id=partition_id)
        self.wait_streaming_query_metric(
            kikimr, query_name, "streaming.query.input.bytes",
            expected_value=input_bytes_before + 1,
        )
        self.wait_completed_checkpoints(kikimr, query_name)

    def _wait_for_idle(self, shared_reading: bool, tasks: int) -> None:
        if shared_reading and tasks > 1:
            # Allow idle timeout to fire in shared reading.
            time.sleep(2 * self.idle_timeout_seconds)

    def _wait_for_shared_reading_start(self, shared_reading: bool) -> None:
        if shared_reading:
            # Allow shared-reading workers to start consuming partitions.
            time.sleep(self.idle_timeout_seconds + 1)

    def _read_topic(self, ydb_client: YdbClient, messages_count: int) -> list[str]:
        return ydb_client.topic_read(self.output_topic, self.consumer_name, messages_count)

    def _read_topic_check_rows(self, ydb_client: YdbClient, expected: list[str]) -> None:
        actual = []
        while len(actual) < len(expected):
            actual.extend(json.loads(self._read_topic(ydb_client, 1)[0]))
        assert sorted(actual) == sorted(expected)

    def _read_topic_check(self, ydb_client: YdbClient, expected: list[str]) -> None:
        actual = self._read_topic(ydb_client, len(expected))
        assert actual == expected

    def _drop_query(self, kikimr: Kikimr, query_name: str) -> None:
        kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`;")

    @link_test_case("#28595")
    @pytest.mark.parametrize("local_topics", [True, False])
    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    def test_watermarks(
        self: Self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        local_topics: bool,
        shared_reading: bool,
        tasks: int,
    ) -> None:
        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"wm_{shared_reading}{tasks}{local_topics}"
        query_name = self._create_query(kikimr, entity_name, query_name, local_topics, shared_reading, tasks)

        try:
            self._write_topic(
                ydb_client,
                [
                    self._event(40, "40"),
                    self._event(50, "50"),
                    self._event(60, "60", filter=True),
                ],
            )
            self._wait_for_idle(shared_reading, tasks)

            expected = ['["40"]', '["50"]']
            self._read_topic_check(ydb_client, expected)
        finally:
            self._drop_query(kikimr, query_name)

    @link_test_case("#28599")
    @pytest.mark.parametrize("local_topics", [True, False])
    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    def test_cascade_hopping_window(
        self: Self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        local_topics: bool,
        shared_reading: bool,
        tasks: int,
    ) -> None:
        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"cascade_hopping_window_{shared_reading}{tasks}{local_topics}"
        query_name = self._create_query(
            kikimr, entity_name, query_name, local_topics, shared_reading, tasks, cascade_hopping=True
        )

        try:
            self._write_topic(
                ydb_client,
                [
                    self._event(40, "40"),
                    self._event(50, "50"),
                    self._event(60, "60", filter=True),
                ],
            )
            self._wait_for_idle(shared_reading, tasks)

            expected = ['[["40"]]', '[["50"]]']
            self._read_topic_check(ydb_client, expected)
        finally:
            self._drop_query(kikimr, query_name)

    @link_test_case("#28600")
    @pytest.mark.parametrize("local_topics", [True, False])
    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    def test_idle_partition_gt_timeout(
        self: Self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        local_topics: bool,
        shared_reading: bool,
    ) -> None:
        idle_timeout_seconds = 10
        keep_alive_interval = 3
        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"idle_partition_gt_timeout_{shared_reading}{local_topics}"
        query_name = self._create_query(
            kikimr, entity_name, query_name, local_topics, shared_reading,
            tasks=1, partitions_count=2, idle_timeout_seconds=idle_timeout_seconds,
        )
        self._wait_for_shared_reading_start(shared_reading)

        try:
            # Both initial events share window [0,1). Use _write_topic_and_wait so
            # each event is buffered in the aggregation state before the next write
            # delivers a write_time watermark that would close that window (see
            # _write_topic_and_wait docstring for the full mechanism).
            self._write_topic_and_wait(ydb_client, kikimr, query_name, self._event(0, "fst-0"), partition_id=0)
            self._write_topic_and_wait(ydb_client, kikimr, query_name, self._event(0, "snd-0"), partition_id=1)
            idle_started = time.monotonic()

            # Visible event that must appear in output.
            self._write_topic(ydb_client, [self._event(10, "fst-10")], partition_id=0)
            self.wait_completed_checkpoints(kikimr, query_name)

            while True:
                input_bytes_before = self.get_streaming_query_metric(
                    kikimr, query_name, "streaming.query.input.bytes"
                )
                self._write_topic(
                    ydb_client,
                    [self._event(10, "keepalive", filter=True)],
                    partition_id=0,
                )
                self.wait_streaming_query_metric(
                    kikimr, query_name, "streaming.query.input.bytes",
                    expected_value=input_bytes_before + 1,
                )
                if time.monotonic() - idle_started >= idle_timeout_seconds + 1:
                    break
                time.sleep(keep_alive_interval)

            # Events fst-20 (p0) and snd-20 (p1) share window [20,21). Both must be
            # delivered to the HoppingWindow aggregation state BEFORE the write_time-
            # based watermark from the next source batch closes that window.
            #
            # Background: the PQ source actor tracks per-partition watermarks using
            # message.GetWriteTime() (wall-clock). When a new source batch arrives
            # for any partition, it notifies the compute actor with a write_time-based
            # watermark (e.g. ~2026). If this becomes the combined watermark, it
            # advances the HoppingWindow's internal HopIndex to ~year-2026, making
            # all events with historical event_time (1970) appear "late" and dropped.
            #
            # Fix: use _write_topic_and_wait (ingest + checkpoint barrier) for every
            # ts=20 event. This ensures each event reaches the aggregation state before
            # any subsequent write causes a new source batch whose write_time watermark
            # would close the [20,21) window.
            self._write_topic_and_wait(ydb_client, kikimr, query_name, self._event(20, "snd-20"), partition_id=1)
            self._write_topic_and_wait(ydb_client, kikimr, query_name, self._event(20, "fst-20"), partition_id=0)

            # Advance both partitions past the window to close [20,21).
            self._write_topic(ydb_client, [self._event(30, "snd-30", filter=True)], partition_id=1)
            self._write_topic(ydb_client, [self._event(30, "fst-30")], partition_id=0)
            self.wait_completed_checkpoints(kikimr, query_name)

            expected = ["fst-0", "snd-0", "fst-10", "fst-20", "snd-20"]
            self._read_topic_check_rows(ydb_client, expected)
        finally:
            self._drop_query(kikimr, query_name)

    @link_test_case("#28601")
    @pytest.mark.parametrize("local_topics", [True, False])
    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    def test_idle_partition_lt_timeout(
        self: Self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        local_topics: bool,
        shared_reading: bool,
    ) -> None:
        idle_timeout_seconds = 20
        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"idle_partition_lt_timeout_{shared_reading}{local_topics}"
        query_name = self._create_query(
            kikimr, entity_name, query_name, local_topics, shared_reading,
            tasks=1, partitions_count=2, idle_timeout_seconds=idle_timeout_seconds,
        )
        self._wait_for_shared_reading_start(shared_reading)

        try:
            # Both initial events share window [0,1). Use _write_topic_and_wait so
            # each event is buffered in the aggregation state before the next write
            # delivers a write_time watermark that would close that window (see
            # _write_topic_and_wait docstring for the full mechanism).
            self._write_topic_and_wait(ydb_client, kikimr, query_name, self._event(0, "fst-0"), partition_id=0)
            self._write_topic_and_wait(ydb_client, kikimr, query_name, self._event(0, "snd-0"), partition_id=1)
            self._write_topic_and_wait(ydb_client, kikimr, query_name, self._event(10, "fst-10"), partition_id=0)

            # Keep the second partition below idle timeout.
            time.sleep(self.idle_timeout_seconds - 1)
            self._write_topic_and_wait(ydb_client, kikimr, query_name, self._event(10, "snd-10"), partition_id=1)

            # Events at ts=20 are not expected in output (window not yet closed).
            self._write_topic(ydb_client, [self._event(20, "fst-20")], partition_id=0)
            self._write_topic(ydb_client, [self._event(20, "snd-20")], partition_id=1)

            expected = ["fst-0", "snd-0", "fst-10", "snd-10"]
            self._read_topic_check_rows(ydb_client, expected)
        finally:
            self._drop_query(kikimr, query_name)

    @link_test_case("#28602")
    @pytest.mark.parametrize("local_topics", [True, False])
    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    def test_idle_topic(
        self: Self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        local_topics: bool,
        shared_reading: bool,
    ) -> None:
        idle_timeout_seconds = 10
        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"idle_topic_{shared_reading}{local_topics}"
        query_name = self._create_query(
            kikimr, entity_name, query_name, local_topics, shared_reading, idle_timeout_seconds=idle_timeout_seconds
        )
        self._wait_for_shared_reading_start(shared_reading)

        try:
            # Both initial events share window [0,1). Use _write_topic_and_wait so
            # each event is buffered in the aggregation state before the next write
            # delivers a write_time watermark that would close that window (see
            # _write_topic_and_wait docstring for the full mechanism).
            self._write_topic_and_wait(ydb_client, kikimr, query_name, self._event(0, "fst-0"), partition_id=0)
            self._write_topic_and_wait(ydb_client, kikimr, query_name, self._event(0, "snd-0"), partition_id=1)

            # Let both partitions become idle after the last ingested event.
            time.sleep(idle_timeout_seconds + 1)

            # Events fst-10 (p0) and snd-10 (p1) share window [10,11). Both must be
            # delivered to the HoppingWindow aggregation state BEFORE the write_time-
            # based watermark from the next source batch closes that window.
            self._write_topic_and_wait(ydb_client, kikimr, query_name, self._event(10, "fst-10"), partition_id=0)
            self._write_topic_and_wait(ydb_client, kikimr, query_name, self._event(10, "snd-10"), partition_id=1)

            # Events at ts=20 close window [10,11) via event-time watermarks; their own
            # window [20,21) is never closed, so they must not appear in the output.
            # Wait for ingest and for output to advance (windows closed) before reading —
            # a bare checkpoint wait here can hang under load after a full-topic idle.
            output_bytes_before = self.get_streaming_query_metric(
                kikimr, query_name, "streaming.query.output.bytes"
            )
            input_bytes_before = self.get_streaming_query_metric(
                kikimr, query_name, "streaming.query.input.bytes"
            )
            self._write_topic(ydb_client, [self._event(20, "fst-20")], partition_id=0)
            self._write_topic(ydb_client, [self._event(20, "snd-20")], partition_id=1)
            self.wait_streaming_query_metric(
                kikimr, query_name, "streaming.query.input.bytes",
                expected_value=input_bytes_before + 2,
            )
            self.wait_streaming_query_metric(
                kikimr, query_name, "streaming.query.output.bytes",
                expected_value=output_bytes_before + 1,
            )

            expected = ["fst-0", "snd-0", "fst-10", "snd-10"]
            self._read_topic_check_rows(ydb_client, expected)
        finally:
            self._drop_query(kikimr, query_name)

    @link_test_case("#28604")
    @pytest.mark.parametrize("local_topics", [True, False])
    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    def test_empty_partition(
        self: Self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        local_topics: bool,
        shared_reading: bool,
    ) -> None:
        idle_timeout_seconds = 10
        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"empty_partition_{shared_reading}{local_topics}"
        query_name = self._create_query(
            kikimr, entity_name, query_name, local_topics, shared_reading,
            tasks=1, partitions_count=2, idle_timeout_seconds=idle_timeout_seconds,
        )
        self._wait_for_shared_reading_start(shared_reading)

        try:
            self._write_topic(ydb_client, [self._event(0, "fst-0")], partition_id=0)
            self._write_topic(ydb_client, [self._event(0, "snd-0", filter=True)], partition_id=1)

            # Start measuring idleness only after both partitions consume the initial events.
            self.wait_completed_checkpoints(kikimr, query_name)

            # Keep the first partition active while the second approaches idle timeout.
            time.sleep(idle_timeout_seconds / 2 + 1)
            self._write_topic(ydb_client, [self._event(10, "fst-10")], partition_id=0)

            # Ensure this event keeps the first partition active before the next interval starts.
            self.wait_completed_checkpoints(kikimr, query_name)

            # Let the second partition exceed idle timeout without idling the first.
            time.sleep(idle_timeout_seconds / 2 + 1)

            # Capture the query's cumulative input byte counter before reactivating the
            # second partition, so we can confirm snd-20 was actually ingested.
            input_bytes_before = self.get_streaming_query_metric(
                kikimr, query_name, "streaming.query.input.bytes"
            )
            self._write_topic(ydb_client, [self._event(20, "snd-20")], partition_id=1)

            self.wait_streaming_query_metric(
                kikimr, query_name, "streaming.query.input.bytes",
                expected_value=input_bytes_before + 1,
            )
            self.wait_completed_checkpoints(kikimr, query_name)

            time.sleep(idle_timeout_seconds + 1)

            self._write_topic(ydb_client, [self._event(20, "fst-20")], partition_id=0)
            self._write_topic(ydb_client, [self._event(30, "fst-30")], partition_id=0)
            self.wait_completed_checkpoints(kikimr, query_name)

            expected = ["fst-0", "fst-10", "fst-20", "snd-20"]
            self._read_topic_check_rows(ydb_client, expected)
        finally:
            self._drop_query(kikimr, query_name)

    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_wm_after_parsing(
        self: Self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        shared_reading: bool,
        tasks: int,
        local_topics: bool,
    ) -> None:
        if shared_reading:
            pytest.skip("Shared reading is not supported for watermarks after parsing yet")

        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"wm_after_parsing_{shared_reading}{tasks}{local_topics}"
        query_name = self._create_query(
            kikimr, entity_name, query_name, local_topics, shared_reading, tasks, input_parsing=True
        )

        try:
            self._write_topic(
                ydb_client,
                [
                    f'{self._event(40, "40")}..{self._event(50, "50")}.{self._event(60, "60", filter=True)}',
                ],
            )
            self._wait_for_idle(shared_reading, tasks)

            expected = ['["40"]', '["50"]']
            self._read_topic_check(ydb_client, expected)
        finally:
            self._drop_query(kikimr, query_name)

    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_wm_after_parsing_2(
        self: Self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        shared_reading: bool,
        tasks: int,
        local_topics: bool,
    ) -> None:
        if shared_reading:
            pytest.skip("Shared reading is not supported for watermarks after parsing yet")

        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"wm_after_parsing_2_{shared_reading}{tasks}{local_topics}"
        query_name = self._create_query(
            kikimr,
            entity_name,
            query_name,
            local_topics,
            shared_reading,
            tasks,
            input_parsing=True,
            replicate_after_parsing=True,
        )

        try:
            self._write_topic(
                ydb_client,
                [
                    f'{self._event(40, "40")}..{self._event(50, "50")}..{self._event(60, "60", filter=True)}',
                ],
            )
            self._wait_for_idle(shared_reading, tasks)

            expected = ["40", "40", "50", "50"]
            self._read_topic_check_rows(ydb_client, expected)
        finally:
            self._drop_query(kikimr, query_name)

    @pytest.mark.parametrize("local_topics", [True, False])
    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    def test_early_events_policy(
        self: Self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        local_topics: bool,
        shared_reading: bool,
        tasks: int,
    ) -> None:
        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"early_events_policy_{shared_reading}{tasks}{local_topics}"
        query_name = self._create_query(kikimr, entity_name, query_name, local_topics, shared_reading, tasks)

        try:
            now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)

            self._write_topic(
                ydb_client,
                [
                    self._event(0 * 60, "0", initial_ts=now),
                    self._event(10 * 60, "600", initial_ts=now),
                    self._event(4 * 60, "240", initial_ts=now, filter=True),
                ],
            )
            self._wait_for_idle(shared_reading, tasks)

            expected = ['["0"]']
            self._read_topic_check(ydb_client, expected)
        finally:
            self._drop_query(kikimr, query_name)

    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    @pytest.mark.parametrize("local_topics", [True, False])
    @pytest.mark.parametrize("policy,expected", [
        ("DROP", ["55", "60"]),
        ("ADJUST", ["40", "55", "60"]),
    ])
    def test_late_events_policy(
        self: Self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        local_topics: bool,
        shared_reading: bool,
        tasks: int,
        policy: str,
        expected: list[str],
    ) -> None:
        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"late_events_{policy.lower()}_{shared_reading}{tasks}{local_topics}"
        query_name = self._create_query(
            kikimr,
            entity_name,
            query_name,
            local_topics,
            shared_reading,
            tasks,
            settings={"WATERMARK_LATE_EVENTS_POLICY": policy},
        )

        try:
            self._write_topic(
                ydb_client,
                [
                    self._event(50, "50"),
                    self._event(60, "60", filter=True),
                    self._event(55, "55"),
                ],
            )
            self._wait_for_idle(shared_reading, tasks)

            self._read_topic_check_rows(ydb_client, ["50"])

            self._write_topic(
                ydb_client,
                [
                    self._event(60, "60"),
                    self._event(40, "40"),
                    self._event(70, "70", filter=True),
                ],
            )
            self._wait_for_idle(shared_reading, tasks)

            self._read_topic_check_rows(ydb_client, expected)
        finally:
            self._drop_query(kikimr, query_name)

    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    @pytest.mark.parametrize("local_topics", [True, False])
    @pytest.mark.parametrize("kikimr", [{"kqp_constraints_transformer": False}], indirect=["kikimr"])
    def test_watermarks_kqp_slj(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str], shared_reading: bool, tasks: int, local_topics: bool) -> None:
        if local_topics and shared_reading:
            pytest.skip("Shared reading is not supported for local topics: YQ-5036")

        endpoint = self.get_endpoint(kikimr, local_topics)
        query_name = f"test_watermarks_kqp_slj_{shared_reading}{tasks}{local_topics}"
        source_name = entity_name(query_name)
        self.init_topics(source_name, partitions_count=tasks, endpoint=endpoint)
        self.create_source(kikimr, source_name, shared_reading)

        cluster = f"{source_name}." if not local_topics else ""
        idleness_clause = ', WATERMARK_IDLE_TIMEOUT = "PT5S"' if tasks > 1 else ''

        table_name = entity_name("slj_table")
        kikimr.ydb_client.query(f"""
            CREATE TABLE `{table_name}` (
                key Int32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            );
        """)
        kikimr.ydb_client.query(f"""
            UPSERT INTO `{table_name}` (key, value) VALUES (1, "one");
            UPSERT INTO `{table_name}` (key, value) VALUES (2, "two");
            UPSERT INTO `{table_name}` (key, value) VALUES (3, "three");
        """)

        sql = f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
            PRAGMA ydb.MaxTasksPerStage = '{tasks}';
            PRAGMA ydb.OverridePlanner = @@[
                {{"tx": 0, "stage": 0, "tasks": {tasks} }},
                {{"tx": 0, "stage": 1, "tasks": {tasks} }}
            ]@@;
            PRAGMA ydb.OptValidateStreamingConstraints="false";
            PRAGMA ydb.OptimizerHints = @@ JoinType(i db Lookup) @@;

            $input = (
                SELECT
                    input.*,
                    CAST(ts AS Timestamp) AS event_time,
                FROM
                    {cluster}{self.input_topic} WITH (
                        FORMAT = json_each_row,
                        SCHEMA (ts String, pass Uint64, k Int32),
                        WATERMARK = CAST(ts AS Timestamp) - Interval('PT5S')
                        {idleness_clause}
                    ) AS input
            );

            $input =
                SELECT
                    i.*,
                    db.*
                FROM $input AS i
                LEFT JOIN `{table_name}` AS db
                  ON i.k=db.key
                ;

            $hop = (
                SELECT
                    CAST(HOP_END() AS String) AS event_time,
                    AGGREGATE_LIST(AsTuple(ts, value)) AS ts
                FROM
                    $input
                WHERE
                    pass > 0
                GROUP BY
                    HoppingWindow(CAST(event_time AS Timestamp), 'PT1S', 'PT1S')
            );

            $output = (
                SELECT
                    CAST(HOP_END() AS String) AS event_time,
                    AGGREGATE_LIST(ts) AS ts
                FROM
                    $hop
                GROUP BY
                    HoppingWindow(CAST(event_time AS Timestamp), 'PT1S', 'PT1S')
            );

            INSERT INTO {cluster}{self.output_topic}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(ts))))
            FROM $output;
            END DO;
        '''
        kikimr.ydb_client.query(sql)

        query_path = f"{kikimr.endpoint.database.rstrip('/')}/{query_name}"
        try:
            self.wait_completed_checkpoints(kikimr, query_name)

            result_sets = kikimr.ydb_client.query(f"""
                SELECT Ast FROM `.sys/streaming_queries` WHERE Path = "{query_path}"
            """)
            assert "KqpCnStreamLookup" in result_sets[0].rows[0]["Ast"]

            self.write_stream(
                data=[
                    '{"ts": "1970-01-01T00:00:40Z", "k":1, "pass": 1}',
                    '{"ts": "1970-01-01T00:00:50Z", "k":2, "pass": 1}',
                    '{"ts": "1970-01-01T00:01:00Z", "k":3, "pass": 0}',
                ],
                endpoint=endpoint,
                partition_key=b'1',
            )
            if shared_reading and tasks > 1:
                time.sleep(10)  # leave a bit more time to fire up idle timeout

            expected = [
                '[[["1970-01-01T00:00:40Z","one"]]]',
                '[[["1970-01-01T00:00:50Z","two"]]]',
            ]

            actual = self.read_stream(len(expected), topic_path=self.output_topic, endpoint=endpoint)
            assert sorted(actual) == expected

        finally:
            self._drop_query(kikimr, query_name)
            kikimr.ydb_client.query(f'''DROP TABLE `{table_name}`;''')
