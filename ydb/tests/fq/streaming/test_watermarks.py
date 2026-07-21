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
        settings: dict[str, str] = {},
        input_parsing: bool = False,
        cascade_hopping: bool = False,
    ) -> str:
        query_name = entity_name(scenario)
        input_name, output_name, _ = self.get_io_names(
            kikimr, query_name, local_topics, entity_name, partitions_count=tasks, shared=shared_reading
        )

        settings_str = f"WITH ({', '.join(f'{k} = {v}' for k, v in settings.items())})" if settings else ""
        idleness_clause = f', WATERMARK_IDLE_TIMEOUT = "PT{self.idle_timeout_seconds}S"' if tasks > 1 else ''
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

            $input = (
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
            if input_parsing
            else f'''
            $input = (
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
        process = '''
            $process = (
                SELECT
                    HOP_END() AS event_time,
                    AGGREGATE_LIST(id) AS id
                FROM
                    $input
                WHERE
                    pass > 0
                GROUP BY
                    HoppingWindow(event_time, 'PT1S', 'PT1S')
            );
        ''' if cascade_hopping else '''
            $process = (
                SELECT
                    event_time,
                    id
                FROM
                    $input
                WHERE
                    pass > 0
            );
        '''
        kikimr.ydb_client.query(f'''
            CREATE STREAMING QUERY `{query_name}` {settings_str} AS DO BEGIN
            PRAGMA ydb.MaxTasksPerStage = '{tasks}';

            {input}

            {process}

            $output = (
                SELECT
                    HOP_END() AS event_time,
                    AGGREGATE_LIST(id) AS id
                FROM
                    $process
                GROUP BY
                    HoppingWindow(event_time, 'PT1S', 'PT1S')
            );

            INSERT INTO {output_name}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(id))))
            FROM $output;
            END DO;
        ''')
        self.wait_completed_checkpoints(kikimr, f"/Root/{query_name}")
        return query_name

    def _write_topic(
        self,
        ydb_client: YdbClient,
        messages: list[str],
        partition_id: int = 0,
    ) -> None:
        ydb_client.topic_write(self.input_topic, messages, partition_id=partition_id)

    def _wait_for_idle(self, shared_reading: bool, tasks: int) -> None:
        if shared_reading and tasks > 1:
            time.sleep(2 * self.idle_timeout_seconds)  # leave a bit more time to fire up idle timeout

    def _wait_for_shared_reading_start(self, shared_reading: bool) -> None:
        if shared_reading:
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
        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"idle_partition_gt_timeout_{shared_reading}{local_topics}"
        query_name = self._create_query(kikimr, entity_name, query_name, local_topics, shared_reading)
        self._wait_for_shared_reading_start(shared_reading)

        try:
            self._write_topic(ydb_client, [self._event(0, "fast-0")], partition_id=0)
            self._write_topic(ydb_client, [self._event(0, "slow-0")], partition_id=1)
            self._write_topic(ydb_client, [self._event(10, "fast-10")], partition_id=0)

            time.sleep(self.idle_timeout_seconds + 1)

            expected = ["fast-0", "slow-0"]
            self._read_topic_check_rows(ydb_client, expected)

            self._write_topic(ydb_client, [self._event(10, "slow-10")], partition_id=1)
            self._write_topic(ydb_client, [self._event(20, "fast-20")], partition_id=0)
            self._write_topic(ydb_client, [self._event(20, "slow-20")], partition_id=1)

            expected = ["fast-10", "slow-10"]
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
        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"idle_partition_lt_timeout_{shared_reading}{local_topics}"
        query_name = self._create_query(kikimr, entity_name, query_name, local_topics, shared_reading)
        self._wait_for_shared_reading_start(shared_reading)

        try:
            self._write_topic(ydb_client, [self._event(0, "fast-0")], partition_id=0)
            self._write_topic(ydb_client, [self._event(0, "slow-0")], partition_id=1)
            self._write_topic(ydb_client, [self._event(10, "fast-10")], partition_id=0)

            time.sleep(self.idle_timeout_seconds - 1)
            self._write_topic(ydb_client, [self._event(10, "slow-10")], partition_id=1)
            self._write_topic(ydb_client, [self._event(20, "fast-20")], partition_id=0)
            self._write_topic(ydb_client, [self._event(20, "slow-20")], partition_id=1)

            expected = ["fast-0", "slow-0"]
            self._read_topic_check_rows(ydb_client, expected)

            expected = ["fast-10", "slow-10"]
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
        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"idle_topic_{shared_reading}{local_topics}"
        query_name = self._create_query(kikimr, entity_name, query_name, local_topics, shared_reading)
        self._wait_for_shared_reading_start(shared_reading)

        try:
            self._write_topic(ydb_client, [self._event(0, "first-0")], partition_id=0)
            self._write_topic(ydb_client, [self._event(0, "second-0")], partition_id=1)

            time.sleep(self.idle_timeout_seconds + 1)
            self._write_topic(ydb_client, [self._event(10, "first-10")], partition_id=0)
            self._write_topic(ydb_client, [self._event(10, "second-10")], partition_id=1)
            self._write_topic(ydb_client, [self._event(20, "first-20")], partition_id=0)
            self._write_topic(ydb_client, [self._event(20, "second-20")], partition_id=1)

            expected = ["first-0", "second-0", "first-10", "second-10"]
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
        ydb_client = self.get_ydb_client(kikimr, local_topics)
        query_name = f"empty_partition_{shared_reading}{local_topics}"
        query_name = self._create_query(kikimr, entity_name, query_name, local_topics, shared_reading)
        self._wait_for_shared_reading_start(shared_reading)

        try:
            self._write_topic(ydb_client, [self._event(0, "active-0")], partition_id=0)
            self._write_topic(ydb_client, [self._event(10, "active-10")], partition_id=0)

            time.sleep(self.idle_timeout_seconds + 1)

            expected = ["active-0"]
            self._read_topic_check_rows(ydb_client, expected)

            self._write_topic(ydb_client, [self._event(10, "new-10")], partition_id=1)
            self._write_topic(ydb_client, [self._event(20, "active-20")], partition_id=0)
            self._write_topic(ydb_client, [self._event(20, "new-20")], partition_id=1)

            expected = ["active-10", "new-10"]
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
        ("DROP", ['["60"]']),
        ("ADJUST", ['["40"]', '["60"]']),
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
                ],
            )
            self._wait_for_idle(shared_reading, tasks)

            expected1 = ['["50"]']
            self._read_topic_check(ydb_client, expected1)

            self._write_topic(
                ydb_client,
                [
                    self._event(60, "60"),
                    self._event(40, "40"),
                    self._event(70, "70", filter=True),
                ],
            )
            self._wait_for_idle(shared_reading, tasks)

            self._read_topic_check(ydb_client, expected)
        finally:
            self._drop_query(kikimr, query_name)
