import json
import logging
import time
import urllib.request

import pytest

from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from ydb.tests.library.common.types import TabletTypes
from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class TestCutHistoryDormant(RollingUpgradeAndDowngradeFixture):
    """The combination the guarded test cannot reach: NEW code rolled against OLD
    config. No CutHistory flags are set, so on >= 26.4 nodes the cutter machinery is
    present but dormant, and the roll proves a mixed-version cluster survives tablet
    generation churn with the feature merely compiled in. Runs on any version matrix.
    """

    rows_count = 100
    restart_rounds = 2

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            column_shard_config={
                "alter_object_enabled": True,
            },
        )

    def test_dormant_roll(self):
        table_name = "olap_cut_history_dormant"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                CREATE TABLE `{table_name}` (
                    ts Timestamp NOT NULL,
                    id Uint64 NOT NULL,
                    payload Utf8,
                    PRIMARY KEY (ts, id)
                )
                PARTITION BY HASH(ts, id)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4)
                """
            )
            values = ",".join(
                f'(Timestamp("2024-01-01T00:{i // 60 % 60:02d}:{i % 60:02d}.000000Z"), {i}, "p{i}")'
                for i in range(self.rows_count)
            )
            session_pool.execute_with_retries(f"INSERT INTO `{table_name}` (ts, id, payload) VALUES {values};")

        client = kikimr_client_factory("localhost", self.cluster.nodes[1].port)

        def restart_column_shards():
            response = client.tablet_state(tablet_type=TabletTypes.COLUMNSHARD)
            for info in response.TabletStateInfo:
                client.tablet_kill(info.TabletId)

        def assert_readable():
            with ydb.QuerySessionPool(self.driver) as session_pool:
                result = session_pool.execute_with_retries(
                    f"SELECT COUNT(*) AS cnt FROM `{table_name}`;",
                    retry_settings=ydb.RetrySettings(idempotent=True),
                )
                assert result[0].rows[0]["cnt"] == self.rows_count

        # Generation churn first, so channel history exists for the roll to stress.
        for _ in range(self.restart_rounds):
            restart_column_shards()
            assert_readable()

        for _ in self.roll():
            assert_readable()
        assert_readable()


class TestCutHistory(RollingUpgradeAndDowngradeFixture):
    """Roll the cluster while CutHistory is trimming ColumnShard channel history.

    History entries only appear on generation changes, so the tablets are restarted
    to produce them; the cutter then nominates the drained ones on its own one-minute
    cadence. The roll happens on top of that, and the test checks that data stays
    readable throughout and that no channel ends up poisoned.
    """

    rows_count = 200
    restart_rounds = 3

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (26, 4):
            pytest.skip("CutHistory is available starting from 26.4")

        yield from self.setup_cluster(
            # Both legs are needed for the pool to converge: the platform flag drives the
            # channel-0/1 cutters, the decommission flag drives ColumnShard's data channels.
            extra_feature_flags=["enable_cut_history", "enable_columnshard_group_decommission"],
            column_shard_config={
                "alter_object_enabled": True,
            },
            # The deny list is Hive's, not ColumnShard's, and ColumnShard is on it by
            # default — which would disable the cutter for the tablets under test.
            hive_config={
                "cut_history_deny_list": "KeyValue,PersQueue,BlobDepot",
            },
        )

    # ---- data ----

    def _create_table(self, table_name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                CREATE TABLE `{table_name}` (
                    ts Timestamp NOT NULL,
                    id Uint64 NOT NULL,
                    payload Utf8,
                    PRIMARY KEY (ts, id)
                )
                PARTITION BY HASH(ts, id)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4)
                """
            )

    def _write_data(self, table_name, offset=0):
        values = []
        for i in range(offset, offset + self.rows_count):
            ts = f"2024-01-01T{i // 3600 % 24:02d}:{i // 60 % 60:02d}:{i % 60:02d}.000000Z"
            values.append(f'(Timestamp("{ts}"), {i}, "payload_{i}")')
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f'INSERT INTO `{table_name}` (ts, id, payload) VALUES {",".join(values)};'
            )

    def _assert_readable(self, table_name, expected_rows):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            result = session_pool.execute_with_retries(
                f"SELECT COUNT(*) AS cnt FROM `{table_name}`;",
                retry_settings=ydb.RetrySettings(idempotent=True),
            )
            assert result[0].rows[0]["cnt"] == expected_rows, (
                f"`{table_name}` returned {result[0].rows[0]['cnt']} rows, expected {expected_rows}"
            )

    # ---- tablets and sensors ----

    def _column_shard_ids(self):
        client = kikimr_client_factory("localhost", self.cluster.nodes[1].port)
        response = client.tablet_state(tablet_type=TabletTypes.COLUMNSHARD)
        return [info.TabletId for info in response.TabletStateInfo]

    def _restart_column_shards(self):
        client = kikimr_client_factory("localhost", self.cluster.nodes[1].port)
        tablet_ids = self._column_shard_ids()
        for tablet_id in tablet_ids:
            client.tablet_kill(tablet_id)
        logger.info("restarted %s ColumnShard tablet(s)", len(tablet_ids))
        return len(tablet_ids)

    def _cut_history_sensors(self):
        """Sum the component=CutHistory sensors over every node, by bare name."""
        totals = {}
        for endpoint in self.http_proxy_endpoints:
            url = f"{endpoint}/counters/counters=tablets/json"
            try:
                with urllib.request.urlopen(url, timeout=30) as response:
                    payload = json.loads(response.read().decode("utf-8", "replace"))
            except Exception as e:
                logger.warning("could not read sensors from %s: %s", url, e)
                continue
            for item in payload.get("sensors", []):
                labels = item.get("labels", {})
                if labels.get("component") != "CutHistory":
                    continue
                name = labels.get("sensor", "")
                for prefix in ("Deriviative/", "Value/"):
                    if name.startswith(prefix):
                        name = name[len(prefix):]
                        break
                try:
                    totals[name] = totals.get(name, 0) + int(item.get("value") or 0)
                except (TypeError, ValueError):
                    continue
        return totals

    def test_cut_history_during_roll(self):
        table_name = "olap_cut_history"
        self._create_table(table_name)
        self._write_data(table_name)
        expected = self.rows_count
        self._assert_readable(table_name, expected)

        assert self._column_shard_ids(), "no ColumnShard tablets found for the column table"

        # Generation churn first, so there is history to cut once the roll starts.
        for round_n in range(self.restart_rounds):
            self._restart_column_shards()
            self._write_data(table_name, offset=(round_n + 1) * self.rows_count)
            expected += self.rows_count
            self._assert_readable(table_name, expected)

        for _ in self.roll():
            self._assert_readable(table_name, expected)
            sensors = self._cut_history_sensors()
            logger.info("cut_history sensors: %s", sensors)
            # A poisoned channel means the cutter saw a refcount underflow, which is
            # a real defect rather than an environmental hiccup.
            assert sensors.get("Channels/Poisoned", 0) == 0, f"cutter poisoned a channel: {sensors}"
            assert sensors.get("Barriers/Failed/Count", 0) == 0, f"barrier send failed: {sensors}"

        # Give the cutter a nomination cadence to act on the post-roll state, then
        # confirm it is still healthy and the data survived.
        time.sleep(90)
        sensors = self._cut_history_sensors()
        logger.info("cut_history sensors after settle: %s", sensors)
        assert sensors.get("Channels/Poisoned", 0) == 0, f"cutter poisoned a channel: {sensors}"
        self._assert_readable(table_name, expected)
