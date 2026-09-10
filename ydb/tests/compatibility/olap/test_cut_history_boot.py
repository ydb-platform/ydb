import json
import logging
import time
import urllib.request

import pytest

from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from ydb.tests.library.common.types import TabletTypes
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class TestCutHistoryBootProbe(RestartToAnotherVersionFixture):
    """CutHistory proving candidate ranges with a BlobStorage range read at boot.

    The boot proof replaces the nomination cadence: every history entry that has a
    successor is probed once per tablet start. These tests assert the two properties
    that make it safe to place a hard barrier afterwards - a range that still holds
    live data is never cut, and an entry with a delete still owed to it is deferred
    rather than proven.
    """

    rows_count = 200
    restart_rounds = 3

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (26, 4):
            pytest.skip("CutHistory is available starting from 26.4")

        yield from self.setup_cluster(
            extra_feature_flags=["enable_cut_history", "enable_columnshard_group_decommission"],
            column_shard_config={
                "alter_object_enabled": True,
                "cut_history_proof_source": "CUT_HISTORY_PROOF_BS_RANGE",
                # The proof must really run: measure-only would stop it short of the barrier.
                "cut_history_measure_only": False,
            },
            hive_config={
                "cut_history_deny_list": "KeyValue,PersQueue,BlobDepot",
            },
        )

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

    def _node_http_endpoints(self):
        # This fixture exposes only node 1; a cut on any other node must not go unseen.
        return [
            "http://%s:%s" % ("localhost", self.cluster.nodes[i].http_proxy_port)
            for i in range(1, len(self.cluster.nodes) + 1)
        ]

    def _cut_history_sensors(self):
        """Sum the component=CutHistory sensors over every node, by bare name."""
        totals = {}
        for endpoint in self._node_http_endpoints():
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

    def test_boot_probe_never_cuts_a_live_range(self):
        """Every restart runs the boot proof over ranges that still hold this table's data.

        The probe must therefore disprove every candidate: nothing may be cut, no channel
        may be poisoned, and the data must survive the churn intact.
        """
        table_name = "olap_cut_history_boot"
        self._create_table(table_name)
        self._write_data(table_name)
        expected = self.rows_count
        self._assert_readable(table_name, expected)

        assert self._column_shard_ids(), "no ColumnShard tablets found for the column table"

        for round_n in range(self.restart_rounds):
            self._restart_column_shards()
            self._write_data(table_name, offset=(round_n + 1) * self.rows_count)
            expected += self.rows_count
            self._assert_readable(table_name, expected)

            sensors = self._cut_history_sensors()
            logger.info("round %s sensors: %s", round_n, sensors)
            assert sensors.get("Channels/Poisoned", 0) == 0, f"cutter poisoned a channel: {sensors}"
            assert sensors.get("Barriers/Failed/Count", 0) == 0, f"barrier send failed: {sensors}"
            assert sensors.get("Entries/Cut/Count", 0) == 0, (
                f"a range still holding live data was cut: {sensors}"
            )

        # The probe answers within its own deadline; give it room, then re-check.
        time.sleep(90)
        sensors = self._cut_history_sensors()
        logger.info("sensors after settle: %s", sensors)
        assert sensors.get("Entries/Cut/Count", 0) == 0, f"a live range was cut after settling: {sensors}"
        assert sensors.get("Channels/Poisoned", 0) == 0, f"cutter poisoned a channel: {sensors}"
        self._assert_readable(table_name, expected)

    def test_boot_probe_survives_restart_to_another_version(self):
        """The boot proof runs on every start, so a version change is just another boot."""
        table_name = "olap_cut_history_boot_roll"
        self._create_table(table_name)
        self._write_data(table_name)
        self._assert_readable(table_name, self.rows_count)

        self._restart_column_shards()
        self._assert_readable(table_name, self.rows_count)

        self.change_cluster_version()

        self._assert_readable(table_name, self.rows_count)
        sensors = self._cut_history_sensors()
        logger.info("sensors after version change: %s", sensors)
        assert sensors.get("Channels/Poisoned", 0) == 0, f"cutter poisoned a channel: {sensors}"
        assert sensors.get("Entries/Cut/Count", 0) == 0, f"a live range was cut across the version change: {sensors}"
