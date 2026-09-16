# -*- coding: utf-8 -*-
"""Compatibility integrity tests for the CutHistory counters gate.

Data written before a history cut must survive the cut intact.
Assertions cover observable outcomes: row content, sensor health, and table readability.
"""

import json
import logging
import urllib.request

import pytest

from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from ydb.tests.library.common.types import TabletTypes
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

_ROWS_TOTAL = 200
_ROWS_PER_ROUND = 100


class TestCutHistoryIntegrity(RestartToAnotherVersionFixture):
    """Row content must survive delete and restart churn while CutHistory counters are live."""

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (26, 4):
            pytest.skip("CutHistory requires >= 26.4")
        yield from self.setup_cluster(
            extra_feature_flags=["enable_cut_history", "enable_columnshard_group_decommission"],
            column_shard_config={
                "alter_object_enabled": True,
                # Perform real cuts, not measure-only.
                "cut_history_measure_only": False,
                # Enable the audit sweep so Audit/* sensors carry verdicts.
                "cut_history_accessor_audit": True,
            },
            hive_config={
                "cut_history_deny_list": "KeyValue,PersQueue,BlobDepot",
            },
        )

    # ---- table helpers ----

    def _create_table(self, name):
        with ydb.QuerySessionPool(self.driver) as pool:
            pool.execute_with_retries(
                f"""
                CREATE TABLE `{name}` (
                    id Uint64 NOT NULL,
                    payload Utf8,
                    PRIMARY KEY (id)
                )
                PARTITION BY HASH(id)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4)
                """
            )

    @staticmethod
    def _expected_payload(row_id):
        return f"value_{row_id}"

    def _write_rows(self, table, row_ids):
        values = ", ".join(f'({i}, "{self._expected_payload(i)}")' for i in row_ids)
        with ydb.QuerySessionPool(self.driver) as pool:
            pool.execute_with_retries(f"UPSERT INTO `{table}` (id, payload) VALUES {values};")

    def _delete_rows(self, table, row_ids):
        id_list = ", ".join(str(i) for i in sorted(row_ids))
        with ydb.QuerySessionPool(self.driver) as pool:
            pool.execute_with_retries(
                f"DELETE FROM `{table}` WHERE id IN ({id_list});",
                retry_settings=ydb.RetrySettings(idempotent=True),
            )

    def _read_all(self, table):
        with ydb.QuerySessionPool(self.driver) as pool:
            result = pool.execute_with_retries(
                f"SELECT id, payload FROM `{table}`;",
                retry_settings=ydb.RetrySettings(idempotent=True),
            )
        return {int(row["id"]): row["payload"] for row in result[0].rows}

    def _assert_exact_content(self, table, expected_ids, label):
        """Fail if the table content differs from expected_ids × expected payload."""
        expected_set = set(expected_ids)
        actual = self._read_all(table)
        missing = sorted(i for i in expected_set if i not in actual)
        wrong_val = sorted(
            i for i in expected_set if i in actual and actual[i] != self._expected_payload(i)
        )
        phantom = sorted(i for i in actual if i not in expected_set)
        errors = []
        if missing:
            errors.append(f"missing {len(missing)} id(s), first 5: {missing[:5]}")
        if wrong_val:
            errors.append(f"corrupted value for {len(wrong_val)} id(s), first 5: {wrong_val[:5]}")
        if phantom:
            errors.append(f"unexpected {len(phantom)} id(s), first 5: {phantom[:5]}")
        assert not errors, f"{label}: {'; '.join(errors)}"

    # ---- cluster helpers ----

    def _node_http_endpoints(self):
        return [
            f"http://localhost:{self.cluster.nodes[i].http_proxy_port}"
            for i in range(1, len(self.cluster.nodes) + 1)
        ]

    def _reassign_column_shard_channels(self):
        """POST channel reassign to Hive: the only operation that creates cuttable history entries."""
        client = kikimr_client_factory("localhost", self.cluster.nodes[1].port)
        hives = client.tablet_state(tablet_type=TabletTypes.FLAT_HIVE)
        hive_ids = [info.TabletId for info in hives.TabletStateInfo]
        assert hive_ids, "no Hive tablet found; cannot create channel history"
        reassigned = 0
        for hive_id in hive_ids:
            url = (
                f"{self._node_http_endpoints()[0]}/tablets/app?TabletID={hive_id}"
                f"&page=ReassignTablet&tablet=all&type={int(TabletTypes.COLUMNSHARD)}"
                "&channel=2,3,4,5,6,7&wait=1&inflight=8"
            )
            try:
                request = urllib.request.Request(url, data=b"", method="POST")
                with urllib.request.urlopen(request, timeout=120) as response:
                    body = response.read().decode("utf-8", "replace")
                logger.info("reassign via hive %s: %s", hive_id, body[:200])
                reassigned += 1
            except Exception as exc:
                logger.warning("reassign via hive %s failed: %s", hive_id, exc)
        return reassigned

    def _restart_column_shards(self):
        client = kikimr_client_factory("localhost", self.cluster.nodes[1].port)
        response = client.tablet_state(tablet_type=TabletTypes.COLUMNSHARD)
        for info in response.TabletStateInfo:
            client.tablet_kill(info.TabletId)
        logger.info("killed %d ColumnShard tablet(s)", len(response.TabletStateInfo))

    def _cut_history_sensors(self):
        """Sum CutHistory sensors over all nodes; strip the Deriviative/ or Value/ type prefix."""
        totals = {}
        for endpoint in self._node_http_endpoints():
            url = f"{endpoint}/counters/counters=tablets/json"
            try:
                with urllib.request.urlopen(url, timeout=30) as resp:
                    payload = json.loads(resp.read().decode("utf-8", "replace"))
            except Exception as exc:
                logger.warning("sensor read failed from %s: %s", endpoint, exc)
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

    # ---- tests ----

    def test_row_values_survive_cut(self):
        """Exact row content must survive: delete + restart on a counter-gated cutter.

        COUNT(*) alone hides substitution bugs; this test records (id, payload) pairs
        and checks each survivor has the correct value after restart.
        Channels are reassigned before writes so the cutter has real candidates,
        making the Nominations/Count vacuity guard meaningful.
        """
        table = "olap_ch_integrity_delete_restart"
        self._create_table(table)
        reassigned = self._reassign_column_shard_channels()
        assert reassigned > 0, "no Hive responded; cannot create channel history"

        all_ids = list(range(_ROWS_TOTAL))
        self._write_rows(table, all_ids)

        keep_ids = [i for i in all_ids if i % 2 != 0]
        delete_ids = [i for i in all_ids if i % 2 == 0]
        self._delete_rows(table, delete_ids)
        self._assert_exact_content(table, keep_ids, "before restart")

        self._restart_column_shards()
        self._assert_exact_content(table, keep_ids, "after restart")

        sensors = self._cut_history_sensors()
        logger.info("sensors: %s", sensors)
        # Vacuity guard: the counters gate nominated at least one entry.
        assert sensors.get("Nominations/Count", 0) > 0, (
            f"no nominations seen; counters gate assertions are vacuous: {sensors}"
        )
        # Counter underflow produces a poisoned channel and blocks all future cuts.
        assert sensors.get("Channels/Poisoned", 0) == 0, f"poisoned channel: {sensors}"
        # Counter underflow (decrement without a matching increment) must not happen.
        assert sensors.get("Underflows/Count", 0) == 0, f"counter underflow: {sensors}"
        # Boot seeding must not fail; a failure leaves counts at zero and skips the cut.
        assert sensors.get("Seed/Tx/Failed/Count", 0) == 0, f"seeding failed: {sensors}"
        # Audit undercount means the accessor found fewer blobs than the counter tracked.
        assert sensors.get("Audit/Undercounts/Count", 0) == 0, f"audit undercount: {sensors}"

    def test_multi_round_writes_survive_churn(self):
        """Three write rounds interleaved with restarts — every written row must keep its exact content."""
        table = "olap_ch_integrity_multi_write"
        self._create_table(table)
        reassigned = self._reassign_column_shard_channels()
        assert reassigned > 0, "no Hive responded; cannot create channel history"

        all_written: set[int] = set()
        for round_n in range(3):
            batch = list(range(round_n * _ROWS_PER_ROUND, (round_n + 1) * _ROWS_PER_ROUND))
            self._write_rows(table, batch)
            all_written.update(batch)
            self._restart_column_shards()
            self._assert_exact_content(table, list(all_written), f"after round {round_n}")

        sensors = self._cut_history_sensors()
        logger.info("sensors after churn: %s", sensors)
        assert sensors.get("Nominations/Count", 0) > 0, (
            f"no nominations; counters gate assertions are vacuous: {sensors}"
        )
        assert sensors.get("Channels/Poisoned", 0) == 0, f"poisoned channel: {sensors}"
        assert sensors.get("Underflows/Count", 0) == 0, f"counter underflow: {sensors}"
        assert sensors.get("Seed/Tx/Failed/Count", 0) == 0, f"seeding failed: {sensors}"
        assert sensors.get("Audit/Undercounts/Count", 0) == 0, f"audit undercount: {sensors}"

    def test_rewrite_delivers_new_values(self):
        """Rows re-written after deletion must carry the new payload, not the old one.

        Stale blobs from the deleted generation are distinguishable because the two
        writes use different payload prefixes: 'value_N' vs 'new_value_N'.
        """
        table = "olap_ch_integrity_rewrite"
        self._create_table(table)
        reassigned = self._reassign_column_shard_channels()
        assert reassigned > 0, "no Hive responded; cannot create channel history"

        row_ids = list(range(_ROWS_PER_ROUND))
        self._write_rows(table, row_ids)
        self._delete_rows(table, row_ids)

        new_values = ", ".join(f'({i}, "new_value_{i}")' for i in row_ids)
        with ydb.QuerySessionPool(self.driver) as pool:
            pool.execute_with_retries(f"UPSERT INTO `{table}` (id, payload) VALUES {new_values};")

        self._restart_column_shards()

        actual = self._read_all(table)
        missing = [i for i in row_ids if i not in actual]
        stale = [i for i in row_ids if i in actual and actual[i].startswith("value_")]
        assert not missing, f"rows missing after rewrite + restart: {missing[:10]}"
        assert not stale, f"stale pre-delete payload survived restart: {stale[:5]}"

        sensors = self._cut_history_sensors()
        logger.info("sensors: %s", sensors)
        assert sensors.get("Nominations/Count", 0) > 0, (
            f"no nominations; assertions are vacuous: {sensors}"
        )
        assert sensors.get("Channels/Poisoned", 0) == 0, f"poisoned channel: {sensors}"
        assert sensors.get("Underflows/Count", 0) == 0, f"counter underflow: {sensors}"
        assert sensors.get("Seed/Tx/Failed/Count", 0) == 0, f"seeding failed: {sensors}"
        assert sensors.get("Audit/Undercounts/Count", 0) == 0, f"audit undercount: {sensors}"

    def test_no_counter_underflow_on_fresh_cluster(self):
        """A cluster that has never had channel reassignment must produce no underflows or poisoning.

        Without reassignment there are no multi-entry histories, so the counters gate
        has no candidates and the nomination cadence should complete cleanly.
        """
        table = "olap_ch_integrity_fresh"
        self._create_table(table)
        self._write_rows(table, list(range(_ROWS_PER_ROUND)))

        self._restart_column_shards()
        self._assert_exact_content(table, list(range(_ROWS_PER_ROUND)), "after restart")

        sensors = self._cut_history_sensors()
        logger.info("sensors: %s", sensors)
        assert sensors.get("Channels/Poisoned", 0) == 0, f"poisoned channel: {sensors}"
        assert sensors.get("Underflows/Count", 0) == 0, f"counter underflow: {sensors}"
        assert sensors.get("Seed/Tx/Failed/Count", 0) == 0, f"seeding failed: {sensors}"
        # No channel history means no nominations; Entries/Cut/Count must be zero.
        assert sensors.get("Entries/Cut/Count", 0) == 0, f"unexpected cut on fresh cluster: {sensors}"
