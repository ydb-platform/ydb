# -*- coding: utf-8 -*-
"""Data-loss detection for CutHistory: row *content* must survive delete + restart churn.

COUNT(*) alone hides substitution bugs where the wrong row survives.  These tests
record the exact (id, value) pairs that must survive, check them row-by-row after
every delete/restart, and also check sensors that directly signal data-loss precursors.
Channels are reassigned before writes so the boot proof has real candidates; cutter
assertions are therefore non-vacuous (the probe ran on actual history entries).
"""

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

_ROWS_TOTAL = 200
_ROWS_PER_ROUND = 100


class TestCutHistoryDataIntegrity(RestartToAnotherVersionFixture):
    """Verify row content — not just count — survives delete and restart churn with CutHistory on."""

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (26, 4):
            pytest.skip("CutHistory requires >= 26.4")
        yield from self.setup_cluster(
            extra_feature_flags=["enable_cut_history", "enable_columnshard_group_decommission"],
            column_shard_config={
                "alter_object_enabled": True,
                "cut_history_proof_source": "CUT_HISTORY_PROOF_BS_RANGE",
                # The proof must run all the way to the barrier, not stop at measure-only.
                "cut_history_measure_only": False,
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
        """Assert the table contains exactly expected_ids, each with the correct payload value."""
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
            "http://%s:%s" % ("localhost", self.cluster.nodes[i].http_proxy_port)
            for i in range(1, len(self.cluster.nodes) + 1)
        ]

    def _reassign_column_shard_channels(self):
        """Rebind channels via Hive: the only operation that creates cuttable history entries."""
        client = kikimr_client_factory("localhost", self.cluster.nodes[1].port)
        hives = client.tablet_state(tablet_type=TabletTypes.FLAT_HIVE)
        hive_ids = [info.TabletId for info in hives.TabletStateInfo]
        assert hive_ids, "no Hive tablet found, cannot create channel history"
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
        totals = {}
        for i in range(1, len(self.cluster.nodes) + 1):
            url = (
                f"http://localhost:{self.cluster.nodes[i].http_proxy_port}"
                f"/counters/counters=tablets/json"
            )
            try:
                with urllib.request.urlopen(url, timeout=30) as resp:
                    payload = json.loads(resp.read().decode("utf-8", "replace"))
            except Exception as exc:
                logger.warning("sensor read failed from node %d: %s", i, exc)
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

    def test_row_values_survive_delete_and_restart(self):
        """Delete half the rows then restart — surviving rows must have their exact original values.

        COUNT(*) == 100 would not catch a case where the wrong 100 rows survive, or
        where a surviving row's payload was swapped with a deleted row's payload.
        The PortionsOnly disagreement sensor directly signals the condition that precedes
        data loss: BlobStorage reported a range empty while the portions index still pinned blobs.
        """
        table = "olap_ch_delete_restart"
        self._create_table(table)
        # Reassign before writes so the boot proof has a real candidate; cutter assertions below are non-vacuous.
        reassigned = self._reassign_column_shard_channels()
        assert reassigned > 0, "no Hive responded; channel history cannot be created"

        all_ids = list(range(_ROWS_TOTAL))
        self._write_rows(table, all_ids)

        # Delete even IDs; odd IDs are the known survivors.
        keep_ids = [i for i in all_ids if i % 2 != 0]
        delete_ids = [i for i in all_ids if i % 2 == 0]
        self._delete_rows(table, delete_ids)

        self._assert_exact_content(table, keep_ids, "before restart")

        self._restart_column_shards()

        self._assert_exact_content(table, keep_ids, "after restart")

        sensors = self._cut_history_sensors()
        logger.info("sensors: %s", sensors)
        # Vacuity guard: the boot proof must have had candidates.
        assert sensors.get("BootProbe/Nominated/Count", 0) > 0, (
            f"nothing nominated; cutter assertions are vacuous: {sensors}"
        )
        assert sensors.get("Channels/Poisoned", 0) == 0, f"poisoned channel: {sensors}"
        # PortionsOnly > 0 means BS said empty but our index disagrees — a data-loss precursor.
        assert sensors.get("RangeProbe/Disagreement/PortionsOnly/Count", 0) == 0, (
            f"BS-vs-portions disagreement detected: {sensors}"
        )

    def test_multi_round_writes_survive_churn_exactly(self):
        """Three write rounds interleaved with restarts — every written row must keep exact content."""
        table = "olap_ch_multi_write"
        self._create_table(table)
        # Reassign before writes so boot probe has candidates; cutter assertions below are non-vacuous.
        reassigned = self._reassign_column_shard_channels()
        assert reassigned > 0, "no Hive responded; channel history cannot be created"

        all_written: set[int] = set()
        for round_n in range(3):
            batch = list(range(round_n * _ROWS_PER_ROUND, (round_n + 1) * _ROWS_PER_ROUND))
            self._write_rows(table, batch)
            all_written.update(batch)
            self._restart_column_shards()
            self._assert_exact_content(table, list(all_written), f"after round {round_n}")

        sensors = self._cut_history_sensors()
        logger.info("sensors after churn: %s", sensors)
        # Vacuity guard: the boot proof must have had candidates.
        assert sensors.get("BootProbe/Nominated/Count", 0) > 0, (
            f"nothing nominated; cutter assertions are vacuous: {sensors}"
        )
        assert sensors.get("Channels/Poisoned", 0) == 0, f"poisoned channel: {sensors}"
        assert sensors.get("Entries/Cut/Count", 0) == 0, f"live range cut: {sensors}"
        assert sensors.get("RangeProbe/Disagreement/PortionsOnly/Count", 0) == 0, (
            f"BS-vs-portions disagreement: {sensors}"
        )

    def test_rewrite_after_delete_delivers_new_values(self):
        """Write, delete, re-write same IDs with distinct values — stale payload must not survive.

        If physical deletion is incomplete and old blobs linger, the engine might return
        the old payload instead of the new one.  The test detects this by using a value
        prefix that differs between the two writes: any 'value_N' after restart means the
        stale blob was returned instead of the new 'new_value_N'.
        """
        table = "olap_ch_rewrite"
        self._create_table(table)
        # Reassign before writes so boot probe has candidates; cutter assertions below are non-vacuous.
        reassigned = self._reassign_column_shard_channels()
        assert reassigned > 0, "no Hive responded; channel history cannot be created"

        row_ids = list(range(_ROWS_PER_ROUND))
        self._write_rows(table, row_ids)
        self._delete_rows(table, row_ids)

        # Re-write same IDs with a different prefix so stale old blobs are distinguishable.
        new_values = ", ".join(f'({i}, "new_value_{i}")' for i in row_ids)
        with ydb.QuerySessionPool(self.driver) as pool:
            pool.execute_with_retries(
                f"UPSERT INTO `{table}` (id, payload) VALUES {new_values};",
            )

        self._restart_column_shards()

        actual = self._read_all(table)
        missing = [i for i in row_ids if i not in actual]
        stale = [i for i in row_ids if i in actual and actual[i].startswith("value_")]
        assert not missing, f"rows missing after rewrite+restart: {missing[:10]}"
        assert not stale, f"stale pre-delete payload survived restart (first 5): {stale[:5]}"

        sensors = self._cut_history_sensors()
        logger.info("sensors: %s", sensors)
        # Vacuity guard: the boot proof must have had candidates.
        assert sensors.get("BootProbe/Nominated/Count", 0) > 0, (
            f"nothing nominated; cutter assertions are vacuous: {sensors}"
        )
        assert sensors.get("Channels/Poisoned", 0) == 0, f"poisoned channel: {sensors}"

    def test_deferred_sensor_is_zero_without_history_entries(self):
        """BootProbe/Deferred must be 0 when there are no multi-entry channel histories.

        A nonzero value in a fresh cluster (where Hive never reassigned groups, so
        no history entry has a successor) indicates the deferral counter misfired.
        The full deferral path E2E — getting a real DoNotKeep into a candidate range —
        requires Hive group reassignment and is not covered by this test.
        """
        table = "olap_ch_deferred"
        self._create_table(table)
        self._write_rows(table, list(range(_ROWS_PER_ROUND)))

        self._restart_column_shards()
        # Give the boot proof time to complete its nomination pass.
        time.sleep(30)

        sensors = self._cut_history_sensors()
        logger.info("sensors: %s", sensors)
        # BootProbe/Deferred is a Value (absolute count), not a derivative.
        assert sensors.get("BootProbe/Deferred", 0) == 0, (
            f"unexpected deferred entries in a fresh cluster: {sensors}"
        )
        assert sensors.get("Channels/Poisoned", 0) == 0, f"poisoned channel: {sensors}"
        assert sensors.get("Entries/Cut/Count", 0) == 0, f"live range cut: {sensors}"
