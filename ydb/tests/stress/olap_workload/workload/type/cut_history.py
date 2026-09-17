# -*- coding: utf-8 -*-
import logging
import threading
import time

from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from ydb.tests.library.common.types import TabletTypes
from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger(__name__)


class WorkloadCutHistory(WorkloadBase):
    """Restart ColumnShard tablets while the cutter runs.

    Restarts do NOT grow channel history (entries are appended only on a Hive
    channel reassignment, TTxUpdateTabletGroups) — what this workload exercises is
    the cutter's restart-safety: every OnBootComplete rebuild, re-nomination and
    in-flight sweep abort happens under generation churn. The positive make-an-
    entry-and-cut-it check lives in the test driver, which can reach Hive's
    monitoring to force a reassignment; a client-side workload cannot.
    """

    def __init__(self, client, prefix, stop, endpoint, period=30):
        super().__init__(client, prefix, "cut_history", stop)
        # kikimr_client_factory speaks plaintext message bus, so reject grpcs:// instead of failing to connect.
        scheme, sep, address = endpoint.rpartition("://")
        if sep and scheme != "grpc":
            raise ValueError(f"cut_history needs a grpc:// endpoint, got {endpoint}")
        host, _, port = address.partition(":")
        self.kikimr_client = kikimr_client_factory(host, port or "2135")
        self.period = period
        self.restarts = 0
        self.errors = 0

    def get_stat(self):
        return f"Restarts: {self.restarts}, Errors: {self.errors}"

    def _column_shard_ids(self):
        response = self.kikimr_client.tablet_state(tablet_type=TabletTypes.COLUMNSHARD)
        return [info.TabletId for info in response.TabletStateInfo]

    def _loop(self):
        while not self.is_stop_requested():
            try:
                tablet_ids = self._column_shard_ids()
                if not tablet_ids:
                    # The other workloads may not have created a column table yet.
                    time.sleep(self.period)
                    continue
                for tablet_id in tablet_ids:
                    if self.is_stop_requested():
                        return
                    self.kikimr_client.tablet_kill(tablet_id)
                    self.restarts += 1
                logger.info("cut_history: restarted %s ColumnShard tablet(s)", len(tablet_ids))
            except Exception as e:
                self.errors += 1
                logger.warning("cut_history: restart round failed: %s", e)
            # Wait past the nomination cadence so a sweep finishes instead of every entry being reopened.
            waited = 0
            while waited < self.period and not self.is_stop_requested():
                time.sleep(1)
                waited += 1

    def get_workload_thread_funcs(self):
        return [self._loop]


class WorkloadCutHistoryVerify(WorkloadBase):
    """Concurrent write/delete/verify cycle that catches row-content regressions.

    Each iteration writes a fresh batch of rows with deterministic content
    (val = "v_{id}"), deletes the previous batch, reads the whole table, and
    asserts: every expected row is present with the right value and no unexpected
    rows appear.  Any mismatch raises, triggering os._exit(1) via
    WorkloadBase.run_with_fatal_handler — the intended loud failure.

    Run alongside WorkloadCutHistory so tablet restarts happen concurrently with
    the write/delete/verify cycle.
    """

    BATCH = 50

    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "cut_history_verify", stop)
        self.cycles = 0
        self.errors = 0
        self._stat_lock = threading.Lock()

    @staticmethod
    def _val(row_id):
        return f"v_{row_id}"

    def get_stat(self):
        with self._stat_lock:
            return f"Cycles: {self.cycles}, Errors: {self.errors}"

    def _loop(self):
        table_path = self.get_table_path("table")
        self.client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Int64 NOT NULL,
                val Utf8,
                PRIMARY KEY(id)
            )
            PARTITION BY HASH(id)
            WITH (STORE = COLUMN)
            """,
            True,
        )
        live_ids: set = set()
        batch_start = 0
        while not self.is_stop_requested():
            batch = list(range(batch_start, batch_start + self.BATCH))
            batch_start += self.BATCH
            values = ", ".join(f'({i}, "{self._val(i)}")' for i in batch)
            self.client.query(
                f"UPSERT INTO `{table_path}` (id, val) VALUES {values}",
                False,
            )
            if live_ids:
                id_list = ", ".join(str(i) for i in sorted(live_ids))
                self.client.query(
                    f"DELETE FROM `{table_path}` WHERE id IN ({id_list})",
                    False,
                )
            live_ids = set(batch)
            rows = self.client.query(
                f"SELECT id, val FROM `{table_path}`",
                False,
            )[0].rows
            actual = {int(r["id"]): r["val"] for r in rows}
            errors = []
            for i in live_ids:
                if i not in actual:
                    errors.append(f"missing id={i}")
                elif actual[i] != self._val(i):
                    errors.append(f"wrong val id={i}: got {actual[i]!r}")
            for i in actual:
                if i not in live_ids:
                    errors.append(f"phantom id={i}")
            if errors:
                with self._stat_lock:
                    self.errors += 1
                raise Exception(f"Data integrity error: {'; '.join(errors[:5])}")
            with self._stat_lock:
                self.cycles += 1

    def get_workload_thread_funcs(self):
        return [self._loop]
