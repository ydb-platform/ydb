import threading
import logging
import re
import os

import ydb
from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class WorkloadTli(WorkloadBase):
    """
    Stress workload exercising TLI (Transaction Lock Information) logging.

    Implements two lock-breaking scenarios from kqp_tli_ut.cpp:
    - Basic: single-table lock break (victim reads key, breaker writes same key, victim aborted)
    - CrossTables: cross-table lock break (victim reads Table1, breaker writes Table1,
      victim writes Table2, victim aborted)
    """

    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "tli", stop)
        self._lock = threading.Lock()
        self._basic_iterations = 0
        self._cross_tables_iterations = 0

    def get_stat(self):
        with self._lock:
            return (
                f"Basic: {self._basic_iterations}, "
                f"CrossTables: {self._cross_tables_iterations}"
            )

    @staticmethod
    def _extract_issue_text(exc):
        return getattr(exc, "message", str(exc))

    @staticmethod
    def _extract_victim_query_span_id(issues):
        match = re.search(r"VictimQuerySpanId:\s*(\d+)", issues)
        if not match:
            return None
        return int(match.group(1))

    def _verify_tli_issue_content(self, issues, scenario_name):
        if "Transaction locks invalidated" not in issues:
            raise Exception(f"{scenario_name}: expected 'Transaction locks invalidated' in issues: {issues}")
        if "BreakerQuerySpanId:" in issues:
            raise Exception(f"{scenario_name}: unexpected 'BreakerQuerySpanId:' in issues: {issues}")
        victim_query_span_id = self._extract_victim_query_span_id(issues)
        if victim_query_span_id is None:
            raise Exception(f"{scenario_name}: expected 'VictimQuerySpanId:' in issues: {issues}.")
        if victim_query_span_id == 0:
            raise Exception(f"{scenario_name}: VictimQuerySpanId should not be 0: {issues}")

    def _drain_query_result_if_needed(self, result):
        # Query Service API returns an iterator that must be consumed.
        if self.client.use_query_service:
            list(result)

    def _run_scenario(self, victim_read_query, breaker_query, victim_commit_query, scenario_name):
        def run_in_victim_session(victim_session):
            with victim_session.transaction() as victim_tx:
                victim_tx.begin()
                self._drain_query_result_if_needed(victim_tx.execute(victim_read_query))

                self.client.query(breaker_query, False)

                try:
                    self._drain_query_result_if_needed(
                        victim_tx.execute(victim_commit_query, commit_tx=True)
                    )
                    raise Exception(f"{scenario_name}: expected ABORTED but victim commit succeeded")
                except ydb.issues.Aborted as e:
                    self._verify_tli_issue_content(self._extract_issue_text(e), scenario_name)

        # retry_operation_sync exists on both SessionPool and QuerySessionPool.
        self.client.session_pool.retry_operation_sync(run_in_victim_session)

    def _run_basic_iteration(self, table_path):
        """Basic TLI scenario: victim reads key 1, breaker writes key 1, victim commit aborted."""
        self._run_scenario(
            victim_read_query=f"SELECT * FROM `{table_path}` WHERE Key = 1u",
            breaker_query=f'UPSERT INTO `{table_path}` (Key, Value) VALUES (1u, "BreakerValue")',
            victim_commit_query=f'UPSERT INTO `{table_path}` (Key, Value) VALUES (1u, "VictimValue")',
            scenario_name="Basic",
        )

        with self._lock:
            self._basic_iterations += 1

    def _run_cross_tables_iteration(self, table1_path, table2_path):
        """CrossTables TLI scenario: victim reads Table1, breaker writes Table1, victim writes Table2, aborted."""
        self._run_scenario(
            victim_read_query=f"SELECT * FROM `{table1_path}` WHERE Key = 1u",
            breaker_query=f'UPSERT INTO `{table1_path}` (Key, Value) VALUES (1u, "Breaker")',
            victim_commit_query=f'UPSERT INTO `{table2_path}` (Key, Value) VALUES (1u, "DstVal")',
            scenario_name="CrossTables",
        )

        with self._lock:
            self._cross_tables_iterations += 1

    def _loop(self):
        table_basic = self.get_table_path("basic")
        table_cross1 = self.get_table_path("cross1")
        table_cross2 = self.get_table_path("cross2")

        for name in ["basic", "cross1", "cross2"]:
            table_path = self.get_table_path(name)
            self.client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                )
                """,
                True,
            )
            self.client.query(
                f'UPSERT INTO `{table_path}` (Key, Value) VALUES (1u, "Init")',
                False,
            )

        while not self.is_stop_requested():
            self._run_basic_iteration(table_basic)
            self._run_cross_tables_iteration(table_cross1, table_cross2)

    def get_workload_thread_funcs(self):
        return [self._loop]
