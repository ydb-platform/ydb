"""End-to-end tests for find_tli_chain against real TLI scenarios.

Uses the same Basic / CrossTables lock-breaking scenarios:
* run them on a live cluster with TLI logging,
* take VictimQuerySpanId from the Aborted issue,
* reconstruct the chain with find_tli_chain.py.
"""

from __future__ import annotations

import io
import os
import re
import sys
import tempfile
import threading
from contextlib import redirect_stdout
from unittest import mock

import pytest
import ydb

from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.stress.common.instrumented_client import InstrumentedYdbClient
from ydb.tests.stress.oltp_workload.workload.type.tli import WorkloadTli
from ydb.tools.tli_analysis import find_tli_chain


RE_BREAKER_ID = re.compile(r"BreakerQuerySpanId:\s*(\d+)")


def _collect_ydbd_log_paths(cluster):
    paths = []
    for group in (cluster.nodes.values(), cluster.slots.values()):
        for proc in group:
            path = proc.ydbd_log_file_path
            if path:
                paths.append(path)
    return paths


def _merge_logs_to_file(log_paths) -> str:
    merged_fd, merged_path = tempfile.mkstemp(prefix="tli_merged_", suffix=".log")
    os.close(merged_fd)
    with open(merged_path, "w", encoding="utf-8") as out:
        for path in log_paths:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                out.write(fh.read())
                out.write("\n")
    return merged_path


def _run_find_tli_chain(victim_id: str, logfile: str) -> str:
    buf = io.StringIO()
    argv = ["find_tli_chain.py", str(victim_id), logfile, "--no-color"]
    with mock.patch.object(sys, "argv", argv), redirect_stdout(buf):
        find_tli_chain.main()
    return buf.getvalue()


def _assert_chain_output(output: str, victim_id: int, victim_query: str, breaker_query: str):
    assert f"VictimQuerySpanId: {victim_id}" in output, output
    assert "VictimQueryText: (not found)" not in output, output
    assert "BreakerQuerySpanId: (not found)" not in output, output
    assert "BreakerQueryText: (not found)" not in output, output

    breaker_m = RE_BREAKER_ID.search(output)
    assert breaker_m and breaker_m.group(1) != "0", output

    assert victim_query in output, f"missing victim query {victim_query!r} in:\n{output}"
    assert breaker_query in output, f"missing breaker query {breaker_query!r} in:\n{output}"
    assert "VictimTx" in output, output
    assert "BreakerTx" in output, output


class TestFindTliChain(StressFixture):
    @pytest.fixture(scope="function")
    def setup_tli(self):
        yield from self.setup_cluster(
            additional_log_configs={
                "TLI": LogLevels.INFO,
            },
            use_log_files=True,
        )

    def _run_scenario_capture_victim_id(
        self,
        client,
        workload: WorkloadTli,
        victim_read_query: str,
        breaker_query: str,
        victim_commit_query: str,
        scenario_name: str,
    ) -> int:
        captured = {}

        def run_in_victim_session(victim_session):
            with victim_session.transaction() as victim_tx:
                victim_tx.begin()
                workload._drain_query_result_if_needed(victim_tx.execute(victim_read_query))

                client.query(breaker_query, False)

                try:
                    workload._drain_query_result_if_needed(
                        victim_tx.execute(victim_commit_query, commit_tx=True)
                    )
                    raise AssertionError(
                        f"{scenario_name}: expected ABORTED but victim commit succeeded"
                    )
                except ydb.issues.Aborted as e:
                    issues = workload._extract_issue_text(e)
                    workload._verify_tli_issue_content(issues, scenario_name)
                    captured["victim_id"] = workload._extract_victim_query_span_id(issues)

        client.session_pool.retry_operation_sync(run_in_victim_session)
        assert "victim_id" in captured, f"{scenario_name}: VictimQuerySpanId was not captured"
        return captured["victim_id"]

    def test_basic_and_cross_tables(self, setup_tli):
        client = InstrumentedYdbClient(self.endpoint, self.database, True)
        client.wait_connection()

        stop = threading.Event()
        workload = WorkloadTli(client, "tli_analysis", stop)

        table_basic = workload.get_table_path("basic")
        table_cross1 = workload.get_table_path("cross1")
        table_cross2 = workload.get_table_path("cross2")

        try:
            for name in ["basic", "cross1", "cross2"]:
                table_path = workload.get_table_path(name)
                client.query(
                    f"""
                    CREATE TABLE `{table_path}` (
                        Key Uint64,
                        Value String,
                        PRIMARY KEY (Key)
                    )
                    """,
                    True,
                )
                client.query(
                    f'UPSERT INTO `{table_path}` (Key, Value) VALUES (1u, "Init")',
                    False,
                )

            basic_victim_read = f"SELECT * FROM `{table_basic}` WHERE Key = 1u"
            basic_breaker = (
                f'UPSERT INTO `{table_basic}` (Key, Value) VALUES (1u, "BreakerValue")'
            )
            basic_victim_commit = (
                f'UPSERT INTO `{table_basic}` (Key, Value) VALUES (1u, "VictimValue")'
            )
            basic_victim_id = self._run_scenario_capture_victim_id(
                client,
                workload,
                basic_victim_read,
                basic_breaker,
                basic_victim_commit,
                "Basic",
            )

            cross_victim_read = f"SELECT * FROM `{table_cross1}` WHERE Key = 1u"
            cross_breaker = (
                f'UPSERT INTO `{table_cross1}` (Key, Value) VALUES (1u, "Breaker")'
            )
            cross_victim_commit = (
                f'UPSERT INTO `{table_cross2}` (Key, Value) VALUES (1u, "DstVal")'
            )
            cross_victim_id = self._run_scenario_capture_victim_id(
                client,
                workload,
                cross_victim_read,
                cross_breaker,
                cross_victim_commit,
                "CrossTables",
            )
        finally:
            client.close()

        log_paths = _collect_ydbd_log_paths(self.cluster)
        assert log_paths, "expected ydbd log files with use_log_files=True"

        merged_path = _merge_logs_to_file(log_paths)
        try:
            basic_out = _run_find_tli_chain(basic_victim_id, merged_path)
            _assert_chain_output(basic_out, basic_victim_id, basic_victim_read, basic_breaker)
            assert basic_victim_commit in basic_out, basic_out

            cross_out = _run_find_tli_chain(cross_victim_id, merged_path)
            _assert_chain_output(cross_out, cross_victim_id, cross_victim_read, cross_breaker)
            assert cross_victim_commit in cross_out, cross_out
        finally:
            os.unlink(merged_path)
