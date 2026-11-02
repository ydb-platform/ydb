"""Cluster tests for find_tli_chain over the KqpTli scenarios that change chain selection."""

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


_ANSI = re.compile(r"\033\[[0-9;]*m")


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


def _run_find_tli_chain(victim_id: str, logfile: str, *, color: bool = False) -> str:
    buf = io.StringIO()
    if color:
        buf.isatty = lambda: True
    argv = ["find_tli_chain.py", str(victim_id), logfile, "--window-sec", "120"]
    if not color:
        argv.append("--no-color")
    with mock.patch.object(sys, "argv", argv), redirect_stdout(buf):
        env = os.environ.copy()
        env.pop("NO_COLOR", None)
        with mock.patch.dict(os.environ, env, clear=True):
            find_tli_chain.main()
    return buf.getvalue()


def _red(text: str) -> str:
    return f"\033[31m{text}\033[0m"


def _summary_field(output: str, name: str) -> str:
    plain = _ANSI.sub("", output)
    marker = f"{name}: "
    start = plain.find(marker)
    assert start >= 0, output
    return plain[start + len(marker):].split("\n", 1)[0]


def _tx_section(output: str, title: str, next_title: str | None = None) -> str:
    body = output.split(title, 1)[1]
    if next_title:
        body = body.split(next_title, 1)[0]
    return body


def _exec(workload: WorkloadTli, tx, query: str, *, commit: bool = False):
    workload._drain_query_result_if_needed(tx.execute(query, commit_tx=commit))


def _expect_aborted(workload: WorkloadTli, scenario: str, action) -> int:
    try:
        action()
    except ydb.issues.Aborted as e:
        issues = workload._extract_issue_text(e)
        workload._verify_tli_issue_content(issues, scenario)
        victim_id = workload._extract_victim_query_span_id(issues)
        assert victim_id, f"{scenario}: VictimQuerySpanId was not captured: {issues}"
        return victim_id
    raise AssertionError(f"{scenario}: expected ABORTED")


class _Expect:
    def __init__(self, name, victim_id, victim_query, breaker_query, victim_tx=(), breaker_tx=(), commit_row=False):
        self.name = name
        self.victim_id = victim_id
        self.victim_query = victim_query
        self.breaker_query = breaker_query
        self.victim_tx = victim_tx
        self.breaker_tx = breaker_tx
        self.commit_row = commit_row


class TestFindTliChain(StressFixture):
    @pytest.fixture(scope="function")
    def setup_tli(self):
        yield from self.setup_cluster(
            additional_log_configs={
                "TLI": LogLevels.INFO,
            },
            use_log_files=True,
        )

    def _create_table(self, client, path: str, rows):
        client.query(
            f"""
            CREATE TABLE `{path}` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            """,
            True,
        )
        for key, value in rows:
            client.query(
                f'UPSERT INTO `{path}` (Key, Value) VALUES ({key}u, "{value}")',
                False,
            )

    def _on_session(self, client, fn):
        def run(session):
            return fn(session)

        return client.session_pool.retry_operation_sync(run)

    def test_chain_scenarios(self, setup_tli):
        client = InstrumentedYdbClient(self.endpoint, self.database, True)
        client.wait_connection()
        workload = WorkloadTli(client, "tli_analysis", threading.Event())
        expects = []
        failures = []
        runners = (
            ("Basic", self._basic),
            ("CrossTables", self._cross_tables),
            ("ConcurrentUpsertSelect", self._concurrent_upsert_select),
            ("VictimReadThenWriteSameTable", self._victim_read_then_write),
            ("ManyUpserts", self._many_upserts),
            ("TwoVictimsOneBreaker", self._two_victims_one_breaker),
            ("BreakerAndVictimInSameTransaction", self._breaker_and_victim),
        )

        try:
            for name, runner in runners:
                try:
                    result = runner(client, workload)
                except Exception as exc:
                    failures.append(f"{name} failed while running: {exc}")
                    continue
                if isinstance(result, tuple):
                    expects.extend(result)
                else:
                    expects.append(result)
        finally:
            client.close()

        log_paths = _collect_ydbd_log_paths(self.cluster)
        assert log_paths, "expected ydbd log files with use_log_files=True"
        merged_path = _merge_logs_to_file(log_paths)
        try:
            for exp in expects:
                try:
                    self._check_chain(exp, merged_path)
                except AssertionError as exc:
                    failures.append(str(exc))
        finally:
            os.unlink(merged_path)

        if failures:
            raise AssertionError("\n\n".join(failures))

    def _check_chain(self, exp: _Expect, merged_path: str):
        output = _run_find_tli_chain(exp.victim_id, merged_path, color=True)
        assert _summary_field(output, "VictimQuerySpanId") == str(exp.victim_id), f"{exp.name}\n{output}"
        assert _summary_field(output, "VictimQueryText") == exp.victim_query, f"{exp.name}\n{output}"
        assert _summary_field(output, "BreakerQueryText") == exp.breaker_query, f"{exp.name}\n{output}"
        assert _summary_field(output, "BreakerQuerySpanId") not in ("(not found)", "0"), f"{exp.name}\n{output}"
        victim_tx = _tx_section(output, "VictimTx", "BreakerTx")
        breaker_tx = _tx_section(output, "BreakerTx")
        for text in exp.victim_tx:
            assert text in victim_tx, f"{exp.name}: missing victim tx text {text!r}\n{output}"
        for text in exp.breaker_tx:
            assert text in breaker_tx, f"{exp.name}: missing breaker tx text {text!r}\n{output}"
        assert _red(exp.victim_query) in victim_tx, f"{exp.name}: victim SQL is not highlighted\n{output}"
        assert _red(exp.breaker_query) in breaker_tx, f"{exp.name}: breaker SQL is not highlighted\n{output}"
        if exp.commit_row:
            assert "COMMIT" in victim_tx, f"{exp.name}\n{output}"
            assert _red("COMMIT") not in victim_tx, f"{exp.name}\n{output}"

    def _basic(self, client, workload: WorkloadTli) -> _Expect:
        table = workload.get_table_path("basic")
        self._create_table(client, table, [(1, "Init")])
        victim = f"SELECT * FROM `{table}` WHERE Key = 1u"
        breaker = f'UPSERT INTO `{table}` (Key, Value) VALUES (1u, "BreakerValue")'
        commit = f'UPSERT INTO `{table}` (Key, Value) VALUES (1u, "VictimValue")'
        victim_id = self._read_then_broken_commit(client, workload, "Basic", victim, breaker, commit)
        return _Expect("Basic", victim_id, victim, breaker, victim_tx=(commit,))

    def _cross_tables(self, client, workload: WorkloadTli) -> _Expect:
        table1 = workload.get_table_path("cross1")
        table2 = workload.get_table_path("cross2")
        self._create_table(client, table1, [(1, "Init")])
        self._create_table(client, table2, [(1, "Init")])
        victim = f"SELECT * FROM `{table1}` WHERE Key = 1u"
        breaker = f'UPSERT INTO `{table1}` (Key, Value) VALUES (1u, "Breaker")'
        commit = f'UPSERT INTO `{table2}` (Key, Value) VALUES (1u, "DstVal")'
        victim_id = self._read_then_broken_commit(client, workload, "CrossTables", victim, breaker, commit)
        return _Expect("CrossTables", victim_id, victim, breaker, victim_tx=(commit,))

    def _read_then_broken_commit(self, client, workload, scenario, victim, breaker, commit) -> int:
        captured = {}

        def run(session):
            with session.transaction() as tx:
                tx.begin()
                _exec(workload, tx, victim)
                client.query(breaker, False)
                captured["id"] = _expect_aborted(
                    workload, scenario, lambda: _exec(workload, tx, commit, commit=True))

        self._on_session(client, run)
        return captured["id"]

    def _concurrent_upsert_select(self, client, workload: WorkloadTli) -> _Expect:
        table = workload.get_table_path("upsert_select")
        self._create_table(client, table, [(key, f"Initial{key}") for key in range(1, 11)])
        victim = (
            f"UPSERT INTO `{table}` (Key, Value) "
            f'SELECT Key, "VictimModified" AS Value FROM `{table}` '
            "WHERE Key >= 1u AND Key <= 5u"
        )
        breaker = f'UPSERT INTO `{table}` (Key, Value) VALUES (3u, "BreakerValue")'
        captured = {}

        def run(session):
            with session.transaction() as tx:
                tx.begin()
                _exec(workload, tx, victim)
                client.query(breaker, False)
                captured["id"] = _expect_aborted(
                    workload, "ConcurrentUpsertSelect", lambda: tx.commit())

        self._on_session(client, run)
        return _Expect(
            "ConcurrentUpsertSelect", captured["id"], victim, breaker, commit_row=True)

    def _victim_read_then_write(self, client, workload: WorkloadTli) -> _Expect:
        table = workload.get_table_path("read_write")
        self._create_table(client, table, [(1, "Init"), (2, "Init2")])
        victim = f"SELECT * FROM `{table}` WHERE Key = 1u"
        write = f'UPSERT INTO `{table}` (Key, Value) VALUES (2u, "VictimWrite")'
        breaker = f'UPSERT INTO `{table}` (Key, Value) VALUES (1u, "BreakerValue")'
        captured = {}

        def run(session):
            with session.transaction() as tx:
                tx.begin()
                _exec(workload, tx, victim)
                _exec(workload, tx, write)
                client.query(breaker, False)
                captured["id"] = _expect_aborted(
                    workload, "VictimReadThenWriteSameTable", lambda: tx.commit())

        self._on_session(client, run)
        return _Expect(
            "VictimReadThenWriteSameTable", captured["id"], victim, breaker, victim_tx=(write,))

    def _many_upserts(self, client, workload: WorkloadTli) -> _Expect:
        tables = [workload.get_table_path(f"mu{i}") for i in range(1, 7)]
        for index, path in enumerate(tables, start=1):
            self._create_table(client, path, [(1, f"Init{index}")])
        select1 = f"SELECT * FROM `{tables[0]}` WHERE Key = 1u"
        select2 = f"SELECT * FROM `{tables[1]}` WHERE Key = 1u"
        select3 = f"SELECT * FROM `{tables[2]}` WHERE Key = 1u"
        update4 = f'UPDATE `{tables[3]}` SET Value = "VictimUpdate" WHERE Key = 1u'
        update5 = f'UPDATE `{tables[4]}` SET Value = "BreakerUpdate5" WHERE Key = 1u'
        update2 = f'UPDATE `{tables[1]}` SET Value = "BreakerUpdate2" WHERE Key = 1u'
        update6 = f'UPDATE `{tables[5]}` SET Value = "BreakerUpdate6" WHERE Key = 1u'
        captured = {}

        def run(session):
            with session.transaction() as tx:
                tx.begin()
                for query in (select1, select2, select3, update4):
                    _exec(workload, tx, query)
                self._on_session(client, lambda breaker_session: self._commit_queries(
                    workload, breaker_session, (update5, update2, update6)))
                captured["id"] = _expect_aborted(workload, "ManyUpserts", lambda: tx.commit())

        self._on_session(client, run)
        return _Expect(
            "ManyUpserts",
            captured["id"],
            select2,
            update2,
            victim_tx=(select1, select3, update4),
            breaker_tx=(update5, update6),
        )

    def _two_victims_one_breaker(self, client, workload: WorkloadTli):
        table1 = workload.get_table_path("tv1")
        table2 = workload.get_table_path("tv2")
        self._create_table(client, table1, [(1, "Init")])
        self._create_table(client, table2, [(1, "Init")])
        select1 = f"SELECT * FROM `{table1}` WHERE Key = 1u"
        select2 = f"SELECT * FROM `{table2}` WHERE Key = 1u"
        update1 = f'UPDATE `{table1}` SET Value = "BreakerA" WHERE Key = 1u'
        update2 = f'UPDATE `{table2}` SET Value = "BreakerB" WHERE Key = 1u'
        commit1 = f'UPSERT INTO `{table1}` (Key, Value) VALUES (1u, "VictimA")'
        commit2 = f'UPSERT INTO `{table2}` (Key, Value) VALUES (1u, "VictimB")'
        captured = {}

        def run(_session):
            with client.session_pool.checkout() as session1, client.session_pool.checkout() as session2:
                with session1.transaction() as tx1, session2.transaction() as tx2:
                    tx1.begin()
                    tx2.begin()
                    _exec(workload, tx1, select1)
                    _exec(workload, tx2, select2)
                    self._on_session(client, lambda breaker_session: self._commit_queries(
                        workload, breaker_session, (update1, update2)))
                    captured["id1"] = _expect_aborted(
                        workload, "TwoVictimsOneBreaker1", lambda: _exec(workload, tx1, commit1, commit=True))
                    captured["id2"] = _expect_aborted(
                        workload, "TwoVictimsOneBreaker2", lambda: _exec(workload, tx2, commit2, commit=True))

        self._on_session(client, run)
        return (
            _Expect("TwoVictims1", captured["id1"], select1, update1, victim_tx=(commit1,), breaker_tx=(update2,)),
            _Expect("TwoVictims2", captured["id2"], select2, update2, victim_tx=(commit2,), breaker_tx=(update1,)),
        )

    def _breaker_and_victim(self, client, workload: WorkloadTli) -> _Expect:
        table1 = workload.get_table_path("role1")
        table2 = workload.get_table_path("role2")
        table3 = workload.get_table_path("role3")
        for path in (table1, table2, table3):
            self._create_table(client, path, [(1, "Init")])
        victim_select = f"SELECT * FROM `{table1}` WHERE Key = 1u"
        victim_update = f'UPDATE `{table3}` SET Value = "VictimOfTWrite" WHERE Key = 1u'
        t_select = f"SELECT * FROM `{table2}` WHERE Key = 1u"
        t_write = f'UPSERT INTO `{table1}` (Key, Value) VALUES (1u, "TWrite1")'
        external = f'UPSERT INTO `{table2}` (Key, Value) VALUES (1u, "ExtBreaker")'
        captured = {}

        def run(_session):
            with client.session_pool.checkout() as victim_session, client.session_pool.checkout() as t_session:
                with victim_session.transaction() as vtx, t_session.transaction() as ttx:
                    vtx.begin()
                    ttx.begin()
                    _exec(workload, vtx, victim_select)
                    _exec(workload, vtx, victim_update)
                    _exec(workload, ttx, t_select)
                    _exec(workload, ttx, t_write)
                    client.query(external, False)
                    captured["id"] = _expect_aborted(
                        workload, "BreakerAndVictimInSameTransaction", lambda: ttx.commit())

        self._on_session(client, run)
        return _Expect(
            "BreakerAndVictimInSameTransaction",
            captured["id"],
            t_select,
            external,
            victim_tx=(t_write,),
        )

    @staticmethod
    def _commit_queries(workload: WorkloadTli, session, queries):
        with session.transaction() as tx:
            tx.begin()
            for index, query in enumerate(queries):
                _exec(workload, tx, query, commit=index == len(queries) - 1)
