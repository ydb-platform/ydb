from itertools import count
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ydb.core.protos.statistics_pb2 import TTableSummaryStatistics
from ydb.tests.stress.statistics_workload.workload import Workload


@pytest.mark.parametrize("rows,expected", [([], None), ([[None]], None),
                                           ([[b""]], None),
                                           ([[TTableSummaryStatistics(RowCount=0).SerializeToString()]], 0),
                                           ([[TTableSummaryStatistics(RowCount=3000).SerializeToString()]], 3000)])
def test_analyzed_row_count_reads_full_summary(rows, expected):
    workload = object.__new__(Workload)
    workload.driver = Mock()
    workload.driver.table_client.scan_query.return_value = iter([
        SimpleNamespace(result_set=SimpleNamespace(rows=[])),
        SimpleNamespace(result_set=SimpleNamespace(rows=rows)),
    ])
    assert workload.analyzed_row_count(".metadata/statistics_v2", 42, 33) == expected
    query = workload.driver.table_client.scan_query.call_args.args[0]
    assert "SELECT data " in query
    assert "owner_id = 42ul" in query
    assert "local_path_id = 33ul" in query
    assert "stat_type = 4u" in query
    assert "column_tags = ''" in query


def test_waits_for_exact_analyzed_count(monkeypatch):
    workload = object.__new__(Workload)
    workload.analyzed_row_count = Mock(side_effect=[None, 5000, 3000])
    monkeypatch.setattr("ydb.tests.stress.statistics_workload.workload.time.monotonic", count().__next__)
    monkeypatch.setattr("ydb.tests.stress.statistics_workload.workload.time.sleep", lambda _: None)
    assert workload.wait_for_analyzed_row_count("stats", 42, 33, 3000, "trace", timeout=5) == 3000
    assert workload.analyzed_row_count.call_count == 3


def test_incorrect_summary_does_not_pass(monkeypatch):
    workload = object.__new__(Workload)
    workload.analyzed_row_count = Mock(return_value=4113)
    monkeypatch.setattr("ydb.tests.stress.statistics_workload.workload.time.monotonic", count().__next__)
    monkeypatch.setattr("ydb.tests.stress.statistics_workload.workload.time.sleep", lambda _: None)
    with pytest.raises(Exception, match="did not become 3000.*last count: 4113"):
        workload.wait_for_analyzed_row_count("stats", 42, 33, 3000, "trace", timeout=3)
