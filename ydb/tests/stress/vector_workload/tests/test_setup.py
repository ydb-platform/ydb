import json
import subprocess
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ydb.tests.stress.vector_workload.workload import YdbVectorWorkload


def test_partial_stats_do_not_start_build():
    workload = SimpleNamespace(database='/Root', table_name='vectors', get_cli_prefix=lambda: ['ydb'])
    replies = [
        SimpleNamespace(stdout=json.dumps({'tableStats': {'rowsEstimate': rows}}))
        for rows in (0, 20000, 100000)
    ]
    with patch.object(subprocess, 'run', side_effect=replies) as run, patch.object(time, 'sleep'):
        YdbVectorWorkload._wait_for_table_stats(workload, expected_rows=100000)
    assert run.call_count == 3
    assert run.call_args.args[0] == [
        'ydb', 'scheme', 'describe', '/Root/vectors', '--stats', '--format', 'proto-json-base64',
    ]


def test_missing_stats_fail_instead_of_building():
    workload = SimpleNamespace(database='/Root', table_name='vectors', get_cli_prefix=lambda: ['ydb'])
    with patch.object(time, 'monotonic', side_effect=[0, 181]):
        with pytest.raises(TimeoutError, match='Refusing to build'):
            YdbVectorWorkload._wait_for_table_stats(workload, expected_rows=100000)


def test_import_waits_before_build():
    events = []
    workload = SimpleNamespace(
        rows='100000',
        get_command_prefix=lambda subcmds: subcmds,
        cmd_run=lambda args: events.append(args[0]),
        cmd_run_with_retry=lambda args: events.append('build'),
        _build_index_subcmds=lambda: ['build-index'],
        _wait_for_table_stats=lambda expected_rows: events.append(('stats', expected_rows)),
    )
    YdbVectorWorkload._import_data(workload)
    assert events == ['init', 'import', ('stats', 100000), 'build']


def test_timeout_applies_to_both_runs():
    workload = SimpleNamespace(
        threads='50', targets='1000', client_timeout='30s', table_name='vectors',
        mode='generate', query_table_name='queries',
    )
    for duration in ('10', '20'):
        args = YdbVectorWorkload._get_select_subcmds(workload, duration)
        assert args[args.index('--client-timeout') + 1] == '30s'
