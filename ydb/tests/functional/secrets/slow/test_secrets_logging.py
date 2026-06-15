# -*- coding: utf-8 -*-
import time

import pytest

from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb

CLUSTER_CONFIG = dict(
    default_log_level=LogLevels.TRACE,  # It slows down tests significantly, but it's necessary for this particular test
    use_log_files=True,
)


def _collect_ydbd_log_paths(ydb_cluster):
    paths = []
    for group in (ydb_cluster.nodes.values(), ydb_cluster.slots.values()):
        for proc in group:
            path = proc.ydbd_log_file_path
            if path:
                paths.append(path)
    return paths


def _read_all_logs_combined(log_paths):
    chunks = []
    for log_path in log_paths:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as fh:
            chunks.append(fh.read())
    return '\n'.join(chunks)


def _wait_for_log_marker(substring, log_paths, timeout_seconds=30.0, step_seconds=1.0):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if substring in _read_all_logs_combined(log_paths):
            return
        time.sleep(step_seconds)
    pytest.fail(f'Log marker {substring!r} not found after {timeout_seconds}s')


def _assert_secret_value_not_in_logs(log_paths, secret_value):
    for log_path in log_paths:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as fh:
            for lineno, line in enumerate(fh, 1):
                if secret_value not in line:
                    continue
                text = line.rstrip('\n\r')
                pytest.fail(
                    f'Secret value appeared in a log file {log_path}:{lineno}: {text!r}'
                )


def test_secret_value_from_params_not_in_trace_logs(db_fixture, ydb_cluster):
    secret_value = 'secret-value-rand-value-mf83ezDr7'
    secret_path = 'x'

    # run all DDL SQL commands for secrets
    query = f"""
        DECLARE $value AS Utf8;
        CREATE SECRET `{secret_path}` WITH (value = $value);
        ALTER SECRET `{secret_path}` WITH (value = $value);
        DROP SECRET `{secret_path}`;
    """
    parameters = {'$value': (secret_value, ydb.PrimitiveType.Utf8.proto)}
    with ydb.Driver(db_fixture) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries(query, parameters=parameters)

    # wait until logging is done
    log_paths = _collect_ydbd_log_paths(ydb_cluster)
    assert log_paths, 'Log files are missing'

    _wait_for_log_marker('DROP SECRET', log_paths)
    time.sleep(2.0)

    # assert that the secret value is not in the logs
    _assert_secret_value_not_in_logs(log_paths, secret_value)
