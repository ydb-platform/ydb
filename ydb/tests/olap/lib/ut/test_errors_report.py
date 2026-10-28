import os

import threading
import yaml

import pytest

from ydb.tests.olap.lib import errors_report
from ydb.tests.olap.lib.errors_report import write_errors_yaml
from ydb.tests.olap.lib.workload_result import ErrorArea, ErrorPriority, WorkloadError


@pytest.fixture(autouse=True)
def _stub_report_info(monkeypatch, tmp_path):
    monkeypatch.setattr(errors_report, 'get_environment_info', lambda: {'db': 'test-db'})
    monkeypatch.setattr(errors_report, 'get_test_info',
                        lambda suite, test, start_time, end_time, refference_set='': {'time': '0s'})


def _error(msg: str, priority=ErrorPriority.ERROR, area=ErrorArea.OTHER) -> WorkloadError:
    return WorkloadError(msg, priority=priority, area=area)


def _read(fn):
    with open(fn, 'r') as f:
        return yaml.safe_load(f)


def test_write_and_merge(tmp_path):
    fn = str(tmp_path / 'errors.yaml')
    write_errors_yaml(fn, 'Suite1', 'Query01', [_error('e1', area=ErrorArea.REQUEST)], 0, 10)
    write_errors_yaml(fn, 'Suite1', 'Query02', [_error('e2', area=ErrorArea.DIFF)], 0, 10)

    data = _read(fn)
    assert data['environment'] == {'db': 'test-db'}
    assert data['errors_by_tests']['Suite1.Query01']['errors'] == [
        {'priority': 'ERROR', 'area': 'REQUEST', 'message': 'e1'}
    ]
    assert data['errors_by_tests']['Suite1.Query02']['errors'][0]['area'] == 'DIFF'


def test_same_key_overwrites(tmp_path):
    fn = str(tmp_path / 'errors.yaml')
    write_errors_yaml(fn, 'Suite1', 'Query01', [_error('e1')], 0, 10)
    write_errors_yaml(fn, 'Suite1', 'Query01', [_error('e2')], 0, 10)
    data = _read(fn)
    assert len(data['errors_by_tests']) == 1
    assert data['errors_by_tests']['Suite1.Query01']['errors'][0]['message'] == 'e2'


def test_no_tmp_file_left(tmp_path):
    fn = str(tmp_path / 'errors.yaml')
    write_errors_yaml(fn, 'Suite1', 'Query01', [_error('e1')], 0, 10)
    assert not os.path.exists(fn + '_')
    assert sorted(os.listdir(str(tmp_path))) == ['errors.yaml', 'errors.yaml.lock']


def test_corrupted_existing_file_starts_fresh(tmp_path):
    fn = str(tmp_path / 'errors.yaml')
    with open(fn, 'w') as f:
        f.write('not: [valid yaml')
    write_errors_yaml(fn, 'Suite1', 'Query01', [_error('e1')], 0, 10)
    data = _read(fn)
    # Поврежденные старые данные отброшены, новые ошибки записаны
    assert list(data['errors_by_tests']) == ['Suite1.Query01']


def test_non_dict_existing_file_starts_fresh(tmp_path):
    fn = str(tmp_path / 'errors.yaml')
    with open(fn, 'w') as f:
        yaml.safe_dump(['old', 'format'], f)
    write_errors_yaml(fn, 'Suite1', 'Query01', [_error('e1')], 0, 10)
    data = _read(fn)
    assert list(data['errors_by_tests']) == ['Suite1.Query01']


def test_concurrent_writers(tmp_path):
    fn = str(tmp_path / 'errors.yaml')
    suites = [f'Suite{i}' for i in range(4)]

    def writer(suite):
        write_errors_yaml(fn, suite, 'Query01', [_error(f'e-{suite}')], 0, 10)

    threads = [threading.Thread(target=writer, args=(s,)) for s in suites]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    data = _read(fn)
    assert sorted(data['errors_by_tests']) == sorted(f'{s}.Query01' for s in suites)
    assert not os.path.exists(fn + '_')
