from __future__ import annotations
from typing import Any

import fcntl
import logging
import os
import yaml

from ydb.tests.olap.lib.allure_utils import get_environment_info, get_test_info
from ydb.tests.olap.lib.workload_result import WorkloadError


def write_errors_yaml(fn: str, suite: str, query_name: str, errors: list[WorkloadError],
                      start_time: float, end_time: float) -> None:
    """Дописывает ошибки теста {suite}.{query_name} в файл отчета fn.

    Файл может обновляться несколькими процессами, поэтому read-modify-write
    целиком делается под блокировкой. Нечитаемый или поврежденный существующий
    файл игнорируется - начинаем с пустого содержимого.
    """
    environment_info = get_environment_info()
    tmp_fn = fn + '_'
    with open(f'{fn}.lock', 'w') as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            data: dict[str, Any] = {}
            if os.path.exists(fn):
                with open(fn, 'r') as f:
                    try:
                        data = yaml.safe_load(f)
                    except Exception:
                        logging.warning(f'Failed to parse {fn}, starting fresh')
                        data = {}
                    if not isinstance(data, dict):
                        data = {}
            errors_by_tests = data.get('errors_by_tests')
            if not isinstance(errors_by_tests, dict):
                errors_by_tests = {}
            errors_by_tests[f'{suite}.{query_name}'] = {
                **get_test_info(suite, query_name, start_time, end_time),
                'errors': [e.serialize() for e in errors],
            }
            data['environment'] = environment_info
            data['errors_by_tests'] = errors_by_tests
            with open(tmp_fn, 'w') as f:
                yaml.safe_dump(data, f, allow_unicode=True)
            os.replace(tmp_fn, fn)
        finally:
            if os.path.exists(tmp_fn):
                os.remove(tmp_fn)
