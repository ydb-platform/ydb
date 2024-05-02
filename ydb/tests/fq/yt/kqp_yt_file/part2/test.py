import yatest

from kqp_yt_file import run_test
from utils import pytest_generate_tests_for_part
from yql_utils import pytest_get_current_part


def pytest_generate_tests(metafunc):
    current_part, part_count = pytest_get_current_part(yatest.common.source_path(__file__))
    return pytest_generate_tests_for_part(metafunc, current_part, part_count)


def test(suite, case, cfg):
    return run_test(suite, case, cfg)
