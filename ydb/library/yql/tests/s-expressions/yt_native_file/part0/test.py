import pytest
import yatest

from common_file import run_test
from common_test_tools import pytest_generate_tests_for_part
from yql_utils import pytest_get_current_part


def pytest_generate_tests(metafunc):
    current_part, part_count = pytest_get_current_part(yatest.common.source_path(__file__))
    return pytest_generate_tests_for_part(metafunc, current_part, part_count)


@pytest.mark.parametrize('what', ['Results', 'Plan', 'Debug', 'RunOnOpt', 'ForceBlocks'])
def test(suite, case, tmpdir, what):
    return run_test('yt', lambda s: s, suite, case, tmpdir, what)
