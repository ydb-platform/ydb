import pytest

from common_file import run_test
from common_test_tools import pytest_generate_tests_for_part


def pytest_generate_tests(metafunc):
    return pytest_generate_tests_for_part(metafunc, 4, 10)


@pytest.mark.parametrize('what', ['Results', 'Plan', 'Debug', 'RunOnOpt', 'ForceBlocks'])
def test(suite, case, tmpdir, what):
    return run_test('yt', lambda s: s, suite, case, tmpdir, what)
