import pytest

from yt_file import run_test
from utils import pytest_generate_tests_for_part


def pytest_generate_tests(metafunc):
    return pytest_generate_tests_for_part(metafunc, 8, 10)


@pytest.mark.parametrize('what', ['Results', 'Plan', 'Debug', 'RunOnOpt', 'Peephole', 'Lineage', 'ForceBlocks'])
def test(suite, case, cfg, tmpdir, what, yql_http_file_server):
    return run_test(suite, case, cfg, tmpdir, what, yql_http_file_server)
