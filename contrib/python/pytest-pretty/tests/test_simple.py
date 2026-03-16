pytest_plugins = 'pytester'


test_str = """
import pytest

def test_add():
    assert 1 + 2 == 3


@pytest.mark.xfail(reason='This test is expected to fail')
def test_subtract():
    assert 2 - 1 == 0


@pytest.mark.skipif(True, reason='This test is skipped')
def test_multiply():
    assert 2 * 2 == 5
"""


def test_base(pytester):
    pytester.makepyfile(test_str)
    result = pytester.runpytest()
    assert any('passed' in x and '1' in x for x in result.outlines)
    assert any('skipped' in x and '1' in x for x in result.outlines)
    assert any('xfailed' in x and '1' in x for x in result.outlines)


def test_fixture(pytester_pretty):
    pytester_pretty.makepyfile(test_str)
    result = pytester_pretty.runpytest()
    result.assert_outcomes(passed=1, skipped=1, xfailed=1)
