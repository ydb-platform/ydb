import pytest

from sqlparse import utils


@pytest.mark.parametrize('value, expected', (
    [None, None],
    ['\'foo\'', 'foo'],
    ['"foo"', 'foo'],
    ['`foo`', 'foo']))
def test_remove_quotes(value, expected):
    assert utils.remove_quotes(value) == expected
