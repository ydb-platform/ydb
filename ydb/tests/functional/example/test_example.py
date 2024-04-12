import pytest


def test_log_1():
    print("Example stdout string")
    assert 1 == 1


def test_log_2():
    print("Example stdout string 2")
    assert 1 == 1


def test_log_3():
    print("Example stdout string 3")
    assert 1 == 0


def test_flake8():
    very_long_variable_name_flake8_should_fail_on_this_line__very_long_variable_name_flake8_should_fail_on_this_line__very_long_variable_name_flake8_should_fail_on_this_line__ = 123

    assert 0 == 0


@pytest.mark.parametrize(['a', 'b', 'c'], [[0, 0, 0], [1, 1, 1], ["a", "b", "c"]])
def test_ptest(a, b, c):
    assert 0 == 0
