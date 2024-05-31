from typing import List
from math import isclose


def assert_rows_equal(expected: List, actual: List):
    assert len(expected) == len(actual)

    for i in range(len(expected)):
        if type(expected[i]) is float:
            assert isclose(expected[i], actual[i], abs_tol=1e-5)
            continue

        assert expected[i] == actual[i], (f"Error at position {i}", expected, actual)


def assert_data_outs_equal(
    expected: List,
    actual: List,
):
    assert len(expected) == len(actual)
    all(map(assert_rows_equal, expected, actual))
