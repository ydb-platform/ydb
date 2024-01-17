from typing import List, Any
from math import isclose


def values_equal(expected: Any, actual: Any) -> bool:
    if type(expected) is float:
        return isclose(expected, actual, abs_tol=1e-5)

    return expected == actual


def rows_equal(expected: List, actual: List) -> bool:
    if len(expected) != len(actual):
        return False

    return all(map(values_equal, expected, actual))


def data_outs_equal(expected: List, actual: List) -> bool:
    if len(expected) != len(actual):
        return False

    return all(map(rows_equal, expected, actual))
