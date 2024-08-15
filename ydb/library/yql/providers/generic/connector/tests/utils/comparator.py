from math import isclose
from typing import List
import json

from ydb.library.yql.providers.generic.connector.tests.utils.schema import Schema


def is_json(value) -> bool:
    if not type(value) is str:
        return False

    try:
        json.loads(value)
    except ValueError:
        return False
    return True


def jsons_are_equal(lhs: str, rhs: str) -> bool:
    return json.loads(lhs) == json.loads(rhs)


def assert_rows_equal(expected: List, actual: List):
    assert len(expected) == len(actual), (f'Columns amount mismatch expected: {len(expected)} actual: {len(actual)}', expected, actual)

    for i in range(len(expected)):
        if type(expected[i]) is float:
            assert isclose(expected[i], actual[i], abs_tol=1e-5), (expected[i], actual[i])
            continue

        if is_json(expected[i]):
            assert jsons_are_equal(expected[i], actual[i]), (expected[i], actual[i])
            continue

        assert expected[i] == actual[i], (f"Error at position {i}", expected, actual)


def assert_data_outs_equal(
    expected: List,
    actual: List,
):
    assert len(expected) == len(actual)
    all(map(assert_rows_equal, expected, actual))


def assert_schemas_equal(expected: Schema, actual: Schema):
    assert len(expected.columns) == len(actual.columns)
    for i in range(len(expected.columns)):
        assert expected.columns[i] == actual.columns[i], (
            f"Error at position {i}",
            expected.columns[i],
            actual.columns[i],
        )
