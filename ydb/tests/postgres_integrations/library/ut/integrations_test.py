from os import path

import pytest
import yatest

from ydb.tests.postgres_integrations.library import pytest_integration
from ydb.tests.postgres_integrations.library.pytest_integration import TestCase, TestState

TEST_DATA_FOLDER=yatest.common.source_path("ydb/tests/postgres_integrations/library/ut/data")

@pytest.mark.parametrize(
    "test",
    [
        TestCase(name="o/OK", state=TestState.PASSED, log=""),
        TestCase(name="f/failed1", state=TestState.FAILED, log="fail mess"),
        TestCase(name="f/failed2", state=TestState.FAILED, log="Failed\nescaped error"),
        TestCase(name="f/error1", state=TestState.FAILED, log="No test result found\npanic and timeout"),
        TestCase(name="s/skipped1", state=TestState.SKIPPED, log="Skipped\nskip message"),
        TestCase(name="s/skipped2", state=TestState.SKIPPED, log="escaped skip message"),
    ],
    ids=lambda item: item.name
)
def test_read_jtest_results(test):
    filepath = path.join(TEST_DATA_FOLDER, "test-results-example.xml")
    parsed_result = pytest_integration._read_tests_result(filepath)

    parsed_test = parsed_result[test.name]
    assert test == parsed_test

def test_read_jtest_with_one_result():
    filepath = path.join(TEST_DATA_FOLDER, "test-results-example1.xml")
    parsed_result = pytest_integration._read_tests_result(filepath)
    parsed_test = parsed_result["f/test-failed"]
    assert parsed_test == TestCase(name="f/test-failed", state=TestState.FAILED, log="Failed\nfailed-mess")
