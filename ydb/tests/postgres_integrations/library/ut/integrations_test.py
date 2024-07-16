from os import path

import yatest

from ydb.tests.postgres_integrations.library import pytest_integration

TEST_DATA_FOLDER=yatest.common.source_path("ydb/tests/postgres_integrations/library/ut/data")

def test_read_jtest_results():
    filepath = path.join(TEST_DATA_FOLDER, "test-results-example.xml")
    parsed_result = pytest_integration._read_tests_result(filepath)

    expected_result = {
        "o/OK": pytest_integration.TestCase(name="o/OK", state=pytest_integration.TestState.PASSED, log=""),
    }

    assert expected_result == parsed_result
