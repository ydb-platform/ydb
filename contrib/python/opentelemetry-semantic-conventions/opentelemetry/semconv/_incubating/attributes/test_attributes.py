# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

TEST_CASE_NAME: Final = "test.case.name"
"""
The fully qualified human readable name of the [test case](https://wikipedia.org/wiki/Test_case).
"""

TEST_CASE_RESULT_STATUS: Final = "test.case.result.status"
"""
The status of the actual test case result from test execution.
"""

TEST_SUITE_NAME: Final = "test.suite.name"
"""
The human readable name of a [test suite](https://wikipedia.org/wiki/Test_suite).
"""

TEST_SUITE_RUN_STATUS: Final = "test.suite.run.status"
"""
The status of the test suite run.
"""


class TestCaseResultStatusValues(Enum):
    PASS = "pass"
    """pass."""
    FAIL = "fail"
    """fail."""


class TestSuiteRunStatusValues(Enum):
    SUCCESS = "success"
    """success."""
    FAILURE = "failure"
    """failure."""
    SKIPPED = "skipped"
    """skipped."""
    ABORTED = "aborted"
    """aborted."""
    TIMED_OUT = "timed_out"
    """timed_out."""
    IN_PROGRESS = "in_progress"
    """in_progress."""
