# -*- coding: utf-8 -*-
import pytest

from test_base import BaseSuiteRunner, get_test_suites


"""
 This module is a specific runner of postgres regression tests. Test suites for this
 module are located at the directory postgres.
 All tests suites are based on the suites from the following repository:
 https://github.com/postgres/postgres/tree/master/src/test/regress
"""


class TestPGSQL(BaseSuiteRunner):
    def execute_assert(self, left, right, message):
        assert left == right, message

    @pytest.mark.parametrize(['kind', 'suite'], get_test_suites("postgres"))
    def test_sql_suite(self, kind, suite):
        return self.run_sql_suite(kind, "postgres", suite)
