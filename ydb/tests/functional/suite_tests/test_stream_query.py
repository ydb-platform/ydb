# -*- coding: utf-8 -*-
import pytest
from test_base import BaseSuiteRunner, get_test_suites


class TestStreamQuery(BaseSuiteRunner):
    def execute_assert(self, left, right, message):
        assert left == right, message

    @pytest.mark.parametrize(['kind', 'suite'], get_test_suites("stream_query"))
    def test_sql_suite(self, kind, suite):
        return self.run_sql_suite(kind, "stream_query", suite)
