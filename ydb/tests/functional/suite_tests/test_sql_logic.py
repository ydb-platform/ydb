# -*- coding: utf-8 -*-
import os
import sqlite3

import pytest
from hamcrest import assert_that, raises

from test_base import BaseSuiteRunner, get_token, get_test_suites, safe_execute, get_statement_and_side_effects

"""
 This module is a specific runner of sqllogic tests. Test suites for this
 module are located at the directory sqllogictest.
 All tests suites are based on the suites from the following repository:
 https://github.com/gregrahn/sqllogictest
"""


class TestSQLLogic(BaseSuiteRunner):
    check_new_engine_query_results = True

    def execute_assert(self, left, right, message):
        assert left == right, message

    @pytest.mark.parametrize(['kind', 'suite'], get_test_suites("sqllogictest"))
    def test_sql_suite(self, kind, suite):
        return self.run_sql_suite(kind, "sqllogictest", suite)

    def setup_method(self, method=None):
        self.sqlitedbname = "%s.db" % get_token()
        self.sqlite_connection = sqlite3.connect(self.sqlitedbname, detect_types=sqlite3.PARSE_COLNAMES)

    def teardown_method(self, method=None):
        self.sqlite_connection.close()
        os.remove(self.sqlitedbname)

    def assert_statement_ok(self, statement):
        super(TestSQLLogic, self).assert_statement_ok(statement)
        safe_execute(lambda: self.__execute_sqlitedb(statement.text), statement)

    def assert_statement_error(self, statement):
        statement_text, side_effects = get_statement_and_side_effects(statement.text)
        assert_that(lambda: self.__execute_sqlitedb(statement_text), raises(sqlite3.Error), str(statement))
        super(TestSQLLogic, self).assert_statement_error(statement)

    def get_query_and_output(self, statement_text):
        return statement_text, self.__execute_sqlitedb(statement_text, query=True)

    def __execute_sqlitedb(self, statement_text, query=False):
        cursor = self.sqlite_connection.cursor()
        cursor.execute(statement_text)
        self.sqlite_connection.commit()
        if query:
            list_of_rows = cursor.fetchall()
            names = list(map(lambda x: x[0], cursor.description))
            return [
                {name: value for name, value in zip(names, row)}
                for row in list_of_rows
            ]
        return None
