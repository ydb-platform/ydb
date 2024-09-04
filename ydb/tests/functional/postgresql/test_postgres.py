from ydb.tests.library.common import yatest_common
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels

from common import find_sql_tests, diff_sql

import yatest.common

import os
import pytest
import re


arcadia_root = yatest.common.source_path('')
DATA_PATH = os.path.join(arcadia_root, yatest.common.test_source_path('cases'))


def get_unique_path_case(sub_folder, file):
    test_name = yatest_common.context.test_name or ""
    test_name = test_name.replace(':', '_')
    lb, rb = re.escape('['), re.escape(']')
    test_case = re.search(lb + '(.+?)' + rb, test_name)
    assert test_case
    dirpath = os.path.join(yatest_common.output_path(), test_case.group(1), sub_folder)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath, exist_ok=True)
    return os.path.join(dirpath, file)


def get_tests():
    _, tests = zip(*find_sql_tests(DATA_PATH))
    return tests


def get_ids():
    ids, _ = zip(*find_sql_tests(DATA_PATH))
    return ids


def psql_binary_path():
    return yatest_common.binary_path('ydb/tests/functional/postgresql/psql/psql')


def execute_binary(binary_name, cmd, stdin_string=None):
    stdout = get_unique_path_case('psql', 'stdout')

    with open(stdout, 'w') as stdout_file:
        yatest.common.execute(
            cmd,
            stdout=stdout_file,
            stderr=stdout_file,
            wait=True
        )

    return stdout


class BasePostgresTest(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(KikimrConfigGenerator(
            additional_log_configs={
                'LOCAL_PGWIRE': LogLevels.DEBUG,
                'KQP_YQL': LogLevels.DEBUG,
                'KQP_COMPILE_ACTOR': LogLevels.DEBUG,
                'KQP_COMPILE_REQUEST': LogLevels.DEBUG,
                'KQP_PROXY': LogLevels.DEBUG
            },
            extra_feature_flags=['enable_table_pg_types', 'enable_temp_tables'],
        ))
        cls.cluster.start()

        cls.pgport = cls.cluster.nodes[1].pgwire_port

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()


class TestPostgresSuite(BasePostgresTest):
    @pytest.mark.parametrize(['sql', 'out'], get_tests(), ids=get_ids())
    def test_postgres_suite(self, sql, out):
        stdout_file = execute_binary(
            'psql',
            [psql_binary_path(), 'postgresql://root:1234@localhost:{}/Root'.format(self.pgport), '-w', '-a', '-f', sql]
        )

        with open(stdout_file, 'rb') as stdout_data:
            diff_sql(stdout_data.read(), sql, out)
