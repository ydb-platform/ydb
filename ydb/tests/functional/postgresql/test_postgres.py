from ydb.tests.library.common import yatest_common
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels

from common import find_sql_tests, diff_sql

from yatest.common import execute

import os
import pytest
import time
import re


DATA_PATH = yatest_common.source_path('ydb/tests/functional/postgresql/cases')


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
    if os.getenv('PSQL_BINARY'):
        return yatest_common.binary_path(os.getenv('PSQL_BINARY'))
    else:
        return yatest_common.work_path('psql/psql')


def pgwire_binary_path():
    return yatest_common.binary_path('ydb/apps/pgwire/pgwire')


def execute_binary(binary_name, cmd, wait, join_stderr=False):
    stdin, stderr, stdout = map(
        lambda x: get_unique_path_case(binary_name, x),
        ['stdin', 'stderr', 'stdout']
    )
    stdin_file = open(stdin, 'w')
    stdout_file = open(stdout, 'w')
    stderr_file = stdout_file
    if not join_stderr:
        stderr_file = open(stderr, 'w')
    process = execute(
        cmd,
        stdin=stdin_file,
        stderr=stderr_file,
        stdout=stdout_file,
        wait=wait
    )
    return process, stdin, stderr, stdout


class BasePostgresTest(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(KikimrConfigGenerator(
            additional_log_configs={'KQP_YQL': LogLevels.DEBUG, 'KQP_COMPILE_ACTOR': LogLevels.DEBUG, 'KQP_COMPILE_REQUEST': LogLevels.DEBUG}
        ))
        cls.cluster.start()
        cls.endpoint = '%s:%s' % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port)
        time.sleep(2)
        cls.pgwire, _, _, _ = execute_binary(
            'pgwire',
            [pgwire_binary_path(), '--endpoint={}'.format(cls.endpoint), '--stderr'],
            wait=False
        )
        time.sleep(2)

    @classmethod
    def teardown_class(cls):
        cls.pgwire.terminate()
        cls.cluster.stop()


class TestPostgresSuite(BasePostgresTest):
    @pytest.mark.parametrize(['sql', 'out'], get_tests(), ids=get_ids())
    def test_postgres_suite(self, sql, out):
        _, _, psql_stderr, psql_stdout = execute_binary(
            'psql',
            [psql_binary_path(), 'postgresql://root:@localhost:5432/Root', '-a', '-f', sql],
            wait=True,
            join_stderr=True
        )

        with open(psql_stdout, 'rb') as stdout_file:
            diff_sql(stdout_file.read(), sql, out)
