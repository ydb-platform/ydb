import yatest.common
import os
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


def ydb_bin():
    if os.getenv('YDB_CLI_BINARY'):
        return yatest.common.binary_path(os.getenv('YDB_CLI_BINARY'))
    raise RuntimeError('YDB_CLI_BINARY enviroment variable is not specified')


def run_cli(argv):
    return yatest.common.execute(
        [
            ydb_bin(),
            '--endpoint',
            YdbCluster.ydb_endpoint,
            '--database',
            '/' + YdbCluster.ydb_database,
        ] + argv
    )
