from typing import List, Optional

import ydb
import yatest.common


class YdbClient:
    def __init__(self, endpoint: str, database: str, ydb_cli_path: Optional[str] = None):
        self.endpoint = endpoint
        self.database = database
        self.ydb_cli_path = ydb_cli_path

        self.driver = ydb.Driver(endpoint=endpoint, database=database, oauth=None)
        self.session_pool = ydb.QuerySessionPool(self.driver)

    def stop(self):
        self.session_pool.stop()
        self.driver.stop()

    def wait_connection(self, timeout=5):
        self.driver.wait(timeout, fail_fast=True)

    def query(self, statement):
        return self.session_pool.execute_with_retries(statement)

    def bulk_upsert(self, table_path, column_types: ydb.BulkUpsertColumns, data_slice):
        self.driver.table_client.bulk_upsert(
            table_path,
            data_slice,
            column_types
        )

    def run_cli_comand(self, cmd: List[str]):
        assert self.ydb_cli_path is not None, "ydb CLI path is not specified"

        cmd = [self.ydb_cli_path, '-e', self.endpoint, '-d', self.database] + cmd
        process = yatest.common.process.execute(cmd, check_exit_code=False)
        if process.exit_code != 0:
            assert False, f'Command\n{cmd}\n finished with exit code {process.exit_code}, stderr:\n\n{process.std_err}\n\nstdout:\n{process.std_out}'
