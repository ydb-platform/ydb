import ydb


class YdbClient:
    def __init__(self, endpoint, database):
        self.driver = ydb.Driver(endpoint=endpoint, database=database, oauth=None)
        self.database = database
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
