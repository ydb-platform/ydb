import ydb
from typing import List

from config import ydb_configuration
from exception import ConnectionFailure


class YDBClient:
    def __init__(self):
        self.config = ydb_configuration
        self.driver = self.create_driver()

    def create_driver(self) -> ydb.Driver:
        driver_config = ydb.DriverConfig(
            self.config.endpoint,
            self.config.database,
            credentials=ydb.construct_credentials_from_environ(),
            root_certificates=ydb.load_ydb_root_certificate(),
        )

        driver = ydb.Driver(driver_config)

        try:
            driver.wait(timeout=5)
        except Exception:
            raise ConnectionFailure(driver.discovery_debug_details())

        return driver

    @property
    def table_client(self) -> ydb.TableClient:
        return self.driver.table_client

    def bulk_upsert(self, rows: List, column_types: ydb.BulkUpsertColumns):
        self.table_client.bulk_upsert(self.config.full_path, rows, column_types)


# Serverless functions can restore context, thus if we connect once we can use same client
# variable in next calls of function.
ydb_client = YDBClient()
