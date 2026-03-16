from __future__ import absolute_import, unicode_literals

import posixpath

import ydb
from .cursor import Cursor
from .errors import DatabaseError


class Connection(object):

    deiver = None
    pool = None

    def __init__(self, endpoint, database=None, **conn_kwargs):
        self.endpoint = endpoint
        self.database = database
        self._conn_kwargs = conn_kwargs
        driver, pool = self._create_driver(self.endpoint, self.database, **conn_kwargs)
        self.driver = driver
        self.pool = pool

    def cursor(self):
        return Cursor(self)

    def execute(self, sql, parameters=None):
        return self.cursor().execute(sql, parameters)

    def executemany(self, sql, parameters):
        return self.cursor().executemany(sql, parameters)

    def describe(self, table_path):
        full_path = posixpath.join(self.database, table_path)
        try:
            res = self.pool.retry_operation_sync(
                lambda cli: cli.describe_table(full_path)
            )
            return res.columns
        except ydb.Error as e:
            raise DatabaseError(e.message, e.issues, e.status)

        except Exception:
            raise DatabaseError("Failed to describe table %r" % (table_path,))

    def check_exists(self, table_path):
        try:
            self.driver.scheme_client.describe_path(table_path)
            return True
        except ydb.SchemeError:
            return False

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        if self.pool is not None:
            self.pool.stop()
        if self.driver is not None:
            self.driver.stop()

    @staticmethod
    def _create_endpoint(host, port):
        return "%s:%d" % (host, port)

    @staticmethod
    def _create_driver(endpoint, database, **conn_kwargs):
        driver_config = ydb.DriverConfig(
            endpoint,
            database=database,
            table_client_settings=ydb.TableClientSettings()
            .with_native_date_in_result_sets(True)
            .with_native_datetime_in_result_sets(True)
            .with_native_json_in_result_sets(True),
            **conn_kwargs
        )
        driver = ydb.Driver(driver_config)
        try:
            driver.wait(timeout=5, fail_fast=True)
        except ydb.Error as e:
            raise DatabaseError(e.message, e.issues, e.status)

        except Exception:
            driver.stop()
            raise DatabaseError(
                "Failed to connect to YDB, details %s"
                % driver.discovery_debug_details()
            )

        return driver, ydb.SessionPool(driver)
