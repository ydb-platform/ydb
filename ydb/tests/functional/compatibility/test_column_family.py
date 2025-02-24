# -*- coding: utf-8 -*-
import yatest
import logging

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.oss.ydb_sdk_import import ydb


logger = logging.getLogger(__name__)


class TestColumnFamily(object):
    @classmethod
    def setup_class(cls):
        last_stable_path = yatest.common.binary_path("ydb/tests/library/compatibility/ydbd-last-stable")
        cls.config = KikimrConfigGenerator(
            erasure=Erasure.NONE,
            extra_feature_flags={"suppress_compatibility_check": True},
            column_shard_config={"disabled_on_scheme_shard": False},
            binary_paths=[last_stable_path],
            use_in_memory_pdisks=False,
        )
        cls.cluster = KiKiMR(cls.config)
        cls.database = cls.config.domain_name
        cls.cluster.start()
        cls.endpoint = "%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port)
        cls.driver = ydb.Driver(endpoint=cls.endpoint, database=cls.database, oauth=None)
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'driver'):
            cls.driver.stop()

        if hasattr(cls, 'cluster'):
            cls.cluster.stop(kill=True)

    def change_binary(self, binary_path: str):
        self.cluster.change_binary_and_restart(binary_path())
        self.driver.stop()
        endpoint = "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port)
        self.driver = ydb.Driver(endpoint=endpoint, database=self.database, oauth=None)
        self.driver.wait()

    def test_column_family_default(self):
        table_name: str = 'test_table'
        table_path: str = f'/{self.database}/{table_name}'

        with ydb.QuerySessionPool(self.driver, size=1) as session_pool:
            session_pool.execute_with_retries(
                f"CREATE TABLE `{table_path}` (id Uint64 NOT NULL, PRIMARY KEY(id)) WITH (STORE = COLUMN);"
            )
            logging.info(f"create table `{table_path}`")

            # Check "Column Family" does not exist in the version
            try:
                session_pool.execute_with_retries(
                    f"ALTER TABLE `{table_path}` ADD FAMILY default (COMPRESSION = 'lz4');"
                )
                assert False
            except ydb.issues.SchemeError as error:
                assert error.issues[0].message == "no data for update"

        self.change_binary(kikimr_driver_path())

        with ydb.QuerySessionPool(self.driver, size=1) as session_pool:
            # Try to add `default` column family in the table. "Default" must exist, so there must be an error.
            try:
                session_pool.execute_with_retries(
                    f"ALTER TABLE `{table_path}` ADD FAMILY default (COMPRESSION = 'zstd');"
                )
                assert False
            except ydb.issues.SchemeError as error:
                assert (
                    error.issues[0].message
                    == "schema update error: column family \'default\' already exists. in alter constructor STANDALONE_UPDATE"
                )

            # Try to alter `default` column family in the table.
            try:
                session_pool.execute_with_retries(
                    f"ALTER TABLE `{table_path}` ALTER FAMILY default SET COMPRESSION 'zstd';"
                )
            except ydb.issues.Error:
                assert False
