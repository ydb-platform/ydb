# -*- coding: utf-8 -*-
import yatest
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.library.common.types import Erasure
from ydb.tests.oss.ydb_sdk_import import ydb


class TestCompatibility(object):
    @classmethod
    def setup_class(cls):
        last_stable_path = yatest.common.binary_path("ydb/tests/library/compatibility/ydbd-last-stable")
        binary_paths = [kikimr_driver_path(), last_stable_path]
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.MIRROR_3_DC, binary_paths=binary_paths))
        cls.cluster.start()
        cls.endpoint = "%s:%s" % (
            cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
        )
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=cls.endpoint
            )
        )
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'driver'):
            cls.driver.stop()

        if hasattr(cls, 'cluster'):
            cls.cluster.stop(kill=True)  # TODO fix

    def test_simple(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                session.execute_scheme(
                    "create table `sample_table` (id Uint64, value Uint64, payload Utf8, PRIMARY KEY(id)) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 1);"
                )
                id_ = 0

                upsert_count = 200
                iteration_count = 1
                for i in range(iteration_count):
                    rows = []
                    for j in range(upsert_count):
                        row = {}
                        row["id"] = id_
                        row["value"] = 1
                        row["payload"] = "DEADBEEF" * 1024 * 16  # 128 kb
                        rows.append(row)
                        id_ += 1

                    column_types = ydb.BulkUpsertColumns()
                    column_types.add_column("id", ydb.PrimitiveType.Uint64)
                    column_types.add_column("value", ydb.PrimitiveType.Uint64)
                    column_types.add_column("payload", ydb.PrimitiveType.Utf8)
                    self.driver.table_client.bulk_upsert(
                        "Root/sample_table", rows, column_types
                    )

                query = "SELECT SUM(value) from sample_table"
                result_sets = session.transaction().execute(
                    query, commit_tx=True
                )
                for row in result_sets[0].rows:
                    print(" ".join([str(x) for x in list(row.values())]))

                assert len(result_sets) == 1
                assert len(result_sets[0].rows) == 1
                result = list(result_sets[0].rows[0].values())
                assert len(result) == 1
                assert result[0] == upsert_count * iteration_count
