import ydb
import os
import yatest.common
import random
import logging

from datetime import date
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from typing import Callable, Any, List


logger = logging.getLogger(__name__)


class TestBase:

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path("ydb/apps/ydbd/ydbd")
        logger.error(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("ascii"))

        cls.database = "/Root"
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=cls.get_cluster_configuration()))
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database=cls.get_database(),
                endpoint=cls.get_endpoint()
            )
        )
        cls.driver.wait()
        cls.pool = ydb.QuerySessionPool(cls.driver)

    @classmethod
    def get_cluster_configuration(self):
        return Erasure.NONE

    @classmethod
    def get_database(self):
        return self.database

    @classmethod
    def get_endpoint(self):
        return "%s:%s" % (
            self.cluster.nodes[1].host, self.cluster.nodes[1].port
            )

    @classmethod
    def teardown_class(cls):
        cls.pool.stop()
        cls.driver.stop()
        cls.cluster.stop()

    def setup_method(self):
        current_test_full_name = os.environ.get("PYTEST_CURRENT_TEST")
        self.table_path = "insert_table_" + current_test_full_name.replace("::", ".").removesuffix(" (setup)")

    def query(self, text, tx: ydb.QueryTxContext | None = None) -> List[Any]:
        results = []
        if tx is None:
            result_sets = self.pool.execute_with_retries(text)
            for result_set in result_sets:
                results.extend(result_set.rows)
        else:
            with tx.execute(text) as result_sets:
                for result_set in result_sets:
                    results.extend(result_set.rows)

        return results

    def transactional(self, fn: Callable[[ydb.QuerySession], List[Any]]):
        return self.pool.retry_operation_sync(lambda session: fn(session))

    def split_data_into_fixed_size_chunks(data, chunk_size):
        """Splits data to N chunks of chunk_size size"""
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]


class TpchTestBaseH1(TestBase):

    @classmethod
    def get_cluster_configuration(self):
        return Erasure.MIRROR_3_DC

    @classmethod
    def run_cli(cls, argv: list[str]) -> yatest.common.process._Execution:
        workload_path = yatest.common.build_path("ydb/apps/ydb/ydb")

        args = [
            '-e', 'grpc://'+cls.get_endpoint(),
            '-d', cls.get_database()
        ]

        args.extend(argv)
        return yatest.common.execute([workload_path] + args)

    def setup_method(cls):
        super().setup_method()
        cls.setup_tpch()

    def teardown_method(cls):
        cls.teardown_tpch()

    def setup_tpch(cls):
        cls.run_cli(['workload', 'tpch', '-p', cls.tpch_default_path()+'/', 'init', '--store=column', '--datetime'])
        cls.run_cli(['workload', 'tpch', '-p', cls.tpch_default_path()+'/', 'import', 'generator', '--scale=1'])

    def teardown_tpch(cls):
        cls.run_cli(['scheme', 'rmdir', '-r', '-f', cls.tpch_default_path()])

    def tpch_default_path(self):
        return 'tpch/s1'

    def build_lineitem_upsert_query(self, table_name, lineitem):
        return f"""
            UPSERT INTO `{table_name}`(
            l_orderkey, l_partkey, l_suppkey, l_linenumber,
            l_quantity, l_discount, l_extendedprice, l_shipdate,
            l_returnflag, l_tax, l_shipinstruct, l_commitdate,
            l_receiptdate, l_linestatus, l_shipmode, l_comment)
            values(
            {lineitem["l_orderkey"]},
            {lineitem["l_partkey"]},
            {lineitem["l_suppkey"]},
            {lineitem["l_linenumber"]},
            {lineitem["l_quantity"]},
            {lineitem["l_discount"]},
            {lineitem["l_extendedprice"]},
            Date('{lineitem["l_shipdate"]}'),
            '{lineitem["l_returnflag"].decode('utf-8')}',
            {lineitem["l_tax"]},
            '{lineitem["l_shipinstruct"].decode('utf-8')}',
            Date('{lineitem["l_commitdate"]}'),
            Date('{lineitem["l_receiptdate"]}'),
            '{lineitem["l_linestatus"].decode('utf-8')}',
            '{lineitem["l_shipmode"].decode('utf-8')}',
            '{lineitem["l_comment"].decode('utf-8')}'
            )
        """

    def tpch_bulk_upsert_col_types(self):
        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("l_orderkey", ydb.PrimitiveType.Int64)
        column_types.add_column("l_partkey", ydb.PrimitiveType.Int64)
        column_types.add_column("l_suppkey", ydb.PrimitiveType.Int64)
        column_types.add_column("l_linenumber", ydb.PrimitiveType.Int32)
        column_types.add_column("l_quantity", ydb.PrimitiveType.Double)
        column_types.add_column("l_discount", ydb.PrimitiveType.Double)
        column_types.add_column("l_extendedprice", ydb.PrimitiveType.Double)
        column_types.add_column("l_comment", ydb.PrimitiveType.Utf8)
        column_types.add_column("l_shipdate", ydb.PrimitiveType.Date)
        column_types.add_column("l_returnflag", ydb.PrimitiveType.Utf8)
        column_types.add_column("l_tax", ydb.PrimitiveType.Double)
        column_types.add_column("l_shipinstruct", ydb.PrimitiveType.Utf8)
        column_types.add_column("l_commitdate", ydb.PrimitiveType.Date)
        column_types.add_column("l_receiptdate", ydb.PrimitiveType.Date)
        column_types.add_column("l_linestatus", ydb.PrimitiveType.Utf8)
        column_types.add_column("l_shipmode", ydb.PrimitiveType.Utf8)
        return column_types

    def tpch_empty_lineitem(self):
        return {
            'l_orderkey': random.randint(1, 1000),
            'l_partkey': random.randint(1, 1000),
            'l_suppkey': random.randint(1, 1000),
            'l_linenumber': random.randint(1, 10),
            'l_quantity': random.randint(1, 1000),
            'l_discount': random.uniform(1.0, 100.0),
            'l_extendedprice': random.uniform(1.0, 100.0),
            'l_comment': b' ', # important! CS does not work with empty strings
            'l_shipdate': date(year=2012, month=2, day=9),
            'l_returnflag': b'',
            'l_tax': 0.0,
            'l_shipinstruct': b'',
            'l_commitdate': date(year=2013, month=2, day=9),
            'l_receiptdate': date(year=2014, month=2, day=9),
            'l_shipmode': b'',
            'l_linestatus': b''
        }

    def create_lineitem(self, update):
        base_lineitem = self.tpch_empty_lineitem()

        for key, value in update.items():
            base_lineitem[key] = value
        return base_lineitem

    def tcph_est_records_count(self):
        return 600_000_000
