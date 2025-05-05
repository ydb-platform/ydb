import datetime
import random
import ydb
import uuid
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


def generate_data(total_rows, days_count=1000):
    """
    Generates total_rows data items with various data types, ensuring N/days_count rows per date.

    Args:
        total_rows: The number of data items to generate.
        days_count: Count of date

        A list of dictionaries containing the generated data.
    """

    data = []
    date_window_date = datetime.date(2000, 1, 1)
    full_names = [
        'ydb/library/yql/providers/generic/connector/tests/datasource/clickhouse/test.py.test_select_positive[constant_HTTP-kqprun]',
        'ydb/library/yql/providers/generic/connector/tests/datasource/clickhouse/test.py.test_select_negative[invalid_column]',
        'ydb/library/yql/providers/generic/connector/tests/datasource/clickhouse/test.py.test_insert_positive[basic_insert]',
        'ydb/library/yql/providers/generic/connector/tests/datasource/clickhouse/test.py.test_delete_positive[basic_delete]',
    ]
    branches = ['main', 'dev', 'feature_branch']

    rows_per_date = total_rows // days_count  # Ensure N/days_count rows per date

    for i in range(days_count):
        for _ in range(rows_per_date):
            data.append(
                {
                    'full_name': random.choice(full_names),
                    'branch': random.choice(branches),
                    'date_window': date_window_date + datetime.timedelta(days=i),
                    'int_value': random.randint(0, 1000),
                    'bytes_value': bytes(random.randint(0, 255) for _ in range(10)),
                    'uuid_value': uuid.uuid1().hex,
                }
            )

    return data


def create_tables(pool, table_path):
    def callee(session):
        session.execute_scheme(
            f"""
            CREATE table IF NOT EXISTS `{table_path}` (
                `uuid_value` Utf8 NOT NULL,
                `full_name` Utf8 NOT NULL,
                `date_window` Date NOT NULL,
                `branch` Utf8 NOT NULL,
                `int_value` Int64,
                `bytes_value` String,
                PRIMARY KEY (uuid_value ,date_window, branch)
            )
             --WITH (STORE = COLUMN)
            """
        )

    return pool.retry_operation_sync(callee)


def bulk_upsert(table_client, table_path, rows):
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("date_window", ydb.OptionalType(ydb.PrimitiveType.Date))
        .add_column("int_value", ydb.OptionalType(ydb.PrimitiveType.Int64))
        .add_column("bytes_value", ydb.OptionalType(ydb.PrimitiveType.String))
        .add_column("uuid_value", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    )
    table_client.bulk_upsert(table_path, rows, column_types)


class BasicQueriesToOlap(object):
    """
    Various tests which uses slice (e.g. host's cluster) for testing
    """

    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(KikimrConfigGenerator(erasure=Erasure.MIRROR_3_DC))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()


class TestBasicQueries(BasicQueriesToOlap):
    def test_count(self):
        """
        Just a sample test to ensure that slice works correctly
        """
        database_path = '/Root'
        table_path = 'test_base'
        rows = 10000

        data = generate_data(rows)
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port), database_path
        )
        with ydb.Driver(driver_config) as driver:
            with ydb.SessionPool(driver, size=1) as pool:
                create_tables(pool, table_path)
            bulk_upsert(driver.table_client, database_path + '/' + table_path, data)

            query_text = f"""
                    SELECT count(*) as count_of_rows
                    FROM `{table_path}`
                """
            query = ydb.ScanQuery(query_text, {})
            it = driver.table_client.scan_query(query)
            result_count_of_rows = None

            while True:
                try:
                    result = next(it)
                    result_count_of_rows = result.result_set.rows[0]['count_of_rows']
                except StopIteration:
                    break
        assert rows == result_count_of_rows

    def test_limit_query(self):

        database_path = '/Root'
        table_path = 'test_base'
        rows = 10000
        limit = 1000

        data = generate_data(rows)
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port), database_path
        )
        with ydb.Driver(driver_config) as driver:
            with ydb.SessionPool(driver, size=1) as pool:
                create_tables(pool, table_path)
            bulk_upsert(driver.table_client, database_path + '/' + table_path, data)

            query_text = f"""
                    SELECT *
                    FROM `{table_path}`
                    limit {limit}
                """
            query = ydb.ScanQuery(query_text, {})
            it = driver.table_client.scan_query(query)
            result_list = []

            while True:
                try:
                    result = next(it)
                    result_list = result_list + result.result_set.rows
                except StopIteration:
                    break
        assert len(result_list) == limit

    def test_uniq_item(self):

        database_path = '/Root'
        table_path = 'test_base'
        rows = 100000

        data = generate_data(rows)
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port), database_path
        )
        with ydb.Driver(driver_config) as driver:
            with ydb.SessionPool(driver, size=1) as pool:
                create_tables(pool, table_path)
            bulk_upsert(driver.table_client, database_path + '/' + table_path, data)

            query_text = f"""
                    SELECT *
                    FROM `{table_path}`
                    WHERE uuid_value = '{data[500]['uuid_value']}'
                """
            query = ydb.ScanQuery(query_text, {})
            it = driver.table_client.scan_query(query)
            result_list = []

            while True:
                try:
                    result = next(it)
                    result_list = result_list + result.result_set.rows
                except StopIteration:
                    break
        assert len(result_list) == 1
        assert result_list[0]['uuid_value'] == data[500]['uuid_value']
