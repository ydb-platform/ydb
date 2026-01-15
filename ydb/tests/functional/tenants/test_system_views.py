# -*- coding: utf-8 -*-
import json
import logging
import os
import time

from hamcrest import (
    anything,
    assert_that,
    greater_than,
    has_length,
    has_properties,
)

import yatest.common

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

pg_sysviews = {'pg_tables', 'tables', 'pg_class'}


class BaseSystemViews(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(
            KikimrConfigGenerator(
                additional_log_configs={
                    'SYSTEM_VIEWS': LogLevels.DEBUG
                }
            )
        )
        cls.cluster.config.yaml_config['feature_flags']['enable_column_statistics'] = False
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def setup_method(self, method=None):
        self.database = "/Root/users/{class_name}_{method_name}".format(
            class_name=self.__class__.__name__,
            method_name=method.__name__,
        )
        logger.debug("Create database %s" % self.database)
        self.cluster.create_database(
            self.database,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        self.cluster.register_and_start_slots(self.database, count=1)
        self.cluster.wait_tenant_up(self.database)

    def teardown_method(self, method=None):
        logger.debug("Remove database %s" % self.database)
        self.cluster.remove_database(self.database)
        self.database = None

    def create_table(self, driver, table):
        with ydb.SessionPool(driver, size=1) as pool:
            with pool.checkout() as session:
                session.execute_scheme(
                    "create table `{}` (key Int32, value String, primary key(key));".format(
                        table
                    )
                )

    def read_partition_stats(self, driver, database):
        table = os.path.join(database, '.sys/partition_stats')
        return self._stream_query_result(
            driver,
            "select PathId, PartIdx, Path from `{}`;".format(table)
        )

    def read_query_metrics(self, driver, database):
        table = os.path.join(database, '.sys/query_metrics_one_minute')
        return self._stream_query_result(
            driver,
            "select Count, SumReadBytes, SumRequestUnits from `{}`;".format(table)
        )

    def read_query_stats(self, driver, database, table_name):
        table = os.path.join(database, '.sys', table_name)
        return self._stream_query_result(
            driver,
            "select Duration, ReadBytes, CPUTime, RequestUnits from `{}`;".format(table)
        )

    def _stream_query_result(self, driver, query):
        it = driver.table_client.scan_query(query)
        result = []

        while True:
            try:
                response = next(it)
            except StopIteration:
                break

            for row in response.result_set.rows:
                result.append(row)

        return result

    def check_query_metrics_and_stats(self, query_count):
        table = os.path.join(self.database, 'table')

        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            self.database
        )

        with ydb.Driver(driver_config) as driver:
            driver.wait(timeout=10)

            self.create_table(driver, table)

            with ydb.SessionPool(driver, size=1) as pool:
                with pool.checkout() as session:
                    for i in range(query_count):
                        query = "select * from `{}` -- {}".format(table, i)
                        session.transaction().execute(query, commit_tx=True)

            time.sleep(70)

            for i in range(60):
                metrics = self.read_query_metrics(driver, self.database)
                if len(metrics) == query_count:
                    break
                time.sleep(5)

            assert_that(metrics, has_length(query_count))
            assert_that(metrics[0], has_properties({
                'Count': 1,
                'SumReadBytes': 0,
                'SumRequestUnits': greater_than(0),
            }))

            for table_name in [
                'top_queries_by_duration_one_minute',
                'top_queries_by_duration_one_hour',
                'top_queries_by_read_bytes_one_minute',
                'top_queries_by_read_bytes_one_hour',
                'top_queries_by_cpu_time_one_minute',
                'top_queries_by_cpu_time_one_hour',
                'top_queries_by_request_units_one_minute',
                'top_queries_by_request_units_one_hour'
            ]:
                stats = self.read_query_stats(driver, self.database, table_name)

                assert_that(stats, has_length(min(query_count, 5)))
                assert_that(stats[0], has_properties({
                    'Duration': anything(),
                    'ReadBytes': 0,
                    'CPUTime': anything(),
                    'RequestUnits': greater_than(0)
                }))


class TestPartitionStats(BaseSystemViews):
    def test_case(self):
        for database in ('/Root', self.database):
            table = os.path.join(database, 'table')

            driver_config = ydb.DriverConfig(
                "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
                database
            )

            with ydb.Driver(driver_config) as driver:
                driver.wait(timeout=10)

                self.create_table(driver, table)
                stats = self.read_partition_stats(driver, database)

                assert_that(stats, has_length(1))
                assert_that(stats[0], has_properties({
                    'Path': table,
                    'PathId': anything(),
                    'PartIdx': 0,
                }))


class TestQueryMetrics(BaseSystemViews):
    def test_case(self):
        self.check_query_metrics_and_stats(1)


class TestQueryMetricsUniqueQueries(BaseSystemViews):
    def test_case(self):
        self.check_query_metrics_and_stats(10)


class TestSysViewsRegistry(BaseSystemViews):
    """
    Test validates that changes in SysViews registry are compatible.

    FORBIDDEN:
    - Changing column data types
    - Renaming existing columns
    - Removing columns from primary key
    - Deleting existing columns
    - Changing sysview types (implementation ids)
    - Renaming existing sysviews
    - Deleting existing sysviews

    ALLOWED:
    - Adding new columns (with sequential IDs)
    - Adding new sysviews
    """
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator())
        cls.cluster.start()

    def collect_sysviews(self, driver, database):
        """Collects information about all sysviews in the database."""
        sysviews = {}

        sys_dir_path = os.path.join(database, '.sys')

        try:
            children = driver.scheme_client.list_directory(sys_dir_path).children
        except Exception as e:
            logger.warning(f"Failed to list .sys directory for '{database}': {e}")
            return sysviews

        with ydb.SessionPool(driver, size=1) as pool:
            with pool.checkout() as session:
                for child in children:
                    sysview_name = child.name
                    sysview_path = os.path.join(sys_dir_path, sysview_name)

                    try:
                        # Try to get table description
                        response = session.describe_table(sysview_path)

                        # Collect column information
                        columns = []
                        for col in response.columns:
                            columns.append({
                                'name': col.name,
                                'type': str(col.type)
                            })

                        # Collect primary key information
                        primary_key = [pk_col for pk_col in response.primary_key]

                        sysviews[sysview_name] = {
                            'columns': columns,
                            'primary_key': primary_key
                        }

                    except Exception as e:
                        # Some sysviews may be unavailable or have special nature (e.g., pg_* views)
                        if sysview_name in pg_sysviews:
                            logger.debug(f"Skipping pg sysview '{sysview_name}': {e}")
                            continue
                        else:
                            raise

        return sysviews

    def load_canonical_file(self, filename):
        """Loads canonical file if it exists."""
        test_name = yatest.common.context.test_name
        # Remove .py and replace :: with .
        test_name = test_name.replace('.py::', '.').replace('::', '.')

        canonical_path = os.path.join(
            yatest.common.test_source_path('canondata'),
            test_name,
            filename
        )

        if os.path.exists(canonical_path):
            try:
                with open(canonical_path, 'r') as f:
                    return json.load(f)

            except Exception as e:
                logger.debug(f"Could not load canonical file '{filename}': {e}")

        return None

    def write_canonical_file(self, content, filename):
        """Writes content to file for canonization."""
        output_path = os.path.join(yatest.common.output_path(), filename)
        with open(output_path, 'w') as f:
            f.write(json.dumps(content, indent=2, sort_keys=True, ensure_ascii=False))
        return output_path

    def validate_compatibility(self, canonical_sysviews, current_sysviews):
        """
        Validates compatibility of changes according to rules above.
        Returns list of compatibility errors.
        """
        if canonical_sysviews is None:
            # No canonical data - this is the first run
            logger.debug('Dry run without canon data')
            return []

        errors = []

        # Check that all canonical sysviews are present
        for sysview_name, canonical_sysview in canonical_sysviews.items():
            if sysview_name not in current_sysviews:
                errors.append(f"Sysview '{sysview_name}' was deleted")
                continue

            current_sysview = current_sysviews[sysview_name]
            canonical_columns = {col['name']: col for col in canonical_sysview['columns']}
            current_columns = {col['name']: col for col in current_sysview['columns']}
            canonical_pk = canonical_sysview['primary_key']
            current_pk = current_sysview['primary_key']

            # Check 1: All canonical columns must be present
            for col_name, col_info in canonical_columns.items():
                if col_name not in current_columns:
                    errors.append(
                        f"Column '{col_name}' was deleted from sysview '{sysview_name}'"
                    )
                else:
                    # Check 2: Column types must not change
                    if col_info['type'] != current_columns[col_name]['type']:
                        errors.append(
                            f"Column '{col_name}' changed type in sysview '{sysview_name}': "
                            f"was '{col_info['type']}', became '{current_columns[col_name]['type']}'"
                        )

            # Check 3: Primary key must contain all canonical columns in the same order
            if len(canonical_pk) > len(current_pk):
                errors.append(f"Primary key was shortened in sysview '{sysview_name}'")
            else:
                for i, pk_col in enumerate(canonical_pk):
                    if pk_col != current_pk[i]:
                        errors.append(
                            f"Primary key differs in sysview '{sysview_name}': "
                            f"expected '{pk_col}' at position {i}, got '{current_pk[i]}'"
                        )

        return errors

    def test_domain_sysviews_registry(self):
        """
        Test validates SysViews registry changes
        for root database.

        Test flow:
        1. Collects current state of sysviews
        2. Loads canonical data (if exists)
        3. Validates compatibility of changes
        4. Canonizes new state
        """
        # Collect sysviews from root database
        root_driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            '/Root'
        )

        with ydb.Driver(root_driver_config) as root_driver:
            root_driver.wait(timeout=10)
            root_sysviews = self.collect_sysviews(root_driver, '/Root')

        # Load canonical data
        canonical_root = self.load_canonical_file('root_sysviews.json')

        # Check whether changes are compatible
        all_errors = []
        all_errors.extend(self.validate_compatibility(canonical_root, root_sysviews))

        # If there are compatibility errors, the test has to drop.
        if all_errors:
            error_message = "SysViews registry validation failed:\n" + "\n".join(all_errors)
            logger.error(error_message)
            assert False, error_message

        # Log information
        logger.info(f"Root sysviews count: {len(root_sysviews)}")
        logger.info(f"Root sysviews: {sorted(root_sysviews.keys())}")

        # Return canonized files
        # If schemas changed incompatibly, test already failed above
        # If changes are compatible - update canon
        return yatest.common.canonical_file(
            local=True,
            universal_lines=True,
            path=self.write_canonical_file(root_sysviews, 'root_sysviews.json')
        )

    def test_tenant_sysviews_registry(self):
        """
        Test validates SysViews registry changes
        for tenant database.

        Test flow:
        1. Collects current state of sysviews
        2. Loads canonical data (if exists)
        3. Validates compatibility of changes
        4. Canonizes new state
        """

        # Collect sysviews from tenant database
        tenant_driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            self.database
        )

        with ydb.Driver(tenant_driver_config) as tenant_driver:
            tenant_driver.wait(timeout=10)
            tenant_sysviews = self.collect_sysviews(tenant_driver, self.database)

        # Load canonical data
        canonical_tenant = self.load_canonical_file('tenant_sysviews.json')

        # Check whether changes are compatible
        all_errors = []
        all_errors.extend(self.validate_compatibility(canonical_tenant, tenant_sysviews))

        # If there are compatibility errors, the test has to drop.
        if all_errors:
            error_message = "SysViews registry validation failed:\n" + "\n".join(all_errors)
            logger.error(error_message)
            assert False, error_message

        # Log information
        logger.info(f"Tenant sysviews count: {len(tenant_sysviews)}")
        logger.info(f"Tenant sysviews: {sorted(tenant_sysviews.keys())}")

        # Return canonized files
        # If schemas changed incompatibly, test already failed above
        # If changes are compatible - update canon
        return yatest.common.canonical_file(
            local=True,
            universal_lines=True,
            path=self.write_canonical_file(tenant_sysviews, 'tenant_sysviews.json')
        )
