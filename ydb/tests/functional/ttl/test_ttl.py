# -*- coding: utf-8 -*-

import os
import time

from hamcrest import (
    any_of,
    assert_that,
    has_item,
    has_length,
    has_properties,
    none,
    not_none,
)

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb


class TestTTL(object):
    ENABLE_TTL_ON_INDEXED_TABLES = None

    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            additional_log_configs={
                'FLAT_TX_SCHEMESHARD': LogLevels.DEBUG,
                'TX_DATASHARD': LogLevels.DEBUG,
            },
        ))
        cls.cluster.start()
        cls.endpoint = "%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port)
        cls.database = '/Root'

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    @classmethod
    def build_table_description(cls):
        return ydb.TableDescription().with_primary_keys(
            'id'
        ).with_columns(
            ydb.Column('id', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
            ydb.Column('expire_at', ydb.OptionalType(ydb.PrimitiveType.Timestamp)),
        ).with_ttl(
            ydb.TtlSettings().with_date_type_column('expire_at')
        )

    # @classmethod
    # def upsert(cls, session, table):
    #     session.transaction().execute(
    #         'upsert into `{}` (id, expire_at) values'
    #         '(1, cast("1970-01-01T00:00:00.000000Z" as Timestamp)),'
    #         '(2, cast("1990-03-01T00:00:00.000000Z" as Timestamp)),'
    #         '(3, cast("2030-04-15T00:00:00.000000Z" as Timestamp));'.format(table),
    #         commit_tx=True
    #     )

    @classmethod
    def upsert(cls, pool, table):
        pool.execute_with_retries(
            f"""upsert into `{table}` (id, expire_at) values
                (1, cast("1970-01-01T00:00:00.000000Z" as Timestamp)),
                (2, cast("1990-03-01T00:00:00.000000Z" as Timestamp)),
                (3, cast("2030-04-15T00:00:00.000000Z" as Timestamp));
            """
        )

    def _run_test(self):
        with ydb.Driver(ydb.DriverConfig(self.endpoint, self.database)) as driver:
            with ydb.QuerySessionPool(driver) as pool:
                with pool.checkout() as session:
                    table = os.path.join(self.database, 'table_with_ttl_column')


                    result_sets = pool.execute_with_retries(
                        f"""
                        CREATE TABLE `{table}`
                        (
                        id UInt64,
                        expire_at Timestamp,
                        PRIMARY KEY (id)
                        ) WITH (
                            TTL = Interval("PT0S") ON expire_at
                        );
                        """
                    )

                    description = session.describe_table(table)
                    assert_that(description.ttl_settings, not_none())
                    assert_that(any_of(
                        description.ttl_settings.date_type_column,
                        description.ttl_settings.value_since_unix_epoch
                    ), not_none())

                    self.upsert(pool, table)

                    # conditional erase runs every 60 second
                    for i in range(60):
                        time.sleep(4)

                        # content = list(self._read_table(session, table, columns=('id',)))
                        content = list(self._read_table(result_sets))
                        if len(content) == 1:
                            break

                    # content = list(self._read_table(session, table, columns=('id',)))
                    content = list(self._read_table(result_sets))
                    assert_that(content, has_length(1))
                    assert_that(content, has_item(has_properties(id=3)))

    # @staticmethod
    # def _read_table(session, *args, **kwargs):
    #     for chunk in iter(session.read_table(*args, **kwargs)):
    #         for row in chunk.rows:
    #             yield row
    
    @staticmethod
    def _read_table(result_sets, *args, **kwargs):
        for result_set in result_sets:
            for row in result_set.rows:
                yield row


class TestTTLDefaultEnv(TestTTL):
    def test_case(self):
        self._run_test()


class TestTTLOnIndexedTable(TestTTL):
    ENABLE_TTL_ON_INDEXED_TABLES = True

    @classmethod
    def build_table_description(cls):
        return super(TestTTLOnIndexedTable, cls).build_table_description().with_index(
            ydb.TableIndex('by_expire_at').with_index_columns('expire_at')
        )

    def test_case(self):
        self._run_test()


class TestTTLValueSinceUnixEpoch(TestTTL):
    @classmethod
    def build_table_description(cls):
        return ydb.TableDescription().with_primary_keys(
            'id'
        ).with_columns(
            ydb.Column('id', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
            ydb.Column('expire_at', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
        ).with_ttl(
            ydb.TtlSettings().with_value_since_unix_epoch('expire_at', ydb.ColumnUnit.UNIT_SECONDS)
        )

    # @classmethod
    # def upsert(cls, session, table):
    #     session.transaction().execute(
    #         'upsert into `{}` (id, expire_at) values'
    #         '(1, 0),'
    #         '(2, 636249600),'
    #         '(3, 1902441600);'.format(table),
    #         commit_tx=True
    #     )

    @classmethod
    def upsert(cls, pool, table):
        pool.execute_with_retries(
            f"""upsert into `{table}` (id, expire_at) values
                (1, 0),
                (2, 636249600),
                (3, 1902441600);
            """
        )

    def test_case(self):
        self._run_test()


class TestTTLAlterSettings(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator())
        cls.cluster.start()
        cls.endpoint = "%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port)
        cls.database = '/Root'

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test_case(self):
        with ydb.Driver(ydb.DriverConfig(self.endpoint, self.database)) as driver:
            with ydb.QuerySessionPool(driver) as pool:
                with pool.checkout() as session:
                    table = os.path.join(self.database, 'table_with_ttl_column')

                    create_result_sets = pool.execute_with_retries(
                        f"""
                        CREATE TABLE `{table}`
                        (
                        id UInt64,
                        expire_at Timestamp,
                        PRIMARY KEY (id)
                        );
                        """
                    )

                    description = session.describe_table(table)
                    assert_that(description.ttl_settings, none())


                    first_alter_result_sets = pool.execute_with_retries(
                        f"""
                        ALTER TABLE `{table}` SET (TTL = Interval("PT0S") ON expire_at);
                        """
                    )

                    description = session.describe_table(table)
                    assert_that(description.ttl_settings, not_none())
                    assert_that(description.ttl_settings.date_type_column, not_none())
                    assert_that(description.ttl_settings.date_type_column, has_properties(column_name='expire_at'))


                    second_alter_result_sets = pool.execute_with_retries(
                        f"""
                        ALTER TABLE `{table}` RESET (TTL);
                        """
                    )

                    description = session.describe_table(table)
                    assert_that(description.ttl_settings, none())
