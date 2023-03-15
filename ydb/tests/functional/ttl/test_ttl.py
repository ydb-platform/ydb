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

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb


class TestTTL(object):
    ENABLE_TTL_ON_INDEXED_TABLES = None

    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(KikimrConfigGenerator(
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

    @classmethod
    def upsert(cls, session, table):
        session.transaction().execute(
            'upsert into `{}` (id, expire_at) values'
            '(1, cast("1970-01-01T00:00:00.000000Z" as Timestamp)),'
            '(2, cast("1990-03-01T00:00:00.000000Z" as Timestamp)),'
            '(3, cast("2030-04-15T00:00:00.000000Z" as Timestamp));'.format(table),
            commit_tx=True
        )

    def _run_test(self):
        with ydb.Driver(ydb.DriverConfig(self.endpoint, self.database)) as driver:
            with ydb.SessionPool(driver) as pool:
                with pool.checkout() as session:
                    table = os.path.join(self.database, 'table_with_ttl_column')

                    session.create_table(table, self.build_table_description())

                    description = session.describe_table(table)
                    assert_that(description.ttl_settings, not_none())
                    assert_that(any_of(
                        description.ttl_settings.date_type_column,
                        description.ttl_settings.value_since_unix_epoch
                    ), not_none())

                    self.upsert(session, table)

                    # conditional erase runs every 60 second
                    for i in range(60):
                        time.sleep(4)

                        content = list(self._read_table(session, table, columns=('id',)))
                        if len(content) == 1:
                            break

                    content = list(self._read_table(session, table, columns=('id',)))
                    assert_that(content, has_length(1))
                    assert_that(content, has_item(has_properties(id=3)))

    @staticmethod
    def _read_table(session, *args, **kwargs):
        for chunk in iter(session.read_table(*args, **kwargs)):
            for row in chunk.rows:
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

    @classmethod
    def upsert(cls, session, table):
        session.transaction().execute(
            'upsert into `{}` (id, expire_at) values'
            '(1, 0),'
            '(2, 636249600),'
            '(3, 1902441600);'.format(table),
            commit_tx=True
        )

    def test_case(self):
        self._run_test()


class TestTTLAlterSettings(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(KikimrConfigGenerator())
        cls.cluster.start()
        cls.endpoint = "%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port)
        cls.database = '/Root'

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test_case(self):
        with ydb.Driver(ydb.DriverConfig(self.endpoint, self.database)) as driver:
            with ydb.SessionPool(driver) as pool:
                with pool.checkout() as session:
                    table = os.path.join(self.database, 'table_with_ttl_column')

                    session.create_table(
                        table, ydb.TableDescription()
                        .with_primary_keys('id')
                        .with_columns(
                            ydb.Column('id', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                            ydb.Column('expire_at', ydb.OptionalType(ydb.PrimitiveType.Timestamp)),
                        )
                    )

                    description = session.describe_table(table)
                    assert_that(description.ttl_settings, none())

                    session.alter_table(table, set_ttl_settings=ydb.TtlSettings().with_date_type_column('expire_at'))

                    description = session.describe_table(table)
                    assert_that(description.ttl_settings, not_none())
                    assert_that(description.ttl_settings.date_type_column, not_none())
                    assert_that(description.ttl_settings.date_type_column, has_properties(column_name='expire_at'))

                    session.alter_table(table, drop_ttl_settings=True)

                    description = session.describe_table(table)
                    assert_that(description.ttl_settings, none())
