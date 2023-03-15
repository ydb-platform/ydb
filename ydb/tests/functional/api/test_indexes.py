# -*- coding: utf-8 -*-
import logging

from hamcrest import assert_that, is_

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class TestSecondaryIndexes(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.logger = logger.getChild(cls.__name__)
        cls.driver = ydb.Driver(
            ydb.DriverConfig("%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port), '/Root'))
        cls.pool = ydb.SessionPool(cls.driver)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'pool'):
            cls.pool.stop()

        if hasattr(cls, 'driver'):
            cls.driver.stop()

        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_create_table_with_global_index(self):
        with self.pool.checkout() as session:
            session.create_table(
                '/Root/table_with_indexes',
                ydb.TableDescription()
                .with_primary_keys('span_id', 'folder_id')
                .with_columns(
                    ydb.Column('span_id', ydb.OptionalType(ydb.PrimitiveType.Utf8)),
                    ydb.Column('folder_id', ydb.OptionalType(ydb.PrimitiveType.Utf8)),
                    ydb.Column('cloud_id', ydb.OptionalType(ydb.PrimitiveType.Utf8)),
                    ydb.Column('timestamp', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                )
                .with_indexes(
                    ydb.TableIndex('by_folder_id').with_index_columns('folder_id'),
                    ydb.TableIndex('by_cloud_id').with_index_columns('cloud_id', 'folder_id'),
                    ydb.TableIndex('timestamp').with_index_columns('timestamp')
                )
            )

            description = session.describe_table('/Root/table_with_indexes')
            assert_that(
                len(description.indexes),
                is_(
                    3
                )
            )
