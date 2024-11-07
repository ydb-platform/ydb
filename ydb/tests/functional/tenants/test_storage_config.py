# -*- coding: utf-8 -*-
import logging
import random
import string
import pytest
from hamcrest import assert_that, equal_to, has_item, has_properties

from ydb.tests.library.common.local_db_scheme import get_scheme
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.predicates.executor import external_blobs_is_present
from ydb.tests.library.common.protobuf_ss import CreateTableRequest

import ydb.tests.library.matchers.scheme_ops as scheme_operations
from ydb.tests.library.common.types import PType
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class TBaseTenant(object):
    LOG_SETTINGS = {
        'CMS_TENANTS': LogLevels.TRACE,
        'TENANT_SLOT_BROKER': LogLevels.DEBUG,
        'TENANT_POOL': LogLevels.DEBUG,
        'LOCAL': LogLevels.DEBUG,
        'NODE_BROKER': LogLevels.DEBUG,
        'TX_DATASHARD': LogLevels.DEBUG,
        'TX_PROXY': LogLevels.DEBUG,
    }

    @classmethod
    def setup_class(cls):
        configurator = KikimrConfigGenerator(additional_log_configs=cls.LOG_SETTINGS)
        cls.kikimr = kikimr_cluster_factory(configurator)
        cls.kikimr.start()
        cls.tenant_path = cls.__create_tenant('common_tenant', ('hdd', 'hdd1', 'hdd2'))
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                "%s:%s" % (cls.kikimr.nodes[1].host, cls.kikimr.nodes[1].port),
                cls.tenant_path
            )
        )
        cls.driver.wait()
        cls.pool = ydb.SessionPool(cls.driver, size=5)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'pool'):
            cls.pool.stop()

        if hasattr(cls, 'driver'):
            cls.driver.stop()

        if hasattr(cls, 'kikimr'):
            cls.kikimr.stop()

    @classmethod
    def __create_tenant(cls, name, storage_pools=('hdd', )):
        tenant_path = '/Root/users/%s' % name
        result = cls.kikimr.create_database(
            tenant_path,
            storage_pool_units_count={
                x: 1 for x in storage_pools
            }
        )

        cls.kikimr.register_and_start_slots(tenant_path, count=1)
        cls.kikimr.wait_tenant_up(tenant_path)
        return result


def write_huge_blobs(pool, table_path, count=1, size=2*1024*1024):
    with pool.checkout() as session:
        prepared = session.prepare("""
            declare $key as Uint64;
            declare $value as Utf8;
            upsert into `%s` (key, value) VALUES ($key, $value);
            """ % table_path)

        for _ in range(count):
            key = random.randint(1, 2 ** 48)
            value = ''.join(random.choice(string.ascii_lowercase) for _ in range(size))
            session.transaction().execute(
                prepared, {'$key': key, '$value': value},
                commit_tx=True,
            )


def list_tablets_in_table(client, table_path):
    table_operations = scheme_operations.TableOperations(client)
    table_operations.describe_options.with_partition_info()
    table_operations.describe_options.with_partition_config()
    response = table_operations.describe(table_path)

    return [
        part.DatashardId
        for part in response.PathDescription.TablePartitions
    ]


def case_0():
    creation_options = CreateTableRequest.Options()
    creation_options.add_column('key', PType.Uint64, is_key=True)
    creation_options.add_column('value', PType.Utf8, column_family=0)
    creation_options.add_column_family(
        0,
        creation_options.ColumnStorage1Ext1,
        creation_options.ColumnCacheNone
    )
    creation_options.partition_config.with_partitioning_policy(0)  # for now external blobs and autosplit not compatible

    has_external = True

    def scheme(table_name):
        return has_item(
            has_properties(
                TableName='__user__{}'.format(table_name),
                ColumnFamilies={
                    0: {'Large': 524288,
                        'Cache': 0,
                        'InMemory': False,
                        'Codec': 0,
                        'Small': 4294967295,
                        'RoomID': 0,
                        'Columns': [1, 2]}
                },
                Rooms={
                    0: {'Main': 1,
                        'Outer': 1,
                        'Blobs': 1,
                        'ExternalBlobs': [1]
                        }
                }
            )
        )
    return (creation_options, has_external, scheme)


def case_11():
    creation_options = CreateTableRequest.Options()
    creation_options.add_column('key', PType.Uint64, is_key=True)
    creation_options.add_column('value', PType.Utf8, column_family=0)
    creation_options.add_column_family(
        0,
        creation_options.ColumnStorageTest_1_2_1k,
        creation_options.ColumnCacheNone
    )
    creation_options.partition_config.with_partitioning_policy(0)  # for now external blobs and autosplit not compatible

    has_external = True

    def scheme(table_name):
        return has_item(
            has_properties(
                TableName='__user__{}'.format(table_name),
                ColumnFamilies={
                    0: {'Large': 1024,
                        'Cache': 0,
                        'InMemory': False,
                        'Codec': 0,
                        'Small': 512,
                        'RoomID': 0,
                        'Columns': [1, 2]}
                },
                Rooms={
                    0: {'Main': 1,
                        'Outer': 1,
                        'Blobs': 2,
                        'ExternalBlobs': [2]}
                }
            )
        )

    return (creation_options, has_external, scheme)


def case_1():
    creation_options = CreateTableRequest.Options()
    creation_options.add_column('key', PType.Uint64, is_key=True)
    creation_options.add_column('value', PType.Utf8, column_family=0)
    creation_options.add_column_family(
        0,
        creation_options.ColumnStorage1,
        creation_options.ColumnCacheNone
    )

    has_external = False

    def scheme(table_name):
        return has_item(
            has_properties(
                TableName='__user__{}'.format(table_name),
                ColumnFamilies={
                    0: {'Large': 4294967295,
                        'Cache': 0,
                        'InMemory': False,
                        'Codec': 0,
                        'Small': 4294967295,
                        'RoomID': 0,
                        'Columns': [1, 2]}
                },
                Rooms={
                    0: {'Main': 1,
                        'Outer': 1,
                        'Blobs': 1,
                        'ExternalBlobs': [1]}
                }
            )
        )

    return (creation_options, has_external, scheme)


def case_12():
    creation_options = CreateTableRequest.Options()
    creation_options.add_column('key', PType.Uint64, is_key=True)
    creation_options.add_column('value', PType.Utf8, column_family=0)
    creation_options.add_column_family(
        0,
        creation_options.ColumnStorage2,
        creation_options.ColumnCacheNone
    )
    creation_options.partition_config.with_partitioning_policy(0)  # for now external blobs and autosplit not compatible

    has_external = True

    def scheme(table_name):
        return has_item(
            has_properties(
                TableName='__user__{}'.format(table_name),
                ColumnFamilies={
                    0: {'Large': 524288,
                        'Cache': 0,
                        'InMemory': False,
                        'Codec': 0,
                        'Small': 4294967295,
                        'RoomID': 0,
                        'Columns': [1, 2]}
                },
                Rooms={
                    0: {
                        'Main': 2,
                        'Outer': 2,
                        'Blobs': 2,
                        'ExternalBlobs': [2]
                    }
                }
            )
        )

    return (creation_options, has_external, scheme)


def case_2():
    creation_options = CreateTableRequest.Options()
    creation_options.add_column('key', PType.Uint64, is_key=True)
    creation_options.add_column('value', PType.Utf8, column_family=0)
    storage_config = creation_options.declare_column_family(family_id=0)
    storage_config.appoint_syslog('hdd')
    storage_config.appoint_log('hdd')

    has_external = False

    def scheme(table_name):
        return has_item(
            has_properties(
                TableName='__user__{}'.format(table_name),
                ColumnFamilies={
                    0: {'Large': 4294967295,
                        'Cache': 0,
                        'InMemory': False,
                        'Codec': 0,
                        'Small': 4294967295,
                        'RoomID': 0,
                        'Columns': [1, 2]}
                },
                Rooms={
                    0: {'Main': 1,
                        'Outer': 1,
                        'Blobs': 1,
                        'ExternalBlobs': [1]}
                }
            )
        )

    return (creation_options, has_external, scheme)


def case_3():
    creation_options = CreateTableRequest.Options()
    creation_options.add_column('key', PType.Uint64, is_key=True)
    creation_options.add_column('value', PType.Utf8, column_family=0)
    storage_config = creation_options.declare_column_family(family_id=0)
    storage_config.appoint_syslog('NotExist', True)
    storage_config.appoint_log('NotExist', True)

    has_external = False

    def scheme(table_name):
        return has_item(
            has_properties(
                TableName='__user__{}'.format(table_name),
                ColumnFamilies={
                    0: {'Large': 4294967295,
                        'Cache': 0,
                        'InMemory': False,
                        'Codec': 0,
                        'Small': 4294967295,
                        'RoomID': 0,
                        'Columns': [1, 2]}
                },
                Rooms={
                    0: {'Main': 1,
                        'Outer': 1,
                        'Blobs': 1,
                        'ExternalBlobs': [1]}
                }
            )
        )

    return (creation_options, has_external, scheme)


def case_4():
    creation_options = CreateTableRequest.Options()
    creation_options.add_column('key', PType.Uint64, is_key=True)
    creation_options.add_column('value', PType.Utf8, column_family=0)
    storage_config = creation_options.declare_column_family(family_id=0)
    storage_config.appoint_syslog('NotExist', True)
    storage_config.appoint_log('NotExist', True)
    storage_config.appoint_data('NotExist', True)
    storage_config.appoint_external('NotExist', True)
    creation_options.partition_config.with_partitioning_policy(0)  # for now external blobs and autosplit not compatible

    has_external = True

    def scheme(table_name):
        return has_item(
            has_properties(
                TableName='__user__{}'.format(table_name),
                ColumnFamilies={
                    0: {'Large': 524288,
                        'Cache': 0,
                        'InMemory': False,
                        'Codec': 0,
                        'Small': 12288,
                        'RoomID': 0,
                        'Columns': [1, 2]}
                },
                Rooms={
                    0: {'Main': 1,
                        'Outer': 1,
                        'Blobs': 1,
                        'ExternalBlobs': [1]}
                }
            )
        )

    return (creation_options, has_external, scheme)


def case_5():
    creation_options = CreateTableRequest.Options()
    creation_options.add_column('key', PType.Uint64, is_key=True)
    creation_options.add_column('value', PType.Utf8, column_family=0)
    storage_config = creation_options.declare_column_family(family_id=0)
    storage_config.appoint_syslog('hdd2')
    storage_config.appoint_log('hdd1')
    storage_config.appoint_data('hdd')
    storage_config.appoint_external('hdd2')
    creation_options.partition_config.with_partitioning_policy(0)  # for now external blobs and autosplit not compatible

    has_external = True

    def scheme(table_name):
        return has_item(
            has_properties(
                TableName='__user__{}'.format(table_name),
                ColumnFamilies={
                    0: {'Large': 524288,
                        'Cache': 0,
                        'InMemory': False,
                        'Codec': 0,
                        'Small': 12288,
                        'RoomID': 0,
                        'Columns': [1, 2]}
                },
                Rooms={
                    0: {'Main': 2,
                        'Outer': 2,
                        'Blobs': 3,
                        'ExternalBlobs': [3]}
                }
            )
        )

    return (creation_options, has_external, scheme)


def case_6():
    creation_options = CreateTableRequest.Options()
    creation_options.add_column('key', PType.Uint64, is_key=True)
    creation_options.add_column('value', PType.Utf8, column_family=0)
    storage_config = creation_options.declare_column_family(family_id=0)
    storage_config.appoint_syslog('hdd')
    storage_config.appoint_log('hdd1')
    storage_config.appoint_data('hdd2')
    storage_config.appoint_external('hdd2')
    creation_options.partition_config.with_partitioning_policy(0)  # for now external blobs and autosplit not compatible

    has_external = True

    def scheme(table_name):
        return has_item(
            has_properties(
                TableName='__user__{}'.format(table_name),
                ColumnFamilies={
                    0: {'Large': 524288,
                        'Cache': 0,
                        'InMemory': False,
                        'Codec': 0,
                        'Small': 12288,
                        'RoomID': 0,
                        'Columns': [1, 2]}
                },
                Rooms={
                    0: {'Main': 2,
                        'Outer': 2,
                        'Blobs': 2,
                        'ExternalBlobs': [2]}
                }
            )
        )

    return (creation_options, has_external, scheme)


def case_7():
    creation_options = CreateTableRequest.Options()
    creation_options.add_column('key', PType.Uint64, is_key=True)
    creation_options.add_column('value', PType.Utf8, column_family=0)
    storage_config = creation_options.declare_column_family(family_id=0)
    storage_config.appoint_syslog('hdd')
    storage_config.appoint_log('hdd1')
    storage_config.appoint_data('hdd2', threshold=12200)
    storage_config.appoint_external('hdd2', threshold=524200)
    creation_options.partition_config.with_partitioning_policy(0)  # for now external blobs and autosplit not compatible

    has_external = True

    def scheme(table_name):
        return has_item(
            has_properties(
                TableName='__user__{}'.format(table_name),
                ColumnFamilies={
                    0: {'Large': 524200,
                        'Cache': 0,
                        'InMemory': False,
                        'Codec': 0,
                        'Small': 12200,
                        'RoomID': 0,
                        'Columns': [1, 2]}
                },
                Rooms={
                    0: {'Main': 2,
                        'Outer': 2,
                        'Blobs': 2,
                        'ExternalBlobs': [2]}
                }
            )
        )

    return (creation_options, has_external, scheme)


def case_8():
    creation_options = CreateTableRequest.Options()
    creation_options.add_column('key', PType.Uint64, is_key=True)
    creation_options.add_column('value', PType.Utf8, column_family=0)
    storage_config = creation_options.declare_column_family(family_id=0)
    storage_config.appoint_syslog('hdd')
    storage_config.appoint_log('hdd1')
    storage_config.appoint_data('hdd2', threshold=0)
    storage_config.appoint_external('hdd2', threshold=0)

    has_external = False

    def scheme(table_name):
        return has_item(
            has_properties(
                TableName='__user__{}'.format(table_name),
                ColumnFamilies={
                    0: {'Large': 4294967295,
                        'Cache': 0,
                        'InMemory': False,
                        'Codec': 0,
                        'Small': 4294967295,
                        'RoomID': 0,
                        'Columns': [1, 2]}
                },
                Rooms={
                    0: {'Main': 2,
                        'Outer': 2,
                        'Blobs': 2,
                        'ExternalBlobs': [2]}
                }
            )
        )

    return (creation_options, has_external, scheme)


def case_9():
    creation_options = CreateTableRequest.Options()
    creation_options.add_column('key', PType.Uint64, is_key=True)
    creation_options.add_column('value', PType.Utf8, column_family=0)
    storage_config = creation_options.declare_column_family(family_id=0)
    storage_config.appoint_syslog('hdd')
    storage_config.appoint_log('hdd')
    storage_config.appoint_data('hdd', threshold=12288)
    storage_config.appoint_external('hdd', threshold=524288)
    creation_options.partition_config.with_partitioning_policy(0)  # for now external blobs and autosplit not compatible

    has_external = True

    def scheme(table_name):
        return has_item(
            has_properties(
                TableName='__user__{}'.format(table_name),
                ColumnFamilies={
                    0: {'Large': 524288,
                        'Cache': 0,
                        'InMemory': False,
                        'Codec': 0,
                        'Small': 12288,
                        'RoomID': 0,
                        'Columns': [1, 2]}
                },
                Rooms={
                    0: {'Main': 1,
                        'Outer': 1,
                        'Blobs': 1,
                        'ExternalBlobs': [1]}
                }
            )
        )

    return (creation_options, has_external, scheme)


def case_10():
    creation_options = CreateTableRequest.Options()
    creation_options.add_column('key', PType.Uint64, is_key=True)
    creation_options.add_column('value', PType.Utf8, column_family=0)
    storage_config = creation_options.declare_column_family(family_id=0)
    storage_config.appoint_syslog('hdd')
    storage_config.appoint_log('hdd')
    storage_config.appoint_data('hdd', threshold=12288)
    storage_config.appoint_external('hdd', threshold=2*1024*1024+1)
    creation_options.partition_config.with_partitioning_policy(0)  # for now external blobs and autosplit not compatible

    has_external = False

    def scheme(table_name):
        return has_item(
            has_properties(
                TableName='__user__{}'.format(table_name),
                ColumnFamilies={
                    0: {'Large': 2*1024*1024+1,
                        'Cache': 0,
                        'InMemory': False,
                        'Codec': 0,
                        'Small': 12288,
                        'RoomID': 0,
                        'Columns': [1, 2]}
                },
                Rooms={
                    0: {'Main': 1,
                        'Outer': 1,
                        'Blobs': 1,
                        'ExternalBlobs': [1]}
                }
            )
        )

    return (creation_options, has_external, scheme)


TESTS = [
    case_0,
    case_1,
    case_2,
    case_3,
    case_4,
    case_5,
    case_6,
    case_7,
    case_8,
    case_9,
    case_10,
    case_11,
    case_12,
]


class TestStorageConfig(TBaseTenant):
    LOG_SETTINGS = {
        'LOCAL': LogLevels.DEBUG,
        'TX_DATASHARD': LogLevels.DEBUG,
        'FLAT_TX_SCHEMESHARD': LogLevels.DEBUG,
        'TX_PROXY': LogLevels.DEBUG,
        'OPS_COMPACT': LogLevels.INFO,
    }

    def test_create_tablet(self):
        table_path = '%s/table-1' % (self.tenant_path)

        with self.pool.checkout() as session:
            session.execute_scheme(
                "create table `{}` (key Int32, value String, primary key(key));".format(
                    table_path
                )
            )

            session.transaction().execute(
                "upsert into `{}` (key) values (101);".format(table_path),
                commit_tx=True,
            )

            session.transaction().execute(
                "select key from `{}`;".format(table_path),
            )

            session.execute_scheme(
                "drop table `{}`;".format(table_path),
            )

    @pytest.mark.parametrize(
        "creation_options, has_external, matcher", [x() for x in TESTS],
        ids=[x.__name__ for x in TESTS]
    )
    def test_cases(self,  creation_options, has_external, matcher):
        table_name = 'user_table_%d' % random.randint(1, 10000)
        table_path = '%s/%s' % (self.tenant_path, table_name)
        table_operations = scheme_operations.TableOperations(self.kikimr.client)
        table_operations.create_and_wait_and_assert(table_path, options=creation_options)

        write_huge_blobs(self.pool, table_path)

        assert_that(
            external_blobs_is_present(
                self.kikimr.nodes[1].monitor,
                list_tablets_in_table(self.kikimr.client, table_path)
            ),
            equal_to(has_external)
        )

        for tablet in list_tablets_in_table(self.kikimr.client, table_path):
            schema = get_scheme(self.kikimr.client, tablet)
            logger.debug("%s", schema)
            assert_that(
                schema,
                matcher(table_name)
            )

        table_operations.remove_and_wait_and_assert(table_path)
