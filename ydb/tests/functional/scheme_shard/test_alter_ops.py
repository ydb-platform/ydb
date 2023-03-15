#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from hamcrest import assert_that, equal_to, raises

from ydb.tests.library.common.msgbus_types import MessageBusStatus
from ydb.tests.library.common.protobuf_ss import CreatePath, AlterTableRequest, CreateTableRequest
from ydb.tests.library.common.protobuf_ss import TPartitionConfig, SchemeDescribeRequest
from ydb.tests.library.common.types import PType
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.matchers.response_matchers import ProtobufWithStatusMatcher
from ydb.tests.oss.ydb_sdk_import import ydb


class TestSchemeShardAlterTest(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()

        host = cls.cluster.nodes[1].host
        port = cls.cluster.nodes[1].grpc_port
        cls.root_dir = os.path.join("/", "Root")
        cls.ydb_client = ydb.Driver(ydb.DriverConfig(
            database=cls.root_dir,
            endpoint="%s:%s" % (host, port)))
        cls.ydb_client.wait(timeout=5)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_alter_table_cant_add_existing_column(self):
        table_name = 'test_alter_table_cant_add_existing_column'
        # Arrange: Create table with single key column
        session = self.ydb_client.table_client.session().create()
        session.create_table(
            os.path.join(self.root_dir, table_name),
            ydb.TableDescription()
            .with_primary_key('key')
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint32)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.String)),
            )
            .with_profile(
                ydb.TableProfile()
                .with_replication_policy(ydb.ReplicationPolicy().with_replicas_count(2))
                .with_partitioning_policy(
                    ydb.PartitioningPolicy()
                    .with_uniform_partitions(
                        100
                    )
                )
            )
        )

        # Act and Assert
        def callee():
            session.alter_table(
                os.path.join(self.root_dir, table_name),
                drop_columns=('value', ),
                add_columns=(
                    ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Uint32)),
                )
            )

        assert_that(
            callee,
            raises(
                ydb.BadRequest,
                "Cannot alter type for column"
            )
        )

    def test_alter_table_add_and_remove_column_many_times_success(self):
        table_name = 'test_alter_table_add_and_remove_column_many_times_success'
        # Arrange: Create table with single key column
        session = self.ydb_client.table_client.session().create()
        session.create_table(
            os.path.join(self.root_dir, table_name),
            ydb.TableDescription()
            .with_primary_key('key')
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint32)),
            )
            .with_profile(
                ydb.TableProfile()
                .with_replication_policy(ydb.ReplicationPolicy().with_replicas_count(2))
                .with_partitioning_policy(
                    ydb.PartitioningPolicy()
                    .with_uniform_partitions(
                        100
                    )
                )
            )
        )

        for it in range(0, 20):
            drop_columns, add_columns = (), ()
            if it % 2 == 0:
                add_columns = (ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8)), )
                expected_columns = ['key', 'value']
            else:
                drop_columns = ('value', )
                expected_columns = ['key']

            session.alter_table(
                os.path.join(self.root_dir, table_name),
                add_columns, drop_columns
            )

            columns = self.cluster.client.send(
                SchemeDescribeRequest(os.path.join(self.root_dir, table_name)).protobuf,
                'SchemeDescribe').PathDescription.Table.Columns
            assert_that(
                sorted([column.Name for column in columns]),
                equal_to(expected_columns),
                "Failed at step %d" % it
            )

    def test_alter_table_by_not_single_key_column_failure(self):
        table_name = 'test_alter_table_by_not_single_key_column_failure'
        # Arrange: Creating table with 2 keys
        session = self.ydb_client.table_client.session().create()
        session.create_table(
            os.path.join(self.root_dir, table_name),
            ydb.TableDescription()
            .with_primary_keys('key1', 'key2')
            .with_columns(
                ydb.Column('key1', ydb.OptionalType(ydb.PrimitiveType.Uint32)),
                ydb.Column('key2', ydb.OptionalType(ydb.PrimitiveType.Uint32)),
            )
            .with_profile(
                ydb.TableProfile()
                .with_replication_policy(ydb.ReplicationPolicy().with_replicas_count(2))
                .with_partitioning_policy(
                    ydb.PartitioningPolicy()
                    .with_uniform_partitions(
                        100
                    )
                )
            )
        )

        # Act and Assert
        def callee():
            session.alter_table(
                os.path.join(self.root_dir, table_name),
                add_columns=(),
                drop_columns=(
                    'key1',
                )
            )

        assert_that(
            callee,
            raises(
                ydb.BadRequest,
                "drop key column:"
            )
        )
        columns = self.cluster.client.send(
            SchemeDescribeRequest(os.path.join(self.root_dir, table_name)).protobuf,
            'SchemeDescribe').PathDescription.Table.Columns

        assert_that(
            sorted([column.Name for column in columns]),
            equal_to(
                ["key1", "key2"]
            )
        )

    def test_alter_table_by_single_key_column_failure(self):
        table_name = 'test_alter_table_by_single_key_column_failure'
        # Arrange: Create table with single key column
        session = self.ydb_client.table_client.session().create()
        session.create_table(
            os.path.join(self.root_dir, table_name),
            ydb.TableDescription()
            .with_primary_keys('key')
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint32)),
            )
            .with_profile(
                ydb.TableProfile()
                .with_replication_policy(ydb.ReplicationPolicy().with_replicas_count(2))
                .with_partitioning_policy(
                    ydb.PartitioningPolicy()
                    .with_uniform_partitions(
                        100
                    )
                )
            )
        )

        # Act and Assert
        def callee():
            session.alter_table(
                os.path.join(self.root_dir, table_name),
                add_columns=(),
                drop_columns=(
                    'key',
                )
            )

        assert_that(
            callee,
            raises(
                ydb.BadRequest,
                "drop key column:"
            )
        )
        columns = self.cluster.client.send(
            SchemeDescribeRequest(os.path.join(self.root_dir, table_name)).protobuf,
            'SchemeDescribe').PathDescription.Table.Columns

        assert_that(
            sorted([column.Name for column in columns]),
            equal_to(
                ["key"]
            )
        )

    def test_alter_table_add_column_after_table_creation_with_data_and_success(self):
        table_name = 'test_alter_table_add_column_after_table_creation_with_data_and_success'
        # Arrange: Creating table with some data
        source_table_name = os.path.join(self.root_dir, table_name)
        session = self.ydb_client.table_client.session().create()
        session.create_table(
            os.path.join(self.root_dir, table_name),
            ydb.TableDescription()
            .with_primary_keys('key')
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint32)),
            )
            .with_profile(
                ydb.TableProfile()
                .with_replication_policy(ydb.ReplicationPolicy().with_replicas_count(2))
                .with_partitioning_policy(
                    ydb.PartitioningPolicy()
                    .with_uniform_partitions(
                        100
                    )
                )
            )
        )

        session.transaction().execute(
            'select * from `{}`'.format(
                source_table_name
            )
        )

        session.alter_table(
            source_table_name,
            drop_columns=(),
            add_columns=(
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8)),
            )
        )

        for key in range(1, 10):
            session.transaction().execute(
                'UPSERT INTO `{}` (key, value) VALUES ({}, "{}");'.format(source_table_name, key, 'value%d' % key),
                commit_tx=True
            )

        for key in range(1, 10):
            result_sets = session.transaction().execute(
                'select value from `{}` where key={};'.format(source_table_name, key)
            )

            assert_that(
                'value%d' % key,
                equal_to(
                    result_sets[0].rows[0].value
                )
            )

    def test_alter_table_can_change_partition_config_options(self):
        dirname = 'test_alter_table_can_change_partition_config_options'
        tablename = 'test_alter_table_can_change_partition_config_options'
        # Arrange
        self.cluster.client.send_and_poll_request(CreatePath(self.root_dir, dirname).protobuf)
        self.cluster.client.send_and_poll_request(
            CreateTableRequest(os.path.join(self.root_dir, dirname), tablename).add_column(
                'key', PType.Uint32, is_key=True).with_partitions(100).protobuf,
        )

        # Act
        scheme = self.cluster.client.send(
            SchemeDescribeRequest(os.path.join(self.root_dir, dirname, tablename)).with_partition_config().protobuf,
            method='SchemeDescribe'
        ).PathDescription.Table.PartitionConfig
        self.cluster.client.send_and_poll_request(
            AlterTableRequest(os.path.join(self.root_dir, dirname), tablename)
            .with_partition_config(
                TPartitionConfig()
                .with_followers(1)
                .with_executor_cache_size(scheme.ExecutorCacheSize * 2)
                .with_tx_read_size_limit(scheme.TxReadSizeLimit * 3)
            ).protobuf,
        )

        # Assert
        new_scheme = self.cluster.client.send(
            SchemeDescribeRequest(os.path.join(self.root_dir, dirname, tablename)).with_partition_config().protobuf,
            method='SchemeDescribe'
        ).PathDescription.Table.PartitionConfig
        assert_that(new_scheme.ExecutorCacheSize, equal_to(scheme.ExecutorCacheSize * 2))
        assert_that(new_scheme.TxReadSizeLimit, equal_to(scheme.TxReadSizeLimit * 3))

    def test_alter_table_can_change_compaction_policy_options(self):
        table_name = 'test_alter_table_can_change_compaction_policy_options'
        # Arrange
        self.cluster.client.send_and_poll_request(
            CreateTableRequest(self.root_dir, table_name)
            .add_column('key', PType.Uint32, is_key=True).with_partitions(100)
            .with_partition_config(
                TPartitionConfig().with_followers(1)
                .with_in_mem_force_size_to_snapshot(16777216)
                .with_in_mem_steps_to_snapshot(300)
            ).protobuf,
        )

        # Act
        c_policy = self.cluster.client.send(
            SchemeDescribeRequest(os.path.join(self.root_dir, table_name)).with_partition_config().protobuf,
            method='SchemeDescribe'
        ).PathDescription.Table.PartitionConfig.CompactionPolicy

        self.cluster.client.send_and_poll_request(
            AlterTableRequest(self.root_dir, table_name)
            .with_partition_config(
                TPartitionConfig()
                .with_followers(1)
                .with_in_mem_force_size_to_snapshot(c_policy.InMemForceSizeToSnapshot * 2)
                .with_in_mem_steps_to_snapshot(c_policy.InMemStepsToSnapshot // 2)
            ).protobuf,
        )

        # Assert
        new_c_policy = self.cluster.client.send(
            SchemeDescribeRequest(os.path.join(self.root_dir, table_name)).with_partition_config().protobuf,
            method='SchemeDescribe'
        ).PathDescription.Table.PartitionConfig.CompactionPolicy
        assert_that(new_c_policy.InMemForceSizeToSnapshot, equal_to(c_policy.InMemForceSizeToSnapshot * 2))
        assert_that(new_c_policy.InMemStepsToSnapshot, equal_to(c_policy.InMemStepsToSnapshot // 2))

    def test_alter_table_decreasing_number_of_generations_it_is_raise_error(self):
        table_name = 'test_alter_table_decreasing_number_of_generations_it_is_raise_error'
        # Arrange
        self.cluster.client.send_and_poll_request(
            CreateTableRequest(self.root_dir, table_name)
            .add_column('key', PType.Uint32, is_key=True).with_partitions(100)
            .protobuf
        )

        # Act + Assert
        resp = self.cluster.client.send(
            AlterTableRequest(self.root_dir, table_name)
            .with_partition_config(
                TPartitionConfig().with_followers(1)
                .with_in_mem_force_size_to_snapshot(16777216)
                .with_in_mem_steps_to_snapshot(300)
            ).protobuf,
            method='SchemeOperation',
        )
        assert_that(
            resp,
            ProtobufWithStatusMatcher(
                MessageBusStatus.MSTATUS_ERROR,
                error_reason="Decreasing number of levels in compaction policy in not supported, "
                             "old level count 3, new level count 0"
            )
        )

    def test_alter_table_after_create_table_it_is_success(self):
        dirname = 'test_alter_table_after_create_table_it_is_success'
        tablename = 'test_alter_table_after_create_table_it_is_success'
        assert_that(
            self.cluster.client.send_and_poll_request(CreatePath(self.root_dir, dirname).protobuf),
            ProtobufWithStatusMatcher()
        )
        assert_that(
            self.cluster.client.send_and_poll_request(
                CreateTableRequest(os.path.join(self.root_dir, dirname), tablename).add_column(
                    'key', PType.Uint32, is_key=True).with_partitions(100).protobuf,
            ),
            ProtobufWithStatusMatcher()
        )
        assert_that(
            self.cluster.client.send_and_poll_request(
                AlterTableRequest(os.path.join(self.root_dir, dirname), tablename).add_column(
                    'value', PType.Uint32).with_partitions(100).protobuf,
            ),
            ProtobufWithStatusMatcher()
        )

        columns = self.cluster.client.send(
            SchemeDescribeRequest(os.path.join(self.root_dir, dirname, tablename)).protobuf,
            method='SchemeDescribe'
        ).PathDescription.Table.Columns
        column_names = [column.Name for column in columns]
        assert_that(
            sorted(column_names),
            equal_to(
                ["key", "value"]
            )
        )

        assert_that(
            self.cluster.client.send_and_poll_request(
                AlterTableRequest(os.path.join(self.root_dir, dirname), tablename).drop_column(
                    'value').protobuf,
            ),
            ProtobufWithStatusMatcher()
        )
        columns = self.cluster.client.send(
            SchemeDescribeRequest(os.path.join(self.root_dir, dirname, tablename)).protobuf,
            method='SchemeDescribe'
        ).PathDescription.Table.Columns
        column_names = [column.Name for column in columns]
        assert_that(
            sorted(column_names),
            equal_to(
                ["key"]
            )
        )
