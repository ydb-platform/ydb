# -*- coding: utf-8 -*-
import os

from hamcrest import assert_that, has_length, has_property, equal_to

from ydb.tests.library.common.protobuf_ss import SchemeDescribeRequest
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.oss.ydb_sdk_import import ydb


class TestSchemeShardCopyOps(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()

        host = cls.cluster.nodes[1].host
        port = cls.cluster.nodes[1].grpc_port
        cls.root_dir = os.path.join("/", "Root")
        cls.ydb_client = ydb.Driver(ydb.DriverConfig("%s:%s" % (host, port)))
        cls.ydb_client.wait(timeout=5)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_given_table_when_create_copy_of_it_then_ok(self):
        table_name = 'test_given_table_when_create_copy_of_it_then_ok'
        source_table_full_name = os.path.join('/', self.root_dir, table_name)

        # Arrange
        session = self.ydb_client.table_client.session().create()
        session.create_table(
            source_table_full_name,
            ydb.TableDescription()
            .with_primary_key('key')
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint32))
            )
            .with_profile(
                ydb.TableProfile()
                .with_partitioning_policy(
                    ydb.PartitioningPolicy()
                    .with_uniform_partitions(
                        100
                    )
                )
            )
        )

        # Act + Assert
        for idx in range(10):
            destination_name = source_table_full_name + '_copy_{}'.format(idx)
            session.copy_table(
                source_table_full_name,
                destination_name,
            )

    def test_when_copy_table_partition_config(self):
        source_table_name = 'test_when_copy_table_partition_config_is_default'
        destination_table_name = source_table_name + '_copy'

        source_table_full_name = os.path.join(self.root_dir, source_table_name)
        destination_table_full_name = os.path.join(self.root_dir, destination_table_name)

        # Arrange + Assert
        session = self.ydb_client.table_client.session().create()
        session.create_table(
            source_table_full_name,
            ydb.TableDescription()
            .with_primary_key('key')
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint32))
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

        description = self.cluster.client.send(
            SchemeDescribeRequest(source_table_full_name).with_partition_config().protobuf, 'SchemeDescribe'
        )

        assert_that(
            description.PathDescription.Table.PartitionConfig.FollowerGroups,
            has_length(
                equal_to(
                    1
                )
            )
        )

        assert_that(
            description.PathDescription.Table.PartitionConfig.FollowerGroups[0],
            has_property(
                'FollowerCount',
                equal_to(
                    2
                )
            )
        )

        # Act + Assert
        session.copy_table(source_table_full_name, destination_table_full_name)
        description = self.cluster.client.send(
            SchemeDescribeRequest(destination_table_full_name).with_partition_config().protobuf, 'SchemeDescribe'
        )

        assert_that(
            description.PathDescription.Table.PartitionConfig.FollowerGroups,
            has_length(
                equal_to(
                    1
                )
            )
        )

        assert_that(
            description.PathDescription.Table.PartitionConfig.FollowerGroups[0],
            has_property(
                'FollowerCount',
                equal_to(
                    2
                )
            )
        )
