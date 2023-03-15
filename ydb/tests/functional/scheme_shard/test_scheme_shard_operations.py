# -*- coding: utf-8 -*-
import os

from hamcrest import assert_that, raises
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.oss.ydb_sdk_import import ydb


class TestSchemeShardSimpleOps(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()

        host = cls.cluster.nodes[1].host
        port = cls.cluster.nodes[1].grpc_port
        cls.root_dir = 'Root'
        cls.ydb_client = ydb.Driver(ydb.DriverConfig("%s:%s" % (host, port)))
        cls.ydb_client.wait(timeout=5)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_delete_table_that_doesnt_exist_failure(self):
        session = self.ydb_client.table_client.session().create()

        def callee():
            session.drop_table(
                os.path.join(
                    '/', self.root_dir, 'test_delete_table_that_doesnt_exist_failure'
                )
            )

        assert_that(
            callee,
            raises(
                ydb.SchemeError,
                "Path does not exist"
            )
        )

    def test_create_and_drop_table_many_times_in_range(self):
        dirname = 'test_create_and_drop_table_many_times_in_range'
        tablename = 'test_create_and_drop_table_many_times_in_range'
        for _ in range(10):
            self.ydb_client.scheme_client.make_directory(
                os.path.join(
                    '/', self.root_dir, dirname
                )
            )

            session = self.ydb_client.table_client.session().create()
            session.create_table(
                os.path.join('/', self.root_dir, dirname, tablename),
                ydb.TableDescription()
                .with_primary_keys('key')
                .with_columns(
                    ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint32)),
                )
            )

            session.drop_table(
                os.path.join('/', self.root_dir, dirname, tablename),
            )

            self.ydb_client.scheme_client.remove_directory(
                os.path.join(
                    '/', self.root_dir, dirname
                )
            )

    def test_create_path_with_long_name_failed(self):
        # Act
        def callee():
            name = 'test_create_path_with_long_name_success' * 500
            self.ydb_client.scheme_client.make_directory(
                os.path.join(
                    '/', self.root_dir, name,
                )
            )

        # Assert
        assert_that(
            callee,
            raises(
                ydb.SchemeError,
                "path part is too long"
            )
        )

    def test_delete_directory_from_leaf_success(self):
        base_name = 'test_delete_directory_from_leaf_success_%d'
        dir_names = tuple([base_name % idx for idx in range(10)])
        # Arrange: create too deep path
        for prefix_size in range(1, len(dir_names) + 1):
            path = os.path.join(self.root_dir, *dir_names[:prefix_size])
            self.ydb_client.scheme_client.make_directory(path)

        # Act + Assert: now let's try to delete that
        for suffix_size in range(len(dir_names), 0, -1):
            path = os.path.join(self.root_dir, *dir_names[:suffix_size])
            self.ydb_client.scheme_client.remove_directory(path)

    def test_create_many_directories_success(self):
        base_name = 'test_create_many_directories_success_%d'
        dir_names = tuple([base_name % idx for idx in range(10)])
        # Arrange: create too deep path
        for dir_name in dir_names:
            self.ydb_client.scheme_client.make_directory(
                os.path.join(
                    self.root_dir,
                    dir_name
                )
            )

        # Act + Assert: now let's try to delete that
        for dir_name in dir_names:
            self.ydb_client.scheme_client.remove_directory(
                os.path.join(
                    self.root_dir,
                    dir_name
                )
            )

    def test_create_table_and_path_with_name_clash_unsuccessful(self):
        name = 'test_create_table_and_path_with_name_clash_unsuccessful'
        self.ydb_client.scheme_client.make_directory(
            os.path.join(
                self.root_dir,
                name,
            )
        )

        def callee():
            session = self.ydb_client.table_client.session().create()
            session.create_table(
                os.path.join(self.root_dir, name),
                ydb.TableDescription()
                .with_primary_key('key')
                .with_columns(
                    ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Utf8)),
                )
            )

        assert_that(
            callee,
            raises(
                ydb.SchemeError,
                "unexpected path type"
            )
        )

    def test_when_create_path_second_time_then_it_is_ok(self):
        for _ in range(3):
            self.ydb_client.scheme_client.make_directory(
                os.path.join(
                    self.root_dir,
                    'test_when_create_path_second_time_then_it_is_ok'
                )
            )

    def test_when_delete_path_with_folder_then_get_error_response(self):
        # Arrange
        self.ydb_client.scheme_client.make_directory(
            os.path.join(
                self.root_dir,
                'test_when_delete_path_with_folder_then_get_error_response'
            )
        )

        self.ydb_client.scheme_client.make_directory(
            os.path.join(
                self.root_dir,
                'test_when_delete_path_with_folder_then_get_error_response',
                'sub_path_1'
            )
        )

        # Act
        def callee():
            self.ydb_client.scheme_client.remove_directory(
                os.path.join(
                    self.root_dir,
                    'test_when_delete_path_with_folder_then_get_error_response',
                )
            )

        # Assert
        assert_that(
            callee,
            raises(
                ydb.SchemeError,
                'path has children'
            )
        )

    def test_given_table_when_drop_table_and_create_with_same_scheme_then_ok(self):
        table_name = 'test_given_table_when_drop_table_and_create_with_same_scheme_then_ok'

        # Act + Assert
        session = self.ydb_client.table_client.session().create()
        for _ in range(3):
            session.create_table(
                os.path.join(self.root_dir, table_name),
                ydb.TableDescription()
                .with_primary_key('key')
                .with_columns(
                    ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint32))
                )
                .with_profile(
                    ydb.TableProfile()
                    .with_partitioning_policy(
                        ydb.PartitioningPolicy()
                        .with_uniform_partitions(100)
                    )
                )
            )

            session.drop_table(
                os.path.join(
                    self.root_dir,
                    table_name
                )
            )

    def test_given_table_when_drop_table_and_create_with_other_keys_then_ok(self):
        table_name = 'test_create_table_with_other_scheme'

        # Arrange
        session = self.ydb_client.table_client.session().create()
        session.create_table(
            os.path.join(self.root_dir, table_name),
            ydb.TableDescription()
            .with_primary_key('key')
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Utf8)),
            )
        )

        # Act + Assert
        session.drop_table(os.path.join(self.root_dir, table_name))

        session.create_table(
            os.path.join(self.root_dir, table_name),
            ydb.TableDescription()
            .with_primary_keys('key1', 'key2')
            .with_columns(
                ydb.Column('key1', ydb.OptionalType(ydb.PrimitiveType.Utf8)),
                ydb.Column('key2', ydb.OptionalType(ydb.PrimitiveType.Utf8)),
            )
        )

        session.drop_table(os.path.join(self.root_dir, table_name))

    def test_given_table_when_drop_table_and_create_with_same_primary_key_and_other_scheme_then_ok(self):
        table_name = 'test_given_table_when_drop_table_and_create_with_same_primary_key_and_other_scheme_then_ok'

        # Arrange
        session = self.ydb_client.table_client.session().create()
        session.create_table(
            os.path.join(self.root_dir, table_name),
            ydb.TableDescription()
            .with_columns(ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint32)))
            .with_primary_keys('key')
            .with_profile(
                ydb.TableProfile().with_partitioning_policy(
                    ydb.PartitioningPolicy().with_uniform_partitions(
                        100
                    )
                )
            )
        )

        session.drop_table(
            os.path.join(self.root_dir, table_name),
        )

        session.create_table(
            os.path.join(self.root_dir, table_name),
            ydb.TableDescription()
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint32)),
                ydb.Column('newkey2', ydb.OptionalType(ydb.PrimitiveType.String)),
            )
            .with_primary_keys('key', 'newkey2')
            .with_profile(
                ydb.TableProfile().with_partitioning_policy(
                    ydb.PartitioningPolicy().with_uniform_partitions(
                        50
                    )
                )
            )
        )

        session.drop_table(
            os.path.join(self.root_dir, table_name),
        )

    def test_ydb_create_and_remove_directory_success(self):
        self.ydb_client.scheme_client.make_directory('/Root/test_ydb_create_and_remove_directory_success')
        self.ydb_client.scheme_client.remove_directory('/Root/test_ydb_create_and_remove_directory_success')

    def test_ydb_remove_directory_that_does_not_exist_failure(self):

        def callee():
            self.ydb_client.scheme_client.remove_directory(
                '/Root/test_ydb_remove_directory_that_does_not_exist_failure')

        assert_that(
            callee,
            raises(
                ydb.SchemeError,
                "Path does not exist"
            )
        )
