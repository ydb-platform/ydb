# -*- coding: utf-8 -*-
import os
import string

from hamcrest import assert_that, raises
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.common.types import Erasure
from ydb.tests.oss.ydb_sdk_import import ydb


class Base(object):
    erasure = None

    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(
            KikimrConfigGenerator(
                erasure=cls.erasure,
                additional_log_configs={
                    'TENANT_POOL': LogLevels.DEBUG,
                    'KQP_PROXY': LogLevels.DEBUG
                }
            )
        )
        cls.cluster.start()

        cls.database_name = "/Root"
        cls.connection_params = ydb.DriverConfig("localhost:%d" % cls.cluster.nodes[1].port)
        cls.connection_params.set_database(cls.database_name)
        cls.driver = ydb.Driver(cls.connection_params)
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop()


class TestSchemeShardLimitsCase0(Base):
    erasure = Erasure.BLOCK_4_2

    def test_effective_acls_are_too_large(self):
        # Arrange
        directory_name = "/Root/test_effective_acls_are_too_large"
        self.driver.scheme_client.make_directory(directory_name)

        # Act
        def callee():
            for idx in string.ascii_lowercase:
                current_directory = os.path.join(directory_name, idx)
                self.driver.scheme_client.make_directory(current_directory)
                subject = idx * (6 * 10 ** 6) + '@staff'
                self.driver.scheme_client.modify_permissions(
                    current_directory,
                    ydb.ModifyPermissionsSettings()
                    .grant_permissions(
                        subject, (
                            'ydb.generic.read',
                        )
                    )
                )
            self.driver.scheme_client.describe_path(current_directory)

        # Assert
        assert_that(
            callee,
            raises(
                ydb.BadRequest,
                "ACL size limit exceeded"
            )
        )


class TestSchemeShardLimitsCase1(Base):
    erasure = Erasure.NONE

    def test_too_large_acls(self):
        # Arrange
        directory_name = "/Root/test_too_large_acls"
        self.driver.scheme_client.make_directory(directory_name)

        # Act
        def callee():
            for chr_val in string.ascii_lowercase:
                self.driver.scheme_client.modify_permissions(
                    directory_name,
                    ydb.ModifyPermissionsSettings()
                    .grant_permissions(
                        'gvit@staff', (
                            'ydb.generic.read',
                        )
                    )
                    .grant_permissions(
                        chr_val * 50 * 10 ** 6 + '@staff', (
                            'ydb.generic.read',
                        )
                    )
                )

        # Assert
        assert_that(
            callee,
            raises(
                ydb.BadRequest,
                "ACL size limit exceeded"
            )
        )
