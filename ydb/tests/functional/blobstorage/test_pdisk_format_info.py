#!/usr/bin/env python
# -*- coding: utf-8 -*-
from hamcrest import assert_that, equal_to

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.common import msgbus_types


class TestPDiskInfo(object):
    """
    See ticket KIKIMR-1831
    """
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test_read_disk_state(self):
        pdisk_infos = self.cluster.config.pdisks_info
        pdisk_paths = []

        for pdisk_info in pdisk_infos:
            pdisk_paths.append(pdisk_info['pdisk_path'])

        for path in pdisk_paths:
            response = self.cluster.client.update_drive_status(
                self.cluster.nodes[1].host,
                self.cluster.nodes[1].ic_port,
                path,
                msgbus_types.EDriveStatus.ACTIVE,
            )

            assert_that(
                response.Status,
                equal_to(
                    msgbus_types.MessageBusStatus.MSTATUS_OK
                )
            )

        for path in pdisk_paths:
            response = self.cluster.client.read_drive_status(
                self.cluster.nodes[1].host,
                self.cluster.nodes[1].ic_port,
                path,
            )
            assert_that(
                response.Status,
                equal_to(
                    1
                )
            )

            assert_that(
                response.BlobStorageConfigResponse.Status[0].DriveStatus[0].Path,
                equal_to(
                    path
                )
            )
