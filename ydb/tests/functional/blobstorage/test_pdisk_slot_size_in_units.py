#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import pytest
import requests
import json

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels
from ydb.core.protos import msgbus_pb2
from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

CONST_64_GB = 64 * 1024**3


class TestPDiskSlotSizeInUnits(object):
    erasure = Erasure.NONE
    pool_name = 'test:1'
    nodes_count = 1

    @pytest.fixture(autouse=True)
    def setup(self):
        log_configs = {
            'BS_NODE': LogLevels.DEBUG,
            'BS_CONTROLLER': LogLevels.DEBUG,
            'BS_SKELETON': LogLevels.DEBUG,
            'BS_PDISK': LogLevels.DEBUG,
        }

        configurator = KikimrConfigGenerator(
            erasure=self.erasure,
            nodes=self.nodes_count,
            use_in_memory_pdisks=False,
            static_pdisk_size=CONST_64_GB,
            dynamic_pdisks=[{'disk_size': CONST_64_GB, 'user_kind': 1}],
            dynamic_pdisks_config=dict(expected_slot_count=4),
            dynamic_storage_pools=[dict(name=self.pool_name, kind="hdd", pdisk_user_kind=1)],
            additional_log_configs=log_configs
        )

        assert len(configurator.pdisks_info) == 2
        self.cluster = KiKiMR(configurator)
        self.cluster.start()

        host_config = self.cluster.client.read_host_configs()[0]
        assert len(host_config.Drive) == 2
        assert host_config.Drive[1].PDiskConfig.ExpectedSlotCount == 4

        base_config = self.cluster.client.query_base_config().BaseConfig
        logger.info(f"{base_config=}")

        self.pdisk_id = base_config.PDisk[1].PDiskId
        self.groups = [group for group in base_config.Group if group.StoragePoolId == 1]
        self.groups.sort(key=lambda g: g.GroupId)
        assert len(self.groups) == 2
        for g in self.groups:
            assert len(g.VSlotId) == 1
            assert g.VSlotId[0].PDiskId == self.pdisk_id

        self.driver = ydb.Driver(
            endpoint=self.cluster.nodes[1].endpoint,
            database='/Root',
        )
        self.driver.wait(timeout=20, fail_fast=True)
        self.session_pool = ydb.QuerySessionPool(self.driver)

        yield
        self.session_pool.stop()
        self.driver.stop()
        self.cluster.stop()

    def change_group_size_in_units(self, new_size, group_id=None):
        storage_pool = self.cluster.client.read_storage_pools()[0]

        request = msgbus_pb2.TBlobStorageConfigRequest()
        request.Domain = 1

        cmd = request.Request.Command.add().ChangeGroupSizeInUnits
        cmd.BoxId = storage_pool.BoxId
        cmd.StoragePoolId = storage_pool.StoragePoolId
        cmd.SizeInUnits = new_size
        if group_id:
            cmd.GroupId.append(group_id)
        cmd.ItemConfigGeneration = storage_pool.ItemConfigGeneration

        logger.info(f"change_group_size_in_units request: {request}")
        response = self.cluster.client.send(request, 'BlobStorageConfig').BlobStorageConfigResponse
        logger.info(f"change_group_size_in_units response: {response}")

        if not response.Success:
            raise RuntimeError(f'change_group_size_in_units request failed: {response.ErrorDescription}')

        for i, status in enumerate(response.Status):
            if not status.Success:
                raise RuntimeError(f'change_group_size_in_units has failed status[{i}]: {status.ErrorDescription}')

    def change_pdisk_slot_size_in_units(self, slot_size_in_units):
        host_config = self.cluster.client.read_host_configs()[0]
        host_config.Drive[1].PDiskConfig.SlotSizeInUnits = slot_size_in_units
        self.cluster.client.define_host_configs([host_config])

    def http_get(self, url):
        host = self.cluster.nodes[1].host
        port = self.cluster.nodes[1].mon_port
        return requests.get("http://%s:%s%s" % (host, port, url))

    def get_storage_groups(self):
        response = self.http_get('/storage/groups?fields_required=all&with=all').json()
        groups = [group for group in response['StorageGroups'] if group['PoolName'] == self.pool_name]
        groups.sort(key=lambda g: g['GroupId'])
        assert len(groups) == 2
        for group in groups:
            vdisk = group['VDisks'][0]
            assert vdisk['Whiteboard'].get('VDiskState') == 'OK'
            assert vdisk['PDisk']['Whiteboard'].get('State') == 'Normal'
        return groups

    def get_pdisk_info(self):
        response = self.http_get('/pdisk/info?node_id=1&pdisk_id=%s' % self.pdisk_id).json()
        return response

    def check_group(self, group, expected_vdisk_weight, expected_num_active_slots):
        vdisk = group['VDisks'][0]
        assert vdisk['PDisk']['Whiteboard']['NumActiveSlots'] == expected_num_active_slots
        assert int(vdisk['Whiteboard']['AllocatedSize']) + int(vdisk['Whiteboard']['AvailableSize']) == \
            expected_vdisk_weight * int(vdisk['PDisk']['Whiteboard']['EnforcedDynamicSlotSize'])

    def check_pdisk(self, pdisk, expected_num_active_slots=None, expected_slot_size_in_units=None):
        if expected_num_active_slots is not None:
            assert pdisk['NumActiveSlots'] == expected_num_active_slots
        if expected_slot_size_in_units is not None:
            assert pdisk['SlotSizeInUnits'] == expected_slot_size_in_units

    def query_sysview_ds_groups(self):
        return self.session_pool.execute_with_retries(
            """SELECT * FROM `.sys/ds_groups` WHERE GroupId > 0 ORDER BY GroupId""",
        )[0].rows

    def query_sysview_ds_vslots(self):
        return self.session_pool.execute_with_retries(
            """SELECT * FROM `.sys/ds_vslots` WHERE GroupId > 0 ORDER BY GroupId""",
        )[0].rows

    def query_sysview_ds_pdisks(self):
        return self.session_pool.execute_with_retries(
            f"""SELECT * FROM `.sys/ds_pdisks` WHERE NodeId = 1 AND PDiskId = {self.pdisk_id}""",
        )[0].rows

    def get_total_sizes(self, rows):
        return [(row.get('AllocatedSize') or 0) + (row.get('AvailableSize') or 0) for row in rows]

    def check_sysview_populated(self):
        groups = self.query_sysview_ds_groups()
        logger.info(json.dumps(groups, default=str))
        assert len(groups) == 2
        total_sizes = self.get_total_sizes(groups)
        assert all([sz > 0 for sz in total_sizes])
        assert total_sizes[0] == total_sizes[1]
        initial_total_size = total_sizes[0]

        vslots = self.query_sysview_ds_vslots()
        logger.info(json.dumps(vslots, default=str))
        assert len(vslots) == 2
        assert all([sz == initial_total_size for sz in self.get_total_sizes(vslots)])

        pdisks = self.query_sysview_ds_pdisks()
        logger.info(json.dumps(pdisks, default=str))
        assert len(pdisks) == 1
        assert pdisks[0]['NumActiveSlots'] == 2

        return initial_total_size

    def test_change_group_size_in_units(self):
        initial_total_size = retry_assertions(self.check_sysview_populated)

        self.change_group_size_in_units(new_size=2, group_id=self.groups[0].GroupId)

        def check_whiteboard_updated():
            groups = self.get_storage_groups()
            logger.info(json.dumps(groups, indent=2))
            self.check_group(groups[0], expected_vdisk_weight=2, expected_num_active_slots=3)
            self.check_group(groups[1], expected_vdisk_weight=1, expected_num_active_slots=3)
        retry_assertions(check_whiteboard_updated)

        def check_pdisk_info_updated():
            pdisk_info = self.get_pdisk_info()
            logger.info(json.dumps(pdisk_info, indent=2))
            self.check_pdisk(pdisk_info['Whiteboard']['PDisk'], expected_num_active_slots=3)
            self.check_pdisk(pdisk_info['BSC']['PDisk'], expected_num_active_slots=3, expected_slot_size_in_units=0)
        retry_assertions(check_pdisk_info_updated)

        def check_sysview_updated():
            groups = self.query_sysview_ds_groups()
            logger.info(json.dumps(groups, default=str))
            assert len(groups) == 2
            assert groups[0]['GroupSizeInUnits'] == 2
            assert groups[1]['GroupSizeInUnits'] == 0
            total_sizes = self.get_total_sizes(groups)
            assert total_sizes[0] == 2 * initial_total_size
            assert total_sizes[1] == 1 * initial_total_size

            vslots = self.query_sysview_ds_vslots()
            logger.info(json.dumps(vslots, default=str))
            assert self.get_total_sizes(vslots) == total_sizes

            pdisks = self.query_sysview_ds_pdisks()
            logger.info(json.dumps(pdisks, default=str))
            assert len(pdisks) == 1
            assert pdisks[0]['NumActiveSlots'] == 3
        retry_assertions(check_sysview_updated)

    def test_change_pdisk_slot_size_in_units(self):
        initial_total_size = retry_assertions(self.check_sysview_populated)

        self.change_group_size_in_units(new_size=2, group_id=self.groups[1].GroupId)
        self.change_pdisk_slot_size_in_units(slot_size_in_units=2)
        self.change_group_size_in_units(new_size=4, group_id=self.groups[1].GroupId)

        def check_whiteboard_updated():
            groups = self.get_storage_groups()
            logger.info(json.dumps(groups, indent=2))
            self.check_group(groups[0], expected_vdisk_weight=1, expected_num_active_slots=3)
            self.check_group(groups[1], expected_vdisk_weight=2, expected_num_active_slots=3)
        retry_assertions(check_whiteboard_updated)

        def check_bsc_updated():
            base_config = self.cluster.client.query_base_config().BaseConfig
            logger.info(base_config.PDisk[1])
            assert base_config.PDisk[1].PDiskMetrics.SlotSizeInUnits == 2
        retry_assertions(check_bsc_updated)

        def check_pdisk_info_updated():
            pdisk_info = self.get_pdisk_info()
            logger.info(json.dumps(pdisk_info, indent=2))
            self.check_pdisk(pdisk_info['Whiteboard']['PDisk'], expected_num_active_slots=3)
            self.check_pdisk(pdisk_info['BSC']['PDisk'], expected_num_active_slots=3, expected_slot_size_in_units=2)
        retry_assertions(check_pdisk_info_updated)

        def check_sysview_updated():
            groups = self.query_sysview_ds_groups()
            logger.info(json.dumps(groups, default=str))
            assert len(groups) == 2
            assert groups[0]['GroupSizeInUnits'] == 0
            assert groups[1]['GroupSizeInUnits'] == 4
            total_sizes = self.get_total_sizes(groups)
            assert total_sizes[0] == 1 * initial_total_size
            assert total_sizes[1] == 2 * initial_total_size

            vslots = self.query_sysview_ds_vslots()
            logger.info(json.dumps(vslots, default=str))
            assert self.get_total_sizes(vslots) == total_sizes

            pdisks = self.query_sysview_ds_pdisks()
            logger.info(json.dumps(pdisks, default=str))
            assert len(pdisks) == 1
            assert pdisks[0]['NumActiveSlots'] == 3
        retry_assertions(check_sysview_updated)
