#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import logging
import pytest
import requests
import resource
import json

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels
from ydb.core.protos import msgbus_pb2

logger = logging.getLogger(__name__)

CONST_64_GB = 64 * 1024**3


class TestPDiskSlotSizeInUnits(object):
    erasure = Erasure.NONE
    pool_name = 'test:1'
    nodes_count = 1

    @pytest.fixture(autouse=True)
    def setup(self):
        resource.setrlimit(
            resource.RLIMIT_CORE,
            (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

        log_configs = {
            'BS_NODE': LogLevels.DEBUG,
            'BS_CONTROLLER': LogLevels.DEBUG,
            'BS_SKELETON': LogLevels.INFO,
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
        assert len(self.groups) == 2
        for g in self.groups:
            assert len(g.VSlotId) == 1
            assert g.VSlotId[0].PDiskId == self.pdisk_id

        yield
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

    def retriable(self, check_fn, timeout=30, delay=1):
        deadline = time.time() + timeout

        while True:
            try:
                return check_fn()
            except AssertionError as e:
                logger.info(str(e))
                if time.time() > deadline:
                    raise e
                else:
                    time.sleep(delay)

    def http_get(self, url):
        host = self.cluster.nodes[1].host
        port = self.cluster.nodes[1].mon_port
        return requests.get("http://%s:%s%s" % (host, port, url))

    def get_storage_groups(self):
        response = self.http_get('/storage/groups?fields_required=all&with=all').json()
        groups = [group for group in response['StorageGroups'] if group['PoolName'] == self.pool_name]
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

    def test_change_group_size_in_units(self):
        self.change_group_size_in_units(new_size=2, group_id=self.groups[0].GroupId)

        def wait_whiteboard_updated():
            groups = self.get_storage_groups()
            logger.info(json.dumps(groups, indent=2))
            self.check_group(groups[0], expected_vdisk_weight=2, expected_num_active_slots=3)
            self.check_group(groups[1], expected_vdisk_weight=1, expected_num_active_slots=3)
        self.retriable(wait_whiteboard_updated)

        pdisk_info = self.get_pdisk_info()
        logger.info(json.dumps(pdisk_info, indent=2))

        self.check_pdisk(pdisk_info['Whiteboard']['PDisk'], expected_num_active_slots=3)
        self.check_pdisk(pdisk_info['BSC']['PDisk'], expected_num_active_slots=3, expected_slot_size_in_units=0)

    def test_change_pdisk_slot_size_in_units(self):
        self.change_pdisk_slot_size_in_units(slot_size_in_units=2)
        self.change_group_size_in_units(new_size=4, group_id=self.groups[1].GroupId)

        def wait_whiteboard_updated():
            groups = self.get_storage_groups()
            logger.info(json.dumps(groups, indent=2))
            self.check_group(groups[0], expected_vdisk_weight=1, expected_num_active_slots=3)
            self.check_group(groups[1], expected_vdisk_weight=2, expected_num_active_slots=3)
        self.retriable(wait_whiteboard_updated)

        pdisk_info = self.get_pdisk_info()
        logger.info(json.dumps(pdisk_info, indent=2))

        self.check_pdisk(pdisk_info['Whiteboard']['PDisk'], expected_num_active_slots=3)
        self.check_pdisk(pdisk_info['BSC']['PDisk'], expected_num_active_slots=3, expected_slot_size_in_units=2)
