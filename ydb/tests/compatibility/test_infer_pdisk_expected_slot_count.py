# -*- coding: utf-8 -*-
import pytest
import time
import logging
import yaml

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.library.compatibility.fixtures import init_stable_binary_path, init_stable_name
from ydb.tests.library.compatibility.fixtures import inter_stable_binary_path, inter_stable_name
from ydb.tests.library.compatibility.fixtures import current_binary_path, current_name
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import retry_assertions
from ydb.core.protos import blobstorage_config_pb2
from ydb.tests.library.clients.kikimr_dynconfig_client import DynConfigClient
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
import ydb.public.api.protos.draft.ydb_dynamic_config_pb2 as dynconfig

logger = logging.getLogger(__name__)

CONST_PDISK_PATH = "SectorMap:TestInferPDiskSettings:480"
CONST_INITIAL_SLOT_COUNT = 14
CONST_CUSTOM_SLOT_COUNT = 17
CONST_480_GB = 480 * 1024**3
CONST_10_GB = 10 * 1024**3

all_binary_combinations_restart = [
    [init_stable_binary_path, inter_stable_binary_path],
    [inter_stable_binary_path, current_binary_path],
    [init_stable_binary_path, current_binary_path],
]
all_binary_combinations_ids_restart = [
    "restart_{}_to_{}".format(init_stable_name, inter_stable_name),
    "restart_{}_to_{}".format(inter_stable_name, current_name),
    "restart_{}_to_{}".format(init_stable_name, current_name),
]


@pytest.mark.parametrize("base_setup",
                         argvalues=all_binary_combinations_restart,
                         ids=all_binary_combinations_ids_restart,
                         indirect=True)
class TestUpgradeThenRollback(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        cluster_generator = self.setup_cluster(
            erasure=Erasure.NONE,
            nodes=2,
            use_in_memory_pdisks=False)
        next(cluster_generator)

        host_configs = self.cluster.client.read_host_configs()
        for host_config in host_configs:
            drive = host_config.Drive.add()
            drive.Path = CONST_PDISK_PATH
            drive.PDiskConfig.ExpectedSlotCount = CONST_INITIAL_SLOT_COUNT
        self.cluster.client.define_host_configs(host_configs)
        self.dynconfig_client = DynConfigClient(self.cluster.nodes[1].host, self.cluster.nodes[1].grpc_port)

        yield

    def pdisk_list(self):
        """Equivalent to `dstool pdisk list`"""
        base_config = self.cluster.client.query_base_config()

        # Collect PDisk information
        pdisks_info = []
        for pdisk in base_config.BaseConfig.PDisk:
            if pdisk.Path != CONST_PDISK_PATH:
                continue
            pdisks_info.append(pdisk)
        return pdisks_info

    def generate_config(self):
        generate_config_response = self.dynconfig_client.fetch_startup_config()
        logger.info(f"{generate_config_response=}")
        assert generate_config_response.operation.status == StatusIds.SUCCESS

        result = dynconfig.FetchStartupConfigResult()
        generate_config_response.operation.result.Unpack(result)

        return {
            "metadata": {
                "kind": "MainConfig",
                "version": 0,
                "cluster": "",
            },
            "config": yaml.safe_load(result.config)
        }

    def replace_config(self, full_config):
        replace_config_response = self.dynconfig_client.replace_config(yaml.dump(full_config))
        logger.info(f"{replace_config_response=}")
        assert replace_config_response.operation.status == StatusIds.SUCCESS

    def test_setup_slot_size_manually(self):
        if self.versions[1] < (25, 3):
            pytest.skip("Supported since version 25.3")

        assert self.current_binary_paths_index == 0
        logger.info(f"Test started on {self.versions[0]} {time.time()=}")
        #################################################################

        t1 = time.time()
        timeout = 20

        def check_pdisks():
            for pdisk in self.pdisk_list():
                assert pdisk.Path == CONST_PDISK_PATH
                assert pdisk.PDiskConfig.ExpectedSlotCount == CONST_INITIAL_SLOT_COUNT
                assert pdisk.DriveStatus == blobstorage_config_pb2.EDriveStatus.ACTIVE
                assert pdisk.PDiskMetrics.TotalSize == CONST_480_GB
                if self.versions[0] < (25, 3):
                    assert not pdisk.PDiskMetrics.HasField('SlotCount')
                    assert not pdisk.PDiskMetrics.HasField('SlotSizeInUnits')
                else:
                    assert pdisk.PDiskMetrics.SlotCount == CONST_INITIAL_SLOT_COUNT
                    assert pdisk.PDiskMetrics.HasField('SlotSizeInUnits') and \
                        pdisk.PDiskMetrics.SlotSizeInUnits == 0
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 > t1
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 < t1 + timeout
        retry_assertions(check_pdisks, timeout)

        self.change_cluster_version()
        assert self.current_binary_paths_index == 1
        logger.info(f"Restarted on version {self.versions[1]} {time.time()=}")
        ######################################################################

        t2 = time.time()
        host_configs = self.cluster.client.read_host_configs()
        for host_config in host_configs:
            drive = host_config.Drive[1]
            assert drive.Path == CONST_PDISK_PATH
            drive.PDiskConfig.ExpectedSlotCount = CONST_CUSTOM_SLOT_COUNT
            drive.PDiskConfig.SlotSizeInUnits = 2
        self.cluster.client.define_host_configs(host_configs)
        logger.info(f"PDisk SlotSizeInUnits applied {time.time()=}")

        self.cluster.client.pdisk_set_all_active(pdisk_path=CONST_PDISK_PATH)
        logger.info(f"Drives activated {time.time()=}")

        deadline = time.time() + timeout

        def check_pdisks():
            for pdisk in self.pdisk_list():
                assert pdisk.Path == CONST_PDISK_PATH
                assert pdisk.DriveStatus == blobstorage_config_pb2.EDriveStatus.ACTIVE
                assert pdisk.ExpectedSlotCount == CONST_CUSTOM_SLOT_COUNT
                assert pdisk.PDiskConfig.ExpectedSlotCount == CONST_CUSTOM_SLOT_COUNT
                assert pdisk.PDiskConfig.SlotSizeInUnits == 2
                assert pdisk.PDiskMetrics.TotalSize == CONST_480_GB
                assert pdisk.PDiskMetrics.SlotCount == CONST_CUSTOM_SLOT_COUNT
                assert pdisk.PDiskMetrics.SlotSizeInUnits == 2
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 > t2
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 < deadline
        retry_assertions(check_pdisks, timeout)

        t3 = time.time()
        self.change_cluster_version()
        assert self.current_binary_paths_index == 0
        logger.info(f"Restarted back on version {self.versions[0]} {time.time()=}")
        ###########################################################################

        self.cluster.client.pdisk_set_all_active(pdisk_path=CONST_PDISK_PATH)
        logger.info(f"Drives activated {time.time()=}")

        deadline = time.time() + timeout

        def check_pdisks():
            for pdisk in self.pdisk_list():
                assert pdisk.Path == CONST_PDISK_PATH
                assert pdisk.DriveStatus == blobstorage_config_pb2.EDriveStatus.ACTIVE
                assert pdisk.ExpectedSlotCount == CONST_CUSTOM_SLOT_COUNT
                assert pdisk.PDiskConfig.ExpectedSlotCount == CONST_CUSTOM_SLOT_COUNT
                assert pdisk.PDiskConfig.SlotSizeInUnits == 2
                assert pdisk.PDiskMetrics.TotalSize == CONST_480_GB
                if self.versions[0] < (25, 3):
                    assert not pdisk.PDiskMetrics.HasField('SlotCount')
                    assert not pdisk.PDiskMetrics.HasField('SlotSizeInUnits')
                else:
                    assert pdisk.PDiskMetrics.SlotCount == CONST_CUSTOM_SLOT_COUNT
                    assert pdisk.PDiskMetrics.SlotSizeInUnits == 2
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 > t3
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 < deadline
        retry_assertions(check_pdisks, timeout)

    def test_setup_in_blob_storage_config(self):
        if self.versions[1] < (25, 5):
            pytest.skip("Unsupported up to version 25.4 included")

        assert self.current_binary_paths_index == 0
        logger.info(f"Test started on {self.versions[0]} {time.time()=}")
        #################################################################

        t1 = time.time()
        timeout = 20

        def check_pdisks():
            for pdisk in self.pdisk_list():
                assert pdisk.Path == CONST_PDISK_PATH
                assert pdisk.DriveStatus == blobstorage_config_pb2.EDriveStatus.ACTIVE
                assert pdisk.PDiskConfig.ExpectedSlotCount == CONST_INITIAL_SLOT_COUNT
                assert pdisk.ExpectedSlotCount == CONST_INITIAL_SLOT_COUNT
                assert pdisk.PDiskMetrics.TotalSize == CONST_480_GB
                if self.versions[0] < (25, 3):
                    assert not pdisk.PDiskMetrics.HasField('SlotCount')
                    assert not pdisk.PDiskMetrics.HasField('SlotSizeInUnits')
                else:
                    assert pdisk.PDiskMetrics.SlotCount == CONST_INITIAL_SLOT_COUNT
                    assert pdisk.PDiskMetrics.HasField('SlotSizeInUnits') and \
                        pdisk.PDiskMetrics.SlotSizeInUnits == 0
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 > t1
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 < t1 + timeout
        retry_assertions(check_pdisks, timeout)

        self.change_cluster_version()
        assert self.current_binary_paths_index == 1
        logger.info(f"Restarted on version {self.versions[1]} {time.time()=}")
        ######################################################################

        t2 = time.time()
        full_config = self.generate_config()
        full_config["config"]["blob_storage_config"]["infer_pdisk_slot_count_settings"] = {
            "rot": {
                "prefer_inferred_settings_over_explicit": True,
                "unit_size": CONST_10_GB,
                "max_slots": 32,
            }
        }
        self.replace_config(full_config)
        inferred_slot_count = 24
        inferred_slot_size_in_units = 2
        logger.info(f"Inferred PDisk setting applied {time.time()=}")

        self.cluster.client.pdisk_set_all_active(pdisk_path=CONST_PDISK_PATH)
        logger.info(f"Drives activated {time.time()=}")

        deadline = time.time() + timeout

        def check_pdisks():
            for pdisk in self.pdisk_list():
                assert pdisk.Path == CONST_PDISK_PATH
                assert pdisk.DriveStatus == blobstorage_config_pb2.EDriveStatus.ACTIVE
                assert pdisk.PDiskConfig.ExpectedSlotCount == CONST_INITIAL_SLOT_COUNT
                assert pdisk.ExpectedSlotCount == CONST_INITIAL_SLOT_COUNT
                assert pdisk.PDiskMetrics.TotalSize == CONST_480_GB
                assert pdisk.PDiskMetrics.SlotCount == inferred_slot_count
                assert pdisk.PDiskMetrics.SlotSizeInUnits == inferred_slot_size_in_units
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 > t2
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 < deadline
        retry_assertions(check_pdisks, timeout)

        t3 = time.time()
        self.change_cluster_version()
        assert self.current_binary_paths_index == 0
        logger.info(f"Restarted back on version {self.versions[0]} {time.time()=}")
        ###########################################################################

        self.cluster.client.pdisk_set_all_active(pdisk_path=CONST_PDISK_PATH)
        logger.info(f"Drives activated {time.time()=}")

        deadline = time.time() + timeout

        def check_pdisks():
            for pdisk in self.pdisk_list():
                assert pdisk.Path == CONST_PDISK_PATH
                assert pdisk.DriveStatus == blobstorage_config_pb2.EDriveStatus.ACTIVE
                assert pdisk.PDiskConfig.ExpectedSlotCount == CONST_INITIAL_SLOT_COUNT
                assert pdisk.ExpectedSlotCount == CONST_INITIAL_SLOT_COUNT
                assert pdisk.PDiskMetrics.TotalSize == CONST_480_GB
                if self.versions[0] < (25, 3):
                    assert not pdisk.PDiskMetrics.HasField('SlotCount')
                    assert not pdisk.PDiskMetrics.HasField('SlotSizeInUnits')
                elif self.versions[0] < (25, 5):
                    assert pdisk.PDiskMetrics.HasField('SlotCount') and pdisk.PDiskMetrics.SlotCount == CONST_INITIAL_SLOT_COUNT
                    assert pdisk.PDiskMetrics.HasField('SlotSizeInUnits') and pdisk.PDiskMetrics.SlotSizeInUnits == 0
                else:
                    assert pdisk.PDiskMetrics.SlotCount == inferred_slot_count
                    assert pdisk.PDiskMetrics.SlotSizeInUnits == inferred_slot_size_in_units
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 > t3
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 < deadline
        retry_assertions(check_pdisks, timeout)
