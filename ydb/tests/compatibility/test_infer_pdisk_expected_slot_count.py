# -*- coding: utf-8 -*-
import pytest
import time
import logging

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.library.compatibility.fixtures import init_stable_binary_path, init_stable_name
from ydb.tests.library.compatibility.fixtures import inter_stable_binary_path, inter_stable_name
from ydb.tests.library.compatibility.fixtures import current_binary_path, current_name
from ydb.tests.library.common.types import Erasure
import ydb.core.protos.blobstorage_config_pb2 as blobstorage_config_pb2

logger = logging.getLogger(__name__)

CONST_PDISK_PATH = "SectorMap:TestInferPDiskSettings:480"
CONST_EXPECTED_SLOT_COUNT = 14
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
            drive.PDiskConfig.ExpectedSlotCount = CONST_EXPECTED_SLOT_COUNT
        self.cluster.client.define_host_configs(host_configs)

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

    def wait_and_check_pdisk_list(self, check_pdisks_fn, deadline, delay=1):
        while True:
            pdisks = self.pdisk_list()
            try:
                check_pdisks_fn(pdisks)
                logger.info(f"pdisk_list good: {pdisks}")
                return
            except AssertionError as e:
                if time.time() > deadline:
                    logger.warning(f"pdisk_list incorrect: {pdisks}")
                    raise e from e
                else:
                    time.sleep(delay)

    def test(self):
        assert self.current_binary_paths_index == 0
        logger.info(f"Test started on {self.versions[0]} {time.time()=}")
        #################################################################

        t1 = time.time()
        timeout = 20

        def check_pdisks(pdisks):
            for pdisk in pdisks:
                assert pdisk.Path == CONST_PDISK_PATH
                assert pdisk.PDiskConfig.ExpectedSlotCount == CONST_EXPECTED_SLOT_COUNT
                assert pdisk.DriveStatus == blobstorage_config_pb2.EDriveStatus.ACTIVE
                assert pdisk.PDiskMetrics.TotalSize == CONST_480_GB
                if self.versions[0] < (25, 3):
                    assert not pdisk.PDiskMetrics.HasField('SlotCount')
                    assert not pdisk.PDiskMetrics.HasField('SlotSizeInUnits')
                else:
                    assert pdisk.PDiskMetrics.SlotCount == CONST_EXPECTED_SLOT_COUNT
                    assert pdisk.PDiskMetrics.HasField('SlotSizeInUnits') and \
                        pdisk.PDiskMetrics.SlotSizeInUnits == 0
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 > t1
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 < t1 + timeout
        self.wait_and_check_pdisk_list(check_pdisks, deadline=t1+timeout)

        self.change_cluster_version()
        assert self.current_binary_paths_index == 1
        logger.info(f"Restarted on version {self.versions[1]} {time.time()=}")
        ######################################################################

        t2 = time.time()
        host_configs = self.cluster.client.read_host_configs()
        for host_config in host_configs:
            drive = host_config.Drive[1]
            assert drive.Path == CONST_PDISK_PATH
            drive.ClearField('PDiskConfig')
            drive.PDiskConfig.SetInParent()
            drive.InferPDiskSlotCountFromUnitSize = CONST_10_GB
            drive.InferPDiskSlotCountMax = 32
        self.cluster.client.define_host_configs(host_configs)
        logger.info(f"Inferred PDisk setting applied {time.time()=}")

        self.cluster.client.pdisk_set_all_active(pdisk_path=CONST_PDISK_PATH)
        logger.info(f"Drives activated {time.time()=}")

        deadline = time.time() + timeout

        def check_pdisks(pdisks):
            for pdisk in pdisks:
                assert pdisk.Path == CONST_PDISK_PATH
                assert pdisk.DriveStatus == blobstorage_config_pb2.EDriveStatus.ACTIVE
                assert not pdisk.HasField('PDiskConfig')
                assert pdisk.ExpectedSlotCount == 16  # hardcoded default
                assert pdisk.PDiskMetrics.TotalSize == CONST_480_GB
                assert pdisk.PDiskMetrics.SlotCount == 24
                assert pdisk.PDiskMetrics.SlotSizeInUnits == 2
                assert pdisk.InferPDiskSlotCountFromUnitSize == CONST_10_GB
                assert pdisk.InferPDiskSlotCountMax == 32
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 > t2
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 < deadline
        self.wait_and_check_pdisk_list(check_pdisks, deadline)

        t3 = time.time()
        self.change_cluster_version()
        assert self.current_binary_paths_index == 0
        logger.info(f"Restarted back on version {self.versions[0]} {time.time()=}")
        ###########################################################################

        self.cluster.client.pdisk_set_all_active(pdisk_path=CONST_PDISK_PATH)
        logger.info(f"Drives activated {time.time()=}")

        deadline = time.time() + timeout

        def check_pdisks(pdisks):
            for pdisk in pdisks:
                assert pdisk.Path == CONST_PDISK_PATH
                assert pdisk.DriveStatus == blobstorage_config_pb2.EDriveStatus.ACTIVE
                assert not pdisk.HasField('PDiskConfig')
                assert pdisk.ExpectedSlotCount == 16  # hardcoded default
                assert pdisk.PDiskMetrics.TotalSize == CONST_480_GB
                if self.versions[0] < (25, 3):
                    assert not pdisk.PDiskMetrics.HasField('SlotCount')
                    assert not pdisk.PDiskMetrics.HasField('SlotSizeInUnits')
                    assert pdisk.InferPDiskSlotCountFromUnitSize == 0
                    assert pdisk.InferPDiskSlotCountMax == 0
                else:
                    assert pdisk.PDiskMetrics.HasField('SlotCount') and pdisk.PDiskMetrics.SlotCount == 24
                    assert pdisk.PDiskMetrics.HasField('SlotSizeInUnits') and pdisk.PDiskMetrics.SlotSizeInUnits == 2
                    assert pdisk.InferPDiskSlotCountFromUnitSize == CONST_10_GB
                    assert pdisk.InferPDiskSlotCountMax == 32
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 > t3
                assert pdisk.PDiskMetrics.UpdateTimestamp * 1e-6 < deadline
        self.wait_and_check_pdisk_list(check_pdisks, deadline)
