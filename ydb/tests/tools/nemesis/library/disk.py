# -*- coding: utf-8 -*-
import abc
import random
import subprocess
import time

from ydb.tests.library.nemesis.nemesis_core import Nemesis
from ydb.tests.library.predicates import blobstorage
from ydb.tests.tools.nemesis.library.base import AbstractMonitoredNemesis
from ydb.tests.library.common.msgbus_types import EDriveStatus
from ydb.core.protos.blobstorage_config_pb2 import TConfigResponse


class AbstractSafeEraseDataOnDisk(Nemesis, AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule=(90, 150)):
        super(AbstractSafeEraseDataOnDisk, self).__init__(schedule)
        AbstractMonitoredNemesis.__init__(self, 'disk')
        self._cluster = cluster
        self._successful_data_erase_count = 0

    def all_vdisks_are_replicated(self):
        return blobstorage.cluster_has_no_unreplicated_vdisks(self._cluster, 1, self.logger)

    @property
    def state_ready(self):
        if len(self._cluster.nodes.values()) < 8:
            self.logger.info("Data erase is prohibited.")
            return False
        return self.all_vdisks_are_replicated()

    def extract_fault(self):
        pass

    def inject_fault(self):
        if self.state_ready:
            self._erase_data()
            self.logger.debug("Successful data erase")
        else:
            self.prepare_state()
            self.logger.debug("Do not erase data this time")
            return False

    def prepare_state(self):
        pass

    @property
    def successful_data_erase_count(self):
        return self._successful_data_erase_count

    @abc.abstractmethod
    def _erase_data(self):
        pass


class SafelyCleanupDisks(AbstractSafeEraseDataOnDisk):
    def __init__(self, cluster, schedule=(90, 150)):
        super(SafelyCleanupDisks, self).__init__(cluster, schedule)
        self._node_ids = self._cluster.nodes.keys()

    def _erase_data(self):
        try:
            node_id = random.choice(list(self._node_ids))
            self._cluster.nodes[node_id].kill_process_and_daemon()
            self._cluster.nodes[node_id].cleanup_disks()
            self._cluster.nodes[node_id].start()
            self._successful_data_erase_count += 1
            self.on_success_inject_fault()
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error("Failed to cleanup disks, %s", str(e))
            return False


class SafelyBreakDisk(AbstractSafeEraseDataOnDisk):
    def __init__(self, cluster, schedule=(90, 150)):
        super(SafelyBreakDisk, self).__init__(cluster, schedule)
        self._currently_broken_drive = None
        self._states = {}
        self._broken_drives = set()

    def prepare_state(self):
        super(SafelyBreakDisk, self).prepare_state()
        self._broken_drives = set()
        for node_id, node in self._cluster.nodes.items():
            drives = self._cluster.client.read_drive_status(node.host, node.ic_port).BlobStorageConfigResponse
            self._states[node_id] = {}
            for status in drives.Status:
                for drive in status.DriveStatus:
                    self._states[node_id][drive.Path] = drive.Status
                    if drive.Status != EDriveStatus.ACTIVE:
                        self._broken_drives.add(
                            (node_id, drive.Path))

    def _change_drive_status(self, node_id, path, status):
        host, ic_port = self._cluster.nodes[node_id].host, self._cluster.nodes[node_id].ic_port
        self.logger.info("Change drive status host %s:%s, path %s, status %s", host, ic_port, path, status.name)
        for _ in range(6):
            response = self._cluster.client.update_drive_status(host, ic_port, path, status).BlobStorageConfigResponse
            if not response.Success and len(response.Status) == 1 and response.Status[0].FailReason == \
                    TConfigResponse.TStatus.EFailReason.kMayLoseData:
                time.sleep(10)
            else:
                break
        self.logger.info("Received response from controller %s" % response)
        return response.Success

    def _erase_data(self):
        self.extract_fault()

        if len(self._states.keys()) > 0:
            node_id = random.choice(list(self._states.keys()))
            if len(self._states[node_id].keys()) > 0:
                path = random.choice(list(self._states[node_id].keys()))
                if self._change_drive_status(node_id, path, EDriveStatus.BROKEN):
                    self._successful_data_erase_count += 1
                    self.on_success_inject_fault()
        self.prepare_state()

    def extract_fault(self):
        for node_id, path in self._broken_drives:
            self._change_drive_status(
                node_id, path, EDriveStatus.ACTIVE)


def data_storage_nemesis_list(cluster):
    return [
        SafelyBreakDisk(cluster),
        SafelyCleanupDisks(cluster),
    ]
