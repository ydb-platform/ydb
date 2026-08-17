# -*- coding: utf-8 -*-
import logging
import time

from google.protobuf import text_format

import ydb.public.api.protos.ydb_cms_pb2 as cms_tenants_pb
from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from ydb.tests.library.common.protobuf_console import AlterTenantRequest, GetTenantStatusRequest
from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger(__name__)


class WorkloadMoveData(WorkloadBase):
    """Shrink and re-grow the storage pool so ColumnShard receives TEvMoveData.

    MoveData has no data-plane trigger: it starts when Hive tells a tablet that its
    blobs must leave a group being decommissioned, which is what removing storage
    units does. Adding them back restores the pool so the cycle can repeat. The
    other workloads in this runner supply the data that has to be moved.
    """

    # settle_time: a decommission is not instant, so the pool stays small for a
    # while before growing back, or the move is cancelled before doing any work.
    def __init__(self, client, prefix, stop, endpoint, database, settle_time=30, converge_timeout=300):
        super().__init__(client, prefix, "move_data", stop)
        self.database = database
        self.settle_time = settle_time
        self.converge_timeout = converge_timeout
        # kikimr_client_factory speaks plaintext message bus, so grpcs:// cannot work
        # here: reject it instead of stripping the scheme and failing to connect.
        scheme, sep, address = endpoint.rpartition("://")
        if sep and scheme != "grpc":
            raise ValueError(f"move_data needs a grpc:// endpoint, got {endpoint}")
        host, _, port = address.partition(":")
        self.kikimr_client = kikimr_client_factory(host, port or "2135")
        self.unit_kind = None
        self.unit_count = 0
        self.shrinks = 0
        self.grows = 0
        self.errors = 0

    def get_stat(self):
        return f"Shrinks: {self.shrinks}, Grows: {self.grows}, Errors: {self.errors}, Units: {self.unit_count}"

    def _storage_units(self):
        request = GetTenantStatusRequest(self.database)
        response = self.kikimr_client.console_request(text_format.MessageToString(request.protobuf))
        result = cms_tenants_pb.GetDatabaseStatusResult()
        response.GetTenantStatusResponse.Response.operation.result.Unpack(result)
        return result.required_resources.storage_units[0]

    def _alter_units(self, delta):
        request = AlterTenantRequest(self.database)
        if delta < 0:
            request.add_storage_groups_to_remove(self.unit_kind, -delta)
        else:
            request.add_storage_groups_to_add(self.unit_kind, delta)
        self.kikimr_client.console_request(text_format.MessageToString(request.protobuf))

    def _wait_units(self, expected):
        deadline = time.time() + self.converge_timeout
        while time.time() < deadline:
            if self.is_stop_requested():
                return False
            if self._storage_units().count == expected:
                self.unit_count = expected
                return True
            time.sleep(1)
        return False

    def _pre_start(self):
        units = self._storage_units()
        self.unit_kind = units.unit_kind
        self.unit_count = units.count
        # Removing a unit must leave at least one behind, so a single-unit pool is
        # grown once up front rather than skipping the workload entirely.
        if self.unit_count < 2:
            logger.info("move_data: pool has %s unit(s), growing to 2 before starting", self.unit_count)
            self._alter_units(2 - self.unit_count)
            if not self._wait_units(2):
                logger.warning("move_data: pool did not reach 2 units, workload disabled")
                return False
        return True

    def _cycle(self):
        target = self.unit_count - 1
        self._alter_units(-1)
        if not self._wait_units(target):
            raise RuntimeError(f"pool did not shrink to {target} units within {self.converge_timeout}s")
        self.shrinks += 1
        logger.info("move_data: shrunk pool to %s units, letting the move run", target)

        # The move runs while the pool is small; the other workloads keep writing.
        waited = 0
        while waited < self.settle_time and not self.is_stop_requested():
            time.sleep(1)
            waited += 1

        target = self.unit_count + 1
        self._alter_units(1)
        if not self._wait_units(target):
            raise RuntimeError(f"pool did not grow back to {target} units within {self.converge_timeout}s")
        self.grows += 1

    def _loop(self):
        while not self.is_stop_requested():
            try:
                self._cycle()
            except Exception as e:
                # A rejected alter (e.g. BSC busy) is expected under load; only an
                # unrecoverable state should stop the workload.
                self.errors += 1
                logger.warning("move_data: cycle failed: %s", e)
                time.sleep(5)

    def get_workload_thread_funcs(self):
        return [self._loop]
