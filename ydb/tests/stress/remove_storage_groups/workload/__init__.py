import logging
import time
import random

from google.protobuf import text_format

from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from ydb.tests.library.common.protobuf_console import AlterTenantRequest, GetTenantStatusRequest
import ydb.public.api.protos.ydb_cms_pb2 as cms_tenants_pb

logger = logging.getLogger(__name__)


class AlterStorageUnitsWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration):
        super().__init__(None, '', 'alter_storage_units', None)
        self.database = database
        self.duration = duration
        if endpoint.startswith('grpc://'):
            endpoint = endpoint[len('grpc://'):]
        host, port = endpoint.split(':')
        self.kikimr_client = kikimr_client_factory(host, port)

    def get_tenant_status(self):
        request = GetTenantStatusRequest(self.database)
        response = self.kikimr_client.console_request(text_format.MessageToString(request.protobuf))
        result = cms_tenants_pb.GetDatabaseStatusResult()
        response.GetTenantStatusResponse.Response.operation.result.Unpack(result)
        return result

    def _pre_start(self):
        storage_units = self.get_tenant_status().required_resources.storage_units[0]
        self.storage_unit_kind = storage_units.unit_kind
        self.storage_unit_count = storage_units.count
        return True

    def _execute_correct(self):
        new_count = random.randint(1, 8)
        request = AlterTenantRequest(self.database)
        if new_count == self.storage_unit_count:
            return
        elif new_count > self.storage_unit_count:
            request.add_storage_groups_to_add(self.storage_unit_kind, new_count - self.storage_unit_count)
        else:
            request.add_storage_groups_to_remove(self.storage_unit_kind, self.storage_unit_count - new_count)
        try:
            self.kikimr_client.console_request(text_format.MessageToString(request.protobuf))
        except Exception as e:
            logger.warning(f"error in console request: {e}")
            return
        self.storage_unit_count = new_count

    def _execute_incorrect(self):
        request = AlterTenantRequest(self.database)
        request.add_storage_groups_to_remove(self.storage_unit_kind, 8)
        try:
            self.kikimr_client.console_request(text_format.MessageToString(request.protobuf), raise_on_error=False)
        except Exception as e:
            logger.warning(f"error in console request: {e}")
            return

    def _post_stop(self):
        storage_units = self.get_tenant_status().required_resources.storage_units[0]
        return storage_units.count == self.storage_unit_count

    def run(self, func):
        def f():
            started_at = time.time()

            while time.time() - started_at < self.duration:
                func()
                time.sleep(random.expovariate(1))

        return f

    def get_workload_thread_funcs(self):
        return map(self.run, [self._execute_correct, self._execute_incorrect])
