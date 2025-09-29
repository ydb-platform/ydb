#!/usr/bin/env python
# -*- coding: utf-8 -*-
import ydb.core.protos.msgbus_pb2 as msgbus

from ydb.tests.library.common.protobuf import AbstractProtobufBuilder, to_bytes


class CreateTenantRequest(AbstractProtobufBuilder):
    """
    See /arcadia/ydb/public/api/protos/ydb_cms.proto
    """

    def __init__(self, path):
        super(CreateTenantRequest, self).__init__(msgbus.TConsoleRequest())
        self.protobuf.CreateTenantRequest.Request.path = path

    def set_user_token(self, token):
        self.protobuf.SecurityToken = to_bytes(token)
        self.protobuf.CreateTenantRequest.UserToken = to_bytes(token)

    def add_storage_pool(self, pool_type, pool_size):
        pool = self.protobuf.CreateTenantRequest.Request.resources.storage_units.add()
        pool.unit_kind = pool_type
        pool.count = pool_size
        return self

    def add_slot(self, count, kind='slot', zone='any'):
        slot = self.protobuf.CreateTenantRequest.Request.resources.computational_units.add()
        slot.unit_kind = kind
        slot.availability_zone = zone
        slot.count = count
        return self

    def add_shared_storage_pool(self, pool_type, pool_size):
        pool = self.protobuf.CreateTenantRequest.Request.shared_resources.storage_units.add()
        pool.unit_kind = pool_type
        pool.count = pool_size
        return self

    def add_shared_slot(self, count, kind='slot', zone='any'):
        slot = self.protobuf.CreateTenantRequest.Request.shared_resources.computational_units.add()
        slot.unit_kind = kind
        slot.availability_zone = zone
        slot.count = count
        return self

    def share_resources_with(self, hostel_db):
        self.protobuf.CreateTenantRequest.Request.serverless_resources.shared_database_path = hostel_db
        return self

    def disable_external_subdomain(self):
        self.protobuf.CreateTenantRequest.Request.options.disable_external_subdomain = True

    def set_schema_quotas(self, schema_quotas):
        quotas = self.protobuf.CreateTenantRequest.Request.schema_operation_quotas
        quotas.SetInParent()
        for (bucket_size, bucket_seconds) in schema_quotas:
            q = quotas.leaky_bucket_quotas.add()
            q.bucket_size = bucket_size
            q.bucket_seconds = bucket_seconds

    def set_disk_quotas(self, disk_quotas):
        quotas = self.protobuf.CreateTenantRequest.Request.database_quotas
        quotas.SetInParent()
        hard = disk_quotas.get('hard')
        if hard is not None:
            quotas.data_size_hard_quota = hard
        soft = disk_quotas.get('soft')
        if soft is not None:
            quotas.data_size_soft_quota = soft

    def set_attribute(self, name, value):
        self.protobuf.CreateTenantRequest.Request.attributes[name] = value


class AlterTenantRequest(AbstractProtobufBuilder):
    """
    See /arcadia/ydb/public/api/protos/ydb_cms.proto
    """

    def __init__(self, path):
        super(AlterTenantRequest, self).__init__(msgbus.TConsoleRequest())
        self.protobuf.AlterTenantRequest.Request.path = path

    def set_schema_quotas(self, schema_quotas):
        quotas = self.protobuf.AlterTenantRequest.Request.schema_operation_quotas
        quotas.SetInParent()
        for (bucket_size, bucket_seconds) in schema_quotas:
            q = quotas.leaky_bucket_quotas.add()
            q.bucket_size = bucket_size
            q.bucket_seconds = bucket_seconds

    def set_disk_quotas(self, disk_quotas):
        quotas = self.protobuf.AlterTenantRequest.Request.database_quotas
        quotas.SetInParent()
        hard = disk_quotas.get('hard')
        if hard is not None:
            quotas.data_size_hard_quota = hard
        soft = disk_quotas.get('soft')
        if soft is not None:
            quotas.data_size_soft_quota = soft


class GetTenantStatusRequest(AbstractProtobufBuilder):
    """
    See /arcadia/ydb/public/api/protos/ydb_cms.proto
    """

    def __init__(self, path):
        super(GetTenantStatusRequest, self).__init__(msgbus.TConsoleRequest())
        self.protobuf.GetTenantStatusRequest.Request.path = path

    def set_user_token(self, token):
        self.protobuf.SecurityToken = to_bytes(token)
        self.protobuf.GetTenantStatusRequest.UserToken = to_bytes(token)


class RemoveTenantRequest(AbstractProtobufBuilder):
    """
    See /arcadia/ydb/public/api/protos/ydb_cms.proto
    """

    def __init__(self, path):
        super(RemoveTenantRequest, self).__init__(msgbus.TConsoleRequest())
        self.protobuf.RemoveTenantRequest.Request.path = path

    def set_user_token(self, token):
        self.protobuf.SecurityToken = to_bytes(token)
        self.protobuf.RemoveTenantRequest.UserToken = to_bytes(token)


class SetConfigRequest(AbstractProtobufBuilder):
    """
    See /arcadia/ydb/core/protos/console.proto
    """

    def __init__(self):
        super(SetConfigRequest, self).__init__(msgbus.TConsoleRequest())

    def set_restrictions(self, tenant_usage_scope_kinds=()):
        restrictions = self.protobuf.SetConfigRequest.Config.ConfigsConfig.UsageScopeRestrictions
        restrictions.AllowedTenantUsageScopeKinds.extend(tenant_usage_scope_kinds)
        return self

    def add_zone(self, kind, dc):
        zone = self.protobuf.SetConfigRequest.Config.TenantsConfig.AvailabilityZoneKinds.add()
        zone.Kind = kind
        zone.DataCenterName = dc
        return self

    def add_zone_set(self, name, zones):
        zone_set = self.protobuf.SetConfigRequest.Config.TenantsConfig.AvailabilityZoneSets.add()
        zone_set.Name = name
        zone_set.ZoneKinds.extend(zones)
        return self

    def add_unit_kind(self, kind, slot_type, zone_set):
        unit = self.protobuf.SetConfigRequest.Config.TenantsConfig.ComputationalUnitKinds.add()
        unit.Kind = kind
        unit.TenantSlotType = slot_type
        unit.AvailabilityZoneSet = zone_set
        return self


class GetOperationRequest(AbstractProtobufBuilder):
    """
    See /arcadia/ydb/core/protos/console.proto
    """

    def __init__(self, op_id):
        super(GetOperationRequest, self).__init__(msgbus.TConsoleRequest())
        self.protobuf.GetOperationRequest.id = op_id

    def set_user_token(self, token):
        self.protobuf.SecurityToken = to_bytes(token)
