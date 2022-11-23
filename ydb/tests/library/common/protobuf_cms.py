#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ydb.core.protos import cms_pb2
import ydb.core.protos.msgbus_pb2 as msgbus
from ydb.tests.library.common.protobuf import AbstractProtobufBuilder


class CmsPermissionRequest(AbstractProtobufBuilder):
    def __init__(self):
        super(CmsPermissionRequest, self).__init__(
            msgbus.TCmsRequest())

        self.protobuf.PermissionRequest.User = "user"
        self.protobuf.PermissionRequest.Schedule = False
        self.protobuf.PermissionRequest.DryRun = False
        self.protobuf.PermissionRequest.PartialPermissionAllowed = True

    def add_action(self, host, type):
        action = self.protobuf.PermissionRequest.Actions.add()
        action.Duration = 60000000
        action.Type = type
        action.Host = host
        if action == cms_pb2.TAction.RESTART_SERVICES:
            action.Services = "storage"

    def set_mode(self, mode):
        self.protobuf.PermissionRequest.AvailabilityMode = mode


class CmsConfigRequest(AbstractProtobufBuilder):
    def __init__(self):
        super(CmsConfigRequest, self).__init__(msgbus.TCmsRequest())

        self.protobuf.SetConfigRequest.Config.ClusterLimits.DisabledNodesRatioLimit = 100
        self.protobuf.SetConfigRequest.Config.TenantLimits.DisabledNodesRatioLimit = 100


class CmsStateRequest(AbstractProtobufBuilder):
    def __init__(self):
        super(CmsStateRequest, self).__init__(msgbus.TCmsRequest())

        self.protobuf.ClusterStateRequest.CopyFrom(cms_pb2.TClusterStateRequest())
