# -*- coding: utf-8 -*-
import typing

# Workaround for good IDE and universal for runtime
if typing.TYPE_CHECKING:
    from ._grpc.v4 import (
        ydb_cms_v1_pb2_grpc,
        ydb_discovery_v1_pb2_grpc,
        ydb_scheme_v1_pb2_grpc,
        ydb_table_v1_pb2_grpc,
        ydb_operation_v1_pb2_grpc,
        ydb_topic_v1_pb2_grpc,
    )

    from ._grpc.v4.protos import (
        ydb_status_codes_pb2,
        ydb_discovery_pb2,
        ydb_scheme_pb2,
        ydb_table_pb2,
        ydb_value_pb2,
        ydb_operation_pb2,
        ydb_common_pb2,
    )
else:
    from ._grpc.common import (
        ydb_cms_v1_pb2_grpc,
        ydb_discovery_v1_pb2_grpc,
        ydb_scheme_v1_pb2_grpc,
        ydb_table_v1_pb2_grpc,
        ydb_operation_v1_pb2_grpc,
        ydb_topic_v1_pb2_grpc,
    )

    from ._grpc.common.protos import (
        ydb_status_codes_pb2,
        ydb_discovery_pb2,
        ydb_scheme_pb2,
        ydb_table_pb2,
        ydb_value_pb2,
        ydb_operation_pb2,
        ydb_common_pb2,
    )


StatusIds = ydb_status_codes_pb2.StatusIds
FeatureFlag = ydb_common_pb2.FeatureFlag
primitive_types = ydb_value_pb2.Type.PrimitiveTypeId
ydb_value = ydb_value_pb2
ydb_scheme = ydb_scheme_pb2
ydb_table = ydb_table_pb2
ydb_discovery = ydb_discovery_pb2
ydb_operation = ydb_operation_pb2


class CmsService(object):
    Stub = ydb_cms_v1_pb2_grpc.CmsServiceStub


class DiscoveryService(object):
    Stub = ydb_discovery_v1_pb2_grpc.DiscoveryServiceStub
    ListEndpoints = "ListEndpoints"


class OperationService(object):
    Stub = ydb_operation_v1_pb2_grpc.OperationServiceStub
    ForgetOperation = "ForgetOperation"
    GetOperation = "GetOperation"
    CancelOperation = "CancelOperation"


class SchemeService(object):
    Stub = ydb_scheme_v1_pb2_grpc.SchemeServiceStub
    MakeDirectory = "MakeDirectory"
    RemoveDirectory = "RemoveDirectory"
    ListDirectory = "ListDirectory"
    DescribePath = "DescribePath"
    ModifyPermissions = "ModifyPermissions"


class TableService(object):
    Stub = ydb_table_v1_pb2_grpc.TableServiceStub

    StreamExecuteScanQuery = "StreamExecuteScanQuery"
    ExplainDataQuery = "ExplainDataQuery"
    CreateTable = "CreateTable"
    DropTable = "DropTable"
    AlterTable = "AlterTable"
    CopyTables = "CopyTables"
    RenameTables = "RenameTables"
    DescribeTable = "DescribeTable"
    CreateSession = "CreateSession"
    DeleteSession = "DeleteSession"
    ExecuteSchemeQuery = "ExecuteSchemeQuery"
    PrepareDataQuery = "PrepareDataQuery"
    ExecuteDataQuery = "ExecuteDataQuery"
    BeginTransaction = "BeginTransaction"
    CommitTransaction = "CommitTransaction"
    RollbackTransaction = "RollbackTransaction"
    KeepAlive = "KeepAlive"
    StreamReadTable = "StreamReadTable"
    BulkUpsert = "BulkUpsert"


class TopicService(object):
    Stub = ydb_topic_v1_pb2_grpc.TopicServiceStub

    CreateTopic = "CreateTopic"
    DescribeTopic = "DescribeTopic"
    DropTopic = "DropTopic"
    StreamRead = "StreamRead"
    StreamWrite = "StreamWrite"
