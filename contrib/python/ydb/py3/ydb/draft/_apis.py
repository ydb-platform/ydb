# -*- coding: utf-8 -*-
import typing

try:
    from ydb.public.api.grpc.draft import (
        ydb_dynamic_config_v1_pb2_grpc,
    )

    from ydb.public.api.protos.draft import (
        ydb_dynamic_config_pb2,
    )
except ImportError:
    from contrib.ydb.public.api.grpc.draft import (
        ydb_dynamic_config_v1_pb2_grpc,
    )

    from contrib.ydb.public.api.protos.draft import (
        ydb_dynamic_config_pb2,
    )


ydb_dynamic_config = ydb_dynamic_config_pb2


class DynamicConfigService(object):
    Stub = ydb_dynamic_config_v1_pb2_grpc.DynamicConfigServiceStub

    ReplaceConfig = "ReplaceConfig"
    SetConfig = "SetConfig"
    GetConfig = "GetConfig"
    GetNodeLabels = "GetNodeLabels"
