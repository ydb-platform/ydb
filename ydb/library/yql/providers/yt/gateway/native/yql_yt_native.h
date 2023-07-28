#pragma once

#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>

#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <util/generic/ptr.h>

namespace NYql {

class TYtGatewayConfig;
using TYtGatewayConfigPtr = std::shared_ptr<TYtGatewayConfig>;

struct TYtNativeServices {
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;

    TFileStoragePtr FileStorage;
    TYtGatewayConfigPtr Config;
    // allow anonymous access for tests
    bool DisableAnonymousClusterAccess = false;
};

IYtGateway::TPtr CreateYtNativeGateway(const TYtNativeServices& services);

}
