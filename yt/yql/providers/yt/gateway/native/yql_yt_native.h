#pragma once

#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>

#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>

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
    IMetricsRegistryPtr Metrics;
};

IYtGateway::TPtr CreateYtNativeGateway(const TYtNativeServices& services);

}
