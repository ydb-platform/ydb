#pragma once

#include <yt/yql/providers/yt/gateway/lib/exec_ctx.h>
#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>
#include <yt/yql/providers/yt/lib/secret_masker/secret_masker.h>

#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>

#include <util/generic/ptr.h>

namespace NYql {

struct TYtNativeServices: public TYtBaseServices {
    using TPtr = TIntrusivePtr<TYtNativeServices>;

    TFileStoragePtr FileStorage;
    // allow anonymous access for tests
    bool DisableAnonymousClusterAccess = false;
    IMetricsRegistryPtr Metrics;
    ISecretMasker::TPtr SecretMasker;
};

IYtGateway::TPtr CreateYtNativeGateway(const TYtNativeServices& services);

}
