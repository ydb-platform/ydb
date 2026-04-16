#include "utils.h"
#include "settings.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql::NFastCheck {

std::unique_ptr<IUdfMeta> LoadUdfMeta(TStringBuf json) {
    return ParseUdfMeta(NJson::ReadJsonFastTree(json));
}

void FillClusters(const TChecksRequest& request, NSQLTranslation::TTranslationSettings& settings) {
    if (!request.ClusterSystem.empty()) {
        Y_ENSURE(FindPtr(Providers, request.ClusterSystem), "Invalid ClusterSystem value: " + request.ClusterSystem);
    }

    switch (request.ClusterMode) {
        case EClusterMode::Many:
            for (const auto& x : request.ClusterMapping) {
                Y_ENSURE(AnyOf(Providers, [&](const auto& p) { return p == x.second; }),
                         "Invalid system: " + x.second);
            }

            settings.ClusterMapping = request.ClusterMapping;
            settings.DynamicClusterProvider = request.ClusterSystem;
            break;
        case EClusterMode::Single:
            Y_ENSURE(!request.ClusterSystem.empty(), "Missing ClusterSystem parameter");
            settings.DefaultCluster = "single";
            settings.ClusterMapping["single"] = request.ClusterSystem;
            settings.DynamicClusterProvider = request.ClusterSystem;
            break;
        case EClusterMode::Unknown:
            settings.DefaultCluster = "single";
            settings.ClusterMapping["single"] = UnknownProviderName;
            settings.DynamicClusterProvider = UnknownProviderName;
            break;
    }
}

} // namespace NYql::NFastCheck
