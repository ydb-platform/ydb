#include "utils.h"
#include "settings.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql::NFastCheck {

TUdfFilter ParseUdfFilter(const NJson::TJsonValue& json) {
    TUdfFilter res;
    for (auto& [module, v] : json.GetMapSafe()) {
        auto& names = res.Modules[to_lower(module)];
        for (auto& item : v.GetArraySafe()) {
            names.insert(to_lower(item.GetMapSafe().at("name").GetStringSafe()));
        }
    }

    return res;
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
