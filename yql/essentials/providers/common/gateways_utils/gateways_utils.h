#pragma once

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/utils/yql_panic.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NYql {

class TGatewaysConfig;
class TActivationPercentage;

using TActivator = std::function<bool(const TActivationPercentage& activation)>;

struct TGatewaySQLFlags {
    THashSet<TString> Unconditional;
    THashSet<TString> Activated;

    void CollectAllTo(THashSet<TString>& target) const;
    THashSet<TString> All() const;

    void ExtendWith(const TGatewaySQLFlags& flags);

    static TGatewaySQLFlags From(const TGatewaysConfig& config, const TActivator& isActive);
    static TGatewaySQLFlags FromTesting(const TGatewaysConfig& config);
};

template <typename T>
void AddClusters(const T& mappings, const TString& providerName, THashMap<TString, TString>* clusters) {
    for (const auto& cluster : mappings) {
        // `USE` statement works with case insensitive name and cluster name is lower-cased inside SQL translator
        auto it = clusters->emplace(cluster.GetName(), providerName);
        YQL_ENSURE(it.second, "duplicate cluster name '" << cluster.GetName() << "'");
    }
}

void GetClusterMappingFromGateways(const NYql::TGatewaysConfig& gateways, THashMap<TString, TString>& clusterMapping);

} // namespace NYql
