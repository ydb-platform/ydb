#pragma once

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/sql/settings/flags/flags.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NYql {

class TGatewaysConfig;
class TActivationPercentage;

using TActivator = std::function<bool(const TActivationPercentage& activation)>;

class TGatewaySQLFlags final {
public:
    void Set(const TString& flag, TVector<TString> args = {});

    /// @note that receiver flags preferred
    void ExtendWith(const TGatewaySQLFlags& flags);

    /// @param map is to reuse a container
    /// @param areOnlyActivated is needed for Activation report
    NSQLTranslation::TExtendedSqlFlags ToMap(
        NSQLTranslation::TExtendedSqlFlags map = {},
        bool areOnlyActivated = false) const;

    static TGatewaySQLFlags From(const TGatewaysConfig& config, const TActivator& isActive);
    static TGatewaySQLFlags FromTesting(const TGatewaysConfig& config);

private:
    THashSet<TString> Activated_;
    NSQLTranslation::TExtendedSqlFlags All_;
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
