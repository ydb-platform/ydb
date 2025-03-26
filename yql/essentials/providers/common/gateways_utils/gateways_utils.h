#pragma once

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/utils/yql_panic.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NYql{
template <typename T>
void AddClusters(const T& mappings, const TString& providerName, THashMap<TString, TString>* clusters) {
    for (const auto& cluster: mappings) {
        // `USE` statement works with case insensitive name and cluster name is lower-cased inside SQL translator
        auto it = clusters->emplace(cluster.GetName(), providerName);
        YQL_ENSURE(it.second, "duplicate cluster name '" << cluster.GetName() << "'");
    }
}

void GetClusterMappingFromGateways(const NYql::TGatewaysConfig& gateways, THashMap<TString, TString>& clusterMapping);

THashSet<TString> ExtractSqlFlags(const TGatewaysConfig& gateways);
}
