#pragma once
#include <util/generic/string.h>
#include <util/generic/hash.h>

template <class TPbConfig>
void FillClusterMapping(THashMap<TString, TString>& clusters, const TPbConfig& config, const TString& provider) {
    for (auto& cluster: config.GetClusterMapping()) {
        clusters.emplace(to_lower(cluster.GetName()), provider);
    }
}
