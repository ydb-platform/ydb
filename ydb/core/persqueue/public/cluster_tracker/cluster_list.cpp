#include "cluster_tracker.h"

#include <util/generic/hash_set.h>

namespace NKikimr::NPQ::NClusterTracker {

void TClustersList::MarkFnxFromBalancers() {
    THashSet<TString> fnxNames;
    for (const auto& [_, names] : Balancers) {
        fnxNames.insert(names.begin(), names.end());
    }
    for (auto& cluster : Clusters) {
        cluster.IsFnx = fnxNames.contains(cluster.Name);
    }
}

void TClustersList::BuildVisibleClusters() {
    DefaultVisibleClusters.clear();
    ClustersByBalancer.clear();

    DefaultVisibleClusters.reserve(Clusters.size());
    for (const auto& cluster : Clusters) {
        if (!cluster.IsFnx) {
            DefaultVisibleClusters.push_back(cluster);
        }
    }

    for (const auto& [host, extraFnxNames] : Balancers) {
        THashSet<TString> extraFnx(extraFnxNames.begin(), extraFnxNames.end());
        auto& visible = ClustersByBalancer[host];
        visible.reserve(Clusters.size());
        for (const auto& cluster : Clusters) {
            if (!cluster.IsFnx || extraFnx.contains(cluster.Name)) {
                visible.push_back(cluster);
            }
        }
    }
}

} // namespace NKikimr::NPQ::NClusterTracker
