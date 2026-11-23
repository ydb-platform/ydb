#include "cluster_tracker.h"

#include <library/cpp/containers/absl/flat_hash_set.h>

namespace NKikimr::NPQ::NClusterTracker {

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
        absl::flat_hash_set<TString> extraFnx(extraFnxNames.begin(), extraFnxNames.end());
        auto& visible = ClustersByBalancer[host];
        visible.reserve(Clusters.size());
        for (const auto& cluster : Clusters) {
            if (!cluster.IsFnx || extraFnx.contains(cluster.Name)) {
                visible.push_back(cluster);
            }
        }
    }
}

TVector<TClustersList::TCluster> TClustersList::GetReadClusters(TStringBuf account) const {
    TString key(account);
    key.to_lower();
    const auto it = Accounts.find(key);
    if (it == Accounts.end()) {
        return DefaultVisibleClusters;
    }
    const auto& spec = it->second;

    TVector<TCluster> visible;
    visible.reserve(Clusters.size());
    for (const auto& cluster : Clusters) {
        const bool readable = cluster.IsFnx
            ? spec.Write.contains(cluster.Name)
            : spec.Read.empty() || spec.Read.contains(cluster.Name);
        if (readable) {
            visible.push_back(cluster);
        }
    }
    return visible;
}

i64 TClustersList::SumAccountVersions() const {
    i64 sum = 0;
    for (const auto& [_, account] : Accounts) {
        sum += account.Version;
    }
    return sum;
}

} // namespace NKikimr::NPQ::NClusterTracker
