#include "cluster_tracker.h"

#include <library/cpp/containers/absl/flat_hash_set.h>

namespace NKikimr::NPQ::NClusterTracker {

void TClustersList::MarkFnxFromBalancers() {
    absl::flat_hash_set<TString> fnxNames;
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
    const TAccountClusters* spec = nullptr;
    if (const auto it = Accounts.find(key); it != Accounts.end()) {
        spec = &it->second;
    }

    bool hasReadList = false;
    if (spec) {
        for (const auto& [_, access] : spec->Clusters) {
            if (access.CanRead) {
                hasReadList = true;
                break;
            }
        }
    }

    TVector<TCluster> visible;
    visible.reserve(Clusters.size());
    for (const auto& cluster : Clusters) {
        const TAccountClusters::TAccess* access = nullptr;
        if (spec) {
            if (const auto it = spec->Clusters.find(cluster.Name); it != spec->Clusters.end()) {
                access = &it->second;
            }
        }
        const bool listed = hasReadList && access && access->CanRead;
        const bool assignedFnx = cluster.IsFnx && access && access->CanWrite;
        const bool everyCommunal = !hasReadList && !cluster.IsFnx;
        if (listed || assignedFnx || everyCommunal) {
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
