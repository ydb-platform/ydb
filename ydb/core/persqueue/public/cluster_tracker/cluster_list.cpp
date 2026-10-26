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

    absl::flat_hash_set<TString> readNames;
    absl::flat_hash_set<TString> writeNames;
    bool hasReadList = false;
    if (spec) {
        for (const auto& access : spec->Clusters) {
            if (access.CanRead) {
                hasReadList = true;
                readNames.insert(access.Name);
            }
            if (access.CanWrite) {
                writeNames.insert(access.Name);
            }
        }
    }

    TVector<TCluster> visible;
    visible.reserve(Clusters.size());
    for (const auto& cluster : Clusters) {
        const bool listed = hasReadList && readNames.contains(cluster.Name);
        const bool assignedFnx = cluster.IsFnx && writeNames.contains(cluster.Name);
        const bool everyCommunal = !hasReadList && !cluster.IsFnx;
        if (listed || assignedFnx || everyCommunal) {
            visible.push_back(cluster);
        }
    }
    return visible;
}

} // namespace NKikimr::NPQ::NClusterTracker
