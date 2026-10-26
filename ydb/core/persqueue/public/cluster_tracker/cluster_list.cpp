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

TVector<TClustersList::TCluster> TClustersList::GetReadClusters(TStringBuf account) const {
    TString key(account);
    key.to_lower();
    const TAccountClusters* spec = nullptr;
    if (const auto it = Accounts.find(key); it != Accounts.end()) {
        spec = &it->second;
    }

    THashSet<TString> readNames;
    THashSet<TString> accountFnx;
    if (spec) {
        readNames.insert(spec->Read.begin(), spec->Read.end());
        if (!spec->Write.empty()) {
            THashSet<TString> writeNames(spec->Write.begin(), spec->Write.end());
            for (const auto& cluster : Clusters) {
                if (cluster.IsFnx && writeNames.contains(cluster.Name)) {
                    accountFnx.insert(cluster.Name);
                }
            }
        }
    }

    TVector<TCluster> visible;
    visible.reserve(Clusters.size());
    for (const auto& cluster : Clusters) {
        const bool hasReadList = spec && !spec->Read.empty();
        const bool listed = hasReadList && readNames.contains(cluster.Name);
        const bool assignedFnx = cluster.IsFnx && accountFnx.contains(cluster.Name);
        const bool everyCommunal = !hasReadList && !cluster.IsFnx;
        if (listed || assignedFnx || everyCommunal) {
            visible.push_back(cluster);
        }
    }
    return visible;
}

} // namespace NKikimr::NPQ::NClusterTracker
