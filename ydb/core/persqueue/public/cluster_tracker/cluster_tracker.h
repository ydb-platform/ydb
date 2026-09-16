#pragma once

#include "cluster_tracker_factory.h"

#include <ydb/core/base/events.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <vector>

namespace NKikimr::NPQ::NClusterTracker {

using NActors::TActorId;

struct TClustersList : public TAtomicRefCount<TClustersList>, TNonCopyable {
    using TConstPtr = TIntrusiveConstPtr<TClustersList>;

    TClustersList() = default;

    struct TCluster {
        TString Name;
        TString Datacenter; // is equal to cluster name at the moment
        TString Balancer;
        bool IsEnabled = false;
        bool IsLocal = false;
        bool IsFnx = false;
        ui64 Weight = 1000;

        TString DebugString() const;

        bool operator==(const TCluster& other) const;
    };

    bool operator==(const TClustersList& other) const;
    TString DebugString() const;

    std::vector<TCluster> Clusters;
    const TCluster* LocalCluster = nullptr;

    // Normalized balancer host -> FNX cluster names listed in Balancer.clusters.
    THashMap<TString, TVector<TString>> Balancers;

    i64 Version = 0;
    i64 ClusterVersion = 0;
    i64 BalancerVersion = 0;
};

struct TEvClusterTracker {
    enum EEv {
        EvClustersUpdate = EventSpaceBegin(TKikimrEvents::ES_PQ_CLUSTER_TRACKER),
        EvSubscribe,
        EvGetClustersList,
        EvGetClustersListResponse,
        EvEnd
    };

    struct TEvClustersUpdate : public NActors::TEventLocal<TEvClustersUpdate, EvClustersUpdate> {
        TClustersList::TConstPtr ClustersList;
        TMaybe<TInstant> ClustersListUpdateTimestamp;
    };

    struct TEvSubscribe : public NActors::TEventLocal<TEvSubscribe, EvSubscribe> {
    };

    struct TEvGetClustersList : public NActors::TEventLocal<TEvGetClustersList, EvGetClustersList> {
    };

    struct TEvGetClustersListResponse : public NActors::TEventLocal<TEvGetClustersListResponse, EvGetClustersListResponse> {
        bool Success = true;
        TClustersList::TConstPtr ClustersList;
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_CLUSTER_TRACKER), "Unexpected TEvClusterTracker event range");
};

} // namespace NKikimr::NPQ::NClusterTracker
