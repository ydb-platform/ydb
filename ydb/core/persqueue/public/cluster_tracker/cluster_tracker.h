#pragma once

#include "cluster_tracker_factory.h"

#include <util/string/join.h>
#include <ydb/core/base/events.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/string/builder.h>

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
        ui64 Weight = 1000;

        TString DebugString() const {
            TStringBuilder builder;
            builder << "(" << Name << ", " << Datacenter << ", " << Balancer << ", ";
            builder << (IsEnabled ? "enabled" : "disabled")  << ", ";
            builder << (IsLocal ? "local" : "remote") << ", ";
            builder << Weight << ")";

            return TString(builder);
        }

        bool operator==(const TCluster& other) const;
    };

    bool operator==(const TClustersList& other) const;

    std::vector<TCluster> Clusters;
    TCluster* LocalCluster = nullptr;

    const TString& GetLocalClusterName() const {
        static const TString Empty = "";
        return LocalCluster ? LocalCluster->Name : Empty;
    }

    TString DebugString() const {
        return TStringBuilder() << "[" << JoinSeq(", ", Clusters | std::views::transform([](const auto& cluster) { return cluster.DebugString(); })) << "]";
    }

    i64 Version = 0;
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
        TClustersList::TConstPtr ClustersList;
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_CLUSTER_TRACKER), "Unexpected TEvClusterTracker event range");
};

} // namespace NKikimr::NPQ::NClusterTracker
