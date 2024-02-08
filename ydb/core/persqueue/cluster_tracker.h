#pragma once

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/event_local.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/string/builder.h>

#include <vector>

namespace NKikimr::NPQ::NClusterTracker {

using NActors::TActorId;

inline TActorId MakeClusterTrackerID() {
    static const char x[12] = "clstr_trckr";
    return TActorId(0, TStringBuf(x, 12));
}

NActors::IActor* CreateClusterTracker();

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

    i64 Version = 0;
};

struct TEvClusterTracker {
    enum EEv {
        EvClustersUpdate = EventSpaceBegin(TKikimrEvents::ES_PQ_CLUSTER_TRACKER),
        EvSubscribe,
        EvEnd
    };

    struct TEvClustersUpdate : public NActors::TEventLocal<TEvClustersUpdate, EvClustersUpdate> {
        TClustersList::TConstPtr ClustersList;
        TMaybe<TInstant> ClustersListUpdateTimestamp;
    };

    struct TEvSubscribe : public NActors::TEventLocal<TEvSubscribe, EvSubscribe> {
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_CLUSTER_TRACKER), "Unexpected TEvClusterTracker event range");
};

} // namespace NKikimr::NPQ::NClusterTracker
