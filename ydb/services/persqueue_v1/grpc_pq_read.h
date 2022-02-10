#pragma once

#include "grpc_pq_actor.h"
#include "persqueue.h"

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/persqueue/cluster_tracker.h>
#include <ydb/core/mind/address_classification/net_classifier.h>

#include <library/cpp/actors/core/actorsystem.h> 

#include <util/generic/hash.h>
#include <util/system/mutex.h>


namespace NKikimr {
namespace NGRpcProxy {
namespace V1 {



inline TActorId GetPQReadServiceActorID() {
    return TActorId(0, "PQReadSvc");
}

IActor* CreatePQReadService(const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
                            TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const ui32 maxSessions);

class TPQReadService : public NActors::TActorBootstrapped<TPQReadService> {
public:
    TPQReadService(const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
                   TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const ui32 maxSessions);

    ~TPQReadService()
    {}

    void Bootstrap(const TActorContext& ctx);

private:
    ui64 NextCookie();

    bool TooMuchSessions();
    TString AvailableLocalCluster();

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::NGRpcService::TEvStreamPQReadRequest, Handle);
            HFunc(NKikimr::NGRpcService::TEvPQReadInfoRequest, Handle);
            hFunc(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate, Handle);
            HFunc(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate, Handle);
            HFunc(TEvPQProxy::TEvSessionDead, Handle);
        }
    }

private:
    void Handle(NKikimr::NGRpcService::TEvStreamPQReadRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcService::TEvPQReadInfoRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev);
    void Handle(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQProxy::TEvSessionDead::TPtr& ev, const TActorContext& ctx);

    NActors::TActorId SchemeCache;
    NActors::TActorId NewSchemeCache;

    TAtomic LastCookie = 0;

    THashMap<ui64, TActorId> Sessions;

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

    ui32 MaxSessions;
    TVector<TString> Clusters;
    TString LocalCluster;

    NAddressClassifier::TLabeledAddressClassifier::TConstPtr DatacenterClassifier; // Detects client's datacenter by IP. May be null
    std::shared_ptr<NPersQueue::TTopicNamesConverterFactory> TopicConverterFactory;
    std::unique_ptr<NPersQueue::TTopicsListController> TopicsHandler;
    bool HaveClusters;
};


}
}
}
