#pragma once

#include "actors/write_session_actor.h"

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/persqueue/cluster_tracker.h>
#include <ydb/core/mind/address_classification/net_classifier.h>

#include <library/cpp/actors/core/actorsystem.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/system/mutex.h>

namespace NKikimr {
namespace NGRpcProxy {
namespace V1 {

IActor* CreatePQWriteService(const NActors::TActorId& schemeCache,
                             TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const ui32 maxSessions);

class TPQWriteService : public NActors::TActorBootstrapped<TPQWriteService> {
public:
    TPQWriteService(const NActors::TActorId& schemeCache,
                    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const ui32 maxSessions);

    ~TPQWriteService()
    {}

    void Bootstrap(const TActorContext& ctx);

private:
    ui64 NextCookie();

    bool TooMuchSessions();
    TString AvailableLocalCluster(const TActorContext& ctx) const;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::NGRpcService::TEvStreamPQWriteRequest, Handle);
            HFunc(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate, Handle);
            HFunc(TEvPQProxy::TEvSessionDead, Handle);
            HFunc(TEvPQProxy::TEvSessionSetPreferredCluster, Handle);
            HFunc(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate, Handle);

        }
    }

private:
    void Handle(NKikimr::NGRpcService::TEvStreamPQWriteRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev, const TActorContext& ctx);
    void Handle(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQProxy::TEvSessionSetPreferredCluster::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvSessionDead::TPtr& ev, const TActorContext& ctx);

    NActors::TActorId SchemeCache;

    TAtomic LastCookie = 0;

    THashMap<ui64, TActorId> Sessions;
    // Created at by session cookie map by remote preferred cluster name
    THashMap<TString, THashMap<ui64, TInstant>> SessionsByRemotePreferredCluster;
    THashMap<ui64, TString> RemotePreferredClusterBySessionCookie;
    // Cluster enabled at time if cluster is currently enabled
    THashMap<TString, TInstant> ClustersEnabledAt;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    ui32 MaxSessions;
    TMaybe<TString> LocalCluster;
    bool Enabled;
    TString SelectSourceIdQuery;
    TString UpdateSourceIdQuery;
    TString DeleteSourceIdQuery;

    NAddressClassifier::TLabeledAddressClassifier::TConstPtr DatacenterClassifier; // Detects client's datacenter by IP. May be null
    bool HaveClusters;
    NPersQueue::TConverterFactoryPtr ConverterFactory;
    std::unique_ptr<NPersQueue::TTopicsListController> TopicsHandler;
};


}
}
}
