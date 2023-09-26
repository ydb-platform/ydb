#pragma once

#include "actors/events.h"

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/persqueue/cluster_tracker.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actorsystem.h>

#include <util/generic/hash.h>
#include <util/system/mutex.h>


namespace NKikimr::NGRpcProxy::V1 {

inline TActorId GetPQSchemaServiceActorID() {
    return TActorId(0, "PQSchmSvc");
}

IActor* CreatePQSchemaService(const NActors::TActorId& schemeCache, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);

class TPQSchemaService : public NActors::TActorBootstrapped<TPQSchemaService> {
public:
    TPQSchemaService(const NActors::TActorId& schemeCache, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);

    void Bootstrap(const TActorContext& ctx);

private:
    TString AvailableLocalCluster();

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::NGRpcService::TEvPQDropTopicRequest, Handle);
            HFunc(NKikimr::NGRpcService::TEvPQCreateTopicRequest, Handle);
            HFunc(NKikimr::NGRpcService::TEvPQAlterTopicRequest, Handle);
            HFunc(NKikimr::NGRpcService::TEvPQAddReadRuleRequest, Handle);
            HFunc(NKikimr::NGRpcService::TEvPQRemoveReadRuleRequest, Handle);
            HFunc(NKikimr::NGRpcService::TEvPQDescribeTopicRequest, Handle);
            HFunc(NKikimr::NGRpcService::TEvDropTopicRequest, Handle);
            HFunc(NKikimr::NGRpcService::TEvCreateTopicRequest, Handle);
            HFunc(NKikimr::NGRpcService::TEvAlterTopicRequest, Handle);
            HFunc(NKikimr::NGRpcService::TEvDescribeTopicRequest, Handle);
            HFunc(NKikimr::NGRpcService::TEvDescribeConsumerRequest, Handle);
            HFunc(NKikimr::NGRpcService::TEvDescribePartitionRequest, Handle);
            hFunc(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate, Handle);
        }
    }

private:
    void Handle(NKikimr::NGRpcService::TEvPQDropTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcService::TEvPQCreateTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcService::TEvPQAlterTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcService::TEvPQAddReadRuleRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcService::TEvPQRemoveReadRuleRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcService::TEvPQDescribeTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcService::TEvDropTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcService::TEvCreateTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcService::TEvAlterTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcService::TEvDescribeTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcService::TEvDescribeConsumerRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcService::TEvDescribePartitionRequest::TPtr& ev, const TActorContext& ctx);

    void Handle(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev);

    NActors::TActorId SchemeCache;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    TVector<TString> Clusters;
    TString LocalCluster;
};

}
