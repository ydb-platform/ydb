#pragma once

#include "actors/read_session_actor.h"
#include "actors/direct_read_actor.h"

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/persqueue/cluster_tracker.h>
#include <ydb/core/mind/address_classification/net_classifier.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <util/generic/hash.h>
#include <util/system/mutex.h>

#include <type_traits>


namespace NKikimr {
namespace NGRpcProxy {
namespace V1 {



IActor* CreatePQReadService(const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
                            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const ui32 maxSessions);

class TPQReadService : public NActors::TActorBootstrapped<TPQReadService> {
public:
    TPQReadService(const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
                   TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const ui32 maxSessions);

    ~TPQReadService()
    {}

    void Bootstrap(const TActorContext& ctx);

private:
    ui64 NextCookie();

    bool TooMuchSessions();
    TString AvailableLocalCluster();

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NGRpcService::TEvStreamTopicReadRequest, Handle);
            HFunc(NGRpcService::TEvStreamTopicDirectReadRequest, Handle);
            HFunc(NGRpcService::TEvStreamPQMigrationReadRequest, Handle);
            HFunc(NGRpcService::TEvCommitOffsetRequest, Handle);
            HFunc(NGRpcService::TEvPQReadInfoRequest, Handle);
            HFunc(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate, Handle);
            HFunc(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate, Handle);
            HFunc(TEvPQProxy::TEvSessionDead, Handle);
        }
    }

    template <typename ReadRequest>
    void HandleStreamPQReadRequest(typename ReadRequest::TPtr& ev, const TActorContext& ctx);

private:
    void Handle(NGRpcService::TEvStreamTopicReadRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NGRpcService::TEvStreamTopicDirectReadRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NGRpcService::TEvStreamPQMigrationReadRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NGRpcService::TEvCommitOffsetRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NGRpcService::TEvPQReadInfoRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev, const TActorContext& ctx);
    void Handle(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQProxy::TEvSessionDead::TPtr& ev, const TActorContext& ctx);

private:
    NActors::TActorId SchemeCache;
    NActors::TActorId NewSchemeCache;

    TAtomic LastCookie = 0;

    THashMap<ui64, TActorId> Sessions;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    ui32 MaxSessions;
    TVector<TString> Clusters;
    TString LocalCluster;

    NAddressClassifier::TLabeledAddressClassifier::TConstPtr DatacenterClassifier; // Detects client's datacenter by IP. May be null
    std::shared_ptr<NPersQueue::TTopicNamesConverterFactory> TopicConverterFactory;
    std::unique_ptr<NPersQueue::TTopicsListController> TopicsHandler;
    bool HaveClusters;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// template methods implementation

template <bool UseMigrationProtocol>
auto FillReadResponse(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode code) {
    using ServerMessage = typename std::conditional<UseMigrationProtocol,
                                                    PersQueue::V1::MigrationStreamingReadServerMessage,
                                                    Topic::StreamReadMessage::FromServer>::type;
    ServerMessage res;
    FillIssue(res.add_issues(), code, errorReason);
    res.set_status(ConvertPersQueueInternalCodeToStatus(code));
    return res;
}

Topic::StreamDirectReadMessage::FromServer FillDirectReadResponse(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode code);


template <typename ReadRequest>
void TPQReadService::HandleStreamPQReadRequest(typename ReadRequest::TPtr& ev, const TActorContext& ctx) {
    constexpr bool UseMigrationProtocol = std::is_same_v<ReadRequest, NGRpcService::TEvStreamPQMigrationReadRequest>;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new grpc connection");

    if (TooMuchSessions()) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, "new grpc connection failed - too much sessions");
        ev->Get()->Attach(ctx.SelfID);
        ev->Get()->WriteAndFinish(
            FillReadResponse<UseMigrationProtocol>("proxy overloaded", PersQueue::ErrorCode::OVERLOAD), Ydb::StatusIds::OVERLOADED); //CANCELLED
        return;
    }
    if (HaveClusters && (Clusters.empty() || LocalCluster.empty())) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, "new grpc connection failed - cluster is not known yet");

        ev->Get()->Attach(ctx.SelfID);
        ev->Get()->WriteAndFinish(
            FillReadResponse<UseMigrationProtocol>("cluster initializing", PersQueue::ErrorCode::INITIALIZING), Ydb::StatusIds::UNAVAILABLE); //CANCELLED
        // TODO: Inc SLI Errors
        return;
    } else {

        Y_ABORT_UNLESS(TopicsHandler != nullptr);
        const ui64 cookie = NextCookie();

        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new session created cookie " << cookie);

        auto ip = ev->Get()->GetPeerName();

        TActorId worker = ctx.Register(new TReadSessionActor<UseMigrationProtocol>(
                ev->Release().Release(), cookie, SchemeCache, NewSchemeCache, Counters,
                DatacenterClassifier ? DatacenterClassifier->ClassifyAddress(NAddressClassifier::ExtractAddress(ip)) : "unknown",
                *TopicsHandler
        ));

        Sessions[cookie] = worker;
    }
}


}
}
}
