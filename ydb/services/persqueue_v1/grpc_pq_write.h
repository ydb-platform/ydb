#pragma once

#include "actors/write_session_actor.h"

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/persqueue/cluster_tracker.h>
#include <ydb/core/mind/address_classification/net_classifier.h>

#include <ydb/library/actors/core/actorsystem.h>

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
            HFunc(NKikimr::NGRpcService::TEvStreamTopicWriteRequest, Handle);
            HFunc(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate, Handle);
            HFunc(TEvPQProxy::TEvSessionDead, Handle);
            HFunc(TEvPQProxy::TEvSessionSetPreferredCluster, Handle);
            HFunc(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate, Handle);

        }
    }

    template <typename WriteRequest>
    void HandleWriteRequest(typename WriteRequest::TPtr& ev, const TActorContext& ctx);

private:
    void Handle(NKikimr::NGRpcService::TEvStreamPQWriteRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcService::TEvStreamTopicWriteRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev, const TActorContext& ctx);
    void Handle(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQProxy::TEvSessionSetPreferredCluster::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvSessionDead::TPtr& ev, const TActorContext& ctx);

private:
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// template methods implementation

template <bool UseMigrationProtocol>
auto FillWriteResponse(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode code) {
    using ServerMessage = typename std::conditional<UseMigrationProtocol,
                                                    PersQueue::V1::StreamingWriteServerMessage,
                                                    Topic::StreamWriteMessage::FromServer>::type;
    ServerMessage res;
    FillIssue(res.add_issues(), code, errorReason);
    res.set_status(ConvertPersQueueInternalCodeToStatus(code));
    return res;
}

template <typename WriteRequest>
void TPQWriteService::HandleWriteRequest(typename WriteRequest::TPtr& ev, const TActorContext& ctx) {
    constexpr bool UseMigrationProtocol = std::is_same_v<WriteRequest, NGRpcService::TEvStreamPQWriteRequest>;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "new grpc connection");

    if (TooMuchSessions()) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "new grpc connection failed - too much sessions");
        ev->Get()->Attach(ctx.SelfID);
        ev->Get()->WriteAndFinish(
            FillWriteResponse<UseMigrationProtocol>("proxy overloaded", PersQueue::ErrorCode::OVERLOAD),
            Ydb::StatusIds::OVERLOADED); // CANCELLED
        return;
    }

    TString localCluster = AvailableLocalCluster(ctx);

    if (HaveClusters && localCluster.empty()) {
        ev->Get()->Attach(ctx.SelfID);
        if (LocalCluster) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "new grpc connection failed - cluster disabled");
            ev->Get()->WriteAndFinish(FillWriteResponse<UseMigrationProtocol>("cluster disabled", PersQueue::ErrorCode::CLUSTER_DISABLED), Ydb::StatusIds::UNSUPPORTED); //CANCELLED
        } else {
            LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "new grpc connection failed - initializing");
            ev->Get()->WriteAndFinish(FillWriteResponse<UseMigrationProtocol>("initializing", PersQueue::ErrorCode::INITIALIZING), Ydb::StatusIds::UNAVAILABLE); //CANCELLED
        }
        return;
    } else {
        if (ConverterFactory == nullptr) {
            ConverterFactory = std::make_shared<NPersQueue::TTopicNamesConverterFactory>(
                    AppData(ctx)->PQConfig, localCluster
            );
        }
        TopicsHandler = std::make_unique<NPersQueue::TTopicsListController>(
                ConverterFactory, TVector<TString>{}
        );
        const ui64 cookie = NextCookie();

        LOG_DEBUG_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "new session created cookie " << cookie);

        auto ip = ev->Get()->GetPeerName();
        TActorId worker = ctx.Register(new TWriteSessionActor<UseMigrationProtocol>(
                ev->Release().Release(), cookie, SchemeCache, Counters,
                DatacenterClassifier ? DatacenterClassifier->ClassifyAddress(NAddressClassifier::ExtractAddress(ip)) : "unknown",
                *TopicsHandler
        ));

        Sessions[cookie] = worker;
    }
}


}
}
}
