#include "grpc_pq_write.h"
#include "actors/helpers.h"
#include "actors/persqueue_utils.h"
#include "actors/write_session_pqv1_actor.h"
#include "actors/write_session_topic_api_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/grpc_request_proxy_handle_methods.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <util/generic/queue.h>
#include <util/generic/vector.h>

#include <optional>
#include <type_traits>

using namespace NActors;
using namespace NKikimrClient;

using grpc::Status;

namespace NKikimr {
namespace NGRpcProxy {
namespace V1 {

using namespace PersQueue::V1;

namespace {

template <EProtocol Protocol>
auto FillWriteResponse(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode code) {
    using ServerMessage = typename std::conditional<Protocol == EProtocol::PQv1,
                                                    PersQueue::V1::StreamingWriteServerMessage,
                                                    Topic::StreamWriteMessage::FromServer>::type;
    ServerMessage res;
    FillIssue(res.add_issues(), code, errorReason);
    res.set_status(ConvertPersQueueInternalCodeToStatus(code));
    return res;
}

} // namespace

///////////////////////////////////////////////////////////////////////////////

IActor* CreatePQWriteService(const TActorId& schemeCache,
                             TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const ui32 maxSessions) {
    return new TPQWriteService(schemeCache, counters, maxSessions);
}



TPQWriteService::TPQWriteService(const TActorId& schemeCache,
                             TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const ui32 maxSessions)
    : SchemeCache(schemeCache)
    , Counters(counters)
    , MaxSessions(maxSessions)
    , Enabled(false)
{
}


void TPQWriteService::Bootstrap(const TActorContext& ctx) {
    HaveClusters = !AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen(); // ToDo[migration]: switch to proper option
    if (HaveClusters) {
        YDB_LOG_DEBUG_CTX_COMP(ctx, NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "TPQWriteService: send TEvClusterTracker::TEvSubscribe");

        ctx.Send(NPQ::NClusterTracker::MakeClusterTrackerID(),
                 new NPQ::NClusterTracker::TEvClusterTracker::TEvSubscribe);
    }
    ctx.Send(NNetClassifier::MakeNetClassifierID(), new NNetClassifier::TEvNetClassifier::TEvSubscribe);
    Become(&TThis::StateFunc);
}


ui64 TPQWriteService::NextCookie() {
    return ++LastCookie;
}

void TPQWriteService::Handle(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate::TPtr& ev, const TActorContext& ctx) {

    if (!DatacenterClassifier) {
        for (auto it = Sessions.begin(); it != Sessions.end(); ++it) {
            ctx.Send(it->second, new TEvPQProxy::TEvDieCommand("datacenter classifier initialized, restart session please", PersQueue::ErrorCode::INITIALIZING));
        }
    }
    DatacenterClassifier = ev->Get()->Classifier;
}


void TPQWriteService::Handle(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev, const TActorContext& ctx) {
    AFL_ENSURE(ev->Get()->ClustersList)("local_cluster", LocalCluster)("enabled", Enabled);
    AFL_ENSURE(ev->Get()->ClustersList->Clusters.size())
        ("clusters", ev->Get()->ClustersList->Clusters.size())
        ("local_cluster", LocalCluster)
        ("enabled", Enabled);

    const auto& clusters = ev->Get()->ClustersList->Clusters;

    LocalCluster = "";
    Enabled = false;

    // Rebalance load on installation clusters: if preferred cluster is enabled and session is alive long enough close it so client can recreate it in preferred cluster
    auto remoteClusterEnabledDelay = TDuration::Seconds(AppData(ctx)->PQConfig.GetRemoteClusterEnabledDelaySec());
    auto closeClientSessionWithEnabledRemotePreferredClusterDelay = TDuration::Seconds(AppData(ctx)->PQConfig.GetCloseClientSessionWithEnabledRemotePreferredClusterDelaySec());
    const auto clustersListUpdatedAt = ev->Get()->ClustersListUpdateTimestamp ? *ev->Get()->ClustersListUpdateTimestamp : TInstant::Now();
    THashSet<TString> remoteClusters;
    THashSet<TString> rebalanceClusters;
    for (const auto& cluster : clusters) {
        if (cluster.IsLocal) {
            LocalCluster = cluster.Name;
            Enabled = cluster.IsEnabled;
            continue;
        }

        remoteClusters.emplace(cluster.Name);

        if (!cluster.IsEnabled) {
            ClustersEnabledAt.erase(cluster.Name);
            continue;
        }

        if (!ClustersEnabledAt.contains(cluster.Name)) {
            ClustersEnabledAt[cluster.Name] = clustersListUpdatedAt;
        }

        const bool readyToCreateSessions = ClustersEnabledAt[cluster.Name] <= (TInstant::Now() - remoteClusterEnabledDelay);
        if (readyToCreateSessions) {
            rebalanceClusters.emplace(cluster.Name);
        }
    }

    if (!Enabled) {
        for (auto it = Sessions.begin(); it != Sessions.end(); ++it) {
            Send(it->second, new TEvPQProxy::TEvDieCommand("cluster disabled", PersQueue::ErrorCode::CLUSTER_DISABLED));
        }
        return;
    }

    for (const auto& sessionsByPreferredCluster : SessionsByRemotePreferredCluster) {
        const auto& cluster = sessionsByPreferredCluster.first;
        if (rebalanceClusters.contains(cluster) || !remoteClusters.contains(cluster)) {
            const TString closeReason = TStringBuilder() << "Session preferred cluster " << cluster.Quote()
                << (remoteClusters.contains(cluster) ? " is enabled for at least " + ToString(closeClientSessionWithEnabledRemotePreferredClusterDelay) : " is unknown")
                << " and session is older than " << closeClientSessionWithEnabledRemotePreferredClusterDelay;

            const auto closeUpToCreatedAt = TInstant::Now() - closeClientSessionWithEnabledRemotePreferredClusterDelay;

            for (const auto& session : sessionsByPreferredCluster.second) {
                const auto& createdAt = session.second;
                if (createdAt <= closeUpToCreatedAt) {
                    const auto& workerID = Sessions[session.first];
                    Send(workerID, new TEvPQProxy::TEvDieCommand(closeReason, PersQueue::ErrorCode::PREFERRED_CLUSTER_MISMATCHED));
                }
            }
        }
    }
}

void TPQWriteService::Handle(TEvPQProxy::TEvSessionSetPreferredCluster::TPtr& ev, const TActorContext& ctx) {
    const auto& cookie = ev->Get()->Cookie;
    const auto& preferredCluster = ev->Get()->PreferredCluster;
    if (!Sessions.contains(cookie)) {
        YDB_LOG_ERROR_CTX_COMP(ctx, NKikimrServices::PQ_WRITE_PROXY, "Got TEvSessionSetPreferredCluster message from session with cookie that is not in session collection",
            {"cookie", cookie});
        return;
    }
    if (!preferredCluster.empty() && *LocalCluster != preferredCluster) {
        SessionsByRemotePreferredCluster[preferredCluster][cookie] = TInstant::Now();
        RemotePreferredClusterBySessionCookie[cookie] = std::move(preferredCluster);
    }
}

void TPQWriteService::Handle(TEvPQProxy::TEvSessionDead::TPtr& ev, const TActorContext&) {
    const auto& cookie = ev->Get()->Cookie;
    Sessions.erase(cookie);
    if (RemotePreferredClusterBySessionCookie.contains(cookie)) {
        const auto& preferredCluster = RemotePreferredClusterBySessionCookie[cookie];
        SessionsByRemotePreferredCluster[preferredCluster].erase(cookie);
        if (SessionsByRemotePreferredCluster[preferredCluster].empty()) {
            SessionsByRemotePreferredCluster.erase(preferredCluster);
        }
        RemotePreferredClusterBySessionCookie.erase(cookie);
    }
}


template <typename WriteRequest>
void TPQWriteService::HandleWriteRequest(typename WriteRequest::TPtr& ev, const TActorContext& ctx) {
    constexpr EProtocol Protocol = std::is_same_v<WriteRequest, NGRpcService::TEvStreamPQWriteRequest> ? EProtocol::PQv1 : EProtocol::Topic;

    YDB_LOG_DEBUG_CTX_COMP(ctx, NKikimrServices::PQ_WRITE_PROXY, "New grpc connection");

    if (TooMuchSessions()) {
        YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::PQ_WRITE_PROXY, "New grpc connection failed - too much sessions");
        ev->Get()->Attach(ctx.SelfID);
        ev->Get()->WriteAndFinish(
            FillWriteResponse<Protocol>("proxy overloaded", PersQueue::ErrorCode::OVERLOAD),
            Ydb::StatusIds::OVERLOADED); // CANCELLED
        return;
    }

    TString localCluster = AvailableLocalCluster(ctx);

    if (HaveClusters && localCluster.empty()) {
        ev->Get()->Attach(ctx.SelfID);
        if (LocalCluster) {
            YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::PQ_WRITE_PROXY, "New grpc connection failed - cluster disabled");
            ev->Get()->WriteAndFinish(FillWriteResponse<Protocol>("cluster disabled", PersQueue::ErrorCode::CLUSTER_DISABLED), Ydb::StatusIds::UNSUPPORTED); //CANCELLED
        } else {
            YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::PQ_WRITE_PROXY, "New grpc connection failed - initializing");
            ev->Get()->WriteAndFinish(FillWriteResponse<Protocol>("initializing", PersQueue::ErrorCode::INITIALIZING), Ydb::StatusIds::UNAVAILABLE); //CANCELLED
        }
        return;
    }

    if (ConverterFactory == nullptr) {
        ConverterFactory = std::make_shared<NPersQueue::TTopicNamesConverterFactory>(
                AppData(ctx)->PQConfig, localCluster
        );
    }
    TopicsHandler = std::make_unique<NPersQueue::TTopicsListController>(
            ConverterFactory, TVector<TString>{}
    );
    const ui64 cookie = NextCookie();

    YDB_LOG_DEBUG_CTX_COMP(ctx, NKikimrServices::PQ_WRITE_PROXY, "New session created cookie",
        {"cookie", cookie});

    auto ip = ev->Get()->GetPeerName();
    std::optional<TString> clientDC;
    if (DatacenterClassifier) {
        if (auto dc = DatacenterClassifier->ClassifyAddress(NAddressClassifier::ExtractAddress(ip))) {
            clientDC = *dc;
        }
    } else {
        clientDC = "unknown";
    }

    NActors::IActor* session = nullptr;
    if constexpr (Protocol == EProtocol::PQv1) {
        session = CreateWriteSessionPQv1Actor(
            ev->Release().Release(), cookie, Counters, clientDC, *TopicsHandler);
    } else {
        session = CreateWriteSessionTopicApiActor(
            ev->Release().Release(), cookie, Counters, clientDC, *TopicsHandler);
    }
    TActorId worker = ctx.Register(session);

    Sessions[cookie] = worker;
}

void TPQWriteService::Handle(NKikimr::NGRpcService::TEvStreamTopicWriteRequest::TPtr& ev, const TActorContext& ctx) {
    HandleWriteRequest<NKikimr::NGRpcService::TEvStreamTopicWriteRequest>(ev, ctx);
}

void TPQWriteService::Handle(NKikimr::NGRpcService::TEvStreamPQWriteRequest::TPtr& ev, const TActorContext& ctx) {
    HandleWriteRequest<NKikimr::NGRpcService::TEvStreamPQWriteRequest>(ev, ctx);
}

bool TPQWriteService::TooMuchSessions() {
    return Sessions.size() >= MaxSessions;
}

TString TPQWriteService::AvailableLocalCluster(const TActorContext&) const {
    return HaveClusters && Enabled ? *LocalCluster : "";
}

///////////////////////////////////////////////////////////////////////////////

}
}
}


void NKikimr::NGRpcService::TGRpcRequestProxyHandleMethods::Handle(NKikimr::NGRpcService::TEvStreamPQWriteRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQWriteServiceActorID(), ev->Release().Release());
}

void NKikimr::NGRpcService::TGRpcRequestProxyHandleMethods::Handle(NKikimr::NGRpcService::TEvStreamTopicWriteRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQWriteServiceActorID(), ev->Release().Release());
}
