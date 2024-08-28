#include "grpc_pq_read.h"
#include "grpc_pq_actor.h"
#include "grpc_pq_session.h"
#include "ydb/core/client/server/grpc_proxy_status.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/tx/scheme_board/cache.h>

using namespace NActors;
using namespace NKikimrClient;

using grpc::Status;

namespace NKikimr {
namespace NGRpcProxy {

///////////////////////////////////////////////////////////////////////////////

using namespace NPersQueue;

void TPQReadService::TSession::OnCreated() {
    // Start waiting for new session.
    Proxy->WaitReadSession();
    if (Proxy->TooMuchSessions()) {
        ReplyWithError("proxy overloaded", NPersQueue::NErrorCode::OVERLOAD);
        return;
    }
    // Create actor for current session.
    auto clusters = Proxy->GetClusters();
    auto localCluster = Proxy->GetLocalCluster();
    if (NeedDiscoverClusters && (clusters.empty() || localCluster.empty())) {
        //TODO: inc sli errors counter
        ReplyWithError("clusters list or local cluster is empty", NPersQueue::NErrorCode::INITIALIZING);
        return;

    }
    if (!TopicConverterFactory->GetLocalCluster().empty()) {
        TopicConverterFactory->SetLocalCluster(localCluster);
    }
    auto topicsHandler = std::make_unique<NPersQueue::TTopicsListController>(
        TopicConverterFactory, clusters
    );

    CreateActor(std::move(topicsHandler));
    ReadyForNextRead();
}

void TPQReadService::TSession::OnRead(const NPersQueue::TReadRequest& request) {
    switch (request.GetRequestCase()) {
        case TReadRequest::kInit: {
            SendEvent(new TEvPQProxy::TEvReadInit(request, GetPeerName(), GetDatabase()));
            break;
        }
        case TReadRequest::kRead: {
            SendEvent(new TEvPQProxy::TEvRead(request));
            break;
        }
        case TReadRequest::kStatus: {
            Y_ABORT_UNLESS(ActorId);
            const auto& req = request.GetStatus();
            const TString& topic = req.GetTopic();
            const ui32 partition = req.GetPartition();
            const ui64 generation = req.GetGeneration();
            SendEvent(new TEvPQProxy::TEvGetStatus(topic, partition, generation));
            ReadyForNextRead();
            break;
        }
        case TReadRequest::kStartRead: {
            Y_ABORT_UNLESS(ActorId);
            const auto& req = request.GetStartRead();
            const TString& topic = req.GetTopic();
            const ui32 partition = req.GetPartition();
            const ui64 readOffset = req.GetReadOffset();
            const ui64 commitOffset = req.GetCommitOffset();
            const bool verifyReadOffset = req.GetVerifyReadOffset();
            const ui64 generation = req.GetGeneration();

            if (request.GetCredentials().GetCredentialsCase() != NPersQueueCommon::TCredentials::CREDENTIALS_NOT_SET) {
                SendEvent(new TEvPQProxy::TEvAuth(request.GetCredentials()));
            }
            SendEvent(new TEvPQProxy::TEvLocked(topic, partition, readOffset, commitOffset, verifyReadOffset, generation));
            ReadyForNextRead();
            break;
        }
        case TReadRequest::kCommit: {
            Y_ABORT_UNLESS(ActorId);
            const auto& req = request.GetCommit();

            if (request.GetCredentials().GetCredentialsCase() != NPersQueueCommon::TCredentials::CREDENTIALS_NOT_SET) {
                SendEvent(new TEvPQProxy::TEvAuth(request.GetCredentials()));
            }

            // Empty cookies list will lead to no effect.
            for (ui32 i = 0; i < req.CookieSize(); ++i) {
                SendEvent(new TEvPQProxy::TEvCommit(req.GetCookie(i)));
            }

            ReadyForNextRead();
            break;
        }

        default: {
            SendEvent(new TEvPQProxy::TEvCloseSession("unsupported request", NPersQueue::NErrorCode::BAD_REQUEST));
            break;
        }
    }
}

void TPQReadService::TSession::OnDone() {
    SendEvent(new TEvPQProxy::TEvDone());
}

void TPQReadService::TSession::OnWriteDone(ui64 size) {
    SendEvent(new TEvPQProxy::TEvWriteDone(size));
}

void TPQReadService::TSession::DestroyStream(const TString& reason, const NPersQueue::NErrorCode::EErrorCode errorCode) {
    // Send poison pill to the actor(if it is alive)
    SendEvent(new TEvPQProxy::TEvDieCommand("read-session " + ToString<ui64>(Cookie) + ": " + reason, errorCode));
    // Remove reference to session from "cookie -> session" map.
    Proxy->ReleaseSession(Cookie);
}

bool TPQReadService::TSession::IsShuttingDown() const {
    return Proxy->IsShuttingDown();
}

TPQReadService::TSession::TSession(std::shared_ptr<TPQReadService> proxy,
         grpc::ServerCompletionQueue* cq, ui64 cookie, const TActorId& schemeCache, const TActorId& newSchemeCache,
         TIntrusivePtr<NMonitoring::TDynamicCounters> counters,  bool needDiscoverClusters,
         const NPersQueue::TConverterFactoryPtr& converterFactory)
    : ISession(cq)
    , Proxy(proxy)
    , Cookie(cookie)
    , ActorId()
    , SchemeCache(schemeCache)
    , NewSchemeCache(newSchemeCache)
    , Counters(counters)
    , NeedDiscoverClusters(needDiscoverClusters)
    , TopicConverterFactory(converterFactory)
{
}

void TPQReadService::TSession::Start() {
    if (!Proxy->IsShuttingDown()) {
        Proxy->RequestSession(&Context, &Stream, CQ, CQ, new TRequestCreated(this));
    }
}

void TPQReadService::TSession::SendEvent(IEventBase* ev) {
    Proxy->ActorSystem->Send(ActorId, ev);
}

void TPQReadService::TSession::CreateActor(std::unique_ptr<NPersQueue::TTopicsListController>&& topicsHandler) {
    auto classifier = Proxy->GetClassifier();

    auto* actor = new TReadSessionActor(this, *topicsHandler, Cookie, SchemeCache, NewSchemeCache, Counters,
                                    classifier ? classifier->ClassifyAddress(GetPeerName()) : "unknown");
    ui32 poolId = Proxy->ActorSystem->AppData<::NKikimr::TAppData>()->UserPoolId;
    ActorId = Proxy->ActorSystem->Register(actor, TMailboxType::HTSwap, poolId);
}



ui64 TPQReadService::TSession::GetCookie() const {
    return Cookie;
}

///////////////////////////////////////////////////////////////////////////////


TPQReadService::TPQReadService(NKikimr::NGRpcService::TGRpcPersQueueService* service,
                             const std::vector<grpc::ServerCompletionQueue*>& cqs,
                             NActors::TActorSystem* as, const TActorId& schemeCache,
                             TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
                             const ui32 maxSessions)
    : Service(service)
    , CQS(cqs)
    , ActorSystem(as)
    , SchemeCache(schemeCache)
    , Counters(counters)
    , MaxSessions(maxSessions)
{
    auto appData = ActorSystem->AppData<TAppData>();
    auto cacheCounters = GetServiceCounters(counters, "pqproxy|schemecache");
    auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(appData, cacheCounters);
    NewSchemeCache = ActorSystem->Register(CreateSchemeBoardSchemeCache(cacheConfig.Get()));
    // ToDo[migration]: Other conditions;
    NeedDiscoverClusters = !ActorSystem->AppData<TAppData>()->PQConfig.GetTopicsAreFirstClassCitizen();
    TopicConverterFactory = std::make_shared<NPersQueue::TTopicNamesConverterFactory>(
            ActorSystem->AppData<TAppData>()->PQConfig, ""
    );

    if (NeedDiscoverClusters) {
        ActorSystem->Register(new TClustersUpdater(this));
    }
}


ui64 TPQReadService::NextCookie() {
    return AtomicIncrement(LastCookie);
}


void TPQReadService::ReleaseSession(ui64 cookie) {
    auto g(Guard(Lock));
    bool erased = Sessions.erase(cookie);
    if (erased)
        ActorSystem->Send(MakeGRpcProxyStatusID(ActorSystem->NodeId), new TEvGRpcProxyStatus::TEvUpdateStatus(0,0,-1,0));

}

void TPQReadService::CheckClusterChange(const TString& localCluster, const bool) {
    auto g(Guard(Lock));
    LocalCluster = localCluster;
    TopicConverterFactory->SetLocalCluster(localCluster);
}

void TPQReadService::NetClassifierUpdated(NAddressClassifier::TLabeledAddressClassifier::TConstPtr classifier) {
    auto g(Guard(Lock));
    if (!DatacenterClassifier) {
        for (auto it = Sessions.begin(); it != Sessions.end();) {
            auto jt = it++;
            jt->second->DestroyStream("datacenter classifier initialized, restart session please", NPersQueue::NErrorCode::INITIALIZING);
        }
    }

    DatacenterClassifier = classifier;
}


void TPQReadService::CheckClustersListChange(const TVector<TString> &clusters) {
    auto g(Guard(Lock));
    Clusters = clusters;
}

void TPQReadService::SetupIncomingRequests() {
    WaitReadSession();
}


void TPQReadService::WaitReadSession() {

    const ui64 cookie = NextCookie();

    ActorSystem->Send(MakeGRpcProxyStatusID(ActorSystem->NodeId), new TEvGRpcProxyStatus::TEvUpdateStatus(0,0,1,0));

    TSessionRef session(new TSession(shared_from_this(), CQS[cookie % CQS.size()], cookie, SchemeCache, NewSchemeCache, Counters,
                                     NeedDiscoverClusters, TopicConverterFactory));

    {
        auto g(Guard(Lock));
        Sessions.insert(std::make_pair(cookie, session));
    }

    session->Start();
}



bool TPQReadService::TooMuchSessions() {
    auto g(Guard(Lock));
    return Sessions.size() >= MaxSessions;
}

///////////////////////////////////////////////////////////////////////////////

}
}
