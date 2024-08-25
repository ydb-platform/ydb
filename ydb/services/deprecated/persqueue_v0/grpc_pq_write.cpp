#include "grpc_pq_write.h"
#include "grpc_pq_actor.h"
#include "grpc_pq_session.h"
#include "ydb/core/client/server/grpc_proxy_status.h"

#include <ydb/core/base/appdata.h>
#include <util/generic/queue.h>

using namespace NActors;
using namespace NKikimrClient;

using grpc::Status;

namespace NKikimr {
namespace NGRpcProxy {

using namespace NPersQueue;

///////////////////////////////////////////////////////////////////////////////


void TPQWriteServiceImpl::TSession::OnCreated() {            // Start waiting for new session.
    Proxy->WaitWriteSession();
    if (Proxy->TooMuchSessions()) {
        ReplyWithError("proxy overloaded", NPersQueue::NErrorCode::OVERLOAD);
        return;
    }
    TMaybe<TString> localCluster = Proxy->AvailableLocalCluster();
    if (NeedDiscoverClusters) {
        if (!localCluster.Defined()) {
            ReplyWithError("initializing", NPersQueue::NErrorCode::INITIALIZING);
            return;
        } else if (localCluster->empty()) {
            ReplyWithError("cluster disabled", NPersQueue::NErrorCode::CLUSTER_DISABLED);
            return;
        } else {
            CreateActor(*localCluster);
        }
    } else {
        CreateActor(TString());
    }
    ReadyForNextRead();
}

void TPQWriteServiceImpl::TSession::OnRead(const TWriteRequest& request) {

    switch (request.GetRequestCase()) {
        case TWriteRequest::kInit: {
            SendEvent(new TEvPQProxy::TEvWriteInit(request, GetPeerName(), GetDatabase()));
            break;
        }
        case TWriteRequest::kDataBatch:
        case TWriteRequest::kData: {
            SendEvent(new TEvPQProxy::TEvWrite(request));
            break;
        }
        default: {
            ReplyWithError("unsupported request", NPersQueue::NErrorCode::BAD_REQUEST);
        }
    }
}

void TPQWriteServiceImpl::TSession::OnDone() {
    SendEvent(new TEvPQProxy::TEvDone());
}

TPQWriteServiceImpl::TSession::TSession(std::shared_ptr<TPQWriteServiceImpl> proxy,
             grpc::ServerCompletionQueue* cq, ui64 cookie, const TActorId& schemeCache,
             TIntrusivePtr<NMonitoring::TDynamicCounters> counters, bool needDiscoverClusters)
    : ISession(cq)
    , Proxy(proxy)
    , Cookie(cookie)
    , SchemeCache(schemeCache)
    , Counters(counters)
    , NeedDiscoverClusters(needDiscoverClusters)
{
}

void TPQWriteServiceImpl::TSession::Start() {
    if (!Proxy->IsShuttingDown()) {
        Proxy->RequestSession(&Context, &Stream, CQ, CQ, new TRequestCreated(this));
    }
}

ui64 TPQWriteServiceImpl::TSession::GetCookie() const {
    return Cookie;
}

void TPQWriteServiceImpl::TSession::DestroyStream(const TString& reason, const NPersQueue::NErrorCode::EErrorCode errorCode) {
    // Send poison pill to the actor(if it is alive)
    SendEvent(new TEvPQProxy::TEvDieCommand("write-session " + ToString<ui64>(Cookie) + ": " + reason, errorCode));
    // Remove reference to session from "cookie -> session" map.
    Proxy->ReleaseSession(this);
}

bool TPQWriteServiceImpl::TSession::IsShuttingDown() const {
    return Proxy->IsShuttingDown();
}

void TPQWriteServiceImpl::TSession::CreateActor(const TString &localCluster) {

    auto classifier = Proxy->GetClassifier();
    ActorId = Proxy->ActorSystem->Register(
        new TWriteSessionActor(this, Cookie, SchemeCache, Counters, localCluster,
                                        classifier ? classifier->ClassifyAddress(GetPeerName())
                                                   : "unknown"), TMailboxType::Simple, 0
    );
}

void TPQWriteServiceImpl::TSession::SendEvent(IEventBase* ev) {
    Proxy->ActorSystem->Send(ActorId, ev);
}

///////////////////////////////////////////////////////////////////////////////


TPQWriteServiceImpl::TPQWriteServiceImpl(const std::vector<grpc::ServerCompletionQueue*>& cqs,
                             NActors::TActorSystem* as, const TActorId& schemeCache,
                             TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const ui32 maxSessions)
    : CQS(cqs)
    , ActorSystem(as)
    , SchemeCache(schemeCache)
    , Counters(counters)
    , MaxSessions(maxSessions)
    , NeedDiscoverClusters(false)
{
}

void TPQWriteServiceImpl::InitClustersUpdater()
{
    TAppData* appData = ActorSystem->AppData<TAppData>();
    NeedDiscoverClusters = !appData->PQConfig.GetTopicsAreFirstClassCitizen();
    if (NeedDiscoverClusters) {
        ActorSystem->Register(new TClustersUpdater(this));
    }
}


ui64 TPQWriteServiceImpl::NextCookie() {
    return AtomicIncrement(LastCookie);
}


void TPQWriteServiceImpl::ReleaseSession(TSessionRef session) {
    with_lock (Lock) {
        bool erased = Sessions.erase(session->GetCookie());
        if (erased) {
            ActorSystem->Send(MakeGRpcProxyStatusID(ActorSystem->NodeId), new TEvGRpcProxyStatus::TEvUpdateStatus(0, 0, -1, 0));
        }
    }
}


void TPQWriteServiceImpl::SetupIncomingRequests() {
    WaitWriteSession();
}


void TPQWriteServiceImpl::WaitWriteSession() {

    const ui64 cookie = NextCookie();

    ActorSystem->Send(MakeGRpcProxyStatusID(ActorSystem->NodeId), new TEvGRpcProxyStatus::TEvUpdateStatus(0,0,1,0));

    TSessionRef session(new TSession(shared_from_this(), CQS[cookie % CQS.size()], cookie, SchemeCache, Counters, NeedDiscoverClusters));
    {
        with_lock (Lock) {
            Sessions[cookie] = session;
        }
    }

    session->Start();
}


bool TPQWriteServiceImpl::TooMuchSessions() {
    with_lock (Lock) {
        return Sessions.size() >= MaxSessions;
    }
}


TMaybe<TString> TPQWriteServiceImpl::AvailableLocalCluster() {
    with_lock (Lock) {
        return AvailableLocalClusterName;
    }
}


void TPQWriteServiceImpl::NetClassifierUpdated(NAddressClassifier::TLabeledAddressClassifier::TConstPtr classifier) {
    auto g(Guard(Lock));
    if (!DatacenterClassifier) {
        for (auto it = Sessions.begin(); it != Sessions.end();) {
            auto jt = it++;
            jt->second->DestroyStream("datacenter classifier initialized, restart session please", NPersQueue::NErrorCode::INITIALIZING);
        }
    }

    DatacenterClassifier = classifier;
}



void TPQWriteServiceImpl::CheckClusterChange(const TString &localCluster, const bool enabled) {
    with_lock (Lock) {
        AvailableLocalClusterName = enabled ? localCluster : TString();

        if (!enabled) {
            for (auto it = Sessions.begin(); it != Sessions.end();) {
                auto jt = it++;
                jt->second->DestroyStream("cluster disabled", NPersQueue::NErrorCode::CLUSTER_DISABLED);
            }
        }
    }
}


///////////////////////////////////////////////////////////////////////////////

}
}
