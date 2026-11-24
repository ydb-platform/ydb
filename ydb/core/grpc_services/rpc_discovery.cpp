#include "grpc_request_proxy.h"

#include "long_poll_list_endpoints.h"
#include "rpc_kqp_base.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/location.h>
#include <ydb/core/base/path.h>
#include <ydb/core/discovery/discovery.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/async/wait_for_event.h>

#include <util/random/shuffle.h>

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace Ydb;
using namespace NKqp;

class TListEndpointsRPC : public TActorBootstrapped<TListEndpointsRPC> {
    THolder<TEvListEndpointsRequest> Request;
    TLongPollListEndpoints* const Poll;
    const TActorId ProxyId;
    const TActorId CacheId;
    TString WatchedPath;
    TActorId Discoverer;

    THolder<TEvDiscovery::TEvDiscoveryData> LookupResponse;
    THolder<TEvInterconnect::TEvNodeInfo> NameserviceResponse;

    NWilson::TSpan Span;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TListEndpointsRPC(TEvListEndpointsRequest::TPtr &msg, TActorId cacheId, TActorId proxyId)
        : Request(msg->Release().Release())
        , Poll(dynamic_cast<TLongPollListEndpoints*>(Request.Get()))
        , ProxyId(proxyId)
        , CacheId(cacheId)
        , Span(TWilsonGrpc::RequestActor, Request->GetWilsonTraceId(), "ListEndpointsRpc")
    {}

    void Bootstrap() {
        // request endpoints
        const TString& database = Request->GetProtoRequest()->database();
        const TString resolvedDatabase = AppData()->FeatureFlags.GetEnableRelativePaths()
            ? PrependDomainIfNeeded(TString("/") + AppData()->DomainsInfo->GetDomain()->Name, database)
            : database;
        Discoverer = Register(CreateDiscoverer(&MakeEndpointsBoardPath,
            Request->NormalizePath(resolvedDatabase),
            !Poll && Request->GetEndpointId().empty() && Request->GetProtoRequest()->Getservice().empty(),
            SelfId(), CacheId));

        // request self node info
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(SelfId().NodeId()));

        Become(&TThis::StateWait);
    }

    void PassAway() override {
        if (!WatchedPath.empty()) {
            Send(CacheId, new TEvDiscovery::TEvUnwatch(WatchedPath));
        }
        if (Discoverer) {
            Send(Discoverer, new TEvents::TEvPoisonPill());
        }
        Span.EndOk();

        TActorBootstrapped<TListEndpointsRPC>::PassAway();
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDiscovery::TEvDiscoveryData, Handle);
            hFunc(TEvInterconnect::TEvNodeInfo, Handle);
            hFunc(TEvDiscovery::TEvError, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        }
    }

    void Handle(TEvDiscovery::TEvDiscoveryData::TPtr &ev) {
        Discoverer = {};

        LookupResponse.Reset(ev->Release().Release());
        TryReplyAndDie();
    }

    void Handle(TEvInterconnect::TEvNodeInfo::TPtr &ev) {
        NameserviceResponse.Reset(ev->Release().Release());
        TryReplyAndDie();
    }

    void Handle(TEvDiscovery::TEvError::TPtr &ev) {
        Discoverer = {};

        auto issue = MakeIssue(ErrorToIssueCode(ev->Get()->Status), ev->Get()->Error);
        Request->RaiseIssue(issue);
        Reply(ErrorToStatusCode(ev->Get()->Status));
    }

    static NKikimrIssues::TIssuesIds::EIssueCode ErrorToIssueCode(TEvDiscovery::TEvError::EStatus status) {
        switch (status) {
            case TEvDiscovery::TEvError::KEY_PARSE_ERROR: return NKikimrIssues::TIssuesIds::KEY_PARSE_ERROR;
            case TEvDiscovery::TEvError::RESOLVE_ERROR: return NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR;
            case TEvDiscovery::TEvError::DATABASE_NOT_EXIST: return NKikimrIssues::TIssuesIds::DATABASE_NOT_EXIST;
            case TEvDiscovery::TEvError::ACCESS_DENIED: return NKikimrIssues::TIssuesIds::ACCESS_DENIED;
            default: return NKikimrIssues::TIssuesIds::DEFAULT_ERROR;
        }
    }

    static Ydb::StatusIds::StatusCode ErrorToStatusCode(TEvDiscovery::TEvError::EStatus status) {
        switch (status) {
            case TEvDiscovery::TEvError::KEY_PARSE_ERROR: return Ydb::StatusIds::BAD_REQUEST;
            case TEvDiscovery::TEvError::RESOLVE_ERROR: return Ydb::StatusIds::UNAVAILABLE;
            case TEvDiscovery::TEvError::DATABASE_NOT_EXIST: return Ydb::StatusIds::NOT_FOUND;
            case TEvDiscovery::TEvError::ACCESS_DENIED: return Ydb::StatusIds::NOT_FOUND;
            default: return Ydb::StatusIds::BAD_REQUEST;
        }
    }

    bool CheckServices(const TSet<TString> &req, const NKikimrStateStorage::TEndpointBoardEntry &entry) {
        if (req.empty())
            return true;

        for (const auto &x : entry.GetServices())
            if (req.count(x))
                return true;

        return false;
    }

    void WaitForChange() {
        WatchedPath = LookupResponse->BoardPath;
        const auto revision = LookupResponse->Revision;
        LookupResponse.Reset();
        const auto self = SelfId();
        Request->SetFinishAction([system = TActivationContext::ActorSystem(), self] {
            system->Send(new IEventHandle(self, {}, new TEvents::TEvPoisonPill));
        });
        Send(CacheId, new TEvDiscovery::TEvWatch(WatchedPath, revision));
        Schedule(Poll->HoldUntil, new TEvDiscovery::TEvChanged);
        co_await ActorWaitForEvent<TEvDiscovery::TEvChanged>(0);
        if (!Request->IsClientLost()) {
            // Resolve credentials, aliases, and the current assignment again before replying.
            Poll->UpdateAuthState(NYdbGrpc::TAuthState::AS_NOT_PERFORMED);
            Send(ProxyId, new TLongPollListEndpoints(Poll->Transport, Poll->HoldUntil));
        }
        PassAway();
    }

    void TryReplyAndDie() {
        if (!NameserviceResponse || !LookupResponse)
            return;

        Y_ABORT_UNLESS(LookupResponse->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok);

        const TSet<TString> services(
            Request->GetProtoRequest()->Getservice().begin(), Request->GetProtoRequest()->Getservice().end());

        TString cachedMessage, cachedMessageSsl;

        TString endpointId = Request->GetEndpointId();

        if (!LookupResponse->CachedMessage.empty() && !LookupResponse->CachedMessageSsl.empty()) {
            cachedMessage = LookupResponse->CachedMessage;
            cachedMessageSsl = LookupResponse->CachedMessageSsl;
        } else {
            NDiscovery::TCachedMessageData cachedMessageData(
                LookupResponse->InfoEntries,
                NameserviceResponse, LookupResponse->BridgeInfo,
                endpointId, services, LookupResponse->Status,
                Poll ? NDiscovery::EEndpointVersion::Include : NDiscovery::EEndpointVersion::Omit);
            if (Poll) {
                const auto& version = Request->SslServer() ? cachedMessageData.EndpointVersionSsl : cachedMessageData.EndpointVersion;
                const auto condition = Poll->Transport->GetPeerMetaValues("x-ydb-if-endpoint-version");
                // Older managed workers already waited before forwarding their route condition.
                if (condition.size() == 1 && condition.front() == version && TInstant::Now() < Poll->HoldUntil
                        && Poll->Transport->GetPeerMetaValues("x-ydb-if-route-generation").empty()) {
                    WaitForChange();
                    return;
                }
                Poll->Transport->AddTrailingMetadata("x-ydb-endpoint-version", version);
            }

            cachedMessage = std::move(cachedMessageData.CachedMessage);
            cachedMessageSsl = std::move(cachedMessageData.CachedMessageSsl);
        }

        if (Request->SslServer()) {
            ReplySerialized(std::move(cachedMessageSsl), Ydb::StatusIds::SUCCESS);
        } else {
            ReplySerialized(std::move(cachedMessage), Ydb::StatusIds::SUCCESS);
        }
    }

    void ReplySerialized(TString message, Ydb::StatusIds::StatusCode status) {
        Request->SendSerializedResult(std::move(message), status);
        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status) {
        Request->ReplyWithYdbStatus(status);
        PassAway();
    }
};

void TGRpcRequestProxy::Handle(TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx) {
    if (!DiscoveryCacheActorID) {
        DiscoveryCacheActorID = ctx.Register(CreateDiscoveryCache());
    }

    ctx.Register(new TListEndpointsRPC(ev, DiscoveryCacheActorID, ctx.SelfID));
}

}
