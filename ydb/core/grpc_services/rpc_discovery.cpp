#include "grpc_request_proxy.h"

#include "rpc_calls.h"
#include "rpc_kqp_base.h"

#include <ydb/core/base/location.h>
#include <ydb/core/discovery/discovery.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/interconnect/interconnect.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/random/shuffle.h>

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace Ydb;
using namespace NKqp;

class TListEndpointsRPC : public TActorBootstrapped<TListEndpointsRPC> {
    THolder<TEvListEndpointsRequest> Request;
    const TActorId CacheId;
    TActorId Discoverer;

    THolder<TEvDiscovery::TEvDiscoveryData> LookupResponse;
    THolder<TEvInterconnect::TEvNodeInfo> NameserviceResponse;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TListEndpointsRPC(TEvListEndpointsRequest::TPtr &msg, TActorId cacheId)
        : Request(msg->Release().Release())
        , CacheId(cacheId)
    {}

    void Bootstrap() {
        // request endpoints
        Discoverer = Register(CreateDiscoverer(&MakeEndpointsBoardPath,
            Request->GetProtoRequest()->database(), SelfId(), CacheId));

        // request self node info
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(SelfId().NodeId()));

        Become(&TThis::StateWait);
    }

    void PassAway() override {
        if (Discoverer) {
            Send(Discoverer, new TEvents::TEvPoisonPill());
        }

        TActorBootstrapped<TListEndpointsRPC>::PassAway();
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDiscovery::TEvDiscoveryData, Handle);
            hFunc(TEvInterconnect::TEvNodeInfo, Handle);
            hFunc(TEvDiscovery::TEvError, Handle);
        }
    }

    void Handle(TEvDiscovery::TEvDiscoveryData::TPtr &ev) {
        Y_VERIFY(ev->Get()->CachedMessageData);
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
        google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issueMessages;
        NYql::IssueToMessage(issue, issueMessages.Add());
        Reply(ErrorToStatusCode(ev->Get()->Status), issueMessages);
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

    void TryReplyAndDie() {
        if (!NameserviceResponse || !LookupResponse)
            return;

        Y_VERIFY(LookupResponse->CachedMessageData && !LookupResponse->CachedMessageData->InfoEntries.empty() &&
            LookupResponse->CachedMessageData->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok);

        const TSet<TString> services(
            Request->GetProtoRequest()->Getservice().begin(), Request->GetProtoRequest()->Getservice().end());

        TString cachedMessage, cachedMessageSsl;

        if (services.empty() && !LookupResponse->CachedMessageData->CachedMessage.empty() &&
                !LookupResponse->CachedMessageData->CachedMessageSsl.empty()) {
            cachedMessage = LookupResponse->CachedMessageData->CachedMessage;
            cachedMessageSsl = LookupResponse->CachedMessageData->CachedMessageSsl;
        } else {
            auto cachedMessageData = NDiscovery::CreateCachedMessage(
                {}, std::move(LookupResponse->CachedMessageData->InfoEntries),
                std::move(services), NameserviceResponse);
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

    template <typename... Args>
    void Reply(Args&&... args) {
        Request->SendResult(std::forward<Args>(args)...);
        PassAway();
    }
};

void TGRpcRequestProxy::Handle(TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx) {
    if (!DiscoveryCacheActorID) {
        DiscoveryCacheActorID = ctx.Register(CreateDiscoveryCache());
    }

    ctx.Register(new TListEndpointsRPC(ev, DiscoveryCacheActorID));
}

}
