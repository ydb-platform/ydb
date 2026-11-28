#include "grpc_request_proxy.h"

#include "rpc_calls.h"
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

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace Ydb;
using namespace NKqp;

class TListEndpointsRPC : public TActorBootstrapped<TListEndpointsRPC> {
    THolder<TEvListEndpointsRequest> Request;
    const TActorId CacheId;
    TActorId Discoverer;

    NWilson::TSpan Span;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TListEndpointsRPC(TEvListEndpointsRequest::TPtr &msg, TActorId cacheId)
        : Request(msg->Release().Release())
        , CacheId(cacheId)
        , Span(TWilsonGrpc::RequestActor, Request->GetWilsonTraceId(), "ListEndpointsRpc")
    {}

    void Bootstrap() {
        const auto& request = *Request->GetProtoRequest();
        const bool wait = AppData()->FeatureFlags.GetEnableDiscoveryLongPolling() && !request.snapshot_hash().empty();
        std::shared_ptr<NDiscovery::TListEndpointsOptions> options;
        if (wait || !Request->GetEndpointId().empty() || !request.service().empty()) {
            options = std::make_shared<NDiscovery::TListEndpointsOptions>();
            options->EndpointId = Request->GetEndpointId();
            options->Services.insert(request.service().begin(), request.service().end());
            options->Ssl = Request->SslServer();
        }
        if (wait) {
            options->SnapshotHash = request.snapshot_hash();
            Request->SetFinishAction([system = TActivationContext::ActorSystem(), self = SelfId()] {
                system->Send(self, new TEvents::TEvPoisonPill);
            });
        }
        const TString& database = request.database();
        const TString resolvedDatabase = AppData()->FeatureFlags.GetEnableRelativePaths()
            ? PrependDomainIfNeeded(TString("/") + AppData()->DomainsInfo->GetDomain()->Name, database)
            : database;
        Discoverer = Register(CreateDiscoverer(&MakeEndpointsBoardPath,
            Request->NormalizePath(resolvedDatabase), true, SelfId(), CacheId, std::move(options)));

        Become(&TThis::StateWait);
    }

    void PassAway() override {
        if (Discoverer) {
            Send(Discoverer, new TEvents::TEvPoisonPill());
        }
        Span.EndOk();

        TActorBootstrapped<TListEndpointsRPC>::PassAway();
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDiscovery::TEvDiscoveryData, Handle);
            hFunc(TEvDiscovery::TEvError, Handle);
            sFunc(TEvents::TEvPoison, Cancel);
        }
    }

    void Cancel() {
        Request->ReplyWithRpcStatus(grpc::StatusCode::CANCELLED, {}, {});
        PassAway();
    }

    void Handle(TEvDiscovery::TEvDiscoveryData::TPtr &ev) {
        Discoverer = {};

        auto* result = ev->Get();
        Request->SendSerializedResult(Request->SslServer() ? std::move(result->CachedMessageSsl) : std::move(result->CachedMessage),
            Ydb::StatusIds::SUCCESS);
        PassAway();
    }

    void Handle(TEvDiscovery::TEvError::TPtr &ev) {
        Discoverer = {};

        auto issue = MakeIssue(ErrorToIssueCode(ev->Get()->Status), ev->Get()->Error);
        Request->RaiseIssue(issue);
        Request->ReplyWithYdbStatus(ErrorToStatusCode(ev->Get()->Status));
        PassAway();
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
};

void TGRpcRequestProxy::Handle(TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx) {
    if (!DiscoveryCacheActorID) {
        DiscoveryCacheActorID = ctx.Register(CreateDiscoveryCache({}, {}, AppData(ctx)->FeatureFlags.GetEnableDiscoveryLongPolling()));
    }
    ctx.Register(new TListEndpointsRPC(ev, DiscoveryCacheActorID));
}

}
