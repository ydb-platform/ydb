#include "grpc_request_proxy.h"
#include "grpc_request_check_actor.h"

#include "rpc_calls.h"
#include "rpc_common.h"
#include "rpc_deferrable.h"

#include <ydb/core/yq/libs/audit/events/events.h>
#include <ydb/core/yq/libs/audit/yq_audit_service.h>
#include <ydb/core/yq/libs/control_plane_proxy/control_plane_proxy.h>
#include <ydb/core/yq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/yq/libs/control_plane_proxy/utils.h>

#include <ydb/library/aclib/aclib.h>

#include <library/cpp/actors/core/hfunc.h>

#include <util/generic/guid.h>
#include <util/string/split.h>

namespace NKikimr {
namespace NGRpcService {

using namespace Ydb;

template<class TEvRequest, class TEvImplRequest, class TEvImplResponse, class TResult>
class TYandexQueryRPC : public TRpcOperationRequestActor<TYandexQueryRPC<TEvRequest, TEvImplRequest, TEvImplResponse, TResult>, TEvRequest> {
private:
    const TVector<TString>& Sids;

    TString Token;
    TString FolderId;
    TString User;
    TString PeerName;
    TString UserAgent;
    TString RequestId;

public:
    using TBase = TRpcOperationRequestActor<TYandexQueryRPC<TEvRequest, TEvImplRequest, TEvImplResponse, TResult>, TEvRequest>;
    using TBase::TBase;
    using TBase::GetProtoRequest;
    using TBase::Request_;
    using TBase::PassAway;
    using TBase::Send;
    using TBase::Become;

    TYandexQueryRPC(IRequestOpCtx* request, const TVector<TString>& sids)
        : TBase(request)
        , Sids(sids)
    { }

    void Bootstrap() {
        auto requestQuery = dynamic_cast<TEvRequest*>(Request_.get());
        Y_VERIFY(requestQuery);

        PeerName = requestQuery->GetPeerName();
        UserAgent = requestQuery->GetPeerMetaValues("user-agent").GetOrElse("empty");
        RequestId = requestQuery->GetPeerMetaValues("x-request-id").GetOrElse(CreateGuidAsString());

        TMaybe<TString> authToken = requestQuery->GetYdbToken();
        if (!authToken) {
            Request_->RaiseIssue(NYql::TIssue("token is empty"));
            ReplyWithResult(StatusIds::BAD_REQUEST);
            return;
        }
        Token = *authToken;

        const TString scope = requestQuery->GetPeerMetaValues("x-yq-scope").GetOrElse("");
        if (!scope.StartsWith("yandexcloud://")) {
            Request_->RaiseIssue(NYql::TIssue("x-yq-scope should start with yandexcloud:// but got " + scope));
            ReplyWithResult(StatusIds::BAD_REQUEST);
            return;
        }

        const TVector<TString> path = StringSplitter(scope).Split('/').SkipEmpty();
        if (path.size() != 2 && path.size() != 3) { // todo: do not check against 3, backward compatibility
            Request_->RaiseIssue(NYql::TIssue("x-yq-scope format is invalid. Must be yandexcloud://folder_id, but got " + scope));
            ReplyWithResult(StatusIds::BAD_REQUEST);
            return;
        }

        FolderId = path.back();
        if (!FolderId) {
            Request_->RaiseIssue(NYql::TIssue("folder id is empty"));
            ReplyWithResult(StatusIds::BAD_REQUEST);
            return;
        }

        if (FolderId.length() > 1024) {
            Request_->RaiseIssue(NYql::TIssue("folder id length greater than 1024 characters: " + FolderId));
            ReplyWithResult(StatusIds::BAD_REQUEST);
            return;
        }

        const TString& internalToken = requestQuery->GetInternalToken();
        TVector<TString> permissions;
        if (internalToken) {
            NACLib::TUserToken userToken(internalToken);
            User = userToken.GetUserSID();
            for (const auto& sid: Sids) {
                if (userToken.IsExist(sid)) {
                    permissions.push_back(sid);
                }
            }
        }

        if (!User) {
            Request_->RaiseIssue(NYql::TIssue("Authorization error. Permission denied"));
            ReplyWithResult(StatusIds::UNAUTHORIZED);
            return;
        }

        auto request = std::make_unique<TEvImplRequest>(FolderId, *GetProtoRequest(), User, Token, permissions);
        Send(NYq::ControlPlaneProxyActorId(), request.release());

        Become(&TYandexQueryRPC::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvImplResponse, Handler);
    )

    template<typename T>
    void ProcessResponse(const T& response) {
        if (response.Issues) {
            Request_->RaiseIssues(response.Issues);
            ReplyWithResult(StatusIds::BAD_REQUEST);
        } else {
            ReplyWithResult(StatusIds::SUCCESS, response.Result);
        }
    }

    template<typename T> requires requires (T t) { t.AuditDetails; }
    void ProcessResponse(const T& response) {
        if (response.Issues) {
            Request_->RaiseIssues(response.Issues);
            ReplyWithResult(StatusIds::BAD_REQUEST);
        } else {
            ReplyWithResult(StatusIds::SUCCESS, response.Result);
        }

        NYq::TEvAuditService::TExtraInfo extraInfo{
            .Token = Token,
            .FolderId = FolderId,
            .User = User,
            .PeerName = PeerName,
            .UserAgent = UserAgent,
            .RequestId = RequestId,
        };

        Send(NYq::YqAuditServiceActorId(), NYq::TEvAuditService::MakeAuditEvent(
            std::move(extraInfo),
            *GetProtoRequest(),
            response.Issues,
            response.AuditDetails));
    }

    void Handler(typename TEvImplResponse::TPtr& ev) {
        const auto& response = *ev->Get();
        ProcessResponse(response);
    }

    void ReplyWithResult(StatusIds::StatusCode status) {
        Request_->ReplyWithYdbStatus(status);
        PassAway();
    }

    void ReplyWithResult(StatusIds::StatusCode status, const TResult& result) {
        Request_->SendResult(result, status);
        PassAway();
    }
};

void TGRpcRequestProxy::Handle(TEvYandexQueryCreateQueryRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.queries.create@as",
        "yq.queries.invoke@as",
        "yq.connections.use@as",
        "yq.bindings.use@as",
        "yq.resources.managePublic@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryCreateQueryRequest,
                                          NYq::TEvControlPlaneProxy::TEvCreateQueryRequest,
                                          NYq::TEvControlPlaneProxy::TEvCreateQueryResponse,
                                          YandexQuery::CreateQueryResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryListQueriesRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.queries.get@as",
        "yq.resources.viewPublic@as",
        "yq.resources.viewPrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryListQueriesRequest,
                                        NYq::TEvControlPlaneProxy::TEvListQueriesRequest,
                                        NYq::TEvControlPlaneProxy::TEvListQueriesResponse,
                                        YandexQuery::ListQueriesResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryDescribeQueryRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.queries.get@as",
        "yq.queries.viewAst@as",
        "yq.resources.viewPublic@as",
        "yq.resources.viewPrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryDescribeQueryRequest,
                                        NYq::TEvControlPlaneProxy::TEvDescribeQueryRequest,
                                        NYq::TEvControlPlaneProxy::TEvDescribeQueryResponse,
                                        YandexQuery::DescribeQueryResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryGetQueryStatusRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.queries.getStatus@as",
        "yq.resources.viewPublic@as",
        "yq.resources.viewPrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryGetQueryStatusRequest,
                                        NYq::TEvControlPlaneProxy::TEvGetQueryStatusRequest,
                                        NYq::TEvControlPlaneProxy::TEvGetQueryStatusResponse,
                                        YandexQuery::GetQueryStatusResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryModifyQueryRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.queries.update@as",
        "yq.queries.invoke@as",
        "yq.resources.managePublic@as",
        "yq.resources.managePrivate@as",
        "yq.connections.use@as",
        "yq.bindings.use@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryModifyQueryRequest,
                                        NYq::TEvControlPlaneProxy::TEvModifyQueryRequest,
                                        NYq::TEvControlPlaneProxy::TEvModifyQueryResponse,
                                        YandexQuery::ModifyQueryResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryDeleteQueryRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.queries.delete@as",
        "yq.resources.managePublic@as",
        "yq.resources.managePrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryDeleteQueryRequest,
                                        NYq::TEvControlPlaneProxy::TEvDeleteQueryRequest,
                                        NYq::TEvControlPlaneProxy::TEvDeleteQueryResponse,
                                        YandexQuery::DeleteQueryResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryControlQueryRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.queries.control@as",
        "yq.resources.managePublic@as",
        "yq.resources.managePrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryControlQueryRequest,
                                        NYq::TEvControlPlaneProxy::TEvControlQueryRequest,
                                        NYq::TEvControlPlaneProxy::TEvControlQueryResponse,
                                        YandexQuery::ControlQueryResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryGetResultDataRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.queries.getData@as",
        "yq.resources.viewPublic@as",
        "yq.resources.viewPrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryGetResultDataRequest,
                                        NYq::TEvControlPlaneProxy::TEvGetResultDataRequest,
                                        NYq::TEvControlPlaneProxy::TEvGetResultDataResponse,
                                        YandexQuery::GetResultDataResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryListJobsRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.jobs.get@as",
        "yq.resources.viewPublic@as",
        "yq.resources.viewPrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryListJobsRequest,
                                        NYq::TEvControlPlaneProxy::TEvListJobsRequest,
                                        NYq::TEvControlPlaneProxy::TEvListJobsResponse,
                                        YandexQuery::ListJobsResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryDescribeJobRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.jobs.get@as",
        "yq.resources.viewPublic@as",
        "yq.resources.viewPrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryDescribeJobRequest,
                                        NYq::TEvControlPlaneProxy::TEvDescribeJobRequest,
                                        NYq::TEvControlPlaneProxy::TEvDescribeJobResponse,
                                        YandexQuery::DescribeJobResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryCreateConnectionRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.connections.create@as",
        "yq.resources.managePublic@as",

        // service account permissions
        "iam.serviceAccounts.use@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryCreateConnectionRequest,
                                        NYq::TEvControlPlaneProxy::TEvCreateConnectionRequest,
                                        NYq::TEvControlPlaneProxy::TEvCreateConnectionResponse,
                                        YandexQuery::CreateConnectionResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryListConnectionsRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.connections.get@as",
        "yq.resources.viewPublic@as",
        "yq.resources.viewPrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryListConnectionsRequest,
                                        NYq::TEvControlPlaneProxy::TEvListConnectionsRequest,
                                        NYq::TEvControlPlaneProxy::TEvListConnectionsResponse,
                                        YandexQuery::ListConnectionsResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryDescribeConnectionRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.connections.get@as",
        "yq.resources.viewPublic@as",
        "yq.resources.viewPrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryDescribeConnectionRequest,
                                        NYq::TEvControlPlaneProxy::TEvDescribeConnectionRequest,
                                        NYq::TEvControlPlaneProxy::TEvDescribeConnectionResponse,
                                        YandexQuery::DescribeConnectionResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryModifyConnectionRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.connections.update@as",
        "yq.resources.managePublic@as",
        "yq.resources.managePrivate@as",

        // service account permissions
        "iam.serviceAccounts.use@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryModifyConnectionRequest,
                                        NYq::TEvControlPlaneProxy::TEvModifyConnectionRequest,
                                        NYq::TEvControlPlaneProxy::TEvModifyConnectionResponse,
                                        YandexQuery::ModifyConnectionResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryDeleteConnectionRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.connections.delete@as",
        "yq.resources.managePublic@as",
        "yq.resources.managePrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryDeleteConnectionRequest,
                                        NYq::TEvControlPlaneProxy::TEvDeleteConnectionRequest,
                                        NYq::TEvControlPlaneProxy::TEvDeleteConnectionResponse,
                                        YandexQuery::DeleteConnectionResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryTestConnectionRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.connections.create@as",

        // service account permissions
        "iam.serviceAccounts.use@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryTestConnectionRequest,
                                        NYq::TEvControlPlaneProxy::TEvTestConnectionRequest,
                                        NYq::TEvControlPlaneProxy::TEvTestConnectionResponse,
                                        YandexQuery::TestConnectionResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryCreateBindingRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.bindings.create@as",
        "yq.resources.managePublic@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryCreateBindingRequest,
                                        NYq::TEvControlPlaneProxy::TEvCreateBindingRequest,
                                        NYq::TEvControlPlaneProxy::TEvCreateBindingResponse,
                                        YandexQuery::CreateBindingResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryListBindingsRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.bindings.get@as",
        "yq.resources.viewPublic@as",
        "yq.resources.viewPrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryListBindingsRequest,
                                        NYq::TEvControlPlaneProxy::TEvListBindingsRequest,
                                        NYq::TEvControlPlaneProxy::TEvListBindingsResponse,
                                        YandexQuery::ListBindingsResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryDescribeBindingRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.bindings.get@as",
        "yq.resources.viewPublic@as",
        "yq.resources.viewPrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryDescribeBindingRequest,
                                        NYq::TEvControlPlaneProxy::TEvDescribeBindingRequest,
                                        NYq::TEvControlPlaneProxy::TEvDescribeBindingResponse,
                                        YandexQuery::DescribeBindingResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryModifyBindingRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.bindings.update@as",
        "yq.resources.managePublic@as",
        "yq.resources.managePrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryModifyBindingRequest,
                                        NYq::TEvControlPlaneProxy::TEvModifyBindingRequest,
                                        NYq::TEvControlPlaneProxy::TEvModifyBindingResponse,
                                        YandexQuery::ModifyBindingResult>(ev->Release().Release(), permissions));
}

void TGRpcRequestProxy::Handle(TEvYandexQueryDeleteBindingRequest::TPtr& ev, const TActorContext& ctx) {
    static const TVector<TString> permissions = {
        // folder permissions
        "yq.bindings.delete@as",
        "yq.resources.managePublic@as",
        "yq.resources.managePrivate@as"
    };
    ctx.Register(new TYandexQueryRPC<TEvYandexQueryDeleteBindingRequest,
                                        NYq::TEvControlPlaneProxy::TEvDeleteBindingRequest,
                                        NYq::TEvControlPlaneProxy::TEvDeleteBindingResponse,
                                        YandexQuery::DeleteBindingResult>(ev->Release().Release(), permissions));
}

template<typename T>
TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> GetAdditionalEntries(const T&)
{
    return {};
}

template<>
TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> GetAdditionalEntries(const TEvYandexQueryCreateConnectionRequest& request)
{
    TString serviceAccountId = NYq::ExtractServiceAccountId(*request.GetProtoRequest());
    if (serviceAccountId) {
        return {{
            {"iam.serviceAccounts.use"},
            {
                {"service_account_id", serviceAccountId},
                {"database_id", "db"}
            }
        }};
    }
    return {};
}

template<>
TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> GetAdditionalEntries(const TEvYandexQueryModifyConnectionRequest& request)
{
    TString serviceAccountId = NYq::ExtractServiceAccountId(*request.GetProtoRequest());
    if (serviceAccountId) {
        return {{
            {"iam.serviceAccounts.use"},
            {
                {"service_account_id", serviceAccountId},
                {"database_id", "db"}
            }
        }};
    }
    return {};
}

template<>
TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> GetAdditionalEntries(const TEvYandexQueryTestConnectionRequest& request)
{
    TString serviceAccountId = NYq::ExtractServiceAccountId(*request.GetProtoRequest());
    if (serviceAccountId) {
        return {{
            {"iam.serviceAccounts.use"},
            {
                {"service_account_id", serviceAccountId},
                {"database_id", "db"}
            }
        }};
    }
    return {};
}

template <typename TEvent>
void TGrpcRequestCheckActor<TEvent>::InitializeAttributesFromYandexQuery(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    CheckedDatabaseName_ = CanonizePath(schemeData.GetPath());
    const TString scope = Request_->Get()->GetPeerMetaValues("x-yq-scope").GetOrElse("");
    if (scope.StartsWith("yandexcloud://")) {
        const TVector<TString> path = StringSplitter(scope).Split('/').SkipEmpty();
        if (path.size() == 2 || path.size() == 3) {
            const TString& folderId = path.back();
            TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> entries {{
                GetPermissions(),
                {
                    {"folder_id", folderId},
                    {"database_id", "db"}
                }
            }};

            const auto& request = *Request_->Get();
            const auto additionalEntries = GetAdditionalEntries(request);
            entries.insert(entries.end(), additionalEntries.begin(), additionalEntries.end());
            TBase::SetEntries(entries);
        }
    }
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryCreateQueryRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.queries.create",
        "yq.queries.invoke",
        "yq.connections.use",
        "yq.bindings.use",
        "yq.resources.managePublic"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryListQueriesRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.queries.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryDescribeQueryRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.queries.get",
        "yq.queries.viewAst",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryGetQueryStatusRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "yq.queries.getStatus",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryModifyQueryRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.queries.update",
        "yq.queries.invoke",
        "yq.resources.managePublic",
        "yq.resources.managePrivate",
        "yq.connections.use",
        "yq.bindings.use"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryDeleteQueryRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.queries.delete",
        "yq.resources.managePublic",
        "yq.resources.managePrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryControlQueryRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.queries.control",
        "yq.resources.managePublic",
        "yq.resources.managePrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryGetResultDataRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.queries.getData",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryListJobsRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.jobs.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryDescribeJobRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.jobs.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryCreateConnectionRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.connections.create",
        "yq.resources.managePublic"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryListConnectionsRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.connections.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryDescribeConnectionRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.connections.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryModifyConnectionRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.connections.update",
        "yq.resources.managePublic",
        "yq.resources.managePrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryDeleteConnectionRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.connections.delete",
        "yq.resources.managePublic",
        "yq.resources.managePrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryTestConnectionRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "yq.connections.create"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryCreateBindingRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.bindings.create",
        "yq.resources.managePublic"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryListBindingsRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.bindings.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryDescribeBindingRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.bindings.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryModifyBindingRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.bindings.update",
        "yq.resources.managePublic",
        "yq.resources.managePrivate"
    };
    return permissions;
}

template <>
const TVector<TString>& TGrpcRequestCheckActor<TEvYandexQueryDeleteBindingRequest>::GetPermissions() {
    static const TVector<TString> permissions = {
        "ydb.tables.list", // TODO: delete after enabling permission validation
        "yq.bindings.delete",
        "yq.resources.managePublic",
        "yq.resources.managePrivate"
    };
    return permissions;
}

// yq behavior
template <>
void TGrpcRequestCheckActor<TEvYandexQueryCreateQueryRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryListQueriesRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryDescribeQueryRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryGetQueryStatusRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryModifyQueryRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryDeleteQueryRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryControlQueryRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryGetResultDataRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryListJobsRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryDescribeJobRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryCreateConnectionRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryListConnectionsRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryDescribeConnectionRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryModifyConnectionRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryDeleteConnectionRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryTestConnectionRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryCreateBindingRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryListBindingsRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryDescribeBindingRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryModifyBindingRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

template <>
void TGrpcRequestCheckActor<TEvYandexQueryDeleteBindingRequest>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    InitializeAttributesFromYandexQuery(schemeData);
}

} // namespace NGRpcService
} // namespace NKikimr
