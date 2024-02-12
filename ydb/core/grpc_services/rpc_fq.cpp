#include "rpc_common/rpc_common.h"
#include "rpc_deferrable.h"

#include <ydb/core/grpc_services/service_fq.h>
#include <ydb/core/fq/libs/audit/events/events.h>
#include <ydb/core/fq/libs/audit/yq_audit_service.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/control_plane_proxy/utils/utils.h>
#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/lib/fq/scope.h>

#include <ydb/library/aclib/aclib.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/guid.h>
#include <util/string/split.h>

namespace NKikimr {
namespace NGRpcService {

using namespace Ydb;
using NPerms = NKikimr::TEvTicketParser::TEvAuthorizeTicket;

template <typename RpcRequestType, typename EvRequestType, typename EvResponseType>
class TFederatedQueryRequestRPC : public TRpcOperationRequestActor<
    TFederatedQueryRequestRPC<RpcRequestType,EvRequestType,EvResponseType>, RpcRequestType> {

public:
    using TBase = TRpcOperationRequestActor<
        TFederatedQueryRequestRPC<RpcRequestType,EvRequestType,EvResponseType>,
        RpcRequestType>;
    using TBase::Become;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Request_;
    using TBase::GetProtoRequest;

protected:
    TString Token;
    TString FolderId;
    TString User;
    TString PeerName;
    TString UserAgent;
    TString RequestId;

public:
    TFederatedQueryRequestRPC(IRequestOpCtx* request)
        : TBase(request) {}

    void Bootstrap() {
        auto requestCtx = Request_.get();

        auto request = dynamic_cast<RpcRequestType*>(requestCtx);
        Y_ABORT_UNLESS(request);

        auto proxyCtx = dynamic_cast<IRequestProxyCtx*>(requestCtx);
        Y_ABORT_UNLESS(proxyCtx);

        PeerName = Request_->GetPeerName();
        UserAgent = Request_->GetPeerMetaValues("user-agent").GetOrElse("empty");
        RequestId = Request_->GetPeerMetaValues("x-request-id").GetOrElse(CreateGuidAsString());

        TMaybe<TString> authToken = proxyCtx->GetYdbToken();
        if (!authToken) {
            ReplyWithStatus("Token is empty", StatusIds::BAD_REQUEST);
            return;
        }
        Token = *authToken;

        TString scope = Request_->GetPeerMetaValues("x-ydb-fq-project").GetOrElse("");
        if (scope.empty()) {
            scope = Request_->GetPeerMetaValues("x-yq-scope").GetOrElse(""); // TODO: remove YQ-1055
        }

        if (!scope.StartsWith("yandexcloud://")) {
            ReplyWithStatus("x-ydb-fq-project should start with yandexcloud:// but got " + scope, StatusIds::BAD_REQUEST);
            return;
        }

        const TVector<TString> path = StringSplitter(scope).Split('/').SkipEmpty();
        if (path.size() != 2 && path.size() != 3) {
            ReplyWithStatus("x-ydb-fq-project format is invalid. Must be yandexcloud://folder_id, but got " + scope, StatusIds::BAD_REQUEST);
            return;
        }

        FolderId = path.back();
        if (!FolderId) {
            ReplyWithStatus("Folder id is empty", StatusIds::BAD_REQUEST);
            return;
        }

        if (FolderId.length() > 1024) {
            ReplyWithStatus("Folder id length greater than 1024 characters: " + FolderId, StatusIds::BAD_REQUEST);
            return;
        }

        const TString& internalToken = proxyCtx->GetSerializedToken();
        TVector<TString> permissions;
        if (internalToken) {
            NACLib::TUserToken userToken(internalToken);
            User = userToken.GetUserSID();
            for (const auto& sid: request->Sids) {
                if (userToken.IsExist(sid)) {
                    permissions.push_back(sid);
                }
            }
        }

        if (!User) {
            ReplyWithStatus("Authorization error. Permission denied", StatusIds::UNAUTHORIZED);
            return;
        }

        const auto* req = GetProtoRequest();
        auto ev         = MakeHolder<EvRequestType>(
            NYdb::NFq::TScope{NYdb::NFq::TScope::YandexCloudScopeSchema + "://" + FolderId}.ToString(),
            *req,
            User,
            Token,
            permissions);
        Send(NFq::ControlPlaneProxyActorId(), ev.Release());
        Become(&TFederatedQueryRequestRPC<RpcRequestType, EvRequestType, EvResponseType>::StateFunc);
    }

protected:
    void ReplyWithStatus(const TString& issueMessage, StatusIds::StatusCode status) {
        Request_->RaiseIssue(NYql::TIssue(issueMessage));
        Request_->ReplyWithYdbStatus(status);
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(EvResponseType, Handle);
    )

    template <typename TResponse, typename TReq>
    void SendResponse(const TResponse& response, TReq& req) {
        if (response.Issues) {
            req.RaiseIssues(response.Issues);
            req.ReplyWithYdbStatus(StatusIds::BAD_REQUEST);
        } else {
            req.SendResult(response.Result, StatusIds::SUCCESS);
        }
    }

    template <typename TResponse, typename TReq> requires requires (TResponse r) { r.AuditDetails; }
    void SendResponse(const TResponse& response, TReq& req) {
        if (response.Issues) {
            req.RaiseIssues(response.Issues);
            req.ReplyWithYdbStatus(StatusIds::BAD_REQUEST);
        } else {
            req.SendResult(response.Result, StatusIds::SUCCESS);
        }

        NFq::TEvAuditService::TExtraInfo extraInfo{
            .Token = Token,
            .CloudId = response.AuditDetails.CloudId,
            .FolderId = FolderId,
            .User = User,
            .PeerName = PeerName,
            .UserAgent = UserAgent,
            .RequestId = RequestId,
            .SubjectType = response.SubjectType
        };


        Send(NFq::YqAuditServiceActorId(), NFq::TEvAuditService::MakeAuditEvent(
            std::move(extraInfo),
            *GetProtoRequest(),
            response.Issues,
            response.AuditDetails));
    }

    void Handle(typename EvResponseType::TPtr& ev) {
        SendResponse(*ev->Get(), *Request_);
        PassAway();
    }
};

using TFederatedQueryCreateQueryRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::CreateQueryRequest, FederatedQuery::CreateQueryResponse>,
    NFq::TEvControlPlaneProxy::TEvCreateQueryRequest,
    NFq::TEvControlPlaneProxy::TEvCreateQueryResponse>;

void DoFederatedQueryCreateQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryCreateQueryRPC(p.release()));
}

using TFederatedQueryListQueriesRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ListQueriesRequest, FederatedQuery::ListQueriesResponse>,
    NFq::TEvControlPlaneProxy::TEvListQueriesRequest,
    NFq::TEvControlPlaneProxy::TEvListQueriesResponse>;

void DoFederatedQueryListQueriesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryListQueriesRPC(p.release()));
}

using TFederatedQueryDescribeQueryRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DescribeQueryRequest, FederatedQuery::DescribeQueryResponse>,
    NFq::TEvControlPlaneProxy::TEvDescribeQueryRequest,
    NFq::TEvControlPlaneProxy::TEvDescribeQueryResponse>;

void DoFederatedQueryDescribeQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDescribeQueryRPC(p.release()));
}

using TFederatedQueryGetQueryStatusRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::GetQueryStatusRequest, FederatedQuery::GetQueryStatusResponse>,
    NFq::TEvControlPlaneProxy::TEvGetQueryStatusRequest,
    NFq::TEvControlPlaneProxy::TEvGetQueryStatusResponse>;

void DoFederatedQueryGetQueryStatusRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryGetQueryStatusRPC(p.release()));
}

using TFederatedQueryModifyQueryRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ModifyQueryRequest, FederatedQuery::ModifyQueryResponse>,
    NFq::TEvControlPlaneProxy::TEvModifyQueryRequest,
    NFq::TEvControlPlaneProxy::TEvModifyQueryResponse>;

void DoFederatedQueryModifyQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryModifyQueryRPC(p.release()));
}

using TFederatedQueryDeleteQueryRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DeleteQueryRequest, FederatedQuery::DeleteQueryResponse>,
    NFq::TEvControlPlaneProxy::TEvDeleteQueryRequest,
    NFq::TEvControlPlaneProxy::TEvDeleteQueryResponse>;

void DoFederatedQueryDeleteQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDeleteQueryRPC(p.release()));
}

using TFederatedQueryControlQueryRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ControlQueryRequest, FederatedQuery::ControlQueryResponse>,
    NFq::TEvControlPlaneProxy::TEvControlQueryRequest,
    NFq::TEvControlPlaneProxy::TEvControlQueryResponse>;

void DoFederatedQueryControlQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryControlQueryRPC(p.release()));
}

using TFederatedQueryGetResultDataRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::GetResultDataRequest, FederatedQuery::GetResultDataResponse>,
    NFq::TEvControlPlaneProxy::TEvGetResultDataRequest,
    NFq::TEvControlPlaneProxy::TEvGetResultDataResponse>;

void DoFederatedQueryGetResultDataRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryGetResultDataRPC(p.release()));
}

using TFederatedQueryListJobsRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ListJobsRequest, FederatedQuery::ListJobsResponse>,
    NFq::TEvControlPlaneProxy::TEvListJobsRequest,
    NFq::TEvControlPlaneProxy::TEvListJobsResponse>;

void DoFederatedQueryListJobsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryListJobsRPC(p.release()));
}

using TFederatedQueryDescribeJobRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DescribeJobRequest, FederatedQuery::DescribeJobResponse>,
    NFq::TEvControlPlaneProxy::TEvDescribeJobRequest,
    NFq::TEvControlPlaneProxy::TEvDescribeJobResponse>;

void DoFederatedQueryDescribeJobRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDescribeJobRPC(p.release()));
}

using TFederatedQueryCreateConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::CreateConnectionRequest, FederatedQuery::CreateConnectionResponse>,
    NFq::TEvControlPlaneProxy::TEvCreateConnectionRequest,
    NFq::TEvControlPlaneProxy::TEvCreateConnectionResponse>;

void DoFederatedQueryCreateConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryCreateConnectionRPC(p.release()));
}

using TFederatedQueryListConnectionsRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ListConnectionsRequest, FederatedQuery::ListConnectionsResponse>,
    NFq::TEvControlPlaneProxy::TEvListConnectionsRequest,
    NFq::TEvControlPlaneProxy::TEvListConnectionsResponse>;

void DoFederatedQueryListConnectionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryListConnectionsRPC(p.release()));
}

using TFederatedQueryDescribeConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DescribeConnectionRequest, FederatedQuery::DescribeConnectionResponse>,
    NFq::TEvControlPlaneProxy::TEvDescribeConnectionRequest,
    NFq::TEvControlPlaneProxy::TEvDescribeConnectionResponse>;

void DoFederatedQueryDescribeConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDescribeConnectionRPC(p.release()));
}

using TFederatedQueryModifyConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ModifyConnectionRequest, FederatedQuery::ModifyConnectionResponse>,
    NFq::TEvControlPlaneProxy::TEvModifyConnectionRequest,
    NFq::TEvControlPlaneProxy::TEvModifyConnectionResponse>;

void DoFederatedQueryModifyConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryModifyConnectionRPC(p.release()));
}

using TFederatedQueryDeleteConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DeleteConnectionRequest, FederatedQuery::DeleteConnectionResponse>,
    NFq::TEvControlPlaneProxy::TEvDeleteConnectionRequest,
    NFq::TEvControlPlaneProxy::TEvDeleteConnectionResponse>;

void DoFederatedQueryDeleteConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDeleteConnectionRPC(p.release()));
}

using TFederatedQueryTestConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::TestConnectionRequest, FederatedQuery::TestConnectionResponse>,
    NFq::TEvControlPlaneProxy::TEvTestConnectionRequest,
    NFq::TEvControlPlaneProxy::TEvTestConnectionResponse>;

void DoFederatedQueryTestConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryTestConnectionRPC(p.release()));
}

using TFederatedQueryCreateBindingRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::CreateBindingRequest, FederatedQuery::CreateBindingResponse>,
    NFq::TEvControlPlaneProxy::TEvCreateBindingRequest,
    NFq::TEvControlPlaneProxy::TEvCreateBindingResponse>;

void DoFederatedQueryCreateBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryCreateBindingRPC(p.release()));
}

using TFederatedQueryListBindingsRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ListBindingsRequest, FederatedQuery::ListBindingsResponse>,
    NFq::TEvControlPlaneProxy::TEvListBindingsRequest,
    NFq::TEvControlPlaneProxy::TEvListBindingsResponse>;

void DoFederatedQueryListBindingsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryListBindingsRPC(p.release()));
}

using TFederatedQueryDescribeBindingRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DescribeBindingRequest, FederatedQuery::DescribeBindingResponse>,
    NFq::TEvControlPlaneProxy::TEvDescribeBindingRequest,
    NFq::TEvControlPlaneProxy::TEvDescribeBindingResponse>;

void DoFederatedQueryDescribeBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDescribeBindingRPC(p.release()));
}

using TFederatedQueryModifyBindingRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ModifyBindingRequest, FederatedQuery::ModifyBindingResponse>,
    NFq::TEvControlPlaneProxy::TEvModifyBindingRequest,
    NFq::TEvControlPlaneProxy::TEvModifyBindingResponse>;

void DoFederatedQueryModifyBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryModifyBindingRPC(p.release()));
}

using TFederatedQueryDeleteBindingRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DeleteBindingRequest, FederatedQuery::DeleteBindingResponse>,
    NFq::TEvControlPlaneProxy::TEvDeleteBindingRequest,
    NFq::TEvControlPlaneProxy::TEvDeleteBindingResponse>;

void DoFederatedQueryDeleteBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDeleteBindingRPC(p.release()));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryCreateQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const FederatedQuery::CreateQueryRequest& request) {
        TVector<NPerms::TPermission> basePermissions{
            NPerms::Required("yq.queries.create")
        };
        if (request.execute_mode() != FederatedQuery::SAVE) {
            basePermissions.push_back(NPerms::Required("yq.queries.invoke"));
        }
        if (request.content().acl().visibility() == FederatedQuery::Acl::SCOPE) {
            basePermissions.push_back(NPerms::Required("yq.resources.managePublic"));
        }
        return basePermissions;
    }};

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::CreateQueryRequest, FederatedQuery::CreateQueryResponse>>(ctx.Release(), &DoFederatedQueryCreateQueryRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListQueriesRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const FederatedQuery::ListQueriesRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ListQueriesRequest, FederatedQuery::ListQueriesResponse>>(ctx.Release(), &DoFederatedQueryListQueriesRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const FederatedQuery::DescribeQueryRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.get"),
            NPerms::Optional("yq.queries.viewAst"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate"),
            NPerms::Optional("yq.queries.viewQueryText")
        };
    }};

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DescribeQueryRequest, FederatedQuery::DescribeQueryResponse>>(ctx.Release(), &DoFederatedQueryDescribeQueryRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryGetQueryStatusRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const FederatedQuery::GetQueryStatusRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.getStatus"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::GetQueryStatusRequest, FederatedQuery::GetQueryStatusResponse>>(ctx.Release(), &DoFederatedQueryGetQueryStatusRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryModifyQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const FederatedQuery::ModifyQueryRequest& request) {
        TVector<NPerms::TPermission> basePermissions{
            NPerms::Required("yq.queries.update"),
            NPerms::Optional("yq.resources.managePrivate")
        };
        if (request.execute_mode() != FederatedQuery::SAVE) {
            basePermissions.push_back(NPerms::Required("yq.queries.invoke"));
        }
        if (request.content().acl().visibility() == FederatedQuery::Acl::SCOPE) {
            basePermissions.push_back(NPerms::Required("yq.resources.managePublic"));
        }
        return basePermissions;
    }};

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ModifyQueryRequest, FederatedQuery::ModifyQueryResponse>>(ctx.Release(), &DoFederatedQueryModifyQueryRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDeleteQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const FederatedQuery::DeleteQueryRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.delete"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate")
        };
    }};

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DeleteQueryRequest, FederatedQuery::DeleteQueryResponse>>(ctx.Release(), &DoFederatedQueryDeleteQueryRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryControlQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const FederatedQuery::ControlQueryRequest& request) -> TVector<NPerms::TPermission> {
        TVector<NPerms::TPermission> basePermissions{
            NPerms::Required("yq.queries.control"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate")
        };
        if (request.action() == FederatedQuery::RESUME) {
            basePermissions.push_back(NPerms::Required("yq.queries.start"));
        } else if (request.action() != FederatedQuery::QUERY_ACTION_UNSPECIFIED) {
            basePermissions.push_back(NPerms::Required("yq.queries.abort"));
        }
        return basePermissions;
    }};

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ControlQueryRequest, FederatedQuery::ControlQueryResponse>>(ctx.Release(), &DoFederatedQueryControlQueryRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryGetResultDataRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::GetResultDataRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.getData"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::GetResultDataRequest, FederatedQuery::GetResultDataResponse>>(ctx.Release(), &DoFederatedQueryGetResultDataRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListJobsRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::ListJobsRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.jobs.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ListJobsRequest, FederatedQuery::ListJobsResponse>>(ctx.Release(), &DoFederatedQueryListJobsRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeJobRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::DescribeJobRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.jobs.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate"),
            NPerms::Optional("yq.queries.viewAst"),
            NPerms::Optional("yq.queries.viewQueryText")
        };
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DescribeJobRequest, FederatedQuery::DescribeJobResponse>>(ctx.Release(), &DoFederatedQueryDescribeJobRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryCreateConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::CreateConnectionRequest& request) -> TVector<NPerms::TPermission> {
        TVector<NPerms::TPermission> basePermissions{
            NPerms::Required("yq.connections.create"),
        };
        if (request.content().acl().visibility() == FederatedQuery::Acl::SCOPE) {
            basePermissions.push_back(NPerms::Required("yq.resources.managePublic"));
        }
        return basePermissions;
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::CreateConnectionRequest, FederatedQuery::CreateConnectionResponse>>(ctx.Release(), &DoFederatedQueryCreateConnectionRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListConnectionsRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::ListConnectionsRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ListConnectionsRequest, FederatedQuery::ListConnectionsResponse>>(ctx.Release(), &DoFederatedQueryListConnectionsRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::DescribeConnectionRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DescribeConnectionRequest, FederatedQuery::DescribeConnectionResponse>>(ctx.Release(), &DoFederatedQueryDescribeConnectionRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryModifyConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::ModifyConnectionRequest& request) -> TVector<NPerms::TPermission> {
        TVector<NPerms::TPermission> basePermissions{
            NPerms::Required("yq.connections.update"),
            NPerms::Required("yq.connections.get"),
            NPerms::Required("yq.bindings.get"),
            NPerms::Optional("yq.resources.managePrivate"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
        if (request.content().acl().visibility() == FederatedQuery::Acl::SCOPE) {
            basePermissions.push_back(NPerms::Required("yq.resources.managePublic"));
        }
        return basePermissions;
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ModifyConnectionRequest, FederatedQuery::ModifyConnectionResponse>>(ctx.Release(), &DoFederatedQueryModifyConnectionRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDeleteConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::DeleteConnectionRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.delete"),
            NPerms::Required("yq.connections.get"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DeleteConnectionRequest, FederatedQuery::DeleteConnectionResponse>>(ctx.Release(), &DoFederatedQueryDeleteConnectionRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryTestConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::TestConnectionRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.create")
        };
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::TestConnectionRequest, FederatedQuery::TestConnectionResponse>>(ctx.Release(), &DoFederatedQueryTestConnectionRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryCreateBindingRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::CreateBindingRequest&) -> TVector<NPerms::TPermission> {
        // For use in binding links on connection with visibility SCOPE,
        // the yq.resources.managePublic permission is required. But there
        // is no information about connection visibility in this place,
        // so yq.resources.managePublic is always requested as optional
        return {
            NPerms::Required("yq.bindings.create"),
            NPerms::Required("yq.connections.get"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::CreateBindingRequest, FederatedQuery::CreateBindingResponse>>(ctx.Release(), &DoFederatedQueryCreateBindingRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListBindingsRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::ListBindingsRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.bindings.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ListBindingsRequest, FederatedQuery::ListBindingsResponse>>(ctx.Release(), &DoFederatedQueryListBindingsRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeBindingRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::DescribeBindingRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.bindings.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DescribeBindingRequest, FederatedQuery::DescribeBindingResponse>>(ctx.Release(), &DoFederatedQueryDescribeBindingRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryModifyBindingRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::ModifyBindingRequest&) -> TVector<NPerms::TPermission> {
        // For use in binding links on connection with visibility SCOPE,
        // the yq.resources.managePublic permission is required. But there
        // is no information about connection visibility in this place,
        // so yq.resources.managePublic is always requested as optional
        return {
            NPerms::Required("yq.bindings.update"),
            NPerms::Required("yq.bindings.get"),
            NPerms::Required("yq.connections.get"),
            NPerms::Optional("yq.resources.managePrivate"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ModifyBindingRequest, FederatedQuery::ModifyBindingResponse>>(ctx.Release(), &DoFederatedQueryModifyBindingRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDeleteBindingRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const FederatedQuery::DeleteBindingRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.bindings.delete"),
            NPerms::Required("yq.bindings.get"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DeleteBindingRequest, FederatedQuery::DeleteBindingResponse>>(ctx.Release(), &DoFederatedQueryDeleteBindingRequest, permissions);
}

} // namespace NGRpcService
} // namespace NKikimr
