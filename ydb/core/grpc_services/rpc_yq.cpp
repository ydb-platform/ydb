// DEPRECATED!!! use rpc_fq.cpp

#include "rpc_common.h"
#include "rpc_deferrable.h"

#include <ydb/core/grpc_services/service_yq.h>
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
using NPerms = NKikimr::TEvTicketParser::TEvAuthorizeTicket;

template <typename RpcRequestType, typename EvRequestType, typename EvResponseType>
class TYandexQueryRequestRPC : public TRpcOperationRequestActor<
    TYandexQueryRequestRPC<RpcRequestType,EvRequestType,EvResponseType>, RpcRequestType> {

public:
    using TBase = TRpcOperationRequestActor<
        TYandexQueryRequestRPC<RpcRequestType,EvRequestType,EvResponseType>,
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
    TYandexQueryRequestRPC(IRequestOpCtx* request)
        : TBase(request) {}

    void Bootstrap() {
        auto requestCtx = Request_.get();

        auto request = dynamic_cast<RpcRequestType*>(requestCtx);
        Y_VERIFY(request);

        auto proxyCtx = dynamic_cast<IRequestProxyCtx*>(requestCtx);
        Y_VERIFY(proxyCtx);

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
        auto ev = MakeHolder<EvRequestType>(FolderId, *req, User, Token, permissions);
        Send(NYq::ControlPlaneProxyActorId(), ev.Release());
        Become(&TYandexQueryRequestRPC<RpcRequestType, EvRequestType, EvResponseType>::StateFunc);
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

        NYq::TEvAuditService::TExtraInfo extraInfo{
            .Token = Token,
            .CloudId = response.AuditDetails.CloudId,
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

    void Handle(typename EvResponseType::TPtr& ev) {
        SendResponse(*ev->Get(), *Request_);
        PassAway();
    }
};

using TYandexQueryCreateQueryRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::CreateQueryRequest, YandexQuery::CreateQueryResponse>,
    NYq::TEvControlPlaneProxy::TEvCreateQueryRequest,
    NYq::TEvControlPlaneProxy::TEvCreateQueryResponse>;

void DoYandexQueryCreateQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryCreateQueryRPC(p.release()));
}

using TYandexQueryListQueriesRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::ListQueriesRequest, YandexQuery::ListQueriesResponse>,
    NYq::TEvControlPlaneProxy::TEvListQueriesRequest,
    NYq::TEvControlPlaneProxy::TEvListQueriesResponse>;

void DoYandexQueryListQueriesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryListQueriesRPC(p.release()));
}

using TYandexQueryDescribeQueryRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::DescribeQueryRequest, YandexQuery::DescribeQueryResponse>,
    NYq::TEvControlPlaneProxy::TEvDescribeQueryRequest,
    NYq::TEvControlPlaneProxy::TEvDescribeQueryResponse>;

void DoYandexQueryDescribeQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryDescribeQueryRPC(p.release()));
}

using TYandexQueryGetQueryStatusRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::GetQueryStatusRequest, YandexQuery::GetQueryStatusResponse>,
    NYq::TEvControlPlaneProxy::TEvGetQueryStatusRequest,
    NYq::TEvControlPlaneProxy::TEvGetQueryStatusResponse>;

void DoYandexQueryGetQueryStatusRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryGetQueryStatusRPC(p.release()));
}

using TYandexQueryModifyQueryRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::ModifyQueryRequest, YandexQuery::ModifyQueryResponse>,
    NYq::TEvControlPlaneProxy::TEvModifyQueryRequest,
    NYq::TEvControlPlaneProxy::TEvModifyQueryResponse>;

void DoYandexQueryModifyQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryModifyQueryRPC(p.release()));
}

using TYandexQueryDeleteQueryRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::DeleteQueryRequest, YandexQuery::DeleteQueryResponse>,
    NYq::TEvControlPlaneProxy::TEvDeleteQueryRequest,
    NYq::TEvControlPlaneProxy::TEvDeleteQueryResponse>;

void DoYandexQueryDeleteQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryDeleteQueryRPC(p.release()));
}

using TYandexQueryControlQueryRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::ControlQueryRequest, YandexQuery::ControlQueryResponse>,
    NYq::TEvControlPlaneProxy::TEvControlQueryRequest,
    NYq::TEvControlPlaneProxy::TEvControlQueryResponse>;

void DoYandexQueryControlQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryControlQueryRPC(p.release()));
}

using TYandexQueryGetResultDataRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::GetResultDataRequest, YandexQuery::GetResultDataResponse>,
    NYq::TEvControlPlaneProxy::TEvGetResultDataRequest,
    NYq::TEvControlPlaneProxy::TEvGetResultDataResponse>;

void DoYandexQueryGetResultDataRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryGetResultDataRPC(p.release()));
}

using TYandexQueryListJobsRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::ListJobsRequest, YandexQuery::ListJobsResponse>,
    NYq::TEvControlPlaneProxy::TEvListJobsRequest,
    NYq::TEvControlPlaneProxy::TEvListJobsResponse>;

void DoYandexQueryListJobsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryListJobsRPC(p.release()));
}

using TYandexQueryDescribeJobRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::DescribeJobRequest, YandexQuery::DescribeJobResponse>,
    NYq::TEvControlPlaneProxy::TEvDescribeJobRequest,
    NYq::TEvControlPlaneProxy::TEvDescribeJobResponse>;

void DoYandexQueryDescribeJobRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryDescribeJobRPC(p.release()));
}

using TYandexQueryCreateConnectionRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::CreateConnectionRequest, YandexQuery::CreateConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvCreateConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvCreateConnectionResponse>;

void DoYandexQueryCreateConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryCreateConnectionRPC(p.release()));
}

using TYandexQueryListConnectionsRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::ListConnectionsRequest, YandexQuery::ListConnectionsResponse>,
    NYq::TEvControlPlaneProxy::TEvListConnectionsRequest,
    NYq::TEvControlPlaneProxy::TEvListConnectionsResponse>;

void DoYandexQueryListConnectionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryListConnectionsRPC(p.release()));
}

using TYandexQueryDescribeConnectionRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::DescribeConnectionRequest, YandexQuery::DescribeConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvDescribeConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvDescribeConnectionResponse>;

void DoYandexQueryDescribeConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryDescribeConnectionRPC(p.release()));
}

using TYandexQueryModifyConnectionRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::ModifyConnectionRequest, YandexQuery::ModifyConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvModifyConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvModifyConnectionResponse>;

void DoYandexQueryModifyConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryModifyConnectionRPC(p.release()));
}

using TYandexQueryDeleteConnectionRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::DeleteConnectionRequest, YandexQuery::DeleteConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvDeleteConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvDeleteConnectionResponse>;

void DoYandexQueryDeleteConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryDeleteConnectionRPC(p.release()));
}

using TYandexQueryTestConnectionRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::TestConnectionRequest, YandexQuery::TestConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvTestConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvTestConnectionResponse>;

void DoYandexQueryTestConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryTestConnectionRPC(p.release()));
}

using TYandexQueryCreateBindingRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::CreateBindingRequest, YandexQuery::CreateBindingResponse>,
    NYq::TEvControlPlaneProxy::TEvCreateBindingRequest,
    NYq::TEvControlPlaneProxy::TEvCreateBindingResponse>;

void DoYandexQueryCreateBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& ) {
    TActivationContext::AsActorContext().Register(new TYandexQueryCreateBindingRPC(p.release()));
}

using TYandexQueryListBindingsRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::ListBindingsRequest, YandexQuery::ListBindingsResponse>,
    NYq::TEvControlPlaneProxy::TEvListBindingsRequest,
    NYq::TEvControlPlaneProxy::TEvListBindingsResponse>;

void DoYandexQueryListBindingsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryListBindingsRPC(p.release()));
}

using TYandexQueryDescribeBindingRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::DescribeBindingRequest, YandexQuery::DescribeBindingResponse>,
    NYq::TEvControlPlaneProxy::TEvDescribeBindingRequest,
    NYq::TEvControlPlaneProxy::TEvDescribeBindingResponse>;

void DoYandexQueryDescribeBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryDescribeBindingRPC(p.release()));
}

using TYandexQueryModifyBindingRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::ModifyBindingRequest, YandexQuery::ModifyBindingResponse>,
    NYq::TEvControlPlaneProxy::TEvModifyBindingRequest,
    NYq::TEvControlPlaneProxy::TEvModifyBindingResponse>;

void DoYandexQueryModifyBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryModifyBindingRPC(p.release()));
}

using TYandexQueryDeleteBindingRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::DeleteBindingRequest, YandexQuery::DeleteBindingResponse>,
    NYq::TEvControlPlaneProxy::TEvDeleteBindingRequest,
    NYq::TEvControlPlaneProxy::TEvDeleteBindingResponse>;

void DoYandexQueryDeleteBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryDeleteBindingRPC(p.release()));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateCreateQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const YandexQuery::CreateQueryRequest& request) {
        TVector<NPerms::TPermission> basePermissions{
            NPerms::Required("yq.queries.create"),
            NPerms::Optional("yq.connections.use"),
            NPerms::Optional("yq.bindings.use")
        };
        if (request.execute_mode() != YandexQuery::SAVE) {
            basePermissions.push_back(NPerms::Required("yq.queries.invoke"));
        }
        if (request.content().acl().visibility() == YandexQuery::Acl::SCOPE) {
            basePermissions.push_back(NPerms::Required("yq.resources.managePublic"));
        }
        return basePermissions;
    }};

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::CreateQueryRequest, YandexQuery::CreateQueryResponse>>(ctx.Release(), &DoYandexQueryCreateQueryRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateListQueriesRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const YandexQuery::ListQueriesRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::ListQueriesRequest, YandexQuery::ListQueriesResponse>>(ctx.Release(), &DoYandexQueryListQueriesRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateDescribeQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const YandexQuery::DescribeQueryRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.get"),
            NPerms::Optional("yq.queries.viewAst"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::DescribeQueryRequest, YandexQuery::DescribeQueryResponse>>(ctx.Release(), &DoYandexQueryDescribeQueryRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateGetQueryStatusRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const YandexQuery::GetQueryStatusRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.getStatus"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::GetQueryStatusRequest, YandexQuery::GetQueryStatusResponse>>(ctx.Release(), &DoYandexQueryGetQueryStatusRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateModifyQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const YandexQuery::ModifyQueryRequest& request) {
        TVector<NPerms::TPermission> basePermissions{
            NPerms::Required("yq.queries.update"),
            NPerms::Optional("yq.connections.use"),
            NPerms::Optional("yq.bindings.use"),
            NPerms::Optional("yq.resources.managePrivate")
        };
        if (request.execute_mode() != YandexQuery::SAVE) {
            basePermissions.push_back(NPerms::Required("yq.queries.invoke"));
        }
        if (request.content().acl().visibility() == YandexQuery::Acl::SCOPE) {
            basePermissions.push_back(NPerms::Required("yq.resources.managePublic"));
        }
        return basePermissions;
    }};

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::ModifyQueryRequest, YandexQuery::ModifyQueryResponse>>(ctx.Release(), &DoYandexQueryModifyQueryRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateDeleteQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const YandexQuery::DeleteQueryRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.delete"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate")
        };
    }};

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::DeleteQueryRequest, YandexQuery::DeleteQueryResponse>>(ctx.Release(), &DoYandexQueryDeleteQueryRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateControlQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{[](const YandexQuery::ControlQueryRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.control"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate")
        };
    }};

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::ControlQueryRequest, YandexQuery::ControlQueryResponse>>(ctx.Release(), &DoYandexQueryControlQueryRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateGetResultDataRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::GetResultDataRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.getData"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::GetResultDataRequest, YandexQuery::GetResultDataResponse>>(ctx.Release(), &DoYandexQueryGetResultDataRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateListJobsRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::ListJobsRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.jobs.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::ListJobsRequest, YandexQuery::ListJobsResponse>>(ctx.Release(), &DoYandexQueryListJobsRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateDescribeJobRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::DescribeJobRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.jobs.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::DescribeJobRequest, YandexQuery::DescribeJobResponse>>(ctx.Release(), &DoYandexQueryDescribeJobRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateCreateConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::CreateConnectionRequest& request) -> TVector<NPerms::TPermission> {
        TVector<NPerms::TPermission> basePermissions{
            NPerms::Required("yq.connections.create"),
        };
        if (request.content().acl().visibility() == YandexQuery::Acl::SCOPE) {
            basePermissions.push_back(NPerms::Required("yq.resources.managePublic"));
        }
        return basePermissions;
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::CreateConnectionRequest, YandexQuery::CreateConnectionResponse>>(ctx.Release(), &DoYandexQueryCreateConnectionRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateListConnectionsRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::ListConnectionsRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::ListConnectionsRequest, YandexQuery::ListConnectionsResponse>>(ctx.Release(), &DoYandexQueryListConnectionsRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateDescribeConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::DescribeConnectionRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::DescribeConnectionRequest, YandexQuery::DescribeConnectionResponse>>(ctx.Release(), &DoYandexQueryDescribeConnectionRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateModifyConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::ModifyConnectionRequest& request) -> TVector<NPerms::TPermission> {
        TVector<NPerms::TPermission> basePermissions{
            NPerms::Required("yq.connections.update"),
            NPerms::Optional("yq.resources.managePrivate")
        };
        if (request.content().acl().visibility() == YandexQuery::Acl::SCOPE) {
            basePermissions.push_back(NPerms::Required("yq.resources.managePublic"));
        }
        return basePermissions;
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::ModifyConnectionRequest, YandexQuery::ModifyConnectionResponse>>(ctx.Release(), &DoYandexQueryModifyConnectionRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateDeleteConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::DeleteConnectionRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.delete"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate")
        };
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::DeleteConnectionRequest, YandexQuery::DeleteConnectionResponse>>(ctx.Release(), &DoYandexQueryDeleteConnectionRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateTestConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::TestConnectionRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.create")
        };
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::TestConnectionRequest, YandexQuery::TestConnectionResponse>>(ctx.Release(), &DoYandexQueryTestConnectionRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateCreateBindingRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::CreateBindingRequest&) -> TVector<NPerms::TPermission> {
        // For use in binding links on connection with visibility SCOPE,
        // the yq.resources.managePublic permission is required. But there
        // is no information about connection visibility in this place,
        // so yq.resources.managePublic is always requested as optional
        return {
            NPerms::Required("yq.bindings.create"),
            NPerms::Optional("yq.resources.managePublic")
        };
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::CreateBindingRequest, YandexQuery::CreateBindingResponse>>(ctx.Release(), &DoYandexQueryCreateBindingRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateListBindingsRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::ListBindingsRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.bindings.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::ListBindingsRequest, YandexQuery::ListBindingsResponse>>(ctx.Release(), &DoYandexQueryListBindingsRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateDescribeBindingRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::DescribeBindingRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.bindings.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::DescribeBindingRequest, YandexQuery::DescribeBindingResponse>>(ctx.Release(), &DoYandexQueryDescribeBindingRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateModifyBindingRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::ModifyBindingRequest&) -> TVector<NPerms::TPermission> {
        // For use in binding links on connection with visibility SCOPE,
        // the yq.resources.managePublic permission is required. But there
        // is no information about connection visibility in this place,
        // so yq.resources.managePublic is always requested as optional
        return {
            NPerms::Required("yq.bindings.update"),
            NPerms::Optional("yq.resources.managePrivate"),
            NPerms::Optional("yq.resources.managePublic")
        };
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::ModifyBindingRequest, YandexQuery::ModifyBindingResponse>>(ctx.Release(), &DoYandexQueryModifyBindingRequest, permissions);
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateDeleteBindingRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx) {
    static const std::function permissions{ [](const YandexQuery::DeleteBindingRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.bindings.delete"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate")
        };
    } };

    return std::make_unique<TGrpcYqRequestOperationCall<YandexQuery::DeleteBindingRequest, YandexQuery::DeleteBindingResponse>>(ctx.Release(), &DoYandexQueryDeleteBindingRequest, permissions);
}

} // namespace NGRpcService
} // namespace NKikimr
