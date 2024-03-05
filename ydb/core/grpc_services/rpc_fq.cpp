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
        IRequestOpCtx* requestCtx = Request_.get();

        using TProtoRequest = typename RpcRequestType::TRequest;
        auto requestPermissions = dynamic_cast<const TFqPermissionsBase<TProtoRequest>*>(requestCtx);
        Y_ABORT_UNLESS(requestPermissions);

        PeerName = Request_->GetPeerName();
        UserAgent = Request_->GetPeerMetaValues("user-agent").GetOrElse("empty");
        RequestId = Request_->GetPeerMetaValues("x-request-id").GetOrElse(CreateGuidAsString());

        TMaybe<TString> authToken = NFederatedQuery::GetYdbToken(*requestCtx);
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

        const TString& internalToken = requestCtx->GetSerializedToken();
        TVector<TString> permissions;
        if (internalToken) {
            NACLib::TUserToken userToken(internalToken);
            User = userToken.GetUserSID();
            for (const auto& sid: requestPermissions->GetSids()) {
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

template<>
IActor* TGrpcRequestOperationCall<FederatedQuery::CreateQueryRequest, FederatedQuery::CreateQueryResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryCreateQueryRPC(msg);
}

using TFederatedQueryListQueriesRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ListQueriesRequest, FederatedQuery::ListQueriesResponse>,
    NFq::TEvControlPlaneProxy::TEvListQueriesRequest,
    NFq::TEvControlPlaneProxy::TEvListQueriesResponse>;

void DoFederatedQueryListQueriesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryListQueriesRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::ListQueriesRequest, FederatedQuery::ListQueriesResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryListQueriesRPC(msg);
}

using TFederatedQueryDescribeQueryRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DescribeQueryRequest, FederatedQuery::DescribeQueryResponse>,
    NFq::TEvControlPlaneProxy::TEvDescribeQueryRequest,
    NFq::TEvControlPlaneProxy::TEvDescribeQueryResponse>;

void DoFederatedQueryDescribeQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDescribeQueryRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::DescribeQueryRequest, FederatedQuery::DescribeQueryResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryDescribeQueryRPC(msg);
}

using TFederatedQueryGetQueryStatusRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::GetQueryStatusRequest, FederatedQuery::GetQueryStatusResponse>,
    NFq::TEvControlPlaneProxy::TEvGetQueryStatusRequest,
    NFq::TEvControlPlaneProxy::TEvGetQueryStatusResponse>;

void DoFederatedQueryGetQueryStatusRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryGetQueryStatusRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::GetQueryStatusRequest, FederatedQuery::GetQueryStatusResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryGetQueryStatusRPC(msg);
}

using TFederatedQueryModifyQueryRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ModifyQueryRequest, FederatedQuery::ModifyQueryResponse>,
    NFq::TEvControlPlaneProxy::TEvModifyQueryRequest,
    NFq::TEvControlPlaneProxy::TEvModifyQueryResponse>;

void DoFederatedQueryModifyQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryModifyQueryRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::ModifyQueryRequest, FederatedQuery::ModifyQueryResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryModifyQueryRPC(msg);
}

using TFederatedQueryDeleteQueryRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DeleteQueryRequest, FederatedQuery::DeleteQueryResponse>,
    NFq::TEvControlPlaneProxy::TEvDeleteQueryRequest,
    NFq::TEvControlPlaneProxy::TEvDeleteQueryResponse>;

void DoFederatedQueryDeleteQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDeleteQueryRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::DeleteQueryRequest, FederatedQuery::DeleteQueryResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryDeleteQueryRPC(msg);
}

using TFederatedQueryControlQueryRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ControlQueryRequest, FederatedQuery::ControlQueryResponse>,
    NFq::TEvControlPlaneProxy::TEvControlQueryRequest,
    NFq::TEvControlPlaneProxy::TEvControlQueryResponse>;

void DoFederatedQueryControlQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryControlQueryRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::ControlQueryRequest, FederatedQuery::ControlQueryResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryControlQueryRPC(msg);
}

using TFederatedQueryGetResultDataRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::GetResultDataRequest, FederatedQuery::GetResultDataResponse>,
    NFq::TEvControlPlaneProxy::TEvGetResultDataRequest,
    NFq::TEvControlPlaneProxy::TEvGetResultDataResponse>;

void DoFederatedQueryGetResultDataRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryGetResultDataRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::GetResultDataRequest, FederatedQuery::GetResultDataResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryGetResultDataRPC(msg);
}

using TFederatedQueryListJobsRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ListJobsRequest, FederatedQuery::ListJobsResponse>,
    NFq::TEvControlPlaneProxy::TEvListJobsRequest,
    NFq::TEvControlPlaneProxy::TEvListJobsResponse>;

void DoFederatedQueryListJobsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryListJobsRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::ListJobsRequest, FederatedQuery::ListJobsResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryListJobsRPC(msg);
}

using TFederatedQueryDescribeJobRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DescribeJobRequest, FederatedQuery::DescribeJobResponse>,
    NFq::TEvControlPlaneProxy::TEvDescribeJobRequest,
    NFq::TEvControlPlaneProxy::TEvDescribeJobResponse>;

void DoFederatedQueryDescribeJobRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDescribeJobRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::DescribeJobRequest, FederatedQuery::DescribeJobResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryDescribeJobRPC(msg);
}

using TFederatedQueryCreateConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::CreateConnectionRequest, FederatedQuery::CreateConnectionResponse>,
    NFq::TEvControlPlaneProxy::TEvCreateConnectionRequest,
    NFq::TEvControlPlaneProxy::TEvCreateConnectionResponse>;

void DoFederatedQueryCreateConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryCreateConnectionRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::CreateConnectionRequest, FederatedQuery::CreateConnectionResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryCreateConnectionRPC(msg);
}

using TFederatedQueryListConnectionsRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ListConnectionsRequest, FederatedQuery::ListConnectionsResponse>,
    NFq::TEvControlPlaneProxy::TEvListConnectionsRequest,
    NFq::TEvControlPlaneProxy::TEvListConnectionsResponse>;

void DoFederatedQueryListConnectionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryListConnectionsRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::ListConnectionsRequest, FederatedQuery::ListConnectionsResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryListConnectionsRPC(msg);
}

using TFederatedQueryDescribeConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DescribeConnectionRequest, FederatedQuery::DescribeConnectionResponse>,
    NFq::TEvControlPlaneProxy::TEvDescribeConnectionRequest,
    NFq::TEvControlPlaneProxy::TEvDescribeConnectionResponse>;

void DoFederatedQueryDescribeConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDescribeConnectionRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::DescribeConnectionRequest, FederatedQuery::DescribeConnectionResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryDescribeConnectionRPC(msg);
}

using TFederatedQueryModifyConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ModifyConnectionRequest, FederatedQuery::ModifyConnectionResponse>,
    NFq::TEvControlPlaneProxy::TEvModifyConnectionRequest,
    NFq::TEvControlPlaneProxy::TEvModifyConnectionResponse>;

void DoFederatedQueryModifyConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryModifyConnectionRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::ModifyConnectionRequest, FederatedQuery::ModifyConnectionResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryModifyConnectionRPC(msg);
}

using TFederatedQueryDeleteConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DeleteConnectionRequest, FederatedQuery::DeleteConnectionResponse>,
    NFq::TEvControlPlaneProxy::TEvDeleteConnectionRequest,
    NFq::TEvControlPlaneProxy::TEvDeleteConnectionResponse>;

void DoFederatedQueryDeleteConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDeleteConnectionRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::DeleteConnectionRequest, FederatedQuery::DeleteConnectionResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryDeleteConnectionRPC(msg);
}

using TFederatedQueryTestConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::TestConnectionRequest, FederatedQuery::TestConnectionResponse>,
    NFq::TEvControlPlaneProxy::TEvTestConnectionRequest,
    NFq::TEvControlPlaneProxy::TEvTestConnectionResponse>;

void DoFederatedQueryTestConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryTestConnectionRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::TestConnectionRequest, FederatedQuery::TestConnectionResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryTestConnectionRPC(msg);
}

using TFederatedQueryCreateBindingRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::CreateBindingRequest, FederatedQuery::CreateBindingResponse>,
    NFq::TEvControlPlaneProxy::TEvCreateBindingRequest,
    NFq::TEvControlPlaneProxy::TEvCreateBindingResponse>;

void DoFederatedQueryCreateBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryCreateBindingRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::CreateBindingRequest, FederatedQuery::CreateBindingResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryCreateBindingRPC(msg);
}

using TFederatedQueryListBindingsRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ListBindingsRequest, FederatedQuery::ListBindingsResponse>,
    NFq::TEvControlPlaneProxy::TEvListBindingsRequest,
    NFq::TEvControlPlaneProxy::TEvListBindingsResponse>;

void DoFederatedQueryListBindingsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryListBindingsRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::ListBindingsRequest, FederatedQuery::ListBindingsResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryListBindingsRPC(msg);
}

using TFederatedQueryDescribeBindingRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DescribeBindingRequest, FederatedQuery::DescribeBindingResponse>,
    NFq::TEvControlPlaneProxy::TEvDescribeBindingRequest,
    NFq::TEvControlPlaneProxy::TEvDescribeBindingResponse>;

void DoFederatedQueryDescribeBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDescribeBindingRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::DescribeBindingRequest, FederatedQuery::DescribeBindingResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryDescribeBindingRPC(msg);
}

using TFederatedQueryModifyBindingRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ModifyBindingRequest, FederatedQuery::ModifyBindingResponse>,
    NFq::TEvControlPlaneProxy::TEvModifyBindingRequest,
    NFq::TEvControlPlaneProxy::TEvModifyBindingResponse>;

void DoFederatedQueryModifyBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryModifyBindingRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::ModifyBindingRequest, FederatedQuery::ModifyBindingResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryModifyBindingRPC(msg);
}

using TFederatedQueryDeleteBindingRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DeleteBindingRequest, FederatedQuery::DeleteBindingResponse>,
    NFq::TEvControlPlaneProxy::TEvDeleteBindingRequest,
    NFq::TEvControlPlaneProxy::TEvDeleteBindingResponse>;

void DoFederatedQueryDeleteBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFederatedQueryDeleteBindingRPC(p.release()));
}

template <>
IActor* TGrpcRequestOperationCall<FederatedQuery::DeleteBindingRequest, FederatedQuery::DeleteBindingResponse>::CreateRpcActor(IRequestOpCtx* msg) {
    return new TFederatedQueryDeleteBindingRPC(msg);
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::CreateQueryRequest> GetFederatedQueryCreateQueryPermissions() {
    return {[](const FederatedQuery::CreateQueryRequest& request) {
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
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::ListQueriesRequest> GetFederatedQueryListQueriesPermissions() {
    return {[](const FederatedQuery::ListQueriesRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::DescribeQueryRequest> GetFederatedQueryDescribeQueryPermissions() {
    return {[](const FederatedQuery::DescribeQueryRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.get"),
            NPerms::Optional("yq.queries.viewAst"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate"),
            NPerms::Optional("yq.queries.viewQueryText")
        };
    }};
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::GetQueryStatusRequest> GetFederatedQueryGetQueryStatusPermissions() {
    return {[](const FederatedQuery::GetQueryStatusRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.getStatus"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::ModifyQueryRequest> GetFederatedQueryModifyQueryPermissions() {
    return {[](const FederatedQuery::ModifyQueryRequest& request) {
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
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::DeleteQueryRequest> GetFederatedQueryDeleteQueryPermissions() {
    return {[](const FederatedQuery::DeleteQueryRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.delete"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate")
        };
    }};
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::ControlQueryRequest> GetFederatedQueryControlQueryPermissions() {
    return {[](const FederatedQuery::ControlQueryRequest& request) -> TVector<NPerms::TPermission> {
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
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::GetResultDataRequest> GetFederatedQueryGetResultDataPermissions() {
    return { [](const FederatedQuery::GetResultDataRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.getData"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::ListJobsRequest> GetFederatedQueryListJobsPermissions() {
    return { [](const FederatedQuery::ListJobsRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.jobs.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::DescribeJobRequest> GetFederatedQueryDescribeJobPermissions() {
    return { [](const FederatedQuery::DescribeJobRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.jobs.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate"),
            NPerms::Optional("yq.queries.viewAst"),
            NPerms::Optional("yq.queries.viewQueryText")
        };
    } };
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::CreateConnectionRequest> GetFederatedQueryCreateConnectionPermissions() {
    return { [](const FederatedQuery::CreateConnectionRequest& request) -> TVector<NPerms::TPermission> {
        TVector<NPerms::TPermission> basePermissions{
            NPerms::Required("yq.connections.create"),
        };
        if (request.content().acl().visibility() == FederatedQuery::Acl::SCOPE) {
            basePermissions.push_back(NPerms::Required("yq.resources.managePublic"));
        }
        return basePermissions;
    } };
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::ListConnectionsRequest> GetFederatedQueryListConnectionsPermissions() {
    return { [](const FederatedQuery::ListConnectionsRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::DescribeConnectionRequest> GetFederatedQueryDescribeConnectionPermissions() {
    return { [](const FederatedQuery::DescribeConnectionRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::ModifyConnectionRequest> GetFederatedQueryModifyConnectionPermissions() {
    return { [](const FederatedQuery::ModifyConnectionRequest& request) -> TVector<NPerms::TPermission> {
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
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::DeleteConnectionRequest> GetFederatedQueryDeleteConnectionPermissions() {
    return { [](const FederatedQuery::DeleteConnectionRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.delete"),
            NPerms::Required("yq.connections.get"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::TestConnectionRequest> GetFederatedQueryTestConnectionPermissions() {
    return { [](const FederatedQuery::TestConnectionRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.create")
        };
    } };
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::CreateBindingRequest> GetFederatedQueryCreateBindingPermissions() {
    return { [](const FederatedQuery::CreateBindingRequest&) -> TVector<NPerms::TPermission> {
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
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::ListBindingsRequest> GetFederatedQueryListBindingsPermissions() {
    return { [](const FederatedQuery::ListBindingsRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.bindings.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::DescribeBindingRequest> GetFederatedQueryDescribeBindingPermissions() {
    return { [](const FederatedQuery::DescribeBindingRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.bindings.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::ModifyBindingRequest> GetFederatedQueryModifyBindingPermissions() {
    return { [](const FederatedQuery::ModifyBindingRequest&) -> TVector<NPerms::TPermission> {
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
}

NFederatedQuery::TPermissionsFunc<FederatedQuery::DeleteBindingRequest> GetFederatedQueryDeleteBindingPermissions() {
    return { [](const FederatedQuery::DeleteBindingRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.bindings.delete"),
            NPerms::Required("yq.bindings.get"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    } };
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryCreateQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryCreateQueryPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::CreateQueryRequest, FederatedQuery::CreateQueryResponse>>(ctx.Release(), &DoFederatedQueryCreateQueryRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListQueriesRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryListQueriesPermissions();return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ListQueriesRequest, FederatedQuery::ListQueriesResponse>>(ctx.Release(), &DoFederatedQueryListQueriesRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryDescribeQueryPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DescribeQueryRequest, FederatedQuery::DescribeQueryResponse>>(ctx.Release(), &DoFederatedQueryDescribeQueryRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryGetQueryStatusRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryGetQueryStatusPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::GetQueryStatusRequest, FederatedQuery::GetQueryStatusResponse>>(ctx.Release(), &DoFederatedQueryGetQueryStatusRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryModifyQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryModifyQueryPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ModifyQueryRequest, FederatedQuery::ModifyQueryResponse>>(ctx.Release(), &DoFederatedQueryModifyQueryRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDeleteQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryDeleteQueryPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DeleteQueryRequest, FederatedQuery::DeleteQueryResponse>>(ctx.Release(), &DoFederatedQueryDeleteQueryRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryControlQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryControlQueryPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ControlQueryRequest, FederatedQuery::ControlQueryResponse>>(ctx.Release(), &DoFederatedQueryControlQueryRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryGetResultDataRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryGetResultDataPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::GetResultDataRequest, FederatedQuery::GetResultDataResponse>>(ctx.Release(), &DoFederatedQueryGetResultDataRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListJobsRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryListJobsPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ListJobsRequest, FederatedQuery::ListJobsResponse>>(ctx.Release(), &DoFederatedQueryListJobsRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeJobRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryDescribeJobPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DescribeJobRequest, FederatedQuery::DescribeJobResponse>>(ctx.Release(), &DoFederatedQueryDescribeJobRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryCreateConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryCreateConnectionPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::CreateConnectionRequest, FederatedQuery::CreateConnectionResponse>>(ctx.Release(), &DoFederatedQueryCreateConnectionRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListConnectionsRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryListConnectionsPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ListConnectionsRequest, FederatedQuery::ListConnectionsResponse>>(ctx.Release(), &DoFederatedQueryListConnectionsRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryDescribeConnectionPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DescribeConnectionRequest, FederatedQuery::DescribeConnectionResponse>>(ctx.Release(), &DoFederatedQueryDescribeConnectionRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryModifyConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryModifyConnectionPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ModifyConnectionRequest, FederatedQuery::ModifyConnectionResponse>>(ctx.Release(), &DoFederatedQueryModifyConnectionRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDeleteConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryDeleteConnectionPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DeleteConnectionRequest, FederatedQuery::DeleteConnectionResponse>>(ctx.Release(), &DoFederatedQueryDeleteConnectionRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryTestConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryTestConnectionPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::TestConnectionRequest, FederatedQuery::TestConnectionResponse>>(ctx.Release(), &DoFederatedQueryTestConnectionRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryCreateBindingRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryCreateBindingPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::CreateBindingRequest, FederatedQuery::CreateBindingResponse>>(ctx.Release(), &DoFederatedQueryCreateBindingRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListBindingsRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryListBindingsPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ListBindingsRequest, FederatedQuery::ListBindingsResponse>>(ctx.Release(), &DoFederatedQueryListBindingsRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeBindingRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryDescribeBindingPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DescribeBindingRequest, FederatedQuery::DescribeBindingResponse>>(ctx.Release(), &DoFederatedQueryDescribeBindingRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryModifyBindingRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryModifyBindingPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::ModifyBindingRequest, FederatedQuery::ModifyBindingResponse>>(ctx.Release(), &DoFederatedQueryModifyBindingRequest, std::move(permissions));
}

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDeleteBindingRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx) {
    std::function permissions = GetFederatedQueryDeleteBindingPermissions();
    return std::make_unique<TGrpcFqRequestOperationCall<FederatedQuery::DeleteBindingRequest, FederatedQuery::DeleteBindingResponse>>(ctx.Release(), &DoFederatedQueryDeleteBindingRequest, std::move(permissions));
}

} // namespace NGRpcService
} // namespace NKikimr
