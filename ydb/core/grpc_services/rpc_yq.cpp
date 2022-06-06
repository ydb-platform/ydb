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

private:
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
            ReplyWithStatus("token is empty", StatusIds::BAD_REQUEST);
            return;
        }
        Token = *authToken;

        const TString scope = Request_->GetPeerMetaValues("x-yq-scope").GetOrElse("");
        if (!scope.StartsWith("yandexcloud://")) {
            ReplyWithStatus("x-yq-scope should start with yandexcloud:// but got " + scope, StatusIds::BAD_REQUEST);
            return;
        }

        const TVector<TString> path = StringSplitter(scope).Split('/').SkipEmpty();
        if (path.size() != 2) {
            ReplyWithStatus("x-yq-scope format is invalid. Must be yandexcloud://folder_id, but got " + scope, StatusIds::BAD_REQUEST);
            return;
        }

        FolderId = path.back();
        if (!FolderId) {
            ReplyWithStatus("folder id is empty", StatusIds::BAD_REQUEST);
            return;
        }

        if (FolderId.length() > 1024) {
            ReplyWithStatus("folder id length greater than 1024 characters: " + FolderId, StatusIds::BAD_REQUEST);
            return;
        }

        const TString& internalToken = proxyCtx->GetInternalToken();
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

        const auto req = GetProtoRequest();
        auto ev = MakeHolder<EvRequestType>(FolderId, *req, User, Token, permissions);
        Send(NYq::ControlPlaneProxyActorId(), ev.Release());
        Become(&TYandexQueryRequestRPC<RpcRequestType, EvRequestType, EvResponseType>::StateFunc);
    }

    void ReplyWithStatus(const TString& issueMessage, StatusIds::StatusCode status) {		
        Request_->RaiseIssue(NYql::TIssue(issueMessage));
        Request_->ReplyWithYdbStatus(status);		
        PassAway();		
    }		

private:
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

void DoGetResultDataRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryGetResultDataRPC(p.release()));
}

using TYandexQueryListJobsRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::ListJobsRequest, YandexQuery::ListJobsResponse>,
    NYq::TEvControlPlaneProxy::TEvListJobsRequest,
    NYq::TEvControlPlaneProxy::TEvListJobsResponse>;

void DoListJobsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryListJobsRPC(p.release()));
}

using TYandexQueryDescribeJobRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::DescribeJobRequest, YandexQuery::DescribeJobResponse>,
    NYq::TEvControlPlaneProxy::TEvDescribeJobRequest,
    NYq::TEvControlPlaneProxy::TEvDescribeJobResponse>;

void DoDescribeJobRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryDescribeJobRPC(p.release()));
}

using TYandexQueryCreateConnectionRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::CreateConnectionRequest, YandexQuery::CreateConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvCreateConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvCreateConnectionResponse>;

void DoCreateConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryCreateConnectionRPC(p.release()));
}

using TYandexQueryListConnectionsRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::ListConnectionsRequest, YandexQuery::ListConnectionsResponse>,
    NYq::TEvControlPlaneProxy::TEvListConnectionsRequest,
    NYq::TEvControlPlaneProxy::TEvListConnectionsResponse>;

void DoListConnectionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryListConnectionsRPC(p.release()));
}

using TYandexQueryDescribeConnectionRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::DescribeConnectionRequest, YandexQuery::DescribeConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvDescribeConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvDescribeConnectionResponse>;

void DoDescribeConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryDescribeConnectionRPC(p.release()));
}

using TYandexQueryModifyConnectionRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::ModifyConnectionRequest, YandexQuery::ModifyConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvModifyConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvModifyConnectionResponse>;

void DoModifyConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryModifyConnectionRPC(p.release()));
}

using TYandexQueryDeleteConnectionRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::DeleteConnectionRequest, YandexQuery::DeleteConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvDeleteConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvDeleteConnectionResponse>;

void DoDeleteConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryDeleteConnectionRPC(p.release()));
}

using TYandexQueryTestConnectionRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::TestConnectionRequest, YandexQuery::TestConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvTestConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvTestConnectionResponse>;

void DoTestConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryTestConnectionRPC(p.release()));
}

using TYandexQueryCreateBindingRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::CreateBindingRequest, YandexQuery::CreateBindingResponse>,
    NYq::TEvControlPlaneProxy::TEvCreateBindingRequest,
    NYq::TEvControlPlaneProxy::TEvCreateBindingResponse>;

void DoCreateBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& ) {
    TActivationContext::AsActorContext().Register(new TYandexQueryCreateBindingRPC(p.release()));
}

using TYandexQueryListBindingsRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::ListBindingsRequest, YandexQuery::ListBindingsResponse>,
    NYq::TEvControlPlaneProxy::TEvListBindingsRequest,
    NYq::TEvControlPlaneProxy::TEvListBindingsResponse>;

void DoListBindingsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryListBindingsRPC(p.release()));
}

using TYandexQueryDescribeBindingRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::DescribeBindingRequest, YandexQuery::DescribeBindingResponse>,
    NYq::TEvControlPlaneProxy::TEvDescribeBindingRequest,
    NYq::TEvControlPlaneProxy::TEvDescribeBindingResponse>;

void DoDescribeBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryDescribeBindingRPC(p.release()));
}

using TYandexQueryModifyBindingRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::ModifyBindingRequest, YandexQuery::ModifyBindingResponse>,
    NYq::TEvControlPlaneProxy::TEvModifyBindingRequest,
    NYq::TEvControlPlaneProxy::TEvModifyBindingResponse>;

void DoModifyBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryModifyBindingRPC(p.release()));
}

using TYandexQueryDeleteBindingRPC = TYandexQueryRequestRPC<
    TGrpcYqRequestOperationCall<YandexQuery::DeleteBindingRequest, YandexQuery::DeleteBindingResponse>,
    NYq::TEvControlPlaneProxy::TEvDeleteBindingRequest,
    NYq::TEvControlPlaneProxy::TEvDeleteBindingResponse>;

void DoDeleteBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYandexQueryDeleteBindingRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
