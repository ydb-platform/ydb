#include "rpc_common.h"
#include "rpc_deferrable.h"

#include <ydb/core/grpc_services/service_fq.h>
#include <ydb/core/yq/libs/audit/events/events.h>
#include <ydb/core/yq/libs/audit/yq_audit_service.h>
#include <ydb/core/yq/libs/control_plane_proxy/control_plane_proxy.h>
#include <ydb/core/yq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/yq/libs/control_plane_proxy/utils.h>
#include <ydb/public/api/protos/fq.pb.h>

#include <ydb/library/aclib/aclib.h>

#include <library/cpp/actors/core/hfunc.h>

#include <util/generic/guid.h>
#include <util/string/split.h>

namespace NKikimr {
namespace NGRpcService {

using namespace Ydb;

template <typename RpcRequestType, typename EvRequestType, typename EvResponseType, typename CastRequest, typename CastResult>
class TFederatedQueryRequestRPC : public TRpcOperationRequestActor<
    TFederatedQueryRequestRPC<RpcRequestType,EvRequestType,EvResponseType, CastRequest, CastResult>, RpcRequestType> {

public:
    using TBase = TRpcOperationRequestActor<
        TFederatedQueryRequestRPC<RpcRequestType,EvRequestType,EvResponseType,CastRequest,CastResult>,
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

        TString ydbProject = Request_->GetPeerMetaValues("x-ydb-fq-project").GetOrElse("");

        if (!ydbProject.StartsWith("yandexcloud://")) {
            ReplyWithStatus("x-ydb-fq-project should start with yandexcloud:// but got " + ydbProject, StatusIds::BAD_REQUEST);
            return;
        }

        const TVector<TString> path = StringSplitter(ydbProject).Split('/').SkipEmpty();
        if (path.size() != 2 && path.size() != 3) {
            ReplyWithStatus("x-ydb-fq-project format is invalid. Must be yandexcloud://folder_id, but got " + ydbProject, StatusIds::BAD_REQUEST);
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
        TProtoStringType protoString;
        if (!req->SerializeToString(&protoString)) {
            ReplyWithStatus("Can't serialize proto", StatusIds::BAD_REQUEST);
            return;
        }
        auto castedRequest = CastRequest();
        if (!castedRequest.ParseFromString(protoString)) {
            ReplyWithStatus("Can't deserialize proto", StatusIds::BAD_REQUEST);
            return;
        }
        auto ev = MakeHolder<EvRequestType>(FolderId, castedRequest, User, Token, permissions);
        Send(NYq::ControlPlaneProxyActorId(), ev.Release());
        Become(&TFederatedQueryRequestRPC<RpcRequestType, EvRequestType, EvResponseType, CastRequest, CastResult>::StateFunc);
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
            TProtoStringType protoString;
            if (!response.Result.SerializeToString(&protoString)) {
                ReplyWithStatus("Can't serialize proto", StatusIds::BAD_REQUEST);
                return;
            }
            auto castedResult = CastResult();
            if (!castedResult.ParseFromString(protoString)) {
                ReplyWithStatus("Can't deserialize proto", StatusIds::BAD_REQUEST);
                return;
            }

            req.SendResult(castedResult, StatusIds::SUCCESS);
        }
    }

    template <typename TResponse, typename TReq> requires requires (TResponse r) { r.AuditDetails; }
    void SendResponse(const TResponse& response, TReq& req) {
        if (response.Issues) {
            req.RaiseIssues(response.Issues);
            req.ReplyWithYdbStatus(StatusIds::BAD_REQUEST);
        } else {
            TProtoStringType protoString;
            if (!response.Result.SerializeToString(&protoString)) {
                ReplyWithStatus("Can't serialize proto", StatusIds::BAD_REQUEST);
                return;
            }
            auto castedResponse = CastResult();
            if (!castedResponse.ParseFromString(protoString)) {
                ReplyWithStatus("Can't deserialize proto", StatusIds::BAD_REQUEST);
                return;
            }
            req.SendResult(castedResponse, StatusIds::SUCCESS);
        }

        NYq::TEvAuditService::TExtraInfo extraInfo{
            .Token = Token,
            .FolderId = FolderId,
            .User = User,
            .PeerName = PeerName,
            .UserAgent = UserAgent,
            .RequestId = RequestId,
        };

        const auto* protoReq = GetProtoRequest();
        TProtoStringType protoString;
        if (!protoReq->SerializeToString(&protoString)) {
            ReplyWithStatus("Can't serialize proto", StatusIds::BAD_REQUEST);
            return;
        }
        auto castedProtoRequest = CastRequest();
        if (!castedProtoRequest.ParseFromString(protoString)) {
            ReplyWithStatus("Can't deserialize proto", StatusIds::BAD_REQUEST);
            return;
        }

        Send(NYq::YqAuditServiceActorId(), NYq::TEvAuditService::MakeAuditEvent(
            std::move(extraInfo),
            castedProtoRequest,
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
    NYq::TEvControlPlaneProxy::TEvCreateQueryRequest,
    NYq::TEvControlPlaneProxy::TEvCreateQueryResponse,
    YandexQuery::CreateQueryRequest,
    FederatedQuery::CreateQueryResult>;

void DoFederatedQueryCreateQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryCreateQueryRPC(p.release()));
}

using TFederatedQueryListQueriesRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ListQueriesRequest, FederatedQuery::ListQueriesResponse>,
    NYq::TEvControlPlaneProxy::TEvListQueriesRequest,
    NYq::TEvControlPlaneProxy::TEvListQueriesResponse,
    YandexQuery::ListQueriesRequest,
    FederatedQuery::ListQueriesResult>;

void DoFederatedQueryListQueriesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryListQueriesRPC(p.release()));
}

using TFederatedQueryDescribeQueryRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DescribeQueryRequest, FederatedQuery::DescribeQueryResponse>,
    NYq::TEvControlPlaneProxy::TEvDescribeQueryRequest,
    NYq::TEvControlPlaneProxy::TEvDescribeQueryResponse,
    YandexQuery::DescribeQueryRequest,
    FederatedQuery::DescribeQueryResult>;

void DoFederatedQueryDescribeQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryDescribeQueryRPC(p.release()));
}

using TFederatedQueryGetQueryStatusRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::GetQueryStatusRequest, FederatedQuery::GetQueryStatusResponse>,
    NYq::TEvControlPlaneProxy::TEvGetQueryStatusRequest,
    NYq::TEvControlPlaneProxy::TEvGetQueryStatusResponse,
    YandexQuery::GetQueryStatusRequest,
    FederatedQuery::GetQueryStatusResult>;

void DoFederatedQueryGetQueryStatusRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryGetQueryStatusRPC(p.release()));
}

using TFederatedQueryModifyQueryRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ModifyQueryRequest, FederatedQuery::ModifyQueryResponse>,
    NYq::TEvControlPlaneProxy::TEvModifyQueryRequest,
    NYq::TEvControlPlaneProxy::TEvModifyQueryResponse,
    YandexQuery::ModifyQueryRequest,
    FederatedQuery::ModifyQueryResult>;

void DoFederatedQueryModifyQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryModifyQueryRPC(p.release()));
}

using TFederatedQueryDeleteQueryRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DeleteQueryRequest, FederatedQuery::DeleteQueryResponse>,
    NYq::TEvControlPlaneProxy::TEvDeleteQueryRequest,
    NYq::TEvControlPlaneProxy::TEvDeleteQueryResponse,
    YandexQuery::DeleteQueryRequest,
    FederatedQuery::DeleteQueryResult>;

void DoFederatedQueryDeleteQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryDeleteQueryRPC(p.release()));
}

using TFederatedQueryControlQueryRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ControlQueryRequest, FederatedQuery::ControlQueryResponse>,
    NYq::TEvControlPlaneProxy::TEvControlQueryRequest,
    NYq::TEvControlPlaneProxy::TEvControlQueryResponse,
    YandexQuery::ControlQueryRequest,
    FederatedQuery::ControlQueryResult>;

void DoFederatedQueryControlQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryControlQueryRPC(p.release()));
}

using TFederatedQueryGetResultDataRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::GetResultDataRequest, FederatedQuery::GetResultDataResponse>,
    NYq::TEvControlPlaneProxy::TEvGetResultDataRequest,
    NYq::TEvControlPlaneProxy::TEvGetResultDataResponse,
    YandexQuery::GetResultDataRequest,
    FederatedQuery::GetResultDataResult>;

void DoGetResultDataRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryGetResultDataRPC(p.release()));
}

using TFederatedQueryListJobsRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ListJobsRequest, FederatedQuery::ListJobsResponse>,
    NYq::TEvControlPlaneProxy::TEvListJobsRequest,
    NYq::TEvControlPlaneProxy::TEvListJobsResponse,
    YandexQuery::ListJobsRequest,
    FederatedQuery::ListJobsResult>;

void DoListJobsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryListJobsRPC(p.release()));
}

using TFederatedQueryDescribeJobRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DescribeJobRequest, FederatedQuery::DescribeJobResponse>,
    NYq::TEvControlPlaneProxy::TEvDescribeJobRequest,
    NYq::TEvControlPlaneProxy::TEvDescribeJobResponse,
    YandexQuery::DescribeJobRequest,
    FederatedQuery::DescribeJobResult>;

void DoDescribeJobRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryDescribeJobRPC(p.release()));
}

using TFederatedQueryCreateConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::CreateConnectionRequest, FederatedQuery::CreateConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvCreateConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvCreateConnectionResponse,
    YandexQuery::CreateConnectionRequest,
    FederatedQuery::CreateConnectionResult>;

void DoCreateConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryCreateConnectionRPC(p.release()));
}

using TFederatedQueryListConnectionsRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ListConnectionsRequest, FederatedQuery::ListConnectionsResponse>,
    NYq::TEvControlPlaneProxy::TEvListConnectionsRequest,
    NYq::TEvControlPlaneProxy::TEvListConnectionsResponse,
    YandexQuery::ListConnectionsRequest,
    FederatedQuery::ListConnectionsResult>;

void DoListConnectionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryListConnectionsRPC(p.release()));
}

using TFederatedQueryDescribeConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DescribeConnectionRequest, FederatedQuery::DescribeConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvDescribeConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvDescribeConnectionResponse,
    YandexQuery::DescribeConnectionRequest,
    FederatedQuery::DescribeConnectionResult>;

void DoDescribeConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryDescribeConnectionRPC(p.release()));
}

using TFederatedQueryModifyConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ModifyConnectionRequest, FederatedQuery::ModifyConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvModifyConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvModifyConnectionResponse,
    YandexQuery::ModifyConnectionRequest,
    FederatedQuery::ModifyConnectionResult>;

void DoModifyConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryModifyConnectionRPC(p.release()));
}

using TFederatedQueryDeleteConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DeleteConnectionRequest, FederatedQuery::DeleteConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvDeleteConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvDeleteConnectionResponse,
    YandexQuery::DeleteConnectionRequest,
    FederatedQuery::DeleteConnectionResult>;

void DoDeleteConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryDeleteConnectionRPC(p.release()));
}

using TFederatedQueryTestConnectionRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::TestConnectionRequest, FederatedQuery::TestConnectionResponse>,
    NYq::TEvControlPlaneProxy::TEvTestConnectionRequest,
    NYq::TEvControlPlaneProxy::TEvTestConnectionResponse,
    YandexQuery::TestConnectionRequest,
    FederatedQuery::TestConnectionResult>;

void DoTestConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryTestConnectionRPC(p.release()));
}

using TFederatedQueryCreateBindingRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::CreateBindingRequest, FederatedQuery::CreateBindingResponse>,
    NYq::TEvControlPlaneProxy::TEvCreateBindingRequest,
    NYq::TEvControlPlaneProxy::TEvCreateBindingResponse,
    YandexQuery::CreateBindingRequest,
    FederatedQuery::CreateBindingResult>;

void DoCreateBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& ) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryCreateBindingRPC(p.release()));
}

using TFederatedQueryListBindingsRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ListBindingsRequest, FederatedQuery::ListBindingsResponse>,
    NYq::TEvControlPlaneProxy::TEvListBindingsRequest,
    NYq::TEvControlPlaneProxy::TEvListBindingsResponse,
    YandexQuery::ListBindingsRequest,
    FederatedQuery::ListBindingsResult>;

void DoListBindingsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryListBindingsRPC(p.release()));
}

using TFederatedQueryDescribeBindingRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DescribeBindingRequest, FederatedQuery::DescribeBindingResponse>,
    NYq::TEvControlPlaneProxy::TEvDescribeBindingRequest,
    NYq::TEvControlPlaneProxy::TEvDescribeBindingResponse,
    YandexQuery::DescribeBindingRequest,
    FederatedQuery::DescribeBindingResult>;

void DoDescribeBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryDescribeBindingRPC(p.release()));
}

using TFederatedQueryModifyBindingRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::ModifyBindingRequest, FederatedQuery::ModifyBindingResponse>,
    NYq::TEvControlPlaneProxy::TEvModifyBindingRequest,
    NYq::TEvControlPlaneProxy::TEvModifyBindingResponse,
    YandexQuery::ModifyBindingRequest,
    FederatedQuery::ModifyBindingResult>;

void DoModifyBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryModifyBindingRPC(p.release()));
}

using TFederatedQueryDeleteBindingRPC = TFederatedQueryRequestRPC<
    TGrpcFqRequestOperationCall<FederatedQuery::DeleteBindingRequest, FederatedQuery::DeleteBindingResponse>,
    NYq::TEvControlPlaneProxy::TEvDeleteBindingRequest,
    NYq::TEvControlPlaneProxy::TEvDeleteBindingResponse,
    YandexQuery::DeleteBindingRequest,
    FederatedQuery::DeleteBindingResult>;

void DoDeleteBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFederatedQueryDeleteBindingRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
