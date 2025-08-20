#pragma once
#include <ydb/core/protos/grpc.grpc.pb.h>
#include <ydb/core/protos/node_broker.pb.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/console_base.pb.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <memory>

namespace Ydb {
class CostInfo;
}

namespace NYdbGrpc {
class IRequestContextBase;
}

namespace NActors {
struct TActorId;
class TActorSystem;
}

namespace NKikimr::NGRpcService {

class IRequestNoOpCtx;
class IFacilityProvider;

namespace NLegacyGrpcService {

namespace NPrivate {

ui32 ToMsgBusStatus(Ydb::StatusIds::StatusCode status);
NKikimrNodeBroker::TStatus::ECode ToNodeBrokerStatus(Ydb::StatusIds::StatusCode status);
NKikimrCms::TStatus::ECode ToCmsStatus(Ydb::StatusIds::StatusCode status);

const TMaybe<TString> ExtractYdbTokenFromHeaders(const NYdbGrpc::IRequestContextBase* ctx);

template <class TReq>
struct TGetYdbTokenLegacyTraits {
    static const TMaybe<TString> GetYdbToken(const TReq& req, const NYdbGrpc::IRequestContextBase* ctx) {
        // Explicit token from request proto
        if (const TString& token = req.GetSecurityToken()) {
            return token;
        }

        // Fallback on headers
        return ExtractYdbTokenFromHeaders(ctx);
    }
};

struct TResponseLegacyCommonTraits {
    static void FillResponse(NKikimrClient::TResponse& resp, const NYql::TIssues& issues, Ydb::CostInfo*, Ydb::StatusIds::StatusCode status) {
        resp.SetStatus(ToMsgBusStatus(status));
        NYql::IssuesToMessage(issues, resp.MutableIssues());
    }
};

} // namespace NPrivate

//
// Accessor traits
//

template <class TReq, class TResp>
struct TLegacyGrpcMethodAccessorTraits;


template <class TReq>
struct TLegacyGrpcMethodAccessorTraits<TReq, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenLegacyTraits<TReq>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TNodeRegistrationRequest, NKikimrClient::TNodeRegistrationResponse>
    : NPrivate::TGetYdbTokenLegacyTraits<NKikimrClient::TNodeRegistrationRequest>
{
    static void FillResponse(NKikimrClient::TNodeRegistrationResponse& resp, const NYql::TIssues& issues, Ydb::CostInfo*, Ydb::StatusIds::StatusCode status) {
        resp.MutableStatus()->SetCode(NPrivate::ToNodeBrokerStatus(status));
        resp.MutableStatus()->SetReason(issues.ToString());
    }
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TCmsRequest, NKikimrClient::TCmsResponse>
    : NPrivate::TGetYdbTokenLegacyTraits<NKikimrClient::TCmsRequest>
{
    static void FillResponse(NKikimrClient::TCmsResponse& resp, const NYql::TIssues& issues, Ydb::CostInfo*, Ydb::StatusIds::StatusCode status) {
        resp.MutableStatus()->SetCode(NPrivate::ToCmsStatus(status));
        resp.MutableStatus()->SetReason(issues.ToString());
    }
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TConsoleRequest, NKikimrClient::TConsoleResponse>
    : NPrivate::TGetYdbTokenLegacyTraits<NKikimrClient::TConsoleRequest>
{
    static void FillResponse(NKikimrClient::TConsoleResponse& resp, const NYql::TIssues& issues, Ydb::CostInfo*, Ydb::StatusIds::StatusCode status) {
        resp.MutableStatus()->SetCode(status);
        resp.MutableStatus()->SetReason(issues.ToString());
    }
};

// Requests that should be forwarded to msg bus proxy
using TCreateActorCallback = std::function<void(std::unique_ptr<IRequestNoOpCtx>, const IFacilityProvider&)>;

TCreateActorCallback DoSchemeOperation(const NActors::TActorId& msgBusProxy, NActors::TActorSystem* actorSystem);
void DoSchemeOperationStatus(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
TCreateActorCallback DoSchemeDescribe(const NActors::TActorId& msgBusProxy, NActors::TActorSystem* actorSystem);
void DoChooseProxy(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
TCreateActorCallback DoPersQueueRequest(const NActors::TActorId& msgBusProxy, NActors::TActorSystem* actorSystem);
TCreateActorCallback DoSchemeInitRoot(const NActors::TActorId& msgBusProxy, NActors::TActorSystem* actorSystem);
void DoResolveNode(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoFillNode(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoDrainNode(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoBlobStorageConfig(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoHiveCreateTablet(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoTestShardControl(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoRegisterNode(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoCmsRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoConsoleRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoInterconnectDebug(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoTabletStateRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoLocalSchemeTx(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoLocalEnumerateTablets(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoTabletKillRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);

} // namespace NLegacyGrpcService
} // namespace NKikimr::NGRpcService
