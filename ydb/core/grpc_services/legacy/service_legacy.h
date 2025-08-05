#pragma once
#include <ydb/core/protos/grpc.grpc.pb.h>
#include <ydb/core/protos/node_broker.pb.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/console_base.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
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

Y_HAS_MEMBER(GetSecurityToken);

template <class TReq>
struct TGetYdbTokenLegacyTraits {
    static const TMaybe<TString> GetYdbToken(const TReq& req, const NYdbGrpc::IRequestContextBase*) {
        if (const TString& token = req.GetSecurityToken()) {
            return token;
        }
        return Nothing();
    }

    static_assert(THasGetSecurityToken<TReq>::value, "Request class must have GetSecurityToken method");
};

template <class TReq>
struct TGetYdbTokenUnsecureTraits {
    static const TMaybe<TString> GetYdbToken(const TReq&, const NYdbGrpc::IRequestContextBase*) {
        return Nothing();
    }

    static_assert(!THasGetSecurityToken<TReq>::value, "Request class has GetSecurityToken method, so TGetYdbTokenUnsecureTraits must not be used");
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

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TSchemeOperation, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenLegacyTraits<NKikimrClient::TSchemeOperation>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TSchemeOperationStatus, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenLegacyTraits<NKikimrClient::TSchemeOperationStatus>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TSchemeDescribe, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenLegacyTraits<NKikimrClient::TSchemeDescribe>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TChooseProxyRequest, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenUnsecureTraits<NKikimrClient::TChooseProxyRequest>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TPersQueueRequest, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenLegacyTraits<NKikimrClient::TPersQueueRequest>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TSchemeInitRoot, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenLegacyTraits<NKikimrClient::TSchemeInitRoot>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TResolveNodeRequest, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenUnsecureTraits<NKikimrClient::TResolveNodeRequest>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TFillNodeRequest, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenLegacyTraits<NKikimrClient::TFillNodeRequest>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TDrainNodeRequest, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenLegacyTraits<NKikimrClient::TDrainNodeRequest>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TBlobStorageConfigRequest, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenLegacyTraits<NKikimrClient::TBlobStorageConfigRequest>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::THiveCreateTablet, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenUnsecureTraits<NKikimrClient::THiveCreateTablet>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TTestShardControlRequest, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenUnsecureTraits<NKikimrClient::TTestShardControlRequest>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TNodeRegistrationRequest, NKikimrClient::TNodeRegistrationResponse>
    : NPrivate::TGetYdbTokenUnsecureTraits<NKikimrClient::TNodeRegistrationRequest> // Unsecure because authorization is performed using client certificates
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

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TInterconnectDebug, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenUnsecureTraits<NKikimrClient::TInterconnectDebug>
    , NPrivate::TResponseLegacyCommonTraits
{
};

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TTabletStateRequest, NKikimrClient::TResponse>
    : NPrivate::TGetYdbTokenUnsecureTraits<NKikimrClient::TTabletStateRequest>
    , NPrivate::TResponseLegacyCommonTraits
{
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

} // namespace NLegacyGrpcService
} // namespace NKikimr::NGRpcService
