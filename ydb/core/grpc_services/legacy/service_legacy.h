#pragma once
#include <ydb/core/protos/grpc.grpc.pb.h>
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
    : NPrivate::TGetYdbTokenUnsecureTraits<NKikimrClient::TSchemeOperationStatus>
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

void DoConsoleRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);

} // namespace NLegacyGrpcService
} // namespace NKikimr::NGRpcService
