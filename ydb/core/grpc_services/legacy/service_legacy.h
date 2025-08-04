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

namespace NKikimr::NGRpcService {

class IRequestNoOpCtx;
class IFacilityProvider;

namespace NLegacyGrpcService {

namespace NPrivate {

template <class TReq>
struct TGetYdbTokenLegacyTraits {
    static const TMaybe<TString> GetYdbToken(const TReq& req, const NYdbGrpc::IRequestContextBase*) {
        if (const TString& token = req.GetSecurityToken()) {
            return token;
        }
        return Nothing();
    }
};

} // namespace NPrivate

template <class TReq, class TResp>
struct TLegacyGrpcMethodAccessorTraits;

template <>
struct TLegacyGrpcMethodAccessorTraits<NKikimrClient::TConsoleRequest, NKikimrClient::TConsoleResponse> : NPrivate::TGetYdbTokenLegacyTraits<NKikimrClient::TConsoleRequest> {
    static void FillResponse(NKikimrClient::TConsoleResponse& resp, const NYql::TIssues& issues, Ydb::CostInfo*, Ydb::StatusIds::StatusCode status) {
        resp.MutableStatus()->SetCode(status);
        resp.MutableStatus()->SetReason(issues.ToString());
    }
};

void DoConsoleRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);

} // namespace NLegacyGrpcService

} // namespace NKikimr::NGRpcService
