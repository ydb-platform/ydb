#include "audit_logins.h"
#include "base/base.h"

#include <ydb/core/audit/login_op.h>
#include <ydb/public/api/protos/ydb_auth.pb.h>

namespace NKikimr {
namespace NGRpcService {

void AuditLogLogin(IAuditCtx* ctx, const TString& database, const Ydb::Auth::LoginRequest& request,
                   const Ydb::Auth::LoginResponse& response, const TString& errorDetails,
                   const TString& sanitizedToken, bool isAdmin)
{
    static const TString GrpcLoginComponentName = "grpc-login";

    auto status = response.operation().status();
    TString detailed_status;
    TString reason = "";
    if (response.operation().issues_size() > 0) {
        reason = response.operation().issues(0).message();
    }

    NAudit::LogLoginOperationResult(GrpcLoginComponentName, ctx->GetPeerName(), database, request.user(), status, reason,
        errorDetails, sanitizedToken, isAdmin);
}

}
}
