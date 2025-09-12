#include "defs.h"

#include <ydb/core/util/address_classifier.h>
#include <ydb/core/audit/audit_log.h>

#include <ydb/public/api/protos/ydb_auth.pb.h>

#include "base/base.h"
#include "audit_logins.h"

namespace NKikimr {
namespace NGRpcService {

void AuditLogLogin(IAuditCtx* ctx, const TString& database, const Ydb::Auth::LoginRequest& request, const Ydb::Auth::LoginResponse& response, const TString& errorDetails)
{
    static const TString GrpcLoginComponentName = "grpc-login";
    static const TString LoginOperationName = "LOGIN";

    //NOTE: EmptyValue couldn't be an empty string as AUDIT_PART() skips parts with an empty values
    static const TString EmptyValue = "{none}";

    auto peerName = NKikimr::NAddressClassifier::ExtractAddress(ctx->GetPeerName());

    auto status = response.operation().status();
    TString detailed_status;
    TString reason;
    if (status != Ydb::StatusIds::SUCCESS) {
        detailed_status = Ydb::StatusIds::StatusCode_Name(status);
        if (response.operation().issues_size() > 0) {
            reason = response.operation().issues(0).message();
        }
        if (errorDetails) {
            if (!reason.empty()) {
                reason += ": ";
            }
            reason += errorDetails;
        }
    }

    // NOTE: audit field set here must be in sync with ydb/core/security/login_page.cpp, AuditLogWebUILogout()
    AUDIT_LOG(
        AUDIT_PART("component", GrpcLoginComponentName)
        AUDIT_PART("remote_address", (!peerName.empty() ? peerName : EmptyValue))
        AUDIT_PART("database", (!database.empty() ? database : EmptyValue))
        AUDIT_PART("operation", LoginOperationName)
        AUDIT_PART("status", (status == Ydb::StatusIds::SUCCESS ? "SUCCESS" : "ERROR"))
        AUDIT_PART("detailed_status", detailed_status, status != Ydb::StatusIds::SUCCESS)
        AUDIT_PART("reason", reason, status != Ydb::StatusIds::SUCCESS)

        // Login
        AUDIT_PART("login_user", (!request.user().empty() ? request.user() : EmptyValue))

        //TODO: (?) it is possible to show masked version of the resulting token here
    );
}

}
}

