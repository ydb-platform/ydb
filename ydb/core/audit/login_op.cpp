#include "login_op.h"

#include <ydb/core/util/address_classifier.h>

namespace NKikimr::NAudit {

void LogLoginOperationResult(const TString& сomponentName, const TString& peerName, const TString& database,
    const TString& username, const Ydb::StatusIds_StatusCode status, const TString& reason,
    const TString& errorDetails, const TString& sanitizedToken, bool isAdmin)
{
    static const TString LoginOperationName = "LOGIN";
    static const TString AdminAccountType = "admin";

    //NOTE: EmptyValue couldn't be an empty string as AUDIT_PART() skips parts with an empty values
    static const TString EmptyValue = "{none}";

    auto peerAddress = NKikimr::NAddressClassifier::ExtractAddress(peerName);

    TString detailed_status;
    TString fullReason = reason;
    if (status != Ydb::StatusIds::SUCCESS) {
        detailed_status = Ydb::StatusIds::StatusCode_Name(status);
        if (errorDetails) {
            if (!fullReason.empty()) {
                fullReason += ": ";
            }

            fullReason += errorDetails;
        }
    }

    // NOTE: audit field set here must be in sync with ydb/core/security/login_page.cpp, AuditLogWebUILogout()
    AUDIT_LOG(
        AUDIT_PART("component", сomponentName)
        AUDIT_PART("remote_address", (!peerAddress.empty() ? peerAddress : EmptyValue))
        AUDIT_PART("database", (!database.empty() ? database : EmptyValue))
        AUDIT_PART("operation", LoginOperationName)
        AUDIT_PART("status", (status == Ydb::StatusIds::SUCCESS ? "SUCCESS" : "ERROR"))
        AUDIT_PART("detailed_status", detailed_status, status != Ydb::StatusIds::SUCCESS)
        AUDIT_PART("reason", fullReason, status != Ydb::StatusIds::SUCCESS)

        // Login
        AUDIT_PART("login_user", (!username.empty() ? username : EmptyValue))
        AUDIT_PART("sanitized_token", (!sanitizedToken.empty() ? sanitizedToken : EmptyValue))
        AUDIT_PART("login_user_level", AdminAccountType, (isAdmin && status == Ydb::StatusIds::SUCCESS))

        //TODO: (?) it is possible to show masked version of the resulting token here
    );
}

} // namespace NKikimr::NAudit
