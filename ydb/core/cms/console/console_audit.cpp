#include "console_audit.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/util/address_classifier.h>
#include <ydb/library/aclib/aclib.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NConsole {

static const TString COMPONENT_NAME = "console";
static const TString EMPTY_VALUE = "{none}";

void AuditLogPatchDatabaseConfig(
    const NKikimrConsole::TPatchDatabaseConfigRequest& request,
    const NKikimrConsole::TDatabaseFeatureFlagsChange& change,
    Ydb::StatusIds::StatusCode status,
    const TString& reason)
{
    NACLibProto::TUserToken tokenProto;
    if (!tokenProto.ParseFromString(request.GetUserToken())) {
        tokenProto.Clear();
    }
    const NACLib::TUserToken token(tokenProto);
    const auto peer = NKikimr::NAddressClassifier::ExtractAddress(request.GetPeerName());
    auto values = [](const auto& changes) {
        TStringBuilder result;
        for (const auto& value : changes) {
            if (!result.empty()) {
                result << ", ";
            }
            result << value.GetName() << '='
                << (value.HasValue() ? (value.GetValue() ? "true" : "false") : "INHERIT");
        }
        return TString(result);
    };
    AUDIT_LOG(
        AUDIT_PART("component", COMPONENT_NAME)
        AUDIT_PART("operation", "ALTER DATABASE FEATURE FLAGS")
        AUDIT_PART("remote_address", peer.empty() ? EMPTY_VALUE : peer)
        AUDIT_PART("subject", token.GetUserSID().empty() ? EMPTY_VALUE : token.GetUserSID())
        AUDIT_PART("sanitized_token", token.GetSanitizedToken().empty() ? EMPTY_VALUE : token.GetSanitizedToken())
        AUDIT_PART("database", request.GetDatabase())
        AUDIT_PART("request_id", request.GetRequestId(), !request.GetRequestId().empty())
        AUDIT_PART("changes", values(request.GetChanges()))
        AUDIT_PART("old_overrides", values(change.GetOldOverrides()))
        AUDIT_PART("new_overrides", values(change.GetNewOverrides()))
        AUDIT_PART("config_version", ToString(change.GetVersion()), change.HasVersion())
        AUDIT_PART("no_op", values(change.GetOldOverrides()) == values(change.GetNewOverrides()) ? "true" : "false",
            status == Ydb::StatusIds::SUCCESS)
        AUDIT_PART("status", status == Ydb::StatusIds::SUCCESS ? "SUCCESS" : "ERROR")
        AUDIT_PART("detailed_status", Ydb::StatusIds::StatusCode_Name(status))
        AUDIT_PART("reason", reason, !reason.empty())
    );
}

void AuditLogReplaceConfigTransaction(
    const TString& peer,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& oldConfig,
    const TString& newConfig,
    const TString& reason,
    bool success)
{
    auto peerName = NKikimr::NAddressClassifier::ExtractAddress(peer);

    AUDIT_LOG(
        AUDIT_PART("component", COMPONENT_NAME)
        AUDIT_PART("remote_address", (!peerName.empty() ? peerName : EMPTY_VALUE))
        AUDIT_PART("subject", (!userSID.empty() ? userSID : EMPTY_VALUE))
        AUDIT_PART("sanitized_token", (!sanitizedToken.empty() ? sanitizedToken : EMPTY_VALUE))
        AUDIT_PART("status", success ? "SUCCESS" : "ERROR")
        AUDIT_PART("reason", reason, !reason.empty())
        AUDIT_PART("operation", "REPLACE DYNCONFIG")
        AUDIT_PART("old_config", oldConfig)
        AUDIT_PART("new_config", newConfig)
    );
}

void AuditLogReplaceDatabaseConfigTransaction(
    const TString& peer,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& database,
    const TString& oldConfig,
    const TString& newConfig,
    const TString& reason,
    bool success)
{
    auto peerName = NKikimr::NAddressClassifier::ExtractAddress(peer);

    AUDIT_LOG(
        AUDIT_PART("component", COMPONENT_NAME)
        AUDIT_PART("remote_address", (!peerName.empty() ? peerName : EMPTY_VALUE))
        AUDIT_PART("subject", (!userSID.empty() ? userSID : EMPTY_VALUE))
        AUDIT_PART("sanitized_token", (!sanitizedToken.empty() ? sanitizedToken : EMPTY_VALUE))
        AUDIT_PART("database", (!database.empty() ? database : EMPTY_VALUE))
        AUDIT_PART("status", success ? "SUCCESS" : "ERROR")
        AUDIT_PART("reason", reason, !reason.empty())
        AUDIT_PART("operation", "REPLACE DATABASE CONFIG")
        AUDIT_PART("old_config", oldConfig)
        AUDIT_PART("new_config", newConfig)
    );
}

void AuditLogBeginConfigureDatabase(
    const TString& peer,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& database)
{
    auto peerName = NKikimr::NAddressClassifier::ExtractAddress(peer);

    AUDIT_LOG(
        AUDIT_PART("component", COMPONENT_NAME)
        AUDIT_PART("remote_address", (!peerName.empty() ? peerName : EMPTY_VALUE))
        AUDIT_PART("subject", (!userSID.empty() ? userSID : EMPTY_VALUE))
        AUDIT_PART("sanitized_token", (!sanitizedToken.empty() ? sanitizedToken : EMPTY_VALUE))
        AUDIT_PART("database", database)
        AUDIT_PART("status", "SUCCESS")
        AUDIT_PART("operation", "BEGIN INIT DATABASE CONFIG")
    );
}

void AuditLogEndConfigureDatabase(
    const TString& peer,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& database,
    const TString& reason,
    bool success)
{
    auto peerName = NKikimr::NAddressClassifier::ExtractAddress(peer);

    AUDIT_LOG(
        AUDIT_PART("component", COMPONENT_NAME)
        AUDIT_PART("remote_address", (!peerName.empty() ? peerName : EMPTY_VALUE))
        AUDIT_PART("subject", (!userSID.empty() ? userSID : EMPTY_VALUE))
        AUDIT_PART("sanitized_token", (!sanitizedToken.empty() ? sanitizedToken : EMPTY_VALUE))
        AUDIT_PART("database", database)
        AUDIT_PART("status", success ? "SUCCESS" : "ERROR")
        AUDIT_PART("reason", reason, !reason.empty())
        AUDIT_PART("operation", "END INIT DATABASE CONFIG")
    );
}

void AuditLogBeginRemoveDatabase(
    const TString& peer,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& database)
{
    auto peerName = NKikimr::NAddressClassifier::ExtractAddress(peer);

    AUDIT_LOG(
        AUDIT_PART("component", COMPONENT_NAME)
        AUDIT_PART("remote_address", (!peerName.empty() ? peerName : EMPTY_VALUE))
        AUDIT_PART("subject", (!userSID.empty() ? userSID : EMPTY_VALUE))
        AUDIT_PART("sanitized_token", (!sanitizedToken.empty() ? sanitizedToken : EMPTY_VALUE))
        AUDIT_PART("database", database)
        AUDIT_PART("status", "SUCCESS")
        AUDIT_PART("operation", "BEGIN REMOVE DATABASE")
    );
}

void AuditLogEndRemoveDatabase(
    const TString& peer,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& database,
    const TString& reason,
    bool success)
{
    auto peerName = NKikimr::NAddressClassifier::ExtractAddress(peer);

    AUDIT_LOG(
        AUDIT_PART("component", COMPONENT_NAME)
        AUDIT_PART("remote_address", (!peerName.empty() ? peerName : EMPTY_VALUE))
        AUDIT_PART("subject", (!userSID.empty() ? userSID : EMPTY_VALUE))
        AUDIT_PART("sanitized_token", (!sanitizedToken.empty() ? sanitizedToken : EMPTY_VALUE))
        AUDIT_PART("database", database)
        AUDIT_PART("status", success ? "SUCCESS" : "ERROR")
        AUDIT_PART("reason", reason, !reason.empty())
        AUDIT_PART("operation", "END REMOVE DATABASE")
    );
}

} // namespace NKikimr::NConsole
