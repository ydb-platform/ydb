#include "console_audit.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/util/address_classifier.h>

namespace NKikimr::NConsole {

static const TString COMPONENT_NAME = "console";
static const TString EMPTY_VALUE = "{none}";

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
        AUDIT_PART("status", TString(success ? "SUCCESS" : "ERROR"))
        AUDIT_PART("reason", reason, !reason.empty())
        AUDIT_PART("operation", TString("REPLACE DYNCONFIG"))
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
        AUDIT_PART("status", TString(success ? "SUCCESS" : "ERROR"))
        AUDIT_PART("reason", reason, !reason.empty())
        AUDIT_PART("operation", TString("REPLACE DATABASE CONFIG"))
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
        AUDIT_PART("status", TString("SUCCESS"))
        AUDIT_PART("operation", TString("BEGIN INIT DATABASE CONFIG"))
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
        AUDIT_PART("status", TString(success ? "SUCCESS" : "ERROR"))
        AUDIT_PART("reason", reason, !reason.empty())
        AUDIT_PART("operation", TString("END INIT DATABASE CONFIG"))
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
        AUDIT_PART("status", TString("SUCCESS"))
        AUDIT_PART("operation", TString("BEGIN REMOVE DATABASE"))
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
        AUDIT_PART("status", TString(success ? "SUCCESS" : "ERROR"))
        AUDIT_PART("reason", reason, !reason.empty())
        AUDIT_PART("operation", TString("END REMOVE DATABASE"))
    );
}

} // namespace NKikimr::NConsole
