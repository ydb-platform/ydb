#include "console_audit.h"

#include <ydb/library/yaml_config/yaml_config_helpers.h>
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
        AUDIT_PART("status", success ? "SUCCESS" : "ERROR")
        AUDIT_PART("reason", reason, !reason.empty())
        AUDIT_PART("operation", "REPLACE DYNCONFIG")
        AUDIT_PART("old_config", NYaml::ConfigTextForDiagnostics(oldConfig))
        AUDIT_PART("new_config", NYaml::ConfigTextForDiagnostics(newConfig))
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
        AUDIT_PART("old_config", NYaml::ConfigTextForDiagnostics(oldConfig))
        AUDIT_PART("new_config", NYaml::ConfigTextForDiagnostics(newConfig))
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
