#include "distconf_audit.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/util/address_classifier.h>

namespace NKikimr::NStorage {

static const TString COMPONENT_NAME = "distconf";
static const TString EMPTY_VALUE = "{none}";

void AuditLogReplaceConfig(
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
        AUDIT_PART("operation", TString("REPLACE CONFIG"))
        AUDIT_PART("old_config", oldConfig)
        AUDIT_PART("new_config", newConfig)
    );
}

} // namespace NKikimr::NStorage
