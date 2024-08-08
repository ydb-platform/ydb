#include "console_audit.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/util/address_classifier.h>

namespace NKikimr::NConsole {

void AuditLogReplaceConfigTransaction(
    const TString& peer,
    const TString& userSID,
    const TString& oldConfig,
    const TString& newConfig,
    const TString& reason,
    bool success)
{
    static const TString COMPONENT_NAME = "console";

    static const TString EMPTY_VALUE = "{none}";

    auto peerName = NKikimr::NAddressClassifier::ExtractAddress(peer);

    AUDIT_LOG(
        AUDIT_PART("component", COMPONENT_NAME)
        AUDIT_PART("remote_address", (!peerName.empty() ? peerName : EMPTY_VALUE))
        AUDIT_PART("subject", (!userSID.empty() ? userSID : EMPTY_VALUE))
        AUDIT_PART("status", TString(success ? "SUCCESS" : "ERROR"))
        AUDIT_PART("reason", reason, !reason.empty())
        AUDIT_PART("operation", TString("replace"))
        AUDIT_PART("old_config", oldConfig)
        AUDIT_PART("new_config", newConfig)
    );
}

} // namespace NKikimr::NConsole
