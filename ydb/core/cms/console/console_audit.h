#pragma once

#include <util/generic/string.h>

namespace NKikimr::NConsole {

void AuditLogReplaceConfigTransaction(
    const TString& peer,
    const TString& userSID,
    const TString& oldConfig,
    const TString& newConfig,
    const TString& reason,
    bool success);

} // namespace NKikimr::NConsole
