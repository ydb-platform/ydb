#pragma once

#include <util/generic/string.h>

namespace NKikimr::NStorage {

void AuditLogReplaceConfig(
    const TString& peer,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& oldConfig,
    const TString& newConfig,
    const TString& reason,
    bool success);

} // namespace NKikimr::NStorage
