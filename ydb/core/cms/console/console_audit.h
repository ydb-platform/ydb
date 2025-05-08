#pragma once

#include <util/generic/string.h>

namespace NKikimr::NConsole {

void AuditLogReplaceConfigTransaction(
    const TString& peer,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& oldConfig,
    const TString& newConfig,
    const TString& reason,
    bool success);

void AuditLogReplaceDatabaseConfigTransaction(
    const TString& peer,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& database,
    const TString& oldConfig,
    const TString& newConfig,
    const TString& reason,
    bool success);

void AuditLogBeginConfigureDatabase(
    const TString& peer,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& database);

void AuditLogEndConfigureDatabase(
    const TString& peer,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& database,
    const TString& reason,
    bool success);

void AuditLogBeginRemoveDatabase(
    const TString& peer,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& database);

void AuditLogEndRemoveDatabase(
    const TString& peer,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& database,
    const TString& reason,
    bool success);

} // namespace NKikimr::NConsole
