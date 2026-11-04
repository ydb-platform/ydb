#pragma once

#include <util/generic/string.h>

#include <ydb/core/protos/console.pb.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NConsole {

void AuditLogPatchDatabaseConfig(
    const NKikimrConsole::TPatchDatabaseConfigRequest& request,
    const NKikimrConsole::TDatabaseFeatureFlagsChange& change,
    Ydb::StatusIds::StatusCode status,
    const TString& reason);

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
