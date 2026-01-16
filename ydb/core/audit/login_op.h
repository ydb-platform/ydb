#pragma once

#include <ydb/core/audit/audit_log.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NAudit {

void LogLoginOperationResult(const TString& сomponentName, const TString& peerName, const TString& database,
    const TString& username, const Ydb::StatusIds_StatusCode status, const TString& reason,
    const TString& errorDetails, const TString& sanitizedToken, bool isAdmin = false);

} // namespace NKikimr::NAudit
