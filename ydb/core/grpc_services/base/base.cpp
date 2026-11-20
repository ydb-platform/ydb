#include "base.h"

#include <util/string/cast.h>

namespace NKikimr::NGRpcService {

void AddAuditLogReason(TAuditLogParts& parts, Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
    if (status == Ydb::StatusIds::SUCCESS) {
        return;
    }
    TString* reason = nullptr;
    for (auto& [name, value] : parts) {
        if (name == "reason") {
            if (!value.empty()) {
                return;
            }
            reason = &value;
            break;
        }
    }
    TString message = issues.ToOneLineString();
    if (message.empty()) {
        message = Ydb::StatusIds::StatusCode_IsValid(status)
            ? TString(Ydb::StatusIds::StatusCode_Name(status))
            : ToString(static_cast<int>(status));
    }
    if (reason) {
        *reason = std::move(message);
    } else {
        parts.emplace_back("reason", std::move(message));
    }
}

void IRequestProxyCtx::CountRequestPaths() const {
    if (const auto database = GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER)) {
        CountDatabasePath(CGIUnescapeRet(*database));
    }
    CountRequestBodyPaths();
}

void IRequestProxyCtx::CountDatabasePath(TStringBuf path) const {
    if (!RelativeDatabaseCounted_ && !path.empty() && !path.StartsWith('/')) {
        if (auto* counters = GetRequestCounters()) {
            counters->CountRelativeDatabase();
            RelativeDatabaseCounted_ = true;
        }
    }
}

void IRequestProxyCtx::CountResourcePath(TStringBuf path) const {
    if (!RelativeResourceCounted_ && !path.empty() && !path.StartsWith('/')) {
        if (auto* counters = GetRequestCounters()) {
            counters->CountRelativeResource();
            RelativeResourceCounted_ = true;
        }
    }
}

} // namespace NKikimr::NGRpcService
