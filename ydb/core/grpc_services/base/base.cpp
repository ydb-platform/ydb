#include "base.h"

namespace NKikimr::NGRpcService {

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
