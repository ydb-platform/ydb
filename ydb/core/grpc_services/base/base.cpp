#include "base.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>

namespace NKikimr::NGRpcService {

IRequestProxyCtx::IRequestProxyCtx(const TAppData* appData) {
    if (!appData && HasAppData()) {
        appData = AppData();
    }
    InitRootPath(appData);
}

void IRequestProxyCtx::InitRootPath(const TAppData* appData) {
    if (appData && appData->DomainsInfo && appData->DomainsInfo->Domain) {
        RootPath = "/" + appData->DomainsInfo->Domain->Name;
        RelativePathsEnabled_ = appData->FeatureFlags.GetEnableRelativePaths();
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

const TMaybe<TString> IRequestProxyCtx::GetDatabaseName() const {
    if (DatabaseName) {
        return DatabaseName;
    }
    const auto database = GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER);
    return ResolveDatabaseName(database ? TMaybe<TString>(CGIUnescapeRet(*database)) : Nothing());
}

TMaybe<TString> IRequestProxyCtx::ResolveDatabaseName(const TMaybe<TString>& database) const {
    if (!DatabaseName && database && !database->empty()) {
        DatabaseName = RelativePathsEnabled_ ? PrependDomainIfNeeded(RootPath, *database) : *database;
    }
    return DatabaseName ? DatabaseName : database;
}

TString IAuditCtx::GetDatabaseRelativePath(TStringBuf path) const {
    return AppData()->FeatureFlags.GetEnableRelativePaths()
        ? ResolvePathToDatabase(GetDatabaseName().GetOrElse(TString()), path)
        : TString(path);
}

} // namespace NKikimr::NGRpcService
