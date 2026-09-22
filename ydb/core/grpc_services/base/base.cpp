#include "base.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>

namespace NKikimr::NGRpcService {

void IRequestProxyCtx::InitRequestPaths(const TString& clusterRoot, bool relativePathsEnabled, NMonitoring::TDynamicCounterPtr counters) {
    // Called by the proxy before dispatch. Deferred requests keep their original settings.
    if (!PathsInitialized_.load(std::memory_order_acquire)) {
        ClusterRoot_ = clusterRoot;
        RelativePathsEnabled_ = relativePathsEnabled;
        PathsInitialized_.store(true, std::memory_order_release);
    }
    InitPathCounters(std::move(counters));
}

void IRequestProxyCtx::InitPathCounters(NMonitoring::TDynamicCounterPtr counters) {
    const auto method = GetRpcMethodName();
    if (RelativePathCounter_ || !counters || method.empty()) {
        return;
    }
    RelativePathCounter_ = GetServiceCounters(counters, "grpc")->GetSubgroup("method", method)
        ->GetNamedCounter("name", "api.grpc.request.relative_path_count", true);
    if (const auto database = GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER)) {
        CountRequestPath(CGIUnescapeRet(*database));
    }
    CountRequestBodyPaths();
}

void IRequestProxyCtx::CountRequestPath(TStringBuf path) const {
    // Count raw input once per RPC, independently of EnableRelativePaths.
    if (RelativePathCounter_ && !path.empty() && !path.StartsWith('/')
        && !RelativePathCounted_.exchange(true, std::memory_order_relaxed)) {
        RelativePathCounter_->Inc();
    }
}

const TMaybe<TString> IRequestProxyCtx::GetDatabaseName() const {
    const auto database = GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER);
    return ResolveDatabaseName(database ? TMaybe<TString>(CGIUnescapeRet(*database)) : Nothing());
}

TMaybe<TString> IRequestProxyCtx::ResolveDatabaseName(const TMaybe<TString>& database) const {
    // Preserve missing/empty headers and calls made before the proxy initializes the request.
    if (!database || database->empty() || !PathsInitialized_.load(std::memory_order_acquire) || !RelativePathsEnabled_) {
        return database;
    }
    std::call_once(DatabaseNameOnce_, [&] {
        ResolvedDatabaseName_ = PrependClusterRootIfNeeded(ClusterRoot_, *database);
    });
    return ResolvedDatabaseName_;
}

TString IAuditCtx::GetDatabaseRelativePath(TStringBuf path) const {
    return AppData()->FeatureFlags.GetEnableRelativePaths()
        ? ResolvePathToDatabase(GetDatabaseName().GetOrElse(TString()), path)
        : TString(path);
}

} // namespace NKikimr::NGRpcService
