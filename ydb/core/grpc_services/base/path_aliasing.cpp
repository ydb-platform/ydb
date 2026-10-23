#include "base.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/database_kind.h>
#include <ydb/core/base/path.h>
#include <ydb/core/path_aliasing/path_normalizer.h>

#include <utility>

namespace NKikimr::NGRpcService {

    void IRequestCtxBaseMtSafe::EnablePathNormalization() noexcept {
        PathNormalizationEnabled_ = true;
    }

    void IRequestCtxBaseMtSafe::DisablePathNormalization() noexcept {
        PathNormalizationEnabled_ = false;
        PathNormalizer_.reset();
    }

    bool IRequestCtxBaseMtSafe::IsPathNormalizationEnabled() const noexcept {
        return PathNormalizationEnabled_;
    }

    bool IRequestCtxBaseMtSafe::CanInitializePathAliasCounter() const noexcept {
        return PathNormalizationEnabled_ && PathNormalizer_ && !PathAliasCounter_;
    }

    void IRequestCtxBaseMtSafe::SetPathNormalizer(
        std::shared_ptr<const NPathAliasing::TPathNormalizer> normalizer) noexcept {
        PathNormalizer_ = std::move(normalizer);
    }

    void IRequestCtxBaseMtSafe::SetPathAliasCounter(NMonitoring::TDynamicCounters::TCounterPtr counter) {
        PathAliasCounter_ = std::move(counter);
        RecordPathAliasUsage(false);
    }

    TString IRequestCtxBaseMtSafe::NormalizePath(TStringBuf path) const {
        TString normalized = PathNormalizer_ ? PathNormalizer_->NormalizePath(path) : TString(path);
        RecordPathAliasUsage(normalized != path);
        return normalized;
    }

    void IRequestCtxBaseMtSafe::RecordPathAliasUsage(bool used) const {
        PathAliasUsed_ |= used;
        if (PathAliasUsed_ && PathAliasCounter_ && !PathAliasCounted_) {
            PathAliasCounter_->Inc();
            PathAliasCounted_ = true;
        }
    }

    void IRequestProxyCtx::InitializePathNormalization(
        std::shared_ptr<const NPathAliasing::TPathNormalizer> normalizer)
    {
        if (PathNormalizationInitialized_ || !IsPathNormalizationEnabled()) {
            return;
        }

        const TString method = GetRpcMethodName();
        if (method.StartsWith("Ydb.PersQueue.V1.") ||
            method.StartsWith("Ydb.Cms.V1.CmsService/")) {
            DisablePathNormalization();
            return;
        }

        auto database = GetDatabaseNameFromRequest();
        SetPathNormalizer(std::move(normalizer));
        if (database) {
            database = NormalizePath(*database);
        }

        EffectiveDatabaseName_ = std::move(database);
        PathNormalizationInitialized_ = true;
    }

    void IRequestProxyCtx::InitializePathAliasCounter(NMonitoring::TDynamicCounterPtr counters,
        const NKikimrScheme::TEvDescribeSchemeResult& database)
    {
        if (!PathNormalizationInitialized_ || !CanInitializePathAliasCounter() || !counters
            || database.GetStatus() != NKikimrScheme::StatusSuccess || !IsDatabase(database)) {
            return;
        }
        const auto effectiveDatabase = GetDatabaseName();
        if (effectiveDatabase && CanonizePath(*effectiveDatabase) != CanonizePath(database.GetPath())) {
            return;
        }
        const TString method = GetRpcMethodName();
        const TStringBuf rpcMethod(method);
        SetPathAliasCounter(GetServiceCounters(counters, "ydb")
            ->GetSubgroup("api_service", TString(rpcMethod.Before('/')))
            ->GetSubgroup("method", TString(rpcMethod.After('/')))
            ->GetSubgroup("database", CanonizePath(GetDatabaseNameFromRequest().GetOrElse("")))
            ->GetNamedCounter("name", "api.grpc.request.path_alias_count", true));
    }

} // namespace NKikimr::NGRpcService
