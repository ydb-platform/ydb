#include "base.h"

#include <ydb/core/base/counters.h>
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

    void IRequestCtxBaseMtSafe::SetPathNormalizer(
        std::shared_ptr<const NPathAliasing::TPathNormalizer> normalizer,
        NMonitoring::TDynamicCounters::TCounterPtr pathAliasCounter) noexcept {
        PathNormalizer_ = std::move(normalizer);
        PathAliasCounter_ = std::move(pathAliasCounter);
    }

    TString IRequestCtxBaseMtSafe::NormalizePath(TStringBuf path) const {
        TString normalized = PathNormalizer_ ? PathNormalizer_->NormalizePath(path) : TString(path);
        if (normalized != path && PathAliasCounter_ && !PathAliasCounted_) {
            PathAliasCounter_->Inc();
            PathAliasCounted_ = true;
        }
        return normalized;
    }

    void IRequestProxyCtx::InitializePathNormalization(
        std::shared_ptr<const NPathAliasing::TPathNormalizer> normalizer,
        NMonitoring::TDynamicCounterPtr counters)
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

        NMonitoring::TDynamicCounters::TCounterPtr pathAliasCounter;
        if (normalizer && counters) {
            const TStringBuf rpcMethod(method);
            pathAliasCounter = GetServiceCounters(counters, "ydb")
                ->GetSubgroup("api_service", TString(rpcMethod.Before('/')))
                ->GetSubgroup("method", TString(rpcMethod.After('/')))
                ->GetNamedCounter("name", "api.grpc.request.path_alias_count", true);
        }

        auto database = GetDatabaseNameFromRequest();
        SetPathNormalizer(std::move(normalizer), std::move(pathAliasCounter));
        if (database) {
            database = NormalizePath(*database);
        }

        EffectiveDatabaseName_ = std::move(database);
        PathNormalizationInitialized_ = true;
    }

} // namespace NKikimr::NGRpcService
