#include "base.h"

#include <ydb/core/base/appdata.h>
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

    void IRequestCtxBaseMtSafe::SetPathNormalizer(
        std::shared_ptr<const NPathAliasing::TPathNormalizer> normalizer) noexcept {
        PathNormalizer_ = std::move(normalizer);
    }

    TString IRequestCtxBaseMtSafe::NormalizePath(TStringBuf path) const {
        return PathNormalizer_ ? PathNormalizer_->NormalizePath(path) : TString(path);
    }

    void IRequestProxyCtx::InitializePathNormalization(
        std::shared_ptr<const NPathAliasing::TPathNormalizer> normalizer, const TAppData* appData)
    {
        if (PathNormalizationInitialized_) {
            return;
        }

        if (IsPathNormalizationEnabled()) {
            const TString method = GetRpcMethodName();
            if (method.StartsWith("Ydb.PersQueue.V1.") ||
                method.StartsWith("Ydb.Cms.V1.CmsService/")) {
                DisablePathNormalization();
            } else {
                SetPathNormalizer(std::move(normalizer));
            }
        }

        auto database = EffectiveDatabaseName_ ? EffectiveDatabaseName_ : GetDatabaseNameFromRequest();
        if (database) {
            if (!EffectiveDatabaseName_ && appData && appData->FeatureFlags.GetEnableRelativePaths()
                && appData->DomainsInfo && appData->DomainsInfo->Domain) {
                database = PrependDomainIfNeeded("/" + appData->DomainsInfo->Domain->Name, *database);
            }
            database = NormalizePath(*database);
        }

        EffectiveDatabaseName_ = std::move(database);
        PathNormalizationInitialized_ = true;
    }

} // namespace NKikimr::NGRpcService
