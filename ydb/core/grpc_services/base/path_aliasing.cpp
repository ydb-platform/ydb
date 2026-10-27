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

    TString IAuditCtx::GetDatabaseRelativePath(TStringBuf path, TStringBuf base) const {
        if (!AppData()->FeatureFlags.GetEnableRelativePaths()) {
            return NormalizePath(path);
        }
        auto database = GetDatabaseName().GetOrElse(TString());
        if (const auto header = GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER)) {
            database = PrependDomainIfNeeded("/" + AppData()->DomainsInfo->Domain->Name, CGIUnescapeRet(*header));
        }
        return NormalizePath(PrependDomainIfNeeded(
            base.empty() ? database : PrependDomainIfNeeded(database, base), path));
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
        std::shared_ptr<const NPathAliasing::TPathNormalizer> normalizer)
    {
        if (PathNormalizationInitialized_) {
            return;
        }

        auto database = GetDatabaseNameFromRequest();
        const auto* appData = HasAppData() ? AppData() : nullptr;
        const bool relativeDatabase = database && !database->empty() && !database->StartsWith('/')
            && appData && appData->FeatureFlags.GetEnableRelativePaths()
            && appData->DomainsInfo && appData->DomainsInfo->Domain;

        const TString method = GetRpcMethodName();
        if (method.StartsWith("Ydb.PersQueue.V1.") ||
            method.StartsWith("Ydb.Cms.V1.CmsService/")) {
            DisablePathNormalization();
        }
        if (!IsPathNormalizationEnabled() && !relativeDatabase) {
            return;
        }

        SetPathNormalizer(IsPathNormalizationEnabled() ? std::move(normalizer) : nullptr);
        if (database) {
            if (relativeDatabase) {
                database = PrependDomainIfNeeded("/" + appData->DomainsInfo->Domain->Name, *database);
            }
            database = NormalizePath(*database);
        }

        EffectiveDatabaseName_ = std::move(database);
        PathNormalizationInitialized_ = true;
    }

} // namespace NKikimr::NGRpcService
