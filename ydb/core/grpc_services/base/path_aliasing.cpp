#include "iface.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr::NGRpcService {

    TPathRewriteSettings TPathRewriteSettings::UserInput() {
        TPathRewriteSettings settings;
        settings.Database = EPathInputOrigin::Logical;
        settings.Resources = EPathInputOrigin::Logical;
        return settings;
    }

    TPathRewriteSettings TPathRewriteSettings::Internal() {
        TPathRewriteSettings settings;
        settings.Database = EPathInputOrigin::Resolved;
        settings.Resources = EPathInputOrigin::Resolved;
        return settings;
    }

    void IRequestCtxBaseMtSafe::SetPathRewriteSettings(TPathRewriteSettings settings) {
        PathRewrite_ = std::move(settings);
        PathRewriteInitialized_ = bool(PathRewrite_.Context);
    }

    const TPathRewriteSettings& IRequestCtxBaseMtSafe::GetPathRewriteSettings() const noexcept {
        return PathRewrite_;
    }

    TString IRequestCtxBaseMtSafe::InitializePathRewriteContext(const TAppData& appData) {
        if (!PathRewrite_.Context && PathRewrite_.Database != EPathInputOrigin::Logical &&
            PathRewrite_.Resources == EPathInputOrigin::Logical &&
            appData.PathNormalizer && !appData.PathNormalizer->Empty()) {
            return "Forwarded logical paths require their original database context";
        }
        if (!PathRewriteInitialized_) {
            PathRewriteInitialized_ = true;
            if (PathRewrite_.Database == EPathInputOrigin::Logical &&
                appData.PathNormalizer && !appData.PathNormalizer->Empty()) {
                PathRewrite_.Context = std::make_shared<NPathAliasing::TPathContext>(
                    *appData.PathNormalizer, GetDatabaseName());
            }
        }
        return PathRewrite_.Context ? PathRewrite_.Context->GetError() : TString{};
    }

    bool IRequestCtxBaseMtSafe::HasActivePathRewriting() const noexcept {
        return PathRewrite_.Resources == EPathInputOrigin::Logical &&
               PathRewrite_.Context && !PathRewrite_.Context->Empty();
    }

    TMaybe<TString> IRequestCtxBaseMtSafe::GetLogicalDatabaseName() const {
        return PathRewrite_.Context ? PathRewrite_.Context->GetLogicalDatabase() : GetDatabaseName();
    }

    TString IRequestCtxBaseMtSafe::GetPathRewriteFingerprint() const {
        return PathRewrite_.Context ? PathRewrite_.Context->GetFingerprint() : TString{};
    }

    TMaybe<TString> IRequestCtxBaseMtSafe::GetPathResolvedDatabase(TMaybe<TString> originalDatabase) const {
        return PathRewrite_.Database == EPathInputOrigin::Logical && PathRewrite_.Context
                   ? PathRewrite_.Context->GetDatabase()
                   : std::move(originalDatabase);
    }

    TConclusion<NPathAliasing::TResolvedSchemaPath> IRequestCtxBaseMtSafe::NormalizePath(
        const TString& completeLogicalCandidate) const {
        if (HasActivePathRewriting()) {
            return PathRewrite_.Context->NormalizePath(completeLogicalCandidate);
        }
        return NPathAliasing::TResolvedSchemaPath{
            completeLogicalCandidate, NPathAliasing::EPathRewriteOutcome::NoMatch};
    }

} // namespace NKikimr::NGRpcService
