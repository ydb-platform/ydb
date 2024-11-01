#include "schemeshard__backup_collection_common.h"

namespace NKikimr::NSchemeShard {

std::optional<TBackupCollectionPaths> ResolveBackupCollectionPaths(
    const TString& rootPathStr,
    const TString& name,
    bool validateFeatureFlag,
    TOperationContext& context,
    THolder<TProposeResponse>& result,
    bool enforceBackupCollectionsDirExists)
{
    bool backupServiceEnabled = AppData()->FeatureFlags.GetEnableBackupService();
    if (!backupServiceEnabled && validateFeatureFlag) {
        result->SetError(NKikimrScheme::StatusPreconditionFailed, "Backup collections are disabled. Please contact your system administrator to enable it");
        return std::nullopt;
    }

    const TPath& rootPath = TPath::Resolve(rootPathStr, context.SS);
    {
        const auto checks = rootPath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .NotUnderDeleting()
            .IsCommonSensePath()
            .IsLikeDirectory()
            .FailOnRestrictedCreateInTempZone();

        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
            return std::nullopt;
        }
    }

    const TString& backupCollectionsDir = JoinPath({rootPath.GetDomainPathString(), ".backups/collections"});

    TPathSplitUnix absPathSplit(name);

    if (absPathSplit.size() > 1 && !absPathSplit.IsAbsolute) {
        result->SetError(NKikimrScheme::EStatus::StatusSchemeError, TStringBuilder() << "Backup collections must be placed directly in " << backupCollectionsDir);
        return std::nullopt;
    }

    const TPath& backupCollectionsPath = TPath::Resolve(backupCollectionsDir, context.SS);
    if (enforceBackupCollectionsDirExists || backupCollectionsPath.IsResolved()) {
        const auto checks = backupCollectionsPath.Check();
        checks.NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .NotUnderDeleting()
            .IsCommonSensePath()
            .IsLikeDirectory();

        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
            return std::nullopt;
        }
    }

    std::optional<TPath> parentPath;
    if (absPathSplit.size() > 1) {
        TString realParent = "/" + JoinRange("/", absPathSplit.begin(), absPathSplit.end() - 1);
        parentPath = TPath::Resolve(realParent, context.SS);
    }

    TPath dstPath = absPathSplit.IsAbsolute && parentPath ? parentPath->Child(TString(absPathSplit.back())) : rootPath.Child(name);
    if (dstPath.PathString() != (backupCollectionsDir + "/" + absPathSplit.back())) {
        result->SetError(NKikimrScheme::EStatus::StatusSchemeError, TStringBuilder() << "Backup collections must be placed in " << backupCollectionsDir);
        return std::nullopt;
    }

    return TBackupCollectionPaths{rootPath, dstPath};
}

}  // namespace NKikimr::NSchemeShard
