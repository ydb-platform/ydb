#include "schemeshard__backup_collection_common.h"

namespace NKikimr::NSchemeShard::NBackup {

std::optional<NBackup::TBackupCollectionPaths> ResolveBackupCollectionPaths(
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

    // Validate the collection name
    if (name.empty()) {
        result->SetError(NKikimrScheme::EStatus::StatusInvalidParameter, "Backup collection name cannot be empty");
        return std::nullopt;
    }

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

    return NBackup::TBackupCollectionPaths{rootPath, dstPath};
}

std::optional<THashMap<TString, THashSet<TString>>> GetBackupRequiredPaths(
    const TTxTransaction& tx,
    const TString& targetDir,
    const TString& targetName,
    const TOperationContext& context)
{
    THashMap<TString, THashSet<TString>> paths;

    const TString& targetPath = JoinPath({tx.GetWorkingDir(), targetName});

    const TPath& bcPath = TPath::Resolve(targetPath, context.SS);
    {
        auto checks = bcPath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsBackupCollection();

        if (!checks) {
            return {};
        }
    }

    Y_ABORT_UNLESS(context.SS->BackupCollections.contains(bcPath->PathId));
    const auto& bc = context.SS->BackupCollections[bcPath->PathId];

    auto& collectionPaths = paths[targetPath];
    collectionPaths.emplace(targetDir);

    for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
        std::pair<TString, TString> paths;
        TString err;
        if (!TrySplitPathByDb(item.GetPath(), tx.GetWorkingDir(), paths, err)) {
            return {};
        }

        auto pathPieces = SplitPath(paths.second);
        if (pathPieces.size() > 1) {
            auto parent = ExtractParent(paths.second);
            collectionPaths.emplace(JoinPath({targetDir, TString(parent)}));
        }
    }

    return paths;
}

std::optional<THashMap<TString, THashSet<TString>>> GetRestoreRequiredPaths(
    const TTxTransaction& tx,
    const TString& targetName,
    const TOperationContext& context)
{
    THashMap<TString, THashSet<TString>> paths;

    const TString& targetPath = JoinPath({tx.GetWorkingDir(), targetName});

    const TPath& bcPath = TPath::Resolve(targetPath, context.SS);
    {
        auto checks = bcPath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsBackupCollection();

        if (!checks) {
            return {};
        }
    }

    Y_ABORT_UNLESS(context.SS->BackupCollections.contains(bcPath->PathId));
    const auto& bc = context.SS->BackupCollections[bcPath->PathId];

    auto& collectionPaths = paths[tx.GetWorkingDir()];

    for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
        std::pair<TString, TString> paths;
        TString err;
        if (!TrySplitPathByDb(item.GetPath(), tx.GetWorkingDir(), paths, err)) {
            return {};
        }

        auto pathPieces = SplitPath(paths.second);
        if (pathPieces.size() > 1) {
            auto parent = ExtractParent(paths.second);
            collectionPaths.emplace(TString(parent));
        }
    }

    return paths;
}

TString ToX509String(const TInstant& datetime) {
    return datetime.FormatLocalTime("%Y%m%d%H%M%SZ");
}

}  // namespace NKikimr::NSchemeShard::NBackup
