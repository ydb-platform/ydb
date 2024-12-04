#include "schemeshard__operation_common.h"

namespace NKikimr::NSchemeShard {

// TODO: add required paths

TVector<ISubOperation::TPtr> CreateRestoreBackupCollection(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;

    TString bcPathStr = JoinPath({tx.GetWorkingDir(), tx.GetRestoreBackupCollection().GetName()});

    const TPath& bcPath = TPath::Resolve(bcPathStr, context.SS);
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
            result = {CreateReject(opId, checks.GetStatus(), checks.GetError())};
            return result;
        }
    }

    Y_ABORT_UNLESS(context.SS->BackupCollections.contains(bcPath->PathId));
    const auto& bc = context.SS->BackupCollections[bcPath->PathId];

    TString lastFullBackupName;
    TVector<TString> incBackupNames;

    if (!bcPath.Base()->GetChildren().size()) {
        return {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, TStringBuilder() << "Nothing to restore")};
    } else {
        for (auto& [child, _] : bcPath.Base()->GetChildren()) {
            if (child.EndsWith("_full")) {
                lastFullBackupName = child;
                incBackupNames.clear();
            } else if (child.EndsWith("_incremental")) {
                incBackupNames.push_back(child);
            }
        }
    }

    NKikimrSchemeOp::TModifyScheme consistentCopyTables;
    consistentCopyTables.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables);
    consistentCopyTables.SetInternal(true);

    auto& cct = *consistentCopyTables.MutableCreateConsistentCopyTables();
    auto& copyTables = *cct.MutableCopyTableDescriptions();
    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);

    size_t cutLen = bcPath.GetDomainPathString().size() + 1;

    for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
        auto& desc = *copyTables.Add();
        desc.SetSrcPath(bcPath.Child(lastFullBackupName).PathString() + item.GetPath().substr(cutLen - 1, item.GetPath().size() - cutLen + 1));
        desc.SetDstPath(item.GetPath());
    }

    CreateConsistentCopyTables(opId, consistentCopyTables, context, result);

    for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
        NKikimrSchemeOp::TModifyScheme restoreIncrs;
        restoreIncrs.SetOperationType(NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups);
        restoreIncrs.SetInternal(true);

        auto& desc = *restoreIncrs.MutableRestoreMultipleIncrementalBackups();
        for (const auto& incr : incBackupNames) {
            auto path = bcPath.Child(incr).PathString() + item.GetPath().substr(cutLen - 1, item.GetPath().size() - cutLen + 1);
            desc.AddSrcTableNames(path);
        }
        desc.SetDstTablePath(item.GetPath());

        CreateRestoreMultipleIncrementalBackups(opId, restoreIncrs, context, true, result);
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
