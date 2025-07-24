#include "schemeshard__backup_collection_common.h"
#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_create_cdc_stream.h"
#include "schemeshard_impl.h"


namespace NKikimr::NSchemeShard {

using TTag = TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpBackupIncrementalBackupCollection>;

namespace NOperation {

template <>
std::optional<THashMap<TString, THashSet<TString>>> GetRequiredPaths<TTag>(
    TTag,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    const auto& backupOp = tx.GetBackupIncrementalBackupCollection();
    return NBackup::GetBackupRequiredPaths(tx, backupOp.GetTargetDir(), backupOp.GetName(), context);
}

template <>
bool Rewrite(TTag, TTxTransaction& tx) {
    auto now = NBackup::ToX509String(TlsActivationContext->AsActorContext().Now());
    tx.MutableBackupIncrementalBackupCollection()->SetTargetDir(now + "_incremental");
    return true;
}

} // namespace NOperation

TVector<ISubOperation::TPtr> CreateBackupIncrementalBackupCollection(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;

    TString bcPathStr = JoinPath({tx.GetWorkingDir(), tx.GetBackupIncrementalBackupCollection().GetName()});

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
    bool incrBackupEnabled = bc->Description.HasIncrementalBackupConfig();

    if (!incrBackupEnabled) {
        return {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, "Incremental backup is disabled on this collection")};
    }

    for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
        std::pair<TString, TString> paths;
        TString err;
        if (!TrySplitPathByDb(item.GetPath(), bcPath.GetDomainPathString(), paths, err)) {
            result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, err)};
            return {};
        }
        auto& relativeItemPath = paths.second;

        NKikimrSchemeOp::TModifyScheme modifyScheme;
        modifyScheme.SetWorkingDir(tx.GetWorkingDir());
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterContinuousBackup);
        modifyScheme.SetInternal(true);
        auto& cb = *modifyScheme.MutableAlterContinuousBackup();
        cb.SetTableName(relativeItemPath);
        auto& ib = *cb.MutableTakeIncrementalBackup();
        ib.SetDstPath(JoinPath({tx.GetBackupIncrementalBackupCollection().GetName(), tx.GetBackupIncrementalBackupCollection().GetTargetDir(), relativeItemPath}));

        if (!CreateAlterContinuousBackup(opId, modifyScheme, context, result)) {
            return result;
        }
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
