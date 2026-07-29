#include "schemeshard__backup_collection_common.h"
#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_states.h"
#include "schemeshard_impl.h"

#define LOG_D(stream) LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_E(stream) LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

// Control op for BackupBackupCollection: Propose -> Done, no shard talk. It
// can't use a barrier (one per op, already taken by "CopyTableBarrier"), so the
// tracked row is finalized atomically with op completion in
// FinalizeFullBackupOnOpComplete. Concurrent backups of one collection are
// rejected via BCPathToFullBackup, not path-state (see CalcPathState).
class TCreateFullBackupOp : public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Propose:
            return MakeHolder<TEmptyPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId, TPathElement::EPathState::EPathStateNoChanges);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& createOp = Transaction.GetCreateFullBackupOp();

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());

        const ui64 id = static_cast<ui64>(OperationId.GetTxId());
        if (context.SS->FullBackups.contains(id)) {
            result->SetError(NKikimrScheme::StatusAlreadyExists,
                TStringBuilder() << "Full backup with id " << id << " already exists");
            return result;
        }

        const TPath workingDirPath = TPath::Resolve(workingDir, context.SS);
        {
            TPath::TChecker checks = workingDirPath.Check();
            checks
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsBackupCollection();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const TPathId bcPathId = workingDirPath->PathId;

        if (context.SS->BCPathToFullBackup.contains(bcPathId)) {
            const ui64 existingId = context.SS->BCPathToFullBackup.at(bcPathId);
            const auto* existing = context.SS->FullBackups.FindPtr(existingId);
            if (existing && !(*existing)->IsFinished()) {
                result->SetError(NKikimrScheme::StatusMultipleModifications,
                    TStringBuilder() << "Another full backup is already in progress on this collection"
                                     << " (existing id " << existingId << ")");
                return result;
            }
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabNewFullBackupOp(context.SS, id);
        context.MemChanges.GrabNewBCPathToFullBackup(context.SS, bcPathId);

        context.DbChanges.PersistTxState(OperationId);
        context.DbChanges.PersistFullBackupOp(id);

        TFullBackupInfo::TPtr info = new TFullBackupInfo(id, workingDirPath->DomainPathId);
        info->State = TFullBackupInfo::EState::Transferring;
        info->BackupCollectionPathId = bcPathId;
        info->StartTime = TAppData::TimeProvider->Now();
        if (context.UserToken) {
            info->UserSID = context.UserToken->GetUserSID();
        }

        info->ExpectedItemCount = createOp.GetExpectedItemCount();

        context.SS->FullBackups[id] = info;
        context.SS->BCPathToFullBackup[bcPathId] = id;

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateFullBackupOp, bcPathId);
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);

        context.OnComplete.ActivateTx(OperationId);

        // Subscribe to our own completion: the control op shares the operation
        // TxId, so this fires once all CopyTable parts finish (drives Finalize).
        context.OnComplete.Send(context.SS->SelfId(),
            new TEvSchemeShard::TEvNotifyTxCompletion(id));

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateFullBackupOp AbortPropose"
            << ": opId# " << OperationId);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        // Do not use forceDropTxId here - the backup id == OperationId.GetTxId().
        LOG_N("TCreateFullBackupOp AbortUnsafe"
            << ": opId# " << OperationId
            << ", forceDropTxId# " << forceDropTxId);

        auto* infoPtr = context.SS->FullBackups.FindPtr(ui64(OperationId.GetTxId()));
        if (infoPtr && (*infoPtr)->State == TFullBackupInfo::EState::Transferring) {
            auto& info = **infoPtr;
            for (auto& [pathId, item] : info.Items) {
                if (item.State == TFullBackupInfo::TItem::EState::Transferring) {
                    item.State = TFullBackupInfo::TItem::EState::Failed;
                }
            }
            info.State = TFullBackupInfo::EState::Failed;
            info.EndTime = TAppData::TimeProvider->Now();
            info.FinalIssues = "force-dropped";
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->PersistFullBackup(db, info);
            context.SS->BCPathToFullBackup.erase(info.BackupCollectionPathId);
        }
        context.OnComplete.DoneOperation(OperationId);
    }
};

ISubOperation::TPtr CreateNewFullBackupOp(TOperationId opId, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateFullBackupOp>(opId, tx);
}

ISubOperation::TPtr CreateNewFullBackupOp(TOperationId opId, TTxState::ETxState state) {
    return MakeSubOperation<TCreateFullBackupOp>(opId, state);
}

bool AppendFullBackupOpToBackupBackupCollection(TOperationId opId, const TPath& bcPath,
        TVector<ISubOperation::TPtr>& result, ui32 expectedItemCount) {
    TTxTransaction tx;
    tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateFullBackupOp);
    tx.SetInternal(true);
    tx.SetWorkingDir(bcPath.PathString());
    tx.MutableCreateFullBackupOp()->SetExpectedItemCount(expectedItemCount);

    const auto partId = NextPartId(opId, result);
    // The tracked row key and the TEvNotifyTxCompletion subscription both use
    // id == OperationId.GetTxId(), so the control op must share the operation TxId.
    Y_ABORT_UNLESS(partId.GetTxId() == opId.GetTxId());
    result.push_back(CreateNewFullBackupOp(partId, tx));
    return true;
}

} // namespace NKikimr::NSchemeShard
