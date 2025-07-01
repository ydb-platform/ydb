#include "schemeshard__backup_collection_common.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

namespace {

class TPropose : public TSubOperationState {
public:
    explicit TPropose(TOperationId id)
        : OperationId(std::move(id))
    {}

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropBackupCollection);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);
        LOG_I(DebugHint() << "HandleReply TEvOperationPlan: step# " << step);

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropBackupCollection);

        const TPathId& pathId = txState->TargetPathId;
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);
        const TPathElement::TPtr parentDirPtr = context.SS->PathsById.at(pathPtr->ParentPathId);

        NIceDb::TNiceDb db(context.GetDB());

        Y_ABORT_UNLESS(!pathPtr->Dropped());
        pathPtr->SetDropped(step, OperationId.GetTxId());
        context.SS->PersistDropStep(db, pathId, step, OperationId);
        context.SS->PersistRemoveBackupCollection(db, pathId);

        auto domainInfo = context.SS->ResolveDomainInfo(pathId);
        domainInfo->DecPathsInside(context.SS);
        DecAliveChildrenDirect(OperationId, parentDirPtr, context); // for correct discard of ChildrenExist prop
        context.SS->TabletCounters->Simple()[COUNTER_BACKUP_COLLECTION_COUNT].Sub(1);

        ++parentDirPtr->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDirPtr);
        context.SS->ClearDescribePathCaches(parentDirPtr);
        context.SS->ClearDescribePathCaches(pathPtr);

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDirPtr->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TDropBackupCollection TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TDropBackupCollection : public TSubOperation {
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
            return MakeHolder<TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

    void DropBackupCollectionPathElement(const TPath& dstPath) const {
        TPathElement::TPtr backupCollection = dstPath.Base();

        backupCollection->PathState = TPathElement::EPathState::EPathStateDrop;
        backupCollection->DropTxId = OperationId.GetTxId();
        backupCollection->LastTxId = OperationId.GetTxId();
    }

    void PersistDropBackupCollection(const TOperationContext& context, const TPath& dstPath) const {
        const TPathId& pathId = dstPath.Base()->PathId;

        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabPath(context.SS, pathId);
        context.MemChanges.GrabPath(context.SS, dstPath->ParentPathId);
        context.MemChanges.GrabBackupCollection(context.SS, pathId);

        context.DbChanges.PersistTxState(OperationId);
        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistPath(dstPath->ParentPathId);
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TString& rootPathStr = Transaction.GetWorkingDir();
        const auto& dropDescription = Transaction.GetDropBackupCollection();
        const TString& name = dropDescription.GetName();
        LOG_N("TDropBackupCollection Propose: opId# " << OperationId << ", path# " << rootPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        auto bcPaths = NBackup::ResolveBackupCollectionPaths(rootPathStr, name, false, context, result);
        if (!bcPaths) {
            return result;
        }

        auto& [_, dstPath] = *bcPaths;

        {
            auto checks = dstPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsBackupCollection()
                .NotUnderOperation()
                .IsCommonSensePath();

            if (checks) {
                const TBackupCollectionInfo::TPtr backupCollection = context.SS->BackupCollections.Value(dstPath->PathId, nullptr);
                if (!backupCollection) {
                    result->SetError(NKikimrScheme::StatusSchemeError, "Backup collection doesn't exist");

                    return result;
                }
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved() && dstPath.Base()->IsBackupCollection() && (dstPath.Base()->PlannedToDrop() || dstPath.Base()->Dropped())) {
                    result->SetPathDropTxId(ui64(dstPath.Base()->DropTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }

                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        auto guard = context.DbGuard();
        PersistDropBackupCollection(context, dstPath);
        context.SS->CreateTx(
                OperationId,
                TTxState::TxDropBackupCollection,
                dstPath.Base()->PathId);

        DropBackupCollectionPathElement(dstPath);

        context.OnComplete.ActivateTx(OperationId);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TDropBackupCollection AbortPropose: opId# " << OperationId);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TDropBackupCollection AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

ISubOperation::TPtr CreateDropBackupCollection(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropBackupCollection>(id, tx);
}

ISubOperation::TPtr CreateDropBackupCollection(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropBackupCollection>(id, state);
}

}  // namespace NKikimr::NSchemeShard
