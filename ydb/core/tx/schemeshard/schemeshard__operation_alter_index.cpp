#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"

namespace {

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NTableIndex;

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TAlterTableIndex TPropose"
            << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterTableIndex);
        Y_ABORT_UNLESS(txState->State == TTxState::Propose);

        NIceDb::TNiceDb db(context.GetDB());

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->Indexes.contains(path->PathId));
        TTableIndexInfo::TPtr indexData = context.SS->Indexes.at(path->PathId);
        context.SS->PersistTableIndex(db, path->PathId);
        context.SS->Indexes[path->PathId] = indexData->AlterData;

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, path->PathId);

        auto parentDir = context.SS->PathsById.at(path->ParentPathId);
        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterTableIndex);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TAlterTableIndex: public TSubOperation {
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

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& tableIndexAlter = Transaction.GetAlterTableIndex();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = tableIndexAlter.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterTableIndex Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", operationId: " << OperationId
                         << ", transaction: " << Transaction.ShortDebugString()
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        if (!Transaction.HasAlterTableIndex()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "AlterTableIndex is not present");
            return result;
        }

        if (!tableIndexAlter.HasName()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Name is not present in AlterTableIndex");
            return result;
        }

        NSchemeShard::TPath parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        NSchemeShard::TPath dstPath = parentPath.Child(name);
        {
            NSchemeShard::TPath::TChecker checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsTable();

            if (!Transaction.GetInternal()) {
                checks.NotAsyncReplicaTable();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            checks
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsTableIndex();

            if (!context.IsAllowedPrivateTables) {
                checks.IsCommonSensePath();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath.Base()->CreateTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        TString errStr;
        if (!context.SS->CheckLocks(parentPath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        auto guard = context.DbGuard();
        auto indexPath = dstPath.Base();
        context.MemChanges.GrabPath(context.SS, indexPath->PathId);
        context.MemChanges.GrabPath(context.SS, indexPath->ParentPathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabIndex(context.SS, indexPath->PathId);

        context.DbChanges.PersistPath(indexPath->PathId);
        context.DbChanges.PersistAlterIndex(indexPath->PathId);
        context.DbChanges.PersistTxState(OperationId);

        TTableIndexInfo::TPtr indexData = context.SS->Indexes.at(indexPath->PathId);
        TTableIndexInfo::TPtr newIndexData = indexData->CreateNextVersion();
        Y_ABORT_UNLESS(newIndexData);
        newIndexData->State = tableIndexAlter.GetState();

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterTableIndex, indexPath->PathId);
        txState.State = TTxState::Propose;

        indexPath->PathState = NKikimrSchemeOp::EPathStateAlter;
        indexPath->LastTxId = OperationId.GetTxId();

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterTableIndex AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterTableIndex AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterTableIndex(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterTableIndex>(id, tx);
}

ISubOperation::TPtr CreateAlterTableIndex(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TAlterTableIndex>(id, state);
}

}
