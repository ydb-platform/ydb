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
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxAlterTableIndex);
        Y_VERIFY(txState->State == TTxState::Propose);

        NIceDb::TNiceDb db(context.Txc.DB);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);
        path->StepCreated = step;
        context.SS->PersistCreateStep(db, path->PathId, step);

        Y_VERIFY(context.SS->Indexes.contains(path->PathId));
        TTableIndexInfo::TPtr indexData = context.SS->Indexes.at(path->PathId);
        context.SS->PersistTableIndex(db, path->PathId);
        context.SS->Indexes[path->PathId] = indexData->AlterData;

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, path->PathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxAlterTableIndex);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TAlterTableIndex: public TSubOperation {
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State = TTxState::Invalid;

    TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) {
        switch(state) {
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) {
        switch(state) {
        case TTxState::Propose:
            return THolder(new TPropose(OperationId));
        case TTxState::Done:
            return THolder(new TDone(OperationId));
        default:
            return nullptr;
        }
    }

    void StateDone(TOperationContext& context) override {
        State = NextState(State);

        if (State != TTxState::Invalid) {
            SetState(SelectStateFunc(State));
            context.OnComplete.ActivateTx(OperationId);
        }
    }

public:
    TAlterTableIndex(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
    {
    }

    TAlterTableIndex(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
        , State(state)
    {
        SetState(SelectStateFunc(state));
    }

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

            if (!checks) {
                TString explain = TStringBuilder() << "parent path fail checks"
                                                   << ", path: " << parentPath.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
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
                TString explain = TStringBuilder() << "dst path fail checks"
                                                   << ", path: " << parentPath.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath.Base()->CreateTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        TString errMsg;

        if (!context.SS->CheckLocks(parentPath.Base()->PathId, Transaction, errMsg)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errMsg);
            return result;
        }

        TTableIndexInfo::TPtr indexData = context.SS->Indexes.at(dstPath.Base()->PathId);
        TTableIndexInfo::TPtr newIndexData = indexData->CreateNextVersion();
        if (!newIndexData) {
            result->SetError(TEvSchemeShard::EStatus::StatusInvalidParameter, errMsg);
            return result;
        }
        newIndexData->State = tableIndexAlter.GetState();

        NIceDb::TNiceDb db(context.Txc.DB);
        // store table index description
        auto indexPath = dstPath.Base();

        Y_VERIFY(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterTableIndex, indexPath->PathId);
        txState.State = TTxState::Propose;

        indexPath->PathState = NKikimrSchemeOp::EPathStateAlter;
        indexPath->LastTxId = OperationId.GetTxId();
        context.SS->PersistLastTxId(db, indexPath);

        context.SS->PersistTableIndexAlterData(db, indexPath->PathId);

        context.SS->PersistTxState(db, OperationId);

        context.OnComplete.ActivateTx(OperationId);

        State = NextState();
        SetState(SelectStateFunc(State));
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TAlterTableIndex");
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


namespace NKikimr {
namespace NSchemeShard {

ISubOperationBase::TPtr CreateAlterTableIndex(TOperationId id, const TTxTransaction& tx) {
    return new TAlterTableIndex(id, tx);
}

ISubOperationBase::TPtr CreateAlterTableIndex(TOperationId id, TTxState::ETxState state) {
    return new TAlterTableIndex(id, state);
}

}
}
